pub mod backend;
pub mod backends;
pub mod graphql;
pub mod graphql_sdl;
pub mod model;
pub mod repo;
mod search_expr;
mod search_filters;
mod search_query;

#[cfg(feature = "python")]
mod pybindings {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Instant;

    use pyo3::exceptions::{PyRuntimeError, PyValueError};
    use pyo3::prelude::*;
    use pyo3::types::PyDict;
    use serde_json::Value;

    use crate::backend::Backend;
    use crate::backends::neo4j::Neo4jBackend;
    use crate::backends::postgres::PostgresBackend;
    use crate::graphql::GraphqlRuntime;
    use crate::repo::RepoService;

    fn parse_json(input: &str) -> PyResult<Value> {
        serde_json::from_str(input).map_err(|e| PyValueError::new_err(format!("invalid json: {e}")))
    }

    fn unit_ref(tenant_id: i64, unit_id: i64) -> crate::model::UnitRef {
        crate::model::UnitRef {
            tenant_id,
            unit_id,
            version: None,
        }
    }

    #[derive(Clone, Default)]
    struct RunningStat {
        count: u64,
        mean_ms: f64,
        m2_ms: f64,
        total_ms: f64,
        min_ms: Option<f64>,
        max_ms: Option<f64>,
        errors: u64,
    }

    impl RunningStat {
        fn add_sample(&mut self, value_ms: f64, ok: bool) {
            self.count = self.count.saturating_add(1);
            self.total_ms += value_ms;
            self.min_ms = Some(self.min_ms.map_or(value_ms, |v| v.min(value_ms)));
            self.max_ms = Some(self.max_ms.map_or(value_ms, |v| v.max(value_ms)));

            let delta = value_ms - self.mean_ms;
            self.mean_ms += delta / self.count as f64;
            let delta2 = value_ms - self.mean_ms;
            self.m2_ms += delta * delta2;

            if !ok {
                self.errors = self.errors.saturating_add(1);
            }
        }

        fn as_json(&self) -> Value {
            let variance = if self.count > 1 {
                self.m2_ms / (self.count as f64 - 1.0)
            } else {
                0.0
            };
            let stddev = variance.sqrt();
            let cv_percent = if self.count > 1 && self.mean_ms != 0.0 {
                (stddev / self.mean_ms) * 100.0
            } else {
                0.0
            };
            serde_json::json!({
                "count": self.count,
                "errors": self.errors,
                "total_ms": self.total_ms,
                "min_ms": self.min_ms,
                "max_ms": self.max_ms,
                "mean_ms": self.mean_ms,
                "variance_ms": variance,
                "stddev_ms": stddev,
                "cv_percent": cv_percent
            })
        }
    }

    #[derive(Default)]
    struct PyRuntimeStats {
        operations: HashMap<String, RunningStat>,
    }

    #[pyclass]
    struct PyIpto {
        repo: RepoService,
        backend: Arc<dyn Backend>,
        backend_name: String,
        stats: Arc<std::sync::Mutex<PyRuntimeStats>>,
    }

    #[pymethods]
    impl PyIpto {
        #[new]
        #[pyo3(signature = (backend = "postgres"))]
        fn new(backend: &str) -> PyResult<Self> {
            let backend_impl: Arc<dyn Backend> = match backend {
                "postgres" | "pg" => Arc::new(PostgresBackend::new()),
                "neo4j" => Arc::new(Neo4jBackend::new()),
                other => {
                    return Err(PyValueError::new_err(format!(
                        "unsupported backend '{other}', expected postgres|pg|neo4j"
                    )))
                }
            };

            Ok(Self {
                repo: RepoService::new(backend_impl.clone()),
                backend: backend_impl,
                backend_name: backend.to_string(),
                stats: Arc::new(std::sync::Mutex::new(PyRuntimeStats::default())),
            })
        }

        fn backend(&self) -> String {
            self.backend_name.clone()
        }

        fn reset_statistics(&self) -> PyResult<()> {
            let mut stats = self
                .stats
                .lock()
                .map_err(|_| PyRuntimeError::new_err("statistics lock poisoned"))?;
            stats.operations.clear();
            Ok(())
        }

        fn get_statistics_json(&self) -> PyResult<String> {
            let result = self.statistics_value()?;
            serde_json::to_string(&result).map_err(|e| PyRuntimeError::new_err(e.to_string()))
        }

        fn get_statistics(&self, py: Python<'_>) -> PyResult<PyObject> {
            let result = self.statistics_value()?;
            let root = PyDict::new_bound(py);
            root.set_item("backend", result["backend"].as_str().unwrap_or_default())?;
            let operations = PyDict::new_bound(py);
            if let Some(obj) = result["operations"].as_object() {
                for (name, stat) in obj {
                    let stat_dict = PyDict::new_bound(py);
                    stat_dict.set_item("count", stat["count"].as_u64().unwrap_or(0))?;
                    stat_dict.set_item("errors", stat["errors"].as_u64().unwrap_or(0))?;
                    stat_dict.set_item("total_ms", stat["total_ms"].as_f64().unwrap_or(0.0))?;
                    match stat["min_ms"].as_f64() {
                        Some(v) => stat_dict.set_item("min_ms", v)?,
                        None => stat_dict.set_item("min_ms", py.None())?,
                    }
                    match stat["max_ms"].as_f64() {
                        Some(v) => stat_dict.set_item("max_ms", v)?,
                        None => stat_dict.set_item("max_ms", py.None())?,
                    }
                    stat_dict.set_item("mean_ms", stat["mean_ms"].as_f64().unwrap_or(0.0))?;
                    stat_dict
                        .set_item("variance_ms", stat["variance_ms"].as_f64().unwrap_or(0.0))?;
                    stat_dict.set_item("stddev_ms", stat["stddev_ms"].as_f64().unwrap_or(0.0))?;
                    stat_dict.set_item("cv_percent", stat["cv_percent"].as_f64().unwrap_or(0.0))?;
                    operations.set_item(name, stat_dict)?;
                }
            }
            root.set_item("operations", operations)?;
            Ok(root.into_py(py))
        }

        fn health(&self) -> PyResult<String> {
            self.timed("health", || {
                let result = self
                    .repo
                    .health()
                    .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
                Ok(result.to_string())
            })
        }

        #[pyo3(signature = (query, variables_json = None))]
        fn graphql_execute(&self, query: &str, variables_json: Option<&str>) -> PyResult<String> {
            self.timed("graphql_execute", || {
                let variables = match variables_json {
                    Some(raw) => Some(parse_json(raw)?),
                    None => None,
                };
                let runtime = GraphqlRuntime::new(RepoService::new(self.backend.clone()));
                let result = runtime
                    .execute(query, variables)
                    .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
                serde_json::to_string(&result).map_err(|e| PyRuntimeError::new_err(e.to_string()))
            })
        }

        #[pyo3(signature = (query, allowlist, variables_json = None))]
        fn graphql_execute_allowlist(
            &self,
            query: &str,
            allowlist: Vec<String>,
            variables_json: Option<&str>,
        ) -> PyResult<String> {
            self.timed("graphql_execute_allowlist", || {
                let variables = match variables_json {
                    Some(raw) => Some(parse_json(raw)?),
                    None => None,
                };
                let allowlist_refs = allowlist.iter().map(String::as_str).collect::<Vec<_>>();
                let runtime = GraphqlRuntime::with_operation_allowlist(
                    RepoService::new(self.backend.clone()),
                    &allowlist_refs,
                )
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
                let result = runtime
                    .execute(query, variables)
                    .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
                serde_json::to_string(&result).map_err(|e| PyRuntimeError::new_err(e.to_string()))
            })
        }

        fn flush_cache(&self) -> PyResult<()> {
            self.timed("flush_cache", || {
                self.repo
                    .flush_cache()
                    .map_err(|e| PyRuntimeError::new_err(e.to_string()))
            })
        }

        #[pyo3(signature = (tenant_id, unit_id, version = None))]
        fn get_unit_json(
            &self,
            tenant_id: i64,
            unit_id: i64,
            version: Option<i64>,
        ) -> PyResult<Option<String>> {
            self.timed("get_unit_json", || {
                let selector = match version {
                    Some(v) => crate::model::VersionSelector::Exact(v),
                    None => crate::model::VersionSelector::Latest,
                };

                let result = self
                    .repo
                    .get_unit_json(tenant_id, unit_id, selector)
                    .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

                result
                    .map(|v| {
                        serde_json::to_string(&v)
                            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
                    })
                    .transpose()
            })
        }

        fn unit_exists(&self, tenant_id: i64, unit_id: i64) -> PyResult<bool> {
            self.timed("unit_exists", || {
                self.repo
                    .unit_exists(tenant_id, unit_id)
                    .map_err(|e| PyRuntimeError::new_err(e.to_string()))
            })
        }

        fn get_unit_by_corrid_json(
            &self,
            corrid: &str,
        ) -> PyResult<Option<String>> {
            self.timed("get_unit_by_corrid_json", || {
                let result = self
                    .repo
                    .get_unit_by_corrid_json(corrid)
                    .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

                result
                    .map(|v| {
                        serde_json::to_string(&v)
                            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
                    })
                    .transpose()
            })
        }

        fn store_unit_json(&self, unit_json: &str) -> PyResult<String> {
            self.timed("store_unit_json", || {
                let payload = parse_json(unit_json)?;
                let stored = self
                    .repo
                    .store_unit_json(payload)
                    .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
                Ok(stored.to_string())
            })
        }

        fn search_units(
            &self,
            expression_json: &str,
            order_field: &str,
            descending: bool,
            limit: i64,
            offset: i64,
        ) -> PyResult<String> {
            self.timed("search_units", || {
                let expr = parse_json(expression_json)?;
                let order = crate::model::SearchOrder {
                    field: order_field.to_string(),
                    descending,
                };
                let paging = crate::model::SearchPaging { limit, offset };

                let result = self
                    .repo
                    .search_units(expr, order, paging)
                    .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

                serde_json::to_string(&result).map_err(|e| PyRuntimeError::new_err(e.to_string()))
            })
        }

        fn search_units_query(
            &self,
            query: &str,
            order_field: &str,
            descending: bool,
            limit: i64,
            offset: i64,
        ) -> PyResult<String> {
            let order = crate::model::SearchOrder {
                field: order_field.to_string(),
                descending,
            };
            let paging = crate::model::SearchPaging { limit, offset };

            let result = self
                .repo
                .search_units_query(query, order, paging)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

            serde_json::to_string(&result).map_err(|e| PyRuntimeError::new_err(e.to_string()))
        }

        fn search_units_query_strict(
            &self,
            query: &str,
            order_field: &str,
            descending: bool,
            limit: i64,
            offset: i64,
        ) -> PyResult<String> {
            let order = crate::model::SearchOrder {
                field: order_field.to_string(),
                descending,
            };
            let paging = crate::model::SearchPaging { limit, offset };

            let result = self
                .repo
                .search_units_query_strict(query, order, paging)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

            serde_json::to_string(&result).map_err(|e| PyRuntimeError::new_err(e.to_string()))
        }

        fn set_status(&self, tenant_id: i64, unit_id: i64, status: i32) -> PyResult<()> {
            self.repo
                .set_status(unit_ref(tenant_id, unit_id), status)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))
        }

        fn add_relation(
            &self,
            tenant_id: i64,
            unit_id: i64,
            relation_type: i32,
            other_tenant_id: i64,
            other_unit_id: i64,
        ) -> PyResult<()> {
            self.repo
                .add_relation(
                    unit_ref(tenant_id, unit_id),
                    relation_type,
                    unit_ref(other_tenant_id, other_unit_id),
                )
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))
        }

        fn remove_relation(
            &self,
            tenant_id: i64,
            unit_id: i64,
            relation_type: i32,
            other_tenant_id: i64,
            other_unit_id: i64,
        ) -> PyResult<()> {
            self.repo
                .remove_relation(
                    unit_ref(tenant_id, unit_id),
                    relation_type,
                    unit_ref(other_tenant_id, other_unit_id),
                )
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))
        }

        fn get_right_relation(
            &self,
            tenant_id: i64,
            unit_id: i64,
            relation_type: i32,
        ) -> PyResult<Option<String>> {
            let result = self
                .repo
                .get_right_relation(unit_ref(tenant_id, unit_id), relation_type)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            result
                .map(|v| {
                    serde_json::to_string(&v).map_err(|e| PyRuntimeError::new_err(e.to_string()))
                })
                .transpose()
        }

        fn get_right_relations(
            &self,
            tenant_id: i64,
            unit_id: i64,
            relation_type: i32,
        ) -> PyResult<String> {
            let result = self
                .repo
                .get_right_relations(unit_ref(tenant_id, unit_id), relation_type)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            serde_json::to_string(&result).map_err(|e| PyRuntimeError::new_err(e.to_string()))
        }

        fn get_left_relations(
            &self,
            tenant_id: i64,
            unit_id: i64,
            relation_type: i32,
        ) -> PyResult<String> {
            let result = self
                .repo
                .get_left_relations(unit_ref(tenant_id, unit_id), relation_type)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            serde_json::to_string(&result).map_err(|e| PyRuntimeError::new_err(e.to_string()))
        }

        fn count_right_relations(
            &self,
            tenant_id: i64,
            unit_id: i64,
            relation_type: i32,
        ) -> PyResult<i64> {
            self.repo
                .count_right_relations(unit_ref(tenant_id, unit_id), relation_type)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))
        }

        fn count_left_relations(
            &self,
            tenant_id: i64,
            unit_id: i64,
            relation_type: i32,
        ) -> PyResult<i64> {
            self.repo
                .count_left_relations(unit_ref(tenant_id, unit_id), relation_type)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))
        }

        fn add_association(
            &self,
            tenant_id: i64,
            unit_id: i64,
            association_type: i32,
            reference: &str,
        ) -> PyResult<()> {
            self.repo
                .add_association(unit_ref(tenant_id, unit_id), association_type, reference)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))
        }

        fn remove_association(
            &self,
            tenant_id: i64,
            unit_id: i64,
            association_type: i32,
            reference: &str,
        ) -> PyResult<()> {
            self.repo
                .remove_association(unit_ref(tenant_id, unit_id), association_type, reference)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))
        }

        fn get_right_association(
            &self,
            tenant_id: i64,
            unit_id: i64,
            association_type: i32,
        ) -> PyResult<Option<String>> {
            let result = self
                .repo
                .get_right_association(unit_ref(tenant_id, unit_id), association_type)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            result
                .map(|v| {
                    serde_json::to_string(&v).map_err(|e| PyRuntimeError::new_err(e.to_string()))
                })
                .transpose()
        }

        fn get_right_associations(
            &self,
            tenant_id: i64,
            unit_id: i64,
            association_type: i32,
        ) -> PyResult<String> {
            let result = self
                .repo
                .get_right_associations(unit_ref(tenant_id, unit_id), association_type)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            serde_json::to_string(&result).map_err(|e| PyRuntimeError::new_err(e.to_string()))
        }

        fn get_left_associations(
            &self,
            association_type: i32,
            reference: &str,
        ) -> PyResult<String> {
            let result = self
                .repo
                .get_left_associations(association_type, reference)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            serde_json::to_string(&result).map_err(|e| PyRuntimeError::new_err(e.to_string()))
        }

        fn count_right_associations(
            &self,
            tenant_id: i64,
            unit_id: i64,
            association_type: i32,
        ) -> PyResult<i64> {
            self.repo
                .count_right_associations(unit_ref(tenant_id, unit_id), association_type)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))
        }

        fn count_left_associations(&self, association_type: i32, reference: &str) -> PyResult<i64> {
            self.repo
                .count_left_associations(association_type, reference)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))
        }

        fn lock_unit(
            &self,
            tenant_id: i64,
            unit_id: i64,
            lock_type: i32,
            purpose: &str,
        ) -> PyResult<()> {
            self.repo
                .lock_unit(unit_ref(tenant_id, unit_id), lock_type, purpose)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))
        }

        fn unlock_unit(&self, tenant_id: i64, unit_id: i64) -> PyResult<()> {
            self.repo
                .unlock_unit(unit_ref(tenant_id, unit_id))
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))
        }

        fn is_unit_locked(&self, tenant_id: i64, unit_id: i64) -> PyResult<bool> {
            self.repo
                .is_unit_locked(unit_ref(tenant_id, unit_id))
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))
        }

        fn activate_unit(&self, tenant_id: i64, unit_id: i64) -> PyResult<()> {
            self.repo
                .activate_unit(unit_ref(tenant_id, unit_id))
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))
        }

        fn inactivate_unit(&self, tenant_id: i64, unit_id: i64) -> PyResult<()> {
            self.repo
                .inactivate_unit(unit_ref(tenant_id, unit_id))
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))
        }

        fn request_status_transition(
            &self,
            tenant_id: i64,
            unit_id: i64,
            requested_status: i32,
        ) -> PyResult<i32> {
            self.repo
                .request_status_transition(unit_ref(tenant_id, unit_id), requested_status)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))
        }

        #[pyo3(signature = (alias, name, qualname, attribute_type, is_array=false))]
        fn create_attribute(
            &self,
            alias: &str,
            name: &str,
            qualname: &str,
            attribute_type: &str,
            is_array: bool,
        ) -> PyResult<String> {
            let result = self
                .repo
                .create_attribute(alias, name, qualname, attribute_type, is_array)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            serde_json::to_string(&result).map_err(|e| PyRuntimeError::new_err(e.to_string()))
        }

        fn instantiate_attribute(&self, name_or_id: &str) -> PyResult<Option<String>> {
            let result = self
                .repo
                .instantiate_attribute(name_or_id)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            result
                .map(|v| {
                    serde_json::to_string(&v).map_err(|e| PyRuntimeError::new_err(e.to_string()))
                })
                .transpose()
        }

        fn inspect_graphql_sdl(&self, sdl: &str) -> PyResult<String> {
            self.timed("inspect_graphql_sdl", || {
                let report = self
                    .repo
                    .inspect_graphql_sdl(sdl)
                    .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
                serde_json::to_string(&report).map_err(|e| PyRuntimeError::new_err(e.to_string()))
            })
        }

        fn configure_graphql_sdl(&self, sdl: &str) -> PyResult<String> {
            self.timed("configure_graphql_sdl", || {
                let report = self
                    .repo
                    .configure_graphql_sdl(sdl)
                    .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
                serde_json::to_string(&report).map_err(|e| PyRuntimeError::new_err(e.to_string()))
            })
        }

        fn configure_graphql_sdl_file(&self, path: &str) -> PyResult<String> {
            self.timed("configure_graphql_sdl_file", || {
                let sdl = std::fs::read_to_string(path).map_err(|e| {
                    PyRuntimeError::new_err(format!("failed to read SDL file '{path}': {e}"))
                })?;
                let report = self
                    .repo
                    .configure_graphql_sdl(&sdl)
                    .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
                serde_json::to_string(&report).map_err(|e| PyRuntimeError::new_err(e.to_string()))
            })
        }

        fn can_change_attribute(&self, name_or_id: &str) -> PyResult<bool> {
            self.repo
                .can_change_attribute(name_or_id)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))
        }

        fn get_attribute_info(&self, name_or_id: &str) -> PyResult<Option<String>> {
            let result = self
                .repo
                .get_attribute_info(name_or_id)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            result
                .map(|v| {
                    serde_json::to_string(&v).map_err(|e| PyRuntimeError::new_err(e.to_string()))
                })
                .transpose()
        }

        fn get_tenant_info(&self, name_or_id: &str) -> PyResult<Option<String>> {
            let result = self
                .repo
                .get_tenant_info(name_or_id)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            result
                .map(|v| {
                    serde_json::to_string(&v).map_err(|e| PyRuntimeError::new_err(e.to_string()))
                })
                .transpose()
        }

        fn attribute_name_to_id(&self, attribute_name: &str) -> PyResult<Option<i64>> {
            self.repo
                .attribute_name_to_id(attribute_name)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))
        }

        fn attribute_id_to_name(&self, attribute_id: i64) -> PyResult<Option<String>> {
            self.repo
                .attribute_id_to_name(attribute_id)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))
        }

        fn tenant_name_to_id(&self, tenant_name: &str) -> PyResult<Option<i64>> {
            self.repo
                .tenant_name_to_id(tenant_name)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))
        }

        fn tenant_id_to_name(&self, tenant_id: i64) -> PyResult<Option<String>> {
            self.repo
                .tenant_id_to_name(tenant_id)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))
        }
    }

    impl PyIpto {
        fn statistics_value(&self) -> PyResult<Value> {
            let stats = self
                .stats
                .lock()
                .map_err(|_| PyRuntimeError::new_err("statistics lock poisoned"))?;
            let mut operations = serde_json::Map::new();
            for (name, stat) in &stats.operations {
                operations.insert(name.clone(), stat.as_json());
            }
            let result = serde_json::json!({
                "backend": self.backend_name,
                "operations": operations
            });
            Ok(result)
        }
    }

    impl PyIpto {
        fn timed<T, F>(&self, operation: &str, call: F) -> PyResult<T>
        where
            F: FnOnce() -> PyResult<T>,
        {
            let start = Instant::now();
            let result = call();
            let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;

            if let Ok(mut stats) = self.stats.lock() {
                stats
                    .operations
                    .entry(operation.to_string())
                    .or_default()
                    .add_sample(elapsed_ms, result.is_ok());
            }

            result
        }
    }

    #[pyfunction]
    fn version() -> &'static str {
        env!("CARGO_PKG_VERSION")
    }

    #[pymodule]
    fn ipto_rust(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
        m.add_class::<PyIpto>()?;
        m.add_function(wrap_pyfunction!(version, m)?)?;
        Ok(())
    }
}
