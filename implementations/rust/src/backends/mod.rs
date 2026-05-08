//! Bundled storage backends.
//!
//! Both adapters implement [`crate::backend::Backend`] and are intended to be
//! selected by wiring them into [`crate::repo::RepoService`]. PostgreSQL is the
//! closest match to the historical repository schema; Neo4j stores equivalent
//! repository concepts as graph nodes and relationships for parity testing and
//! graph-oriented deployments.

pub mod neo4j;
pub mod postgres;
