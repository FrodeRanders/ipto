use std::sync::Arc;

use ipto_rust::backends::neo4j::Neo4jBackend;
use ipto_rust::repo::RepoService;

fn neo4j_integration_enabled() -> bool {
    matches!(
        std::env::var("IPTO_NEO4J_INTEGRATION").ok().as_deref(),
        Some("1") | Some("true") | Some("TRUE") | Some("yes") | Some("YES")
    )
}

#[test]
fn neo4j_health_reports_backend() {
    if !neo4j_integration_enabled() {
        return;
    }

    let repo = RepoService::new(Arc::new(Neo4jBackend::new()));
    let health = repo.health().expect("health should be available");
    assert_eq!(health["backend"], "neo4j");
}
