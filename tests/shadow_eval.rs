use axum::{Json, Router, routing::post};
use serde_json::json;
use std::{fs, process::Command};
use uuid::Uuid;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires loopback sockets for two mock memory services"]
async fn shadow_runner_writes_private_reproducible_comparison() {
    let v5 = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let v6 = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let v5_addr = v5.local_addr().unwrap();
    let v6_addr = v6.local_addr().unwrap();
    let v5_router = Router::new().route(
        "/v5/recall",
        post(|| async { Json(json!({"facts":[{"id":1,"content":"stale value"}]})) }),
    );
    let v6_router = Router::new().route(
        "/v6/recall",
        post(|Json(body): Json<serde_json::Value>| async move {
            if body["query"] == "broken" {
                Json(json!({"items":[]}))
            } else {
                Json(json!({"mandatory_policy":[],"items":[{"id":Uuid::nil(),"rendered":"current value"}],"token_count":100,"retrieval_mode":"hybrid","snapshot_revision":7}))
            }
        }),
    );
    let v5_task = tokio::spawn(async move { axum::serve(v5, v5_router).await });
    let v6_task = tokio::spawn(async move { axum::serve(v6, v6_router).await });

    let directory = std::env::temp_dir().join(format!("foreman-v6-shadow-test-{}", Uuid::new_v4()));
    fs::create_dir(&directory).unwrap();
    let manifest = directory.join("manifest.json");
    let v5_token = directory.join("v5.token");
    let v6_token = directory.join("v6.token");
    let output = directory.join("report.json");
    fs::write(&manifest, serde_json::to_vec(&json!({
        "schema_version":1,"fixture_revision":"test-v1","snapshot_id":"snapshot-1","cases":[{
            "id":"case-1","revision":1,"query":"state","intent":"current",
            "scope":{"project_key":null,"repository_key":null,"thread_id":null,"session_id":null},
            "token_budget":128,"limit":10,"required_text":["current value"],
            "forbidden_text":["stale value"],"required_ids":[],"forbidden_ids":[],
            "adjudication":"current replaces stale","failure_classes":["supersession"]
        }]
    })).unwrap()).unwrap();
    fs::write(&v5_token, "v5-test-token\n").unwrap();
    fs::write(&v6_token, "v6-test-token\n").unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&v5_token, fs::Permissions::from_mode(0o600)).unwrap();
        fs::set_permissions(&v6_token, fs::Permissions::from_mode(0o600)).unwrap();
    }

    let status = Command::new(env!("CARGO_BIN_EXE_shadow_eval"))
        .env("FOREMAN_EVAL_MANIFEST", &manifest)
        .env("FOREMAN_EVAL_OUTPUT", &output)
        .env("FOREMAN_EVAL_V5_URL", format!("http://{v5_addr}"))
        .env("FOREMAN_EVAL_V6_URL", format!("http://{v6_addr}"))
        .env("FOREMAN_EVAL_V5_TOKEN_FILE", &v5_token)
        .env("FOREMAN_EVAL_V6_TOKEN_FILE", &v6_token)
        .env("FOREMAN_EVAL_GIT_COMMIT", "1234567")
        .status()
        .unwrap();
    assert!(status.success());
    let report_bytes = fs::read(&output).unwrap();
    let report_text = String::from_utf8(report_bytes.clone()).unwrap();
    assert!(!report_text.contains("current value"));
    assert!(!report_text.contains("stale value"));
    let report: serde_json::Value = serde_json::from_slice(&report_bytes).unwrap();
    assert_eq!(report["release_gate_pass"], true);
    assert_eq!(report["v5"]["hard_gate_failures"], 1);
    assert_eq!(report["v6"]["hard_gate_failures"], 0);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            fs::metadata(&output).unwrap().permissions().mode() & 0o077,
            0
        );
    }

    let failed_output = directory.join("failed-report.json");
    let mut failed_manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(&manifest).unwrap()).unwrap();
    failed_manifest["cases"][0]["query"] = json!("broken");
    fs::write(&manifest, serde_json::to_vec(&failed_manifest).unwrap()).unwrap();
    let failed_status = Command::new(env!("CARGO_BIN_EXE_shadow_eval"))
        .env("FOREMAN_EVAL_MANIFEST", &manifest)
        .env("FOREMAN_EVAL_OUTPUT", &failed_output)
        .env("FOREMAN_EVAL_V5_URL", format!("http://{v5_addr}"))
        .env("FOREMAN_EVAL_V6_URL", format!("http://{v6_addr}"))
        .env("FOREMAN_EVAL_V5_TOKEN_FILE", &v5_token)
        .env("FOREMAN_EVAL_V6_TOKEN_FILE", &v6_token)
        .env("FOREMAN_EVAL_GIT_COMMIT", "1234567")
        .status()
        .unwrap();
    assert_eq!(failed_status.code(), Some(2));
    let failed_report: serde_json::Value =
        serde_json::from_slice(&fs::read(&failed_output).unwrap()).unwrap();
    assert_eq!(failed_report["release_gate_pass"], false);
    assert_eq!(failed_report["v6"]["execution_errors"], 1);
    assert_eq!(
        failed_report["cases"][0]["v6_packet"]["execution_error"],
        "invalid_v6_packet"
    );
    v5_task.abort();
    v6_task.abort();
    fs::remove_dir_all(directory).unwrap();
}
