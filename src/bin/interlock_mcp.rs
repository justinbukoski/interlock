use reqwest::{Client, Url, redirect::Policy};
use serde_json::{Value, json};
use std::{
    fs::OpenOptions,
    io::Read,
    net::IpAddr,
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::io::{self, AsyncBufRead, AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use uuid::Uuid;

const MAX_FRAME_BYTES: usize = 1_048_576;
const MAX_RESPONSE_BYTES: usize = 4_194_304;
const MAX_ERROR_BODY_CHARS: usize = 300;

struct Config {
    base_url: Url,
    reader_token: String,
    write_token: String,
}

impl Config {
    fn from_env() -> Result<Self, String> {
        // Deployments configured before the Interlock naming was settled may
        // use older env names; accept INTERLOCK_* first, FOREMAN_V6_* as the fallback,
        // so one binary serves every deployed configuration.
        let base_url = loopback_url(
            &std::env::var("INTERLOCK_URL")
                .or_else(|_| std::env::var("FOREMAN_V6_URL"))
                .unwrap_or_else(|_| "http://127.0.0.1:8851".into()),
        )?;
        let reader_path = configured_path_multi(
            &["INTERLOCK_READER_TOKEN_FILE", "FOREMAN_V6_READER_TOKEN_FILE"],
            ".config/interlock/reader-token",
        )?;
        // Routine agents run with the WRITER credential (observations, normal
        // memories, corrections, handoffs); the server's role checks stop a
        // writer from minting owner-authority records. The OWNER credential
        // remains supported for a deliberate administrative session — see
        // docs/OWNER_ADMINISTRATION.md. When both are set, writer wins.
        // Each role honors both env families, INTERLOCK_* taking precedence.
        let writer_keys = &["INTERLOCK_WRITER_TOKEN_FILE", "FOREMAN_V6_WRITER_TOKEN_FILE"];
        let owner_keys = &["INTERLOCK_OWNER_TOKEN_FILE", "FOREMAN_V6_OWNER_TOKEN_FILE"];
        let write_path = if writer_keys.iter().any(|key| std::env::var_os(key).is_some()) {
            configured_path_multi(writer_keys, ".config/interlock/writer-token")?
        } else if owner_keys.iter().any(|key| std::env::var_os(key).is_some()) {
            configured_path_multi(owner_keys, ".config/interlock/owner-token")?
        } else {
            configured_path_multi(writer_keys, ".config/interlock/writer-token")?
        };
        Ok(Self {
            base_url,
            reader_token: read_secret(&reader_path)?,
            write_token: read_secret(&write_path)?,
        })
    }
}

fn configured_path_multi(keys: &[&str], default_relative: &str) -> Result<PathBuf, String> {
    for key in keys {
        if let Ok(path) = std::env::var(key) {
            if path.trim().is_empty() {
                return Err(format!("{key} cannot be empty"));
            }
            return Ok(path.into());
        }
    }
    let home = std::env::var_os("HOME").ok_or_else(|| "HOME is required".to_string())?;
    Ok(PathBuf::from(home).join(default_relative))
}

fn loopback_url(input: &str) -> Result<Url, String> {
    let mut url = Url::parse(input).map_err(|_| "INTERLOCK_URL is invalid".to_string())?;
    if url.scheme() != "http"
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(
            "INTERLOCK_URL must be credential-free loopback HTTP without query/fragment".into(),
        );
    }
    let host = url
        .host_str()
        .ok_or_else(|| "INTERLOCK_URL requires a host".to_string())?;
    let host_ip = host.trim_start_matches('[').trim_end_matches(']');
    let loopback = host == "localhost"
        || host_ip
            .parse::<IpAddr>()
            .is_ok_and(|address| address.is_loopback());
    if !loopback {
        return Err("INTERLOCK_URL must use a loopback host through the SSH tunnel".into());
    }
    if url.path() != "/" && !url.path().is_empty() {
        return Err("INTERLOCK_URL must not contain a path".into());
    }
    url.set_path("/");
    Ok(url)
}

fn read_secret(path: &Path) -> Result<String, String> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(nix::libc::O_CLOEXEC | nix::libc::O_NOFOLLOW);
    }
    let mut file = options
        .open(path)
        .map_err(|_| format!("cannot open token file: {}", path.display()))?;
    let metadata = file
        .metadata()
        .map_err(|_| "cannot inspect token file".to_string())?;
    if !metadata.file_type().is_file() {
        return Err("token path must be a regular non-symlink file".into());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        if metadata.uid() != nix::unistd::Uid::current().as_raw()
            || metadata.permissions().mode() & 0o077 != 0
        {
            return Err("token file must be owned by this user with mode 0600 or stricter".into());
        }
    }
    let mut token = String::new();
    file.by_ref()
        .take(8193)
        .read_to_string(&mut token)
        .map_err(|_| "cannot read token file".to_string())?;
    let token = token.trim().to_owned();
    if token.is_empty() || token.len() > 8192 {
        return Err("token has invalid length".into());
    }
    Ok(token)
}

fn endpoint(base: &Url, path: &str) -> Url {
    let mut url = base.clone();
    url.set_path(path);
    url
}

fn scope_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "properties": {
            "project_key": {"type": "string", "maxLength": 512},
            "repository_key": {"type": "string", "maxLength": 512},
            "thread_id": {"type": "string", "maxLength": 512},
            "session_id": {"type": "string", "maxLength": 512}
        }
    })
}

fn tools() -> Vec<Value> {
    let scope = scope_schema();
    vec![
        json!({
            "name": "bootstrap",
            "description": "Load mandatory directives, scoped project state, and the latest exact-project handoff before acting. The server enforces a dynamic minimum token_budget sized to the mandatory policy (currently ~11k); pass 16000.",
            "inputSchema": {"type":"object","additionalProperties":false,"properties":{"scope":scope,"token_budget":{"type":"integer","minimum":64,"maximum":32768,"default":16000}},"required":["token_budget"]},
            "annotations": {"readOnlyHint":true,"destructiveHint":false,"idempotentHint":true,"openWorldHint":false}
        }),
        json!({
            "name": "recall",
            "description": "Retrieve current, attributable, scope-aware memory within a server-enforced token budget.",
            "inputSchema": {"type":"object","additionalProperties":false,"properties":{"query":{"type":"string","minLength":1,"maxLength":4096},"intent":{"type":"string","enum":["current","why","procedure","explore"],"default":"current"},"scope":scope,"token_budget":{"type":"integer","minimum":64,"maximum":32768,"default":4096},"limit":{"type":"integer","minimum":1,"maximum":100,"default":20}},"required":["query","token_budget"]},
            "annotations": {"readOnlyHint":true,"destructiveHint":false,"idempotentHint":true,"openWorldHint":false}
        }),
        json!({
            "name": "history",
            "description": "Retrieve the separate lexical evidence/history lane without promoting it to canonical memory.",
            "inputSchema": {"type":"object","additionalProperties":false,"properties":{"query":{"type":"string","minLength":1,"maxLength":4096},"scope":scope,"token_budget":{"type":"integer","minimum":64,"maximum":32768,"default":4096},"limit":{"type":"integer","minimum":1,"maximum":100,"default":20}},"required":["query","token_budget"]},
            "annotations": {"readOnlyHint":true,"destructiveHint":false,"idempotentHint":true,"openWorldHint":false}
        }),
        json!({
            "name": "observe",
            "description": "Ingest a redacted, auditable observation. This does not make the observation canonical memory.",
            "inputSchema": {"type":"object","additionalProperties":false,"properties":{"request_id":{"type":"string","format":"uuid"},"source_event_id":{"type":"string"},"event_kind":{"type":"string"},"scope":scope,"observed_at":{"type":"string","format":"date-time"},"content":{"type":"string"},"raw_content_ref":{"type":["string","null"]}},"required":["request_id","source_event_id","event_kind","observed_at","content"]},
            "annotations": {"readOnlyHint":false,"destructiveHint":false,"idempotentHint":true,"openWorldHint":false}
        }),
        json!({
            "name": "remember",
            "description": "Write an authoritative canonical proposition with explicit scope, provenance, authority, epistemic status, and reason.",
            "inputSchema": canonical_write_schema(scope.clone()),
            "annotations": {"readOnlyHint":false,"destructiveHint":false,"idempotentHint":true,"openWorldHint":false}
        }),
        json!({
            "name": "correct",
            "description": "Correct canonical state by writing a higher-authority proposition; structural supersession is handled by v6, never correction prose.",
            "inputSchema": canonical_write_schema(scope.clone()),
            "annotations": {"readOnlyHint":false,"destructiveHint":false,"idempotentHint":true,"openWorldHint":false}
        }),
        json!({
            "name": "handoff",
            "description": "Write short-lived exact-project continuation state. Handoffs remain separate from recall and canonical memory.",
            "inputSchema": {"type":"object","additionalProperties":false,"properties":{"request_id":{"type":"string","format":"uuid"},"project_key":{"type":"string","minLength":1,"maxLength":512},"content":{"type":"string"},"session_id":{"type":"string","minLength":1,"maxLength":512},"expires_at":{"type":["string","null"],"format":"date-time"}},"required":["request_id","project_key","content","session_id"]},
            "annotations": {"readOnlyHint":false,"destructiveHint":false,"idempotentHint":true,"openWorldHint":false}
        }),
        json!({
            "name": "evidence",
            "description": "Retrieve the exact archived conversation events (by event ID) that support a candidate or proposition. Labeled raw history, never canonical truth.",
            "inputSchema": {"type":"object","additionalProperties":false,"properties":{"event_ids":{"type":"array","items":{"type":"string","format":"uuid"},"minItems":1,"maxItems":1000}},"required":["event_ids"]},
            "annotations": {"readOnlyHint":true,"destructiveHint":false,"idempotentHint":true,"openWorldHint":false}
        }),
        json!({
            "name": "archive_search",
            "description": "Search the normalized conversation archive (raw history) with application/project/thread filters. Never promoted into canonical memory.",
            "inputSchema": {"type":"object","additionalProperties":false,"properties":{"query":{"type":["string","null"],"maxLength":4096},"consumer_id":{"type":["string","null"],"format":"uuid"},"project_key":{"type":["string","null"]},"thread_id":{"type":["string","null"]},"session_id":{"type":["string","null"]},"from":{"type":["string","null"],"format":"date-time"},"to":{"type":["string","null"],"format":"date-time"},"limit":{"type":"integer","minimum":1,"maximum":1000,"default":50}}},
            "annotations": {"readOnlyHint":true,"destructiveHint":false,"idempotentHint":true,"openWorldHint":false}
        }),
        json!({
            "name": "handoff_get_exact",
            "description": "Retrieve the single active, unexpired handoff for an exact typed context. Never broadens the search; returns handoff_identity:unavailable when no safe identity resolves.",
            "inputSchema": context_ref_schema(),
            "annotations": {"readOnlyHint":true,"destructiveHint":false,"idempotentHint":true,"openWorldHint":false}
        }),
        json!({
            "name": "handoff_validate_context",
            "description": "Report whether a typed handoff context key is safe and whether it has an active handoff. Forbidden broad keys (home dir, roots) are rejected.",
            "inputSchema": context_ref_schema(),
            "annotations": {"readOnlyHint":true,"destructiveHint":false,"idempotentHint":true,"openWorldHint":false}
        }),
        json!({
            "name": "handoff_history",
            "description": "List the handoff history for an exact typed context, including superseded and completed handoffs.",
            "inputSchema": {"type":"object","additionalProperties":false,"properties":{"context":context_ref_schema(),"limit":{"type":"integer","minimum":1,"maximum":200,"default":50}},"required":["context"]},
            "annotations": {"readOnlyHint":true,"destructiveHint":false,"idempotentHint":true,"openWorldHint":false}
        }),
        json!({
            "name": "handoff_write",
            "description": "Write a structured continuation packet for an exact typed context. Supersession is a compare-and-swap; pass expected_active_id to detect concurrent writers.",
            "inputSchema": handoff_write_schema(),
            "annotations": {"readOnlyHint":false,"destructiveHint":false,"idempotentHint":true,"openWorldHint":false}
        }),
        json!({
            "name": "handoff_acknowledge",
            "description": "Idempotently record that this consumer received a handoff after injecting it. Does not make it canonical or delete it.",
            "inputSchema": {"type":"object","additionalProperties":false,"properties":{"handoff_id":{"type":"string","format":"uuid"},"session_id":{"type":"string","minLength":1,"maxLength":256}},"required":["handoff_id","session_id"]},
            "annotations": {"readOnlyHint":false,"destructiveHint":false,"idempotentHint":true,"openWorldHint":false}
        }),
        json!({
            "name": "handoff_complete_items",
            "description": "Mark individual continuation items completed by stable item ID without rewriting unrelated content.",
            "inputSchema": {"type":"object","additionalProperties":false,"properties":{"handoff_id":{"type":"string","format":"uuid"},"item_ids":{"type":"array","items":{"type":"string","format":"uuid"},"minItems":1,"maxItems":600}},"required":["handoff_id","item_ids"]},
            "annotations": {"readOnlyHint":false,"destructiveHint":false,"idempotentHint":true,"openWorldHint":false}
        }),
        json!({
            "name": "handoff_close",
            "description": "Close the active handoff for an exact context when work is cleanly complete, leaving no misleading active continuation. Compare-and-swap on the active handoff.",
            "inputSchema": {"type":"object","additionalProperties":false,"properties":{"context":context_ref_schema(),"expected_active_id":{"type":"string","format":"uuid"}},"required":["context","expected_active_id"]},
            "annotations": {"readOnlyHint":false,"destructiveHint":true,"idempotentHint":true,"openWorldHint":false}
        }),
    ]
}

fn context_ref_schema() -> Value {
    json!({
        "type":"object",
        "additionalProperties":false,
        "properties":{
            "kind":{"type":"string","enum":["repository_worktree","durable_project","thread","installation_projectless"]},
            "key":{"type":"string","minLength":1,"maxLength":1024},
            "family_id":{"type":["string","null"],"maxLength":256}
        },
        "required":["kind","key"]
    })
}

fn handoff_write_schema() -> Value {
    json!({
        "type":"object",
        "additionalProperties":false,
        "properties":{
            "request_id":{"type":"string","description":"Unique request id. Any string is accepted; non-UUID strings are deterministically mapped to a UUIDv5 before the request reaches the server (same string always maps to the same UUID, preserving idempotent replay)."},
            "context":context_ref_schema(),
            "session_id":{"type":"string","minLength":1,"maxLength":256,"description":"Identifying session string. Any string is accepted; non-UUID strings are deterministically mapped to a UUIDv5 before the request reaches the server."},
            "thread_id":{"type":["string","null"],"maxLength":512},
            "summary":{"type":"string","minLength":1,"maxLength":16384},
            "written_by":{"type":"string","minLength":1,"maxLength":256},
            "completed":{"type":"array","items":{"type":"string"}},
            "in_progress":{"type":"array","items":{"type":"string"}},
            "next_actions":{"type":"array","items":{"type":"string"}},
            "blockers":{"type":"array","items":{"type":"string"}},
            "artifacts":{"type":"array","items":{"type":"string"}},
            "verification_state":{"type":["string","null"]},
            "do_not_repeat":{"type":"array","items":{"type":"string"}},
            "expected_active_id":{"type":["string","null"],"format":"uuid"},"expect_no_active":{"type":"boolean","default":false,"description":"CAS guard for creation: the writer observed no active handoff. Conflicts if one exists. Mutually exclusive with expected_active_id. Always pass one of the two guards after reading state."},
            "source_snapshot_revision":{"type":["integer","null"]},
            "expires_at":{"type":["string","null"],"format":"date-time"}
        },
        "required":["request_id","context","session_id","summary","written_by"]
    })
}

fn canonical_write_schema(scope: Value) -> Value {
    json!({
        "type":"object",
        "additionalProperties":false,
        "properties":{
            "request_id":{"type":"string","format":"uuid"},
            "scope":scope,
            "subject":{"type":"string"},
            "predicate":{"type":"string"},
            "object":{},
            "authority":{"type":"string","enum":["owner_instruction","mechanically_verified","canonical_documentation","repository_state","trusted_agent_report","inference","raw_history"]},
            "epistemic_status":{"type":"string","enum":["verified","asserted","inferred","uncertain","disputed"]},
            "source_type":{"type":"string"},
            "source_ref":{"type":"string"},
            "reason":{"type":"string"}
        },
        "required":["request_id","subject","predicate","object","authority","epistemic_status","source_type","source_ref","reason"]
    })
}

fn response(id: Value, result: Value) -> Value {
    json!({"jsonrpc":"2.0","id":id,"result":result})
}

fn protocol_error(id: Value, code: i64, message: impl Into<String>) -> Value {
    json!({"jsonrpc":"2.0","id":id,"error":{"code":code,"message":message.into()}})
}

fn tool_result(value: Value, is_error: bool) -> Value {
    let text = serde_json::to_string_pretty(&value).unwrap_or_else(|_| "null".into());
    json!({"content":[{"type":"text","text":text}],"isError":is_error})
}

async fn call_api(
    client: &Client,
    config: &Config,
    path: &str,
    token: &str,
    body: Value,
) -> Result<Value, String> {
    let mut response = client
        .post(endpoint(&config.base_url, path))
        .bearer_auth(token)
        .json(&body)
        .send()
        .await
        .map_err(|_| "Interlock request failed".to_string())?;
    let status = response.status();
    if response
        .content_length()
        .is_some_and(|length| length > MAX_RESPONSE_BYTES as u64)
    {
        return Err("Interlock response exceeds size limit".into());
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| "cannot read Interlock response".to_string())?
    {
        if bytes.len().saturating_add(chunk.len()) > MAX_RESPONSE_BYTES {
            return Err("Interlock response exceeds size limit".into());
        }
        bytes.extend_from_slice(&chunk);
    }
    if !status.is_success() {
        return Err(non_success_error(status.as_u16(), &bytes));
    }
    let value: Value = serde_json::from_slice(&bytes)
        .map_err(|_| "Interlock returned invalid JSON".to_string())?;
    Ok(value)
}

/// Non-2xx bodies carry the server's explanation, and not always as JSON:
/// axum answers malformed request bodies with a plain-text 422 ("Failed to
/// deserialize the JSON body ..."). Prefer the JSON body when it parses, and
/// fall back to truncated body text instead of reporting an invalid-JSON
/// parse failure that hides the real cause.
fn non_success_error(status: u16, body: &[u8]) -> String {
    if let Ok(value) = serde_json::from_slice::<Value>(body) {
        return format!("Interlock returned HTTP {status}: {value}");
    }
    format!(
        "Interlock returned HTTP {status}: {}",
        error_body_text(body)
    )
}

fn error_body_text(body: &[u8]) -> String {
    let text = String::from_utf8_lossy(body);
    let text = text.trim();
    if text.is_empty() {
        return "(empty body)".into();
    }
    let mut truncated: String = text.chars().take(MAX_ERROR_BODY_CHARS).collect();
    if truncated.len() < text.len() {
        truncated.push('…');
    }
    truncated
}

/// The server types handoff_write's request_id/session_id as strict UUIDs,
/// while callers naturally pass harness identifiers ("sess-...", dates,
/// hostnames), which the server rejects with a 422. Accept any string and
/// deterministically map non-UUID strings to UUIDv5 in a fixed adapter
/// namespace, so the same input always yields the same server-visible id —
/// idempotent replay of handoff writes depends on that for request_id.
fn normalize_handoff_ids(arguments: &mut Value) {
    let Some(fields) = arguments.as_object_mut() else {
        return;
    };
    for field in ["request_id", "session_id"] {
        let derived = fields
            .get(field)
            .and_then(Value::as_str)
            .filter(|id| Uuid::parse_str(id).is_err())
            .map(|id| Uuid::new_v5(&adapter_id_namespace(), id.as_bytes()).to_string());
        if let Some(uuid) = derived {
            fields.insert(field.into(), json!(uuid));
        }
    }
}

/// Chained under a DNS name rather than NAMESPACE_OID so adapter-derived ids
/// cannot collide with v5(OID, ...) ids minted by unrelated tooling.
fn adapter_id_namespace() -> Uuid {
    Uuid::new_v5(&Uuid::NAMESPACE_DNS, b"foreman-v6-mcp-adapter")
}

async fn call_tool(client: &Client, config: &Config, params: &Value) -> Result<Value, String> {
    let name = params
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| "tools/call requires a tool name".to_string())?;
    let mut arguments = params
        .get("arguments")
        .cloned()
        .unwrap_or_else(|| json!({}));
    if !arguments.is_object() {
        return Err("tool arguments must be an object".into());
    }
    let (path, token) = match name {
        "bootstrap" => ("/v6/bootstrap", config.reader_token.as_str()),
        "recall" => ("/v6/recall", config.reader_token.as_str()),
        "history" => {
            arguments
                .as_object_mut()
                .expect("checked object")
                .insert("intent".into(), json!("history"));
            ("/v6/recall", config.reader_token.as_str())
        }
        "observe" => ("/v6/observations", config.write_token.as_str()),
        "remember" | "correct" => ("/v6/memories", config.write_token.as_str()),
        "handoff" => ("/v6/handoffs", config.write_token.as_str()),
        "evidence" => ("/v6.5/archive/evidence", config.reader_token.as_str()),
        "archive_search" => ("/v6.5/archive/search", config.reader_token.as_str()),
        "handoff_get_exact" => ("/v6.5/handoff/get_exact", config.reader_token.as_str()),
        "handoff_validate_context" => (
            "/v6.5/handoff/validate_context",
            config.reader_token.as_str(),
        ),
        "handoff_history" => ("/v6.5/handoff/history", config.reader_token.as_str()),
        "handoff_write" => ("/v6.5/handoff/write", config.write_token.as_str()),
        "handoff_acknowledge" => ("/v6.5/handoff/acknowledge", config.write_token.as_str()),
        "handoff_complete_items" => ("/v6.5/handoff/complete_items", config.write_token.as_str()),
        "handoff_close" => ("/v6.5/handoff/close", config.write_token.as_str()),
        _ => return Err(format!("unknown tool: {name}")),
    };
    if name == "handoff_write" {
        normalize_handoff_ids(&mut arguments);
    }
    call_api(client, config, path, token, arguments).await
}

async fn handle(client: &Client, config: &Config, message: Value) -> Option<Value> {
    let id = message.get("id").cloned()?;
    let method = message.get("method").and_then(Value::as_str);
    let result = match method {
        Some("initialize") => Ok(json!({
            "protocolVersion":"2025-06-18",
            "capabilities":{"tools":{"listChanged":false}},
            "serverInfo":{"name":"interlock","version":env!("CARGO_PKG_VERSION")},
            "instructions":"Always call bootstrap before acting. Treat directives as mandatory. Use recall before asking for known facts. Keep handoffs separate from canonical memory. Observe records evidence; remember/correct require explicit authority and provenance. Never broaden scope or promote history/handoffs implicitly."
        })),
        Some("ping") => Ok(json!({})),
        Some("tools/list") => Ok(json!({"tools":tools()})),
        Some("tools/call") => match call_tool(
            client,
            config,
            message.get("params").unwrap_or(&Value::Null),
        )
        .await
        {
            Ok(value) => Ok(tool_result(value, false)),
            Err(error) => Ok(tool_result(json!({"error":error}), true)),
        },
        Some(_) => Err((-32601, "method not found")),
        None => Err((-32600, "invalid request")),
    };
    Some(match result {
        Ok(value) => response(id, value),
        Err((code, message)) => protocol_error(id, code, message),
    })
}

async fn read_frame<R: AsyncBufRead + Unpin>(reader: &mut R) -> Result<Option<Vec<u8>>, String> {
    let mut frame = Vec::new();
    loop {
        let available = reader
            .fill_buf()
            .await
            .map_err(|_| "cannot read MCP input".to_string())?;
        if available.is_empty() {
            return if frame.is_empty() {
                Ok(None)
            } else {
                Ok(Some(frame))
            };
        }
        let newline = available.iter().position(|byte| *byte == b'\n');
        let take = newline.unwrap_or(available.len());
        if frame.len().saturating_add(take) > MAX_FRAME_BYTES {
            return Err("MCP frame exceeds size limit".into());
        }
        frame.extend_from_slice(&available[..take]);
        reader.consume(take + usize::from(newline.is_some()));
        if newline.is_some() {
            if frame.last() == Some(&b'\r') {
                frame.pop();
            }
            return Ok(Some(frame));
        }
    }
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("interlock-mcp: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), String> {
    let config = Config::from_env()?;
    let client = Client::builder()
        .redirect(Policy::none())
        .connect_timeout(Duration::from_secs(3))
        .timeout(Duration::from_secs(20))
        .build()
        .map_err(|_| "cannot build Interlock HTTP client".to_string())?;
    let mut stdin = BufReader::new(io::stdin());
    let mut stdout = BufWriter::new(io::stdout());
    while let Some(frame) = read_frame(&mut stdin).await? {
        if frame.is_empty() {
            continue;
        }
        let message: Value = match serde_json::from_slice(&frame) {
            Ok(message) => message,
            Err(_) => {
                let reply = protocol_error(Value::Null, -32700, "parse error");
                stdout
                    .write_all(format!("{reply}\n").as_bytes())
                    .await
                    .map_err(|_| "cannot write MCP output".to_string())?;
                stdout
                    .flush()
                    .await
                    .map_err(|_| "cannot flush MCP output")?;
                continue;
            }
        };
        if let Some(reply) = handle(&client, &config, message).await {
            stdout
                .write_all(format!("{reply}\n").as_bytes())
                .await
                .map_err(|_| "cannot write MCP output".to_string())?;
            stdout
                .flush()
                .await
                .map_err(|_| "cannot flush MCP output")?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_requires_loopback() {
        assert!(loopback_url("http://127.0.0.1:8851").is_ok());
        assert!(loopback_url("http://[::1]:8851").is_ok());
        assert!(loopback_url("http://203.0.113.10:8851").is_err());
        assert!(loopback_url("https://example.com").is_err());
        assert!(loopback_url("http://127.0.0.1:8851/path").is_err());
    }

    #[test]
    fn tool_surface_matches_codex_contract() {
        let surface = tools();
        let names: Vec<_> = surface
            .iter()
            .filter_map(|tool| tool["name"].as_str())
            .collect();
        assert_eq!(
            names,
            [
                "bootstrap",
                "recall",
                "history",
                "observe",
                "remember",
                "correct",
                "handoff",
                "evidence",
                "archive_search",
                "handoff_get_exact",
                "handoff_validate_context",
                "handoff_history",
                "handoff_write",
                "handoff_acknowledge",
                "handoff_complete_items",
                "handoff_close"
            ]
        );
    }

    #[test]
    fn non_success_error_prefers_json_body() {
        let message = non_success_error(400, br#"{"error":"scope is required"}"#);
        assert_eq!(
            message,
            r#"Interlock returned HTTP 400: {"error":"scope is required"}"#
        );
    }

    #[test]
    fn non_success_error_surfaces_plain_text_body() {
        let body = b"Failed to deserialize the JSON body: request_id: UUID parsing failed: invalid character: expected an optional prefix of `h` or `H` at line 1 column 6";
        let message = non_success_error(422, body);
        assert!(message.starts_with("Interlock returned HTTP 422: "));
        assert!(message.contains("UUID parsing failed"));
        assert!(!message.contains("invalid JSON"));
    }

    #[test]
    fn non_success_error_truncates_long_bodies() {
        let body = vec![b'x'; 5_000];
        let message = non_success_error(500, &body);
        let text = message
            .strip_prefix("Interlock returned HTTP 500: ")
            .unwrap();
        assert!(text.ends_with('…'));
        assert!(text.chars().count() <= MAX_ERROR_BODY_CHARS + 1);
    }

    #[test]
    fn non_success_error_marks_empty_body() {
        assert_eq!(
            non_success_error(503, b"  \n"),
            "Interlock returned HTTP 503: (empty body)"
        );
    }

    #[test]
    fn handoff_ids_pass_uuids_through_untouched() {
        let uuid = "550e8400-e29b-41d4-a716-446655440000";
        let mut arguments = json!({"request_id": uuid, "session_id": uuid});
        normalize_handoff_ids(&mut arguments);
        assert_eq!(arguments["request_id"], json!(uuid));
        assert_eq!(arguments["session_id"], json!(uuid));
    }

    #[test]
    fn handoff_ids_derive_deterministically() {
        let mut first =
            json!({"request_id": "my-task-checkpoint-01", "session_id": "session-alpha-42"});
        let mut second =
            json!({"request_id": "my-task-checkpoint-01", "session_id": "session-alpha-42"});
        normalize_handoff_ids(&mut first);
        normalize_handoff_ids(&mut second);
        assert_eq!(first["request_id"], second["request_id"]);
        assert_eq!(first["session_id"], second["session_id"]);
        for field in ["request_id", "session_id"] {
            let derived = first[field].as_str().unwrap();
            assert!(Uuid::parse_str(derived).is_ok());
            assert_ne!(derived, "my-task-checkpoint-01");
        }
        assert_ne!(first["request_id"], first["session_id"]);
    }

    #[test]
    fn handoff_id_derivation_leaves_other_fields_alone() {
        let mut arguments =
            json!({"request_id": "not-a-uuid", "thread_id": "also-not-a-uuid", "written_by": "agent-a"});
        normalize_handoff_ids(&mut arguments);
        assert_eq!(arguments["thread_id"], json!("also-not-a-uuid"));
        assert_eq!(arguments["written_by"], json!("agent-a"));
        assert!(Uuid::parse_str(arguments["request_id"].as_str().unwrap()).is_ok());
    }

    #[test]
    fn handoff_write_schema_documents_id_derivation() {
        let schema = handoff_write_schema();
        for field in ["request_id", "session_id"] {
            let property = &schema["properties"][field];
            assert!(
                property["description"]
                    .as_str()
                    .unwrap_or_default()
                    .contains("UUIDv5"),
                "{field} description must mention UUIDv5 derivation"
            );
        }
        assert!(schema["properties"]["request_id"].get("format").is_none());
    }
}
