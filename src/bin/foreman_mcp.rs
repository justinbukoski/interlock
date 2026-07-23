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

const MAX_FRAME_BYTES: usize = 1_048_576;
const MAX_RESPONSE_BYTES: usize = 4_194_304;

struct Config {
    base_url: Url,
    reader_token: String,
    owner_token: String,
}

impl Config {
    fn from_env() -> Result<Self, String> {
        let base_url = loopback_url(
            &std::env::var("FOREMAN_V6_URL").unwrap_or_else(|_| "http://127.0.0.1:8851".into()),
        )?;
        let reader_path = configured_path(
            "FOREMAN_V6_READER_TOKEN_FILE",
            ".config/foreman/v6-reader-token",
        )?;
        let owner_path = configured_path(
            "FOREMAN_V6_OWNER_TOKEN_FILE",
            ".config/foreman/v6-owner-token",
        )?;
        Ok(Self {
            base_url,
            reader_token: read_secret(&reader_path)?,
            owner_token: read_secret(&owner_path)?,
        })
    }
}

fn configured_path(key: &str, default_relative: &str) -> Result<PathBuf, String> {
    if let Ok(path) = std::env::var(key) {
        if path.trim().is_empty() {
            return Err(format!("{key} cannot be empty"));
        }
        return Ok(path.into());
    }
    let home = std::env::var_os("HOME").ok_or_else(|| "HOME is required".to_string())?;
    Ok(PathBuf::from(home).join(default_relative))
}

fn loopback_url(input: &str) -> Result<Url, String> {
    let mut url = Url::parse(input).map_err(|_| "FOREMAN_V6_URL is invalid".to_string())?;
    if url.scheme() != "http"
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(
            "FOREMAN_V6_URL must be credential-free loopback HTTP without query/fragment".into(),
        );
    }
    let host = url
        .host_str()
        .ok_or_else(|| "FOREMAN_V6_URL requires a host".to_string())?;
    let host_ip = host.trim_start_matches('[').trim_end_matches(']');
    let loopback = host == "localhost"
        || host_ip
            .parse::<IpAddr>()
            .is_ok_and(|address| address.is_loopback());
    if !loopback {
        return Err("FOREMAN_V6_URL must use a loopback host through the SSH tunnel".into());
    }
    if url.path() != "/" && !url.path().is_empty() {
        return Err("FOREMAN_V6_URL must not contain a path".into());
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
            "description": "Load mandatory directives, scoped project state, and the latest exact-project handoff before acting.",
            "inputSchema": {"type":"object","additionalProperties":false,"properties":{"scope":scope,"token_budget":{"type":"integer","minimum":64,"maximum":32768,"default":4096}},"required":["token_budget"]},
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
    ]
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
        .map_err(|_| "Foreman v6 request failed".to_string())?;
    let status = response.status();
    if response
        .content_length()
        .is_some_and(|length| length > MAX_RESPONSE_BYTES as u64)
    {
        return Err("Foreman v6 response exceeds size limit".into());
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| "cannot read Foreman v6 response".to_string())?
    {
        if bytes.len().saturating_add(chunk.len()) > MAX_RESPONSE_BYTES {
            return Err("Foreman v6 response exceeds size limit".into());
        }
        bytes.extend_from_slice(&chunk);
    }
    let value: Value = serde_json::from_slice(&bytes)
        .map_err(|_| "Foreman v6 returned invalid JSON".to_string())?;
    if !status.is_success() {
        return Err(format!(
            "Foreman v6 returned HTTP {}: {value}",
            status.as_u16()
        ));
    }
    Ok(value)
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
        "observe" => ("/v6/observations", config.owner_token.as_str()),
        "remember" | "correct" => ("/v6/memories", config.owner_token.as_str()),
        "handoff" => ("/v6/handoffs", config.owner_token.as_str()),
        _ => return Err(format!("unknown tool: {name}")),
    };
    call_api(client, config, path, token, arguments).await
}

async fn handle(client: &Client, config: &Config, message: Value) -> Option<Value> {
    let id = message.get("id").cloned()?;
    let method = message.get("method").and_then(Value::as_str);
    let result = match method {
        Some("initialize") => Ok(json!({
            "protocolVersion":"2025-06-18",
            "capabilities":{"tools":{"listChanged":false}},
            "serverInfo":{"name":"foreman-memory-v6","version":env!("CARGO_PKG_VERSION")},
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
        eprintln!("foreman-mcp: {error}");
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
        .map_err(|_| "cannot build Foreman v6 HTTP client".to_string())?;
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
        assert!(loopback_url("http://10.0.0.42:8851").is_err());
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
                "handoff"
            ]
        );
    }
}
