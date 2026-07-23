use chrono::{DateTime, Utc};
use foreman_memory_v6::evaluation::{
    AggregateScore, CaseComparison, EvaluationManifest, NormalizedPacket, aggregate, error_packet,
    normalize_v5, normalize_v6, regressions_by_class, score,
};
use reqwest::{Client, Url, redirect::Policy};
use serde::Serialize;
use serde_json::{Value, json};
use std::{
    fs::OpenOptions,
    io::{Read, Write},
    net::IpAddr,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

const MAX_MANIFEST_BYTES: u64 = 1_048_576;
const MAX_RESPONSE_BYTES: usize = 4_194_304;
const MAX_REPORT_BYTES: usize = 16_777_216;

#[derive(Serialize)]
struct CaseReport {
    comparison: CaseComparison,
    v5_packet: PacketEvidence,
    v6_packet: PacketEvidence,
    v5_latency_ms: u128,
    v6_latency_ms: u128,
}

#[derive(Serialize)]
struct PacketEvidence {
    system: String,
    item_count: usize,
    reported_token_count: Option<usize>,
    retrieval_mode: Option<String>,
    snapshot_revision: Option<i64>,
    response_sha256: String,
    execution_error: Option<String>,
}

impl From<&NormalizedPacket> for PacketEvidence {
    fn from(packet: &NormalizedPacket) -> Self {
        Self {
            system: packet.system.clone(),
            item_count: packet.items.len(),
            reported_token_count: packet.reported_token_count,
            retrieval_mode: packet.retrieval_mode.clone(),
            snapshot_revision: packet.snapshot_revision,
            response_sha256: packet.response_sha256.clone(),
            execution_error: packet.execution_error.clone(),
        }
    }
}

#[derive(Serialize)]
struct ShadowReport {
    schema_version: u32,
    created_at: DateTime<Utc>,
    git_commit: String,
    fixture_revision: String,
    fixture_sha256: String,
    snapshot_id: String,
    host: String,
    v5_url: String,
    v6_url: String,
    v5: AggregateScore,
    v6: AggregateScore,
    regressions_by_class: std::collections::BTreeMap<String, usize>,
    release_gate_pass: bool,
    cases: Vec<CaseReport>,
}

struct Config {
    manifest: PathBuf,
    output: PathBuf,
    v5_url: Url,
    v6_url: Url,
    v5_token_file: PathBuf,
    v6_token_file: PathBuf,
    git_commit: String,
}

impl Config {
    fn from_env() -> Result<Self, String> {
        let get = |key: &str| std::env::var(key).map_err(|_| format!("{key} is required"));
        let v5_url = private_url(&get("FOREMAN_EVAL_V5_URL")?)?;
        let v6_url = private_url(&get("FOREMAN_EVAL_V6_URL")?)?;
        let git_commit = get("FOREMAN_EVAL_GIT_COMMIT")?;
        if git_commit.len() < 7
            || git_commit.len() > 64
            || !git_commit.bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            return Err("FOREMAN_EVAL_GIT_COMMIT must be a hexadecimal commit ID".into());
        }
        Ok(Self {
            manifest: get("FOREMAN_EVAL_MANIFEST")?.into(),
            output: get("FOREMAN_EVAL_OUTPUT")?.into(),
            v5_url,
            v6_url,
            v5_token_file: get("FOREMAN_EVAL_V5_TOKEN_FILE")?.into(),
            v6_token_file: get("FOREMAN_EVAL_V6_TOKEN_FILE")?.into(),
            git_commit,
        })
    }
}

fn private_url(input: &str) -> Result<Url, String> {
    let mut url = Url::parse(input).map_err(|_| "evaluation URL is invalid".to_string())?;
    if !matches!(url.scheme(), "http" | "https")
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err("evaluation URL must be credential-free HTTP(S) without query/fragment".into());
    }
    let host = url
        .host_str()
        .ok_or_else(|| "evaluation URL requires a host".to_string())?;
    let private = host == "localhost"
        || host.parse::<IpAddr>().is_ok_and(|ip| match ip {
            IpAddr::V4(ip) => ip.is_private() || ip.is_loopback(),
            IpAddr::V6(ip) => ip.is_loopback() || (ip.segments()[0] & 0xfe00) == 0xfc00,
        });
    if !private {
        return Err(
            "evaluation endpoints must use localhost or literal private/loopback IPs".into(),
        );
    }
    url.set_query(None);
    url.set_fragment(None);
    Ok(url)
}

fn read_secret(path: &Path) -> Result<String, String> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(nix::libc::O_NOFOLLOW | nix::libc::O_CLOEXEC);
    }
    let file = options
        .open(path)
        .map_err(|_| "cannot open token file".to_string())?;
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
    file.take(8193)
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

async fn post(
    client: &Client,
    url: Url,
    token: &str,
    body: Value,
) -> Result<(Value, u128), String> {
    let started = Instant::now();
    let mut response = client
        .post(url)
        .bearer_auth(token)
        .json(&body)
        .send()
        .await
        .map_err(|_| "shadow endpoint unavailable".to_string())?
        .error_for_status()
        .map_err(|_| "shadow endpoint returned an error".to_string())?;
    if response
        .content_length()
        .is_some_and(|length| length > MAX_RESPONSE_BYTES as u64)
    {
        return Err("shadow response exceeds byte limit".into());
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| "shadow response read failed".to_string())?
    {
        if bytes.len().saturating_add(chunk.len()) > MAX_RESPONSE_BYTES {
            return Err("shadow response exceeds byte limit".into());
        }
        bytes.extend_from_slice(&chunk);
    }
    let value = serde_json::from_slice::<Value>(&bytes)
        .map_err(|_| "shadow endpoint returned invalid JSON".to_string())?;
    Ok((value, started.elapsed().as_millis()))
}

fn write_private_new(path: &Path, bytes: &[u8]) -> Result<(), String> {
    if bytes.len() > MAX_REPORT_BYTES {
        return Err("report exceeds byte limit".into());
    }
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600).custom_flags(nix::libc::O_NOFOLLOW);
    }
    let mut file = options
        .open(path)
        .map_err(|_| "output must be a new writable file".to_string())?;
    file.write_all(bytes)
        .and_then(|_| file.sync_all())
        .map_err(|_| "failed to durably write report".to_string())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::from_env().map_err(std::io::Error::other)?;
    let mut manifest_bytes = Vec::new();
    OpenOptions::new()
        .read(true)
        .open(&config.manifest)?
        .take(MAX_MANIFEST_BYTES + 1)
        .read_to_end(&mut manifest_bytes)?;
    if manifest_bytes.len() as u64 > MAX_MANIFEST_BYTES {
        return Err("manifest exceeds byte limit".into());
    }
    let manifest: EvaluationManifest = serde_json::from_slice(&manifest_bytes)?;
    manifest.validate().map_err(std::io::Error::other)?;
    let fixture_sha256 = manifest.checksum()?;
    let v5_token = read_secret(&config.v5_token_file).map_err(std::io::Error::other)?;
    let v6_token = read_secret(&config.v6_token_file).map_err(std::io::Error::other)?;
    let client = Client::builder()
        .connect_timeout(Duration::from_secs(2))
        .timeout(Duration::from_secs(15))
        .redirect(Policy::none())
        .build()?;
    let mut case_reports = Vec::new();
    let mut comparisons = Vec::new();
    for case in &manifest.cases {
        let v5_started = Instant::now();
        let v5_packet = match post(
            &client,
            endpoint(&config.v5_url, "/v5/recall"),
            &v5_token,
            json!({"query":case.query,"top_k":case.limit}),
        )
        .await
        {
            Ok((raw, _)) => {
                normalize_v5(&raw).unwrap_or_else(|_| error_packet("v5", "invalid_v5_packet"))
            }
            Err(_) => error_packet("v5", "v5_request_failed"),
        };
        let v5_latency_ms = v5_started.elapsed().as_millis();
        let v6_started = Instant::now();
        let v6_packet = match post(&client, endpoint(&config.v6_url, "/v6/recall"), &v6_token,
            json!({"query":case.query,"intent":case.intent,"scope":case.scope,"token_budget":case.token_budget,"limit":case.limit})).await {
            Ok((raw, _)) => normalize_v6(&raw)
                .unwrap_or_else(|_| error_packet("v6", "invalid_v6_packet")),
            Err(_) => error_packet("v6", "v6_request_failed"),
        };
        let v6_latency_ms = v6_started.elapsed().as_millis();
        let v5 = score(case, &v5_packet);
        let v6 = score(case, &v6_packet);
        let v6_regressed = v6.required_hits < v5.required_hits
            || v6.forbidden_hit_sha256.len() > v5.forbidden_hit_sha256.len()
            || (!v6.hard_gate_pass && v5.hard_gate_pass);
        let comparison = CaseComparison {
            case_id: case.id.clone(),
            revision: case.revision,
            failure_classes: case.failure_classes.clone(),
            v5,
            v6,
            v6_regressed,
        };
        comparisons.push(comparison.clone());
        case_reports.push(CaseReport {
            comparison,
            v5_packet: PacketEvidence::from(&v5_packet),
            v6_packet: PacketEvidence::from(&v6_packet),
            v5_latency_ms,
            v6_latency_ms,
        });
    }
    let v5 = aggregate(&comparisons, "v5");
    let v6 = aggregate(&comparisons, "v6");
    let regressions = regressions_by_class(&comparisons);
    let release_gate_pass = v5.execution_errors == 0
        && v6.execution_errors == 0
        && v6.hard_gate_failures == 0
        && regressions.is_empty()
        && v6.required_hits > v5.required_hits;
    let report = ShadowReport {
        schema_version: 1,
        created_at: Utc::now(),
        git_commit: config.git_commit,
        fixture_revision: manifest.fixture_revision,
        fixture_sha256,
        snapshot_id: manifest.snapshot_id,
        host: std::env::var("HOSTNAME").unwrap_or_else(|_| "unknown".into()),
        v5_url: config.v5_url.to_string(),
        v6_url: config.v6_url.to_string(),
        v5,
        v6,
        regressions_by_class: regressions,
        release_gate_pass,
        cases: case_reports,
    };
    write_private_new(&config.output, &serde_json::to_vec_pretty(&report)?)?;
    if !report.release_gate_pass {
        std::process::exit(2);
    }
    Ok(())
}
