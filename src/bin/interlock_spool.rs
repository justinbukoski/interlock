//! `interlock-spool` — the adapter-side durable capture queue CLI for Interlock 6.5.
//!
//! Capture adapters (Codex, Claude, Hermes, generic) shell out to this binary to
//! append a captured conversation event durably before acknowledging the host
//! turn, and to drain the queue into the archive when connectivity returns.
//!
//! Commands:
//!   enqueue   Read one archive event JSON object from stdin, append it durably,
//!             and print the assigned sequence. Exit status encodes the adapter
//!             capture level when the spool is full (see below).
//!   status    Print spool health as JSON.
//!   flush     POST pending events to the archive ingestion endpoint in order,
//!             acknowledging the spool only after a durable 2xx per batch.
//!
//! Capture levels (`INTERLOCK_ADAPTER_LEVEL`):
//!   A  blocking      — on a full spool, exit non-zero so the host hook can block
//!                      the turn. A Level A adapter may claim zero-loss capture.
//!   B  gap-detecting — on a full spool, append a durable gap marker to the gap
//!                      log, surface a red state, and exit with a distinct code.
//!                      It never silently discards the event's existence.

use interlock::spool::{Spool, SpoolCapacity, SpoolError};
use serde_json::{Value, json};
use std::io::{Read, Write};
use std::path::PathBuf;
use std::process::ExitCode;

const EXIT_OK: u8 = 0;
const EXIT_ERROR: u8 = 1;
/// Level A full-spool: the host turn must be blocked.
const EXIT_BLOCK_HOST: u8 = 3;
/// Level B full-spool: a gap was recorded; the turn proceeds but history has a hole.
const EXIT_GAP_RECORDED: u8 = 4;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Level {
    A,
    B,
}

struct Config {
    path: PathBuf,
    capacity: SpoolCapacity,
    level: Level,
    gap_log: PathBuf,
}

impl Config {
    fn from_env() -> Result<Self, String> {
        let path = PathBuf::from(
            std::env::var("INTERLOCK_SPOOL_PATH")
                .map_err(|_| "INTERLOCK_SPOOL_PATH is required")?,
        );
        if path.as_os_str().is_empty() {
            return Err("INTERLOCK_SPOOL_PATH cannot be empty".into());
        }
        let max_records = parse_bound("INTERLOCK_SPOOL_MAX_RECORDS", 100_000)?;
        let max_bytes = parse_bound("INTERLOCK_SPOOL_MAX_BYTES", 256 * 1024 * 1024)?;
        let capacity =
            SpoolCapacity::new(max_records, max_bytes).map_err(|error| error.to_string())?;
        let level = match std::env::var("INTERLOCK_ADAPTER_LEVEL").as_deref() {
            Ok("A") | Ok("a") => Level::A,
            Ok("B") | Ok("b") | Err(_) => Level::B,
            Ok(other) => {
                return Err(format!(
                    "INTERLOCK_ADAPTER_LEVEL must be A or B, got {other}"
                ));
            }
        };
        let gap_log = std::env::var("INTERLOCK_SPOOL_GAP_LOG")
            .map(PathBuf::from)
            .unwrap_or_else(|_| path.with_extension("gaps"));
        Ok(Self {
            path,
            capacity,
            level,
            gap_log,
        })
    }
}

fn parse_bound(key: &str, default: u64) -> Result<u64, String> {
    match std::env::var(key) {
        Ok(value) => value
            .parse::<u64>()
            .map_err(|_| format!("{key} must be a positive integer")),
        Err(_) => Ok(default),
    }
}

fn main() -> ExitCode {
    match run() {
        Ok(code) => ExitCode::from(code),
        Err(error) => {
            eprintln!("interlock-spool: {error}");
            ExitCode::from(EXIT_ERROR)
        }
    }
}

fn run() -> Result<u8, String> {
    let command = std::env::args().nth(1).unwrap_or_default();
    let config = Config::from_env()?;
    match command.as_str() {
        "enqueue" => enqueue(&config),
        "status" => status(&config),
        "flush" => flush(&config),
        "" => Err("usage: interlock-spool <enqueue|status|flush>".into()),
        other => Err(format!("unknown command: {other}")),
    }
}

fn open(config: &Config) -> Result<Spool, String> {
    Spool::open(&config.path, config.capacity).map_err(|error| error.to_string())
}

fn enqueue(config: &Config) -> Result<u8, String> {
    let mut raw = Vec::new();
    std::io::stdin()
        .take(8 * 1024 * 1024)
        .read_to_end(&mut raw)
        .map_err(|_| "cannot read event from stdin")?;
    // Validate it is a single JSON object so a malformed capture is rejected at
    // the boundary rather than poisoning a later archive batch.
    let event: Value =
        serde_json::from_slice(&raw).map_err(|_| "spool event must be a JSON object")?;
    if !event.is_object() {
        return Err("spool event must be a JSON object".into());
    }
    let mut spool = open(config)?;
    match spool.append(&raw) {
        Ok(sequence) => {
            println!("{}", json!({"status": "spooled", "sequence": sequence}));
            Ok(EXIT_OK)
        }
        Err(SpoolError::Full { .. }) => match config.level {
            Level::A => {
                eprintln!(
                    "{}",
                    json!({"status":"spool_full","level":"A","action":"block_host_turn"})
                );
                Ok(EXIT_BLOCK_HOST)
            }
            Level::B => {
                record_gap(config, &event)?;
                eprintln!(
                    "{}",
                    json!({"status":"spool_full","level":"B","action":"gap_recorded"})
                );
                Ok(EXIT_GAP_RECORDED)
            }
        },
        Err(other) => Err(other.to_string()),
    }
}

/// Append a durable gap marker recording that an event could not be captured.
/// This is the Level B honesty guarantee: the hole in history is itself recorded.
fn record_gap(config: &Config, event: &Value) -> Result<(), String> {
    let marker = json!({
        "gap": true,
        "source_event_id": event.get("source_event_id"),
        "consumer_id": event.get("consumer_id"),
        "thread_id": event.get("thread_id"),
        "reason": "spool_capacity_exhausted",
    });
    let mut options = std::fs::OpenOptions::new();
    options.create(true).append(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options
        .open(&config.gap_log)
        .map_err(|_| "cannot open gap log")?;
    writeln!(file, "{marker}").map_err(|_| "cannot write gap marker")?;
    file.sync_all().map_err(|_| "cannot flush gap marker")?;
    Ok(())
}

fn status(config: &Config) -> Result<u8, String> {
    let spool = open(config)?;
    let health = spool.health();
    let gap_count = std::fs::read_to_string(&config.gap_log)
        .map(|contents| contents.lines().filter(|line| !line.is_empty()).count())
        .unwrap_or(0);
    let state = if gap_count > 0 || health.is_full() {
        "red"
    } else if !health.is_empty() {
        "amber"
    } else {
        "green"
    };
    println!(
        "{:#}",
        json!({
            "level": match config.level { Level::A => "A", Level::B => "B" },
            "state": state,
            "pending_records": health.pending_records,
            "pending_bytes": health.pending_bytes,
            "max_records": health.max_records,
            "max_bytes": health.max_bytes,
            "next_sequence": health.next_sequence,
            "recorded_gaps": gap_count,
        })
    );
    Ok(EXIT_OK)
}

fn flush(config: &Config) -> Result<u8, String> {
    let base = loopback_url(
        &std::env::var("INTERLOCK_URL").unwrap_or_else(|_| "http://127.0.0.1:8851".into()),
    )?;
    let token = std::env::var("INTERLOCK_ADAPTER_TOKEN")
        .map_err(|_| "INTERLOCK_ADAPTER_TOKEN is required to flush")?;
    let batch_size: usize = parse_bound("INTERLOCK_FLUSH_BATCH", 128)? as usize;
    let mut spool = open(config)?;
    let mut delivered = 0u64;
    let client = ureq_agent()?;
    loop {
        let pending = spool
            .pending(batch_size)
            .map_err(|error| error.to_string())?;
        if pending.is_empty() {
            break;
        }
        let last = pending.last().map(|record| record.sequence).unwrap();
        let events: Vec<Value> = pending
            .iter()
            .map(|record| serde_json::from_slice::<Value>(&record.payload))
            .collect::<Result<_, _>>()
            .map_err(|_| "spooled payload is not valid JSON")?;
        post_batch(&client, &base, &token, &events)?;
        spool.ack(last).map_err(|error| error.to_string())?;
        delivered += pending.len() as u64;
    }
    println!("{}", json!({"status": "flushed", "delivered": delivered}));
    Ok(EXIT_OK)
}

// A tiny blocking HTTP client built on std, avoiding a new async runtime just to
// drain a spool. It only ever targets a validated loopback address.
struct Agent {
    connect_timeout: std::time::Duration,
}

fn ureq_agent() -> Result<Agent, String> {
    Ok(Agent {
        connect_timeout: std::time::Duration::from_secs(5),
    })
}

fn post_batch(
    agent: &Agent,
    base: &url_lite::Url,
    token: &str,
    events: &[Value],
) -> Result<(), String> {
    use std::io::{BufRead, BufReader};
    use std::net::TcpStream;
    let body = json!({"events": events}).to_string();
    let stream = TcpStream::connect_timeout(&base.socket_addr()?, agent.connect_timeout)
        .map_err(|_| "cannot connect to archive endpoint")?;
    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(20)))
        .ok();
    stream
        .set_write_timeout(Some(std::time::Duration::from_secs(20)))
        .ok();
    let mut writer = &stream;
    let request = format!(
        "POST /v6.5/archive/events HTTP/1.1\r\nHost: {host}\r\nAuthorization: Bearer {token}\r\nContent-Type: application/json\r\nContent-Length: {len}\r\nConnection: close\r\n\r\n",
        host = base.host_header(),
        len = body.len(),
    );
    writer
        .write_all(request.as_bytes())
        .and_then(|_| writer.write_all(body.as_bytes()))
        .map_err(|_| "cannot send archive request")?;
    let mut reader = BufReader::new(&stream);
    let mut status_line = String::new();
    reader
        .read_line(&mut status_line)
        .map_err(|_| "cannot read archive response")?;
    let code = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|value| value.parse::<u16>().ok())
        .ok_or("malformed archive response")?;
    if !(200..300).contains(&code) {
        return Err(format!("archive ingestion returned HTTP {code}"));
    }
    Ok(())
}

fn loopback_url(input: &str) -> Result<url_lite::Url, String> {
    url_lite::Url::parse_loopback(input)
}

/// Minimal loopback URL parsing so the flush client stays dependency-light while
/// still refusing any non-loopback target.
mod url_lite {
    use std::net::{IpAddr, SocketAddr};

    pub struct Url {
        host: String,
        port: u16,
    }

    impl Url {
        pub fn parse_loopback(input: &str) -> Result<Self, String> {
            let rest = input
                .strip_prefix("http://")
                .ok_or("INTERLOCK_URL must be loopback http")?;
            let authority = rest.split('/').next().unwrap_or(rest);
            if authority.is_empty() {
                return Err("INTERLOCK_URL requires a host".into());
            }
            let (host, port) = match authority.rsplit_once(':') {
                Some((host, port)) => (
                    host.to_string(),
                    port.parse::<u16>().map_err(|_| "invalid port")?,
                ),
                None => (authority.to_string(), 8851),
            };
            let bare = host.trim_start_matches('[').trim_end_matches(']');
            let loopback = bare == "localhost"
                || bare
                    .parse::<IpAddr>()
                    .is_ok_and(|address| address.is_loopback());
            if !loopback {
                return Err("INTERLOCK_URL must target a loopback host".into());
            }
            Ok(Self {
                host: bare.to_string(),
                port,
            })
        }

        pub fn socket_addr(&self) -> Result<SocketAddr, String> {
            let ip: IpAddr = if self.host == "localhost" {
                IpAddr::from([127, 0, 0, 1])
            } else {
                self.host.parse().map_err(|_| "invalid loopback host")?
            };
            Ok(SocketAddr::new(ip, self.port))
        }

        pub fn host_header(&self) -> String {
            format!("{}:{}", self.host, self.port)
        }
    }
}
