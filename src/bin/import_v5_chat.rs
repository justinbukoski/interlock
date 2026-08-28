//! One-shot historical importer: v5 chat stores -> Interlock v6.5 archive.
//!
//! Chat-lane migration (docs/RUNBOOK-CHAT-LANE-MIGRATION.md, step 3).
//! Reads CSV dumps produced by scripts/chatlane-dump.sh (never touches the
//! source databases directly), maps rows to ArchiveEventInput, applies the
//! decision-R1 dedupe, and POSTs batches to /v6.5/archive/events.
//!
//! Dedupe design (R1, owner-approved 2026-08-23; provenance-split per
//! review of 5aff572):
//! - The archive's server-side conflict key is source_event_id only, so all
//!   cross-source dedupe happens HERE, before POSTing.
//! - A (session_id -> {sha256(redact(content)) -> provenance}) map is seeded
//!   from the existing NON-TOMBSTONED archive events (hashes CSV), then
//!   updated with every emitted row. Processing order: cowork_chat_history
//!   first (frozen, higher fidelity), then foreman_chat. A row whose
//!   (session, hash) is already present is skipped and classified by the
//!   provenance of the earlier copy (archive / cowork / intra-source).
//!   redact() is this crate's own function, so hashes match what the server
//!   computed for previously ingested events.
//! - foreman_chat rows at/after the boundary (2026-08-14T00:00:00Z) are
//!   skipped: live v6.5 capture covers them.
//! - Known caveat: identical content repeated within one session collapses
//!   to a single event by design; every skip is counted in the manifest.
//!
//! Apply-mode safety (review findings 2 and 3):
//! - Apply first runs a full classification scan without POSTing anything.
//!   If any rows are unimportable (empty, oversize, bad timestamp), apply
//!   halts unless --allow-skips lists each present class. Empty content can
//!   never be imported (the server rejects it), so allowing that skip is an
//!   explicit owner decision, not a default.
//! - Server acks are deserialized into the crate's own contract types and
//!   validated (every pending id acked, counts consistent, statuses only
//!   accepted/already_present) before a batch is considered done.
//!
//! Idempotent: source_event_id is v5chat:{db}:{row_id} and installation_id
//! is a fixed derived UUID, so re-running apply converges (already_present).
//!
//! Usage:
//!   import_v5_chat --mode dry-run|apply \
//!     --cowork-csv F --foreman-csv F --hashes-csv F \
//!     --api-url http://127.0.0.1:18861 --token-file PATH \
//!     --manifest-out PATH [--allow-skips empty,oversize,bad_timestamp]

use chrono::{DateTime, Utc};
use interlock::archive::{ArchiveIngestResponse, IngestStatus};
use interlock::redaction::redact;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::Read;
use uuid::Uuid;

const BOUNDARY: &str = "2026-08-14T00:00:00Z";
const ADAPTER_VERSION: &str = "v5-chat-import/0.2.0"; // FROZEN: request_hash covers this; changing it breaks idempotent re-runs against rows already ingested;
const MAX_CONTENT_BYTES: usize = 256 * 1024;
const BATCH: usize = 500;

fn installation_id() -> Uuid {
    let mut hash = Sha256::new();
    hash.update(b"foreman-v65-installation:v5-chat-import");
    let bytes: [u8; 16] = hash.finalize()[..16].try_into().expect("sha256 >= 16 bytes");
    Uuid::from_bytes(bytes)
}

#[derive(Serialize)]
struct EventOut {
    source_event_id: String,
    installation_id: Uuid,
    #[serde(skip_serializing_if = "Option::is_none")]
    thread_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    turn_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    sequence_number: Option<i64>,
    actor: String,
    event_kind: String,
    content_type: String,
    schema_version: i32,
    content: String,
    source_timestamp: DateTime<Utc>,
    capture_adapter_version: String,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Provenance {
    Archive,
    Cowork,
    Foreman,
}

#[derive(Default, Clone, Serialize)]
struct Tally {
    total_rows: u64,
    emitted: u64,
    dedup_hash_match_archive: u64,
    dedup_hash_match_cowork: u64,
    dedup_hash_match_intra: u64,
    post_boundary_capture_covered: u64,
    invalid_empty: u64,
    oversize: u64,
    bad_timestamp: u64,
    accepted: u64,
    already_present: u64,
}

#[derive(Serialize)]
struct Manifest {
    mode: String,
    boundary: String,
    installation_id: Uuid,
    adapter_version: String,
    inputs: BTreeMap<String, String>,
    seeded_archive_hashes: u64,
    seeded_sessions: u64,
    per_source: BTreeMap<String, Tally>,
    finished_at: DateTime<Utc>,
}

struct Args {
    mode: String,
    cowork_csv: String,
    foreman_csv: String,
    hashes_csv: String,
    api_url: String,
    token_file: Option<String>,
    manifest_out: String,
    allow_skips: HashSet<String>,
}

fn parse_args() -> Args {
    let mut m: HashMap<String, String> = HashMap::new();
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0;
    while i + 1 < argv.len() {
        m.insert(argv[i].clone(), argv[i + 1].clone());
        i += 2;
    }
    let get = |k: &str| -> Option<String> { m.get(k).cloned() };
    Args {
        mode: get("--mode").unwrap_or_else(|| "dry-run".into()),
        cowork_csv: get("--cowork-csv").expect("--cowork-csv required"),
        foreman_csv: get("--foreman-csv").expect("--foreman-csv required"),
        hashes_csv: get("--hashes-csv").expect("--hashes-csv required"),
        api_url: get("--api-url").unwrap_or_else(|| "http://127.0.0.1:18861".into()),
        token_file: get("--token-file"),
        manifest_out: get("--manifest-out").unwrap_or_else(|| "chatlane-manifest.json".into()),
        allow_skips: get("--allow-skips")
            .map(|v| v.split(',').map(|s| s.trim().to_string()).collect())
            .unwrap_or_default(),
    }
}

fn parse_pg_ts(raw: &str) -> Option<DateTime<Utc>> {
    // COPY csv emits e.g. "2026-03-13 18:52:36.65+00" (fraction optional,
    // offset +00 / +00:00). Try common shapes, then RFC3339.
    for fmt in [
        "%Y-%m-%d %H:%M:%S%.f%#z",
        "%Y-%m-%d %H:%M:%S%#z",
        "%Y-%m-%d %H:%M:%S%.f%:z",
        "%Y-%m-%d %H:%M:%S%:z",
    ] {
        if let Ok(dt) = DateTime::parse_from_str(raw, fmt) {
            return Some(dt.with_timezone(&Utc));
        }
    }
    DateTime::parse_from_rfc3339(raw)
        .ok()
        .map(|dt| dt.with_timezone(&Utc))
}

fn actor_for(role: &str) -> &'static str {
    match role.trim().to_ascii_lowercase().as_str() {
        "user" | "human" => "user",
        "assistant" | "agent" | "ai" | "model" => "assistant",
        r if r.starts_with("tool") => "tool",
        _ => "system",
    }
}

fn content_hash(content: &str) -> [u8; 32] {
    let (redacted, _) = redact(content);
    Sha256::digest(redacted.as_bytes()).into()
}

fn file_sha256(path: &str) -> String {
    let mut file = std::fs::File::open(path).unwrap_or_else(|e| panic!("open {path}: {e}"));
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; 1 << 20];
    loop {
        let n = file.read(&mut buf).expect("read");
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    hex::encode(hasher.finalize())
}

type DedupMap = HashMap<String, HashMap<[u8; 32], Provenance>>;

fn seed_hashes(path: &str) -> (DedupMap, u64) {
    let mut map: DedupMap = HashMap::new();
    let mut count = 0u64;
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(false)
        .from_path(path)
        .unwrap_or_else(|e| panic!("open {path}: {e}"));
    for rec in rdr.records() {
        let rec = rec.expect("csv record");
        let session = rec.get(0).expect("session col").to_string();
        let hash_hex = rec.get(1).expect("hash col");
        let bytes = hex::decode(hash_hex).expect("hash must be hex");
        let hash: [u8; 32] = bytes.try_into().expect("hash must be 32 bytes");
        map.entry(session).or_default().insert(hash, Provenance::Archive);
        count += 1;
    }
    (map, count)
}

struct Sink {
    post: bool,
    api_url: String,
    token: Option<String>,
    client: reqwest::blocking::Client,
    pending: Vec<EventOut>,
    accepted: u64,
    already_present: u64,
    posted_batches: u64,
}

impl Sink {
    fn push(&mut self, event: EventOut) {
        self.pending.push(event);
        if self.pending.len() >= BATCH {
            self.flush();
        }
    }

    fn flush(&mut self) {
        if self.pending.is_empty() {
            return;
        }
        if !self.post {
            self.accepted += self.pending.len() as u64;
            self.pending.clear();
            return;
        }
        let body = serde_json::json!({ "events": self.pending });
        let token = self.token.as_deref().expect("apply mode requires token");
        // Transport errors and 5xx are retried with backoff: the batch is
        // idempotent server-side (source_event_id conflict key), so a resend
        // after an ambiguous failure converges to already_present. 4xx are
        // permanent contract errors and halt immediately.
        let mut attempt = 0u32;
        let resp = loop {
            attempt += 1;
            let result = self
                .client
                .post(format!("{}/v6.5/archive/events", self.api_url))
                .bearer_auth(token)
                .json(&body)
                .send();
            match result {
                Ok(resp) if resp.status().is_success() => break resp,
                Ok(resp) if resp.status().is_client_error() => {
                    let status = resp.status();
                    let text = resp.text().unwrap_or_default();
                    panic!("HALT: archive ingest {status}: {text}");
                }
                Ok(resp) => {
                    if attempt >= 8 {
                        panic!("HALT: archive ingest failed after {attempt} attempts: {}", resp.status());
                    }
                    eprintln!("retryable status {} (attempt {attempt}), backing off", resp.status());
                }
                Err(err) => {
                    if attempt >= 8 {
                        panic!("HALT: archive ingest transport failure after {attempt} attempts: {err}");
                    }
                    eprintln!("transport error (attempt {attempt}): {err}; backing off");
                }
            }
            std::thread::sleep(std::time::Duration::from_secs(2u64.pow(attempt.min(5))));
        };
        let ack: ArchiveIngestResponse = resp.json().expect("ack must match contract type");

        // Validate the ack against what we sent (review finding 3): every
        // pending id acked exactly once, counts consistent, and no status
        // other than accepted/already_present.
        if ack.acks.len() != self.pending.len() {
            panic!(
                "HALT: sent {} events, got {} acks",
                self.pending.len(),
                ack.acks.len()
            );
        }
        if ack.accepted + ack.already_present + ack.rejected != self.pending.len() {
            panic!(
                "HALT: ack counts inconsistent: accepted={} already_present={} rejected={} sent={}",
                ack.accepted,
                ack.already_present,
                ack.rejected,
                self.pending.len()
            );
        }
        let sent_ids: HashSet<&str> = self
            .pending
            .iter()
            .map(|event| event.source_event_id.as_str())
            .collect();
        let mut acked_ids: HashSet<&str> = HashSet::with_capacity(ack.acks.len());
        let mut bad: Vec<String> = Vec::new();
        let (mut seen_accepted, mut seen_present, mut seen_bad) = (0usize, 0usize, 0usize);
        for item in &ack.acks {
            if !sent_ids.contains(item.source_event_id.as_str())
                || !acked_ids.insert(item.source_event_id.as_str())
            {
                panic!(
                    "HALT: ack for unknown or duplicate source_event_id {}",
                    item.source_event_id
                );
            }
            match item.status {
                IngestStatus::Accepted => seen_accepted += 1,
                IngestStatus::AlreadyPresent => seen_present += 1,
                IngestStatus::Rejected | IngestStatus::Quarantined => {
                    seen_bad += 1;
                    if bad.len() < 5 {
                        bad.push(format!(
                            "{} status={:?} reason={:?}",
                            item.source_event_id, item.status, item.reason
                        ));
                    }
                }
            }
        }
        if !bad.is_empty() {
            panic!("HALT: rejected/quarantined events in batch; first: {bad:?}");
        }
        if seen_accepted != ack.accepted
            || seen_present != ack.already_present
            || seen_bad != ack.rejected
        {
            panic!(
                "HALT: per-ack statuses disagree with aggregate counts: statuses \
                 accepted={seen_accepted} already_present={seen_present} bad={seen_bad} vs \
                 aggregates accepted={} already_present={} rejected={}",
                ack.accepted, ack.already_present, ack.rejected
            );
        }
        self.accepted += ack.accepted as u64;
        self.already_present += ack.already_present as u64;
        self.pending.clear();
        self.posted_batches += 1;
        if self.posted_batches.is_multiple_of(100) {
            eprintln!(
                "progress: {} batches, accepted={}, already_present={}",
                self.posted_batches, self.accepted, self.already_present
            );
        }
    }
}

struct Row {
    id: String,
    session: String,
    thread: String,
    turn: Option<String>,
    seq: Option<i64>,
    role: String,
    content_type: String,
    ts_raw: String,
    content: String,
}

fn parse_row(label: &str, rec: &csv::StringRecord) -> Row {
    if label == "cowork_chat_history" {
        // id,session_id,msg_uuid,parent_tool_use_id,ts,role,content
        let id = rec.get(0).expect("id");
        let session = rec.get(1).expect("session_id");
        let msg_uuid = rec.get(2).unwrap_or("");
        let parent = rec.get(3).unwrap_or("");
        Row {
            id: id.to_string(),
            session: session.to_string(),
            thread: if parent.is_empty() {
                session.to_string()
            } else {
                format!("v5:{parent}")
            },
            turn: if msg_uuid.is_empty() {
                None
            } else {
                Some(msg_uuid.to_string())
            },
            seq: id.parse::<i64>().ok(),
            role: rec.get(5).expect("role").to_string(),
            content_type: "text/markdown".to_string(),
            ts_raw: rec.get(4).expect("ts").to_string(),
            content: rec.get(6).expect("content").to_string(),
        }
    } else {
        // id,session_id,agent_id,role,content,content_type,ts
        let id = rec.get(0).expect("id");
        let session = rec.get(1).expect("session_id");
        let agent = rec.get(2).unwrap_or("");
        let ctype = rec.get(5).unwrap_or("");
        Row {
            id: id.to_string(),
            session: session.to_string(),
            thread: if agent.is_empty() {
                session.to_string()
            } else {
                format!("v5:{agent}")
            },
            turn: None,
            seq: id.parse::<i64>().ok(),
            role: rec.get(3).expect("role").to_string(),
            content_type: if ctype.trim().is_empty() || ctype.len() > 128 {
                "text/markdown".to_string()
            } else {
                ctype.to_string()
            },
            ts_raw: rec.get(6).expect("ts").to_string(),
            content: rec.get(4).expect("content").to_string(),
        }
    }
}

/// One full pass over a source. When `sink` is None this is a pure
/// classification scan (no events are built or POSTed).
#[allow(clippy::too_many_arguments)] // import plumbing; bundling these into a struct is churn without benefit
fn process_source(
    label: &str,
    path: &str,
    dedup: &mut DedupMap,
    mut sink: Option<&mut Sink>,
    boundary: DateTime<Utc>,
    enforce_boundary: bool,
    installation: Uuid,
    quiet: bool,
) -> Tally {
    let own_provenance = if label == "cowork_chat_history" {
        Provenance::Cowork
    } else {
        Provenance::Foreman
    };
    let mut tally = Tally::default();
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(false)
        .flexible(false)
        .from_path(path)
        .unwrap_or_else(|e| panic!("open {path}: {e}"));
    for rec in rdr.records() {
        let rec = rec.expect("csv record");
        tally.total_rows += 1;
        let row = parse_row(label, &rec);

        if row.content.is_empty() {
            tally.invalid_empty += 1;
            continue;
        }
        if row.content.len() > MAX_CONTENT_BYTES {
            tally.oversize += 1;
            if !quiet {
                eprintln!(
                    "oversize skip: {label} id={} bytes={}",
                    row.id,
                    row.content.len()
                );
            }
            continue;
        }
        let Some(ts) = parse_pg_ts(&row.ts_raw) else {
            tally.bad_timestamp += 1;
            if !quiet {
                eprintln!("bad timestamp skip: {label} id={} raw={:?}", row.id, row.ts_raw);
            }
            continue;
        };
        if enforce_boundary && ts >= boundary {
            tally.post_boundary_capture_covered += 1;
            continue;
        }
        let hash = content_hash(&row.content);
        let set = dedup.entry(row.session.clone()).or_default();
        if let Some(prior) = set.get(&hash) {
            match prior {
                Provenance::Archive => tally.dedup_hash_match_archive += 1,
                Provenance::Cowork if own_provenance == Provenance::Cowork => {
                    tally.dedup_hash_match_intra += 1
                }
                Provenance::Cowork => tally.dedup_hash_match_cowork += 1,
                Provenance::Foreman => tally.dedup_hash_match_intra += 1,
            }
            continue;
        }
        set.insert(hash, own_provenance);
        tally.emitted += 1;
        if let Some(sink) = sink.as_deref_mut() {
            sink.push(EventOut {
                source_event_id: format!("v5chat:{label}:{}", row.id),
                installation_id: installation,
                thread_id: Some(row.thread),
                session_id: Some(row.session),
                turn_id: row.turn,
                sequence_number: row.seq,
                actor: actor_for(&row.role).to_string(),
                event_kind: "message".to_string(),
                content_type: row.content_type,
                schema_version: 1,
                content: row.content,
                source_timestamp: ts,
                capture_adapter_version: ADAPTER_VERSION.to_string(),
            });
        }
    }
    if let Some(sink) = sink {
        sink.flush();
    }
    tally
}

fn run_passes(
    args: &Args,
    seeded: &DedupMap,
    sink: Option<&mut Sink>,
    boundary: DateTime<Utc>,
    installation: Uuid,
    quiet: bool,
) -> BTreeMap<String, Tally> {
    let mut dedup = seeded.clone();
    let mut per_source = BTreeMap::new();
    match sink {
        Some(sink) => {
            let before = (sink.accepted, sink.already_present);
            let mut tally = process_source(
                "cowork_chat_history",
                &args.cowork_csv,
                &mut dedup,
                Some(sink),
                boundary,
                false,
                installation,
                quiet,
            );
            tally.accepted = sink.accepted - before.0;
            tally.already_present = sink.already_present - before.1;
            per_source.insert("cowork_chat_history".to_string(), tally);
            let before = (sink.accepted, sink.already_present);
            let mut tally = process_source(
                "foreman_chat",
                &args.foreman_csv,
                &mut dedup,
                Some(sink),
                boundary,
                true,
                installation,
                quiet,
            );
            tally.accepted = sink.accepted - before.0;
            tally.already_present = sink.already_present - before.1;
            per_source.insert("foreman_chat".to_string(), tally);
        }
        None => {
            let tally = process_source(
                "cowork_chat_history",
                &args.cowork_csv,
                &mut dedup,
                None,
                boundary,
                false,
                installation,
                quiet,
            );
            per_source.insert("cowork_chat_history".to_string(), tally);
            let tally = process_source(
                "foreman_chat",
                &args.foreman_csv,
                &mut dedup,
                None,
                boundary,
                true,
                installation,
                quiet,
            );
            per_source.insert("foreman_chat".to_string(), tally);
        }
    }
    per_source
}

fn main() {
    let args = parse_args();
    assert!(
        args.mode == "dry-run" || args.mode == "apply",
        "--mode must be dry-run or apply"
    );
    let boundary: DateTime<Utc> = BOUNDARY.parse().expect("static boundary parses");
    let installation = installation_id();
    eprintln!(
        "mode={} boundary={} installation_id={}",
        args.mode, BOUNDARY, installation
    );

    let mut inputs = BTreeMap::new();
    for (name, path) in [
        ("cowork_csv", &args.cowork_csv),
        ("foreman_csv", &args.foreman_csv),
        ("hashes_csv", &args.hashes_csv),
    ] {
        eprintln!("hashing input {path} ...");
        inputs.insert(name.to_string(), format!("{path} sha256={}", file_sha256(path)));
    }

    let token = args.token_file.as_ref().map(|p| {
        std::fs::read_to_string(p)
            .unwrap_or_else(|e| panic!("token file {p}: {e}"))
            .trim()
            .to_string()
    });
    if args.mode == "apply" && token.is_none() {
        panic!("apply mode requires --token-file");
    }

    let (seeded, seeded_count) = seed_hashes(&args.hashes_csv);
    let seeded_sessions = seeded.len() as u64;
    eprintln!("seeded {seeded_count} archive hashes across {seeded_sessions} sessions");

    // Classification scan. In apply mode this is the pre-write gate (review
    // finding 2): unimportable rows halt unless each class present was
    // explicitly allowed via --allow-skips.
    let scan = run_passes(&args, &seeded, None, boundary, installation, args.mode == "apply");
    if args.mode == "apply" {
        let mut blocking: Vec<String> = Vec::new();
        for (source, tally) in &scan {
            for (class, count) in [
                ("empty", tally.invalid_empty),
                ("oversize", tally.oversize),
                ("bad_timestamp", tally.bad_timestamp),
            ] {
                if count > 0 && !args.allow_skips.contains(class) {
                    blocking.push(format!("{source}: {class}={count}"));
                }
            }
        }
        if !blocking.is_empty() {
            eprintln!(
                "HALT before any write: unimportable rows present and not allowed: {blocking:?}\n\
                 re-run with --allow-skips listing each class to accept losing them"
            );
            std::process::exit(2);
        }
        let _ = scan;
    }

    let per_source = if args.mode == "apply" {
        let mut sink = Sink {
            post: true,
            api_url: args.api_url.clone(),
            token,
            client: reqwest::blocking::Client::builder()
                .timeout(std::time::Duration::from_secs(120))
                .build()
                .expect("http client"),
            pending: Vec::with_capacity(BATCH),
            accepted: 0,
            already_present: 0,
            posted_batches: 0,
        };
        run_passes(&args, &seeded, Some(&mut sink), boundary, installation, true)
    } else {
        scan
    };

    let manifest = Manifest {
        mode: args.mode.clone(),
        boundary: BOUNDARY.to_string(),
        installation_id: installation,
        adapter_version: ADAPTER_VERSION.to_string(),
        inputs,
        seeded_archive_hashes: seeded_count,
        seeded_sessions,
        per_source,
        finished_at: Utc::now(),
    };
    let json = serde_json::to_string_pretty(&manifest).expect("manifest json");
    std::fs::write(&args.manifest_out, &json).expect("write manifest");
    println!("{json}");
    eprintln!("manifest written to {}", args.manifest_out);
}
