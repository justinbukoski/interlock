//! Durable local capture spool for Interlock 6.5 conversation capture adapters.
//!
//! The spool is the adapter-side half of the 6.5 continuity guarantee: an
//! adapter appends a captured conversation event here and only acknowledges the
//! host capture after the record and its ordering metadata are flushed durably.
//! Sudden process termination or power loss therefore cannot lose an event that
//! received a capture acknowledgement.
//!
//! Design properties, mirroring `the 6.5 design document` §6.1 and the adversarial
//! review's spool blockers:
//!
//! * **Flush before acknowledgement.** [`Spool::append`] fsyncs the record and
//!   the updated tail before returning. The returned sequence number is only
//!   produced once the bytes are durable.
//! * **Bounded capacity, never age-evicted.** Enqueue is rejected with
//!   [`SpoolError::Full`] once the configured byte or record ceiling is reached.
//!   The spool never silently discards an event to make room; exhaustion is a
//!   fail-closed signal for the adapter.
//! * **Crash recovery.** Every record carries a length prefix and CRC32. On open
//!   the log is scanned and any torn tail from an interrupted write is truncated,
//!   because a torn record was, by construction, never acknowledged.
//! * **In-order delivery and durable cursor.** Records are delivered in append
//!   order. [`Spool::ack`] advances a durable consumed-cursor so a crash mid
//!   drain cannot replay already-delivered events out of order or lose the
//!   position.
//!
//! The on-disk format is deliberately small and dependency-free (a
//! "SQLite-equivalent durable queue"): a header, then a sequence of framed
//! records, then a durable trailer recording how many leading records have been
//! consumed.

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

/// Magic identifying a Interlock 6.5 spool file. Bumping the version byte is a
/// forward-incompatible format change.
const MAGIC: &[u8; 8] = b"F65SPOOL";
const FORMAT_VERSION: u8 = 1;
const HEADER_LEN: u64 = 16;
/// Frame layout: `u32` payload length, `u32` payload CRC32, payload bytes.
const FRAME_PREFIX_LEN: usize = 8;
/// Hard ceiling on a single record so a corrupt length prefix cannot request an
/// unbounded allocation during recovery.
const MAX_RECORD_BYTES: u32 = 8 * 1024 * 1024;

/// Bounds for a spool. Neither bound is an age; the spool never evicts by age.
#[derive(Debug, Clone, Copy)]
pub struct SpoolCapacity {
    pub max_records: u64,
    pub max_bytes: u64,
}

impl SpoolCapacity {
    pub fn new(max_records: u64, max_bytes: u64) -> Result<Self, SpoolError> {
        if max_records == 0 || max_bytes == 0 {
            return Err(SpoolError::Config("spool capacity must be positive"));
        }
        Ok(Self {
            max_records,
            max_bytes,
        })
    }
}

impl Default for SpoolCapacity {
    fn default() -> Self {
        Self {
            max_records: 100_000,
            max_bytes: 256 * 1024 * 1024,
        }
    }
}

#[derive(Debug)]
pub enum SpoolError {
    /// The configured capacity is exhausted. The adapter must go fail-closed;
    /// the event was NOT enqueued and must not be acknowledged to the host.
    Full {
        max_records: u64,
        max_bytes: u64,
    },
    /// The payload is empty or exceeds the per-record ceiling.
    InvalidPayload(&'static str),
    Config(&'static str),
    Io(io::Error),
    Corrupt(&'static str),
}

impl std::fmt::Display for SpoolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Full {
                max_records,
                max_bytes,
            } => write!(
                f,
                "spool full (limit {max_records} records / {max_bytes} bytes); capture must fail closed"
            ),
            Self::InvalidPayload(reason) => write!(f, "invalid spool payload: {reason}"),
            Self::Config(reason) => write!(f, "invalid spool configuration: {reason}"),
            Self::Io(error) => write!(f, "spool io error: {error}"),
            Self::Corrupt(reason) => write!(f, "spool corruption: {reason}"),
        }
    }
}

impl std::error::Error for SpoolError {}

impl From<io::Error> for SpoolError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

/// A record awaiting delivery to the archive.
#[derive(Debug, Clone)]
pub struct SpooledRecord {
    /// Monotonic sequence number, stable across restarts.
    pub sequence: u64,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, Copy)]
struct Frame {
    sequence: u64,
    /// Byte offset of the frame prefix within the file.
    offset: u64,
    payload_len: u32,
}

/// Observable spool state for health reporting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpoolHealth {
    pub pending_records: u64,
    pub pending_bytes: u64,
    pub max_records: u64,
    pub max_bytes: u64,
    /// The next sequence number that will be assigned.
    pub next_sequence: u64,
}

impl SpoolHealth {
    pub fn is_empty(&self) -> bool {
        self.pending_records == 0
    }
    pub fn is_full(&self) -> bool {
        self.pending_records >= self.max_records || self.pending_bytes >= self.max_bytes
    }
}

/// A durable, bounded, crash-recoverable capture spool.
pub struct Spool {
    path: PathBuf,
    file: File,
    capacity: SpoolCapacity,
    /// All frames currently present in the file, in append order.
    frames: Vec<Frame>,
    /// Index into `frames` of the first undelivered record. Frames before this
    /// have been acknowledged as delivered and are eligible for compaction.
    head: usize,
    next_sequence: u64,
    pending_bytes: u64,
    /// The consumed cursor persisted in the trailer, so a crash cannot lose it.
    /// It holds the sequence one past the last delivered record.
    consumed_through: u64,
    /// The sequence of the first frame this file was (re)written with. Consumed
    /// sentinels store a bounded delta from this base so a 32-bit marker cannot
    /// overflow at large absolute sequence numbers.
    file_base: u64,
    /// End offset of the last durable frame. Writes always land here — never at
    /// `SeekFrom::End` — so garbage left by a failed write can never end up in
    /// front of a later successful frame, where recovery's torn-tail truncation
    /// would silently discard the acknowledged later frame.
    logical_end: u64,
}

impl Spool {
    /// Open or create a spool at `path`, recovering any prior state and
    /// truncating a torn tail from an interrupted write.
    pub fn open(path: impl AsRef<Path>, capacity: SpoolCapacity) -> Result<Self, SpoolError> {
        let path = path.as_ref().to_path_buf();
        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&path)?;
        let len = file.metadata()?.len();
        if len == 0 {
            write_header(&mut file)?;
            file.sync_all()?;
            // A freshly created file is durable only once its directory entry
            // is — without this, power loss can drop the whole spool file even
            // though every append inside it was fsynced.
            sync_parent_dir(&path)?;
            return Ok(Self {
                path,
                file,
                capacity,
                frames: Vec::new(),
                head: 0,
                next_sequence: 0,
                pending_bytes: 0,
                consumed_through: 0,
                file_base: 0,
                logical_end: HEADER_LEN,
            });
        }
        Self::recover(path, file, capacity, len)
    }

    fn recover(
        path: PathBuf,
        mut file: File,
        capacity: SpoolCapacity,
        len: u64,
    ) -> Result<Self, SpoolError> {
        let mut header = [0u8; HEADER_LEN as usize];
        file.seek(SeekFrom::Start(0))?;
        file.read_exact(&mut header)?;
        if &header[0..8] != MAGIC || header[8] != FORMAT_VERSION {
            return Err(SpoolError::Corrupt("unrecognized spool header"));
        }
        // Bytes 9..16 hold the base sequence (the sequence of the first frame in
        // this file after the most recent compaction).
        let base_sequence = read_base_sequence(&header);

        let mut frames = Vec::new();
        let mut offset = HEADER_LEN;
        let mut sequence = base_sequence;
        let mut consumed_through = base_sequence;
        let mut truncate_to = HEADER_LEN;
        let mut buf = Vec::new();
        loop {
            if offset + FRAME_PREFIX_LEN as u64 > len {
                break;
            }
            let mut prefix = [0u8; FRAME_PREFIX_LEN];
            file.seek(SeekFrom::Start(offset))?;
            if file.read_exact(&mut prefix).is_err() {
                break;
            }
            let payload_len = u32::from_le_bytes(prefix[0..4].try_into().unwrap());
            let expected_crc = u32::from_le_bytes(prefix[4..8].try_into().unwrap());
            // A consumed-cursor sentinel frame has payload_len == 0 and encodes
            // the consumed sequence in its CRC slot.
            if payload_len == 0 {
                // Sentinel: CRC slot holds a bounded delta from base_sequence.
                consumed_through = base_sequence + expected_crc as u64;
                offset += FRAME_PREFIX_LEN as u64;
                truncate_to = offset;
                continue;
            }
            let frame_end = offset + FRAME_PREFIX_LEN as u64 + payload_len as u64;
            if payload_len > MAX_RECORD_BYTES {
                if frame_end > len {
                    // Garbage length at the physical tail: an interrupted
                    // append. Truncate.
                    break;
                }
                // An impossible length with more file after it is corruption,
                // not a torn write. Truncating here would silently discard
                // every durable, possibly acknowledged frame that follows —
                // fail closed and leave the file for operator recovery.
                return Err(SpoolError::Corrupt(
                    "corrupt frame length before end of spool; refusing to truncate durable records",
                ));
            }
            if frame_end > len {
                break; // partial payload from an interrupted append
            }
            buf.resize(payload_len as usize, 0);
            file.seek(SeekFrom::Start(offset + FRAME_PREFIX_LEN as u64))?;
            if file.read_exact(&mut buf).is_err() {
                break;
            }
            if crc32(&buf) != expected_crc {
                if frame_end == len {
                    break; // torn payload at the physical tail; truncate
                }
                // A checksum failure mid-file with durable frames after it is
                // bit rot, not a torn write — fail closed instead of silently
                // discarding everything that follows.
                return Err(SpoolError::Corrupt(
                    "frame checksum failure before end of spool; refusing to truncate durable records",
                ));
            }
            frames.push(Frame {
                sequence,
                offset,
                payload_len,
            });
            sequence += 1;
            offset = frame_end;
            truncate_to = frame_end;
        }
        if truncate_to != len {
            file.set_len(truncate_to)?;
            file.sync_all()?;
        }
        let head = frames
            .iter()
            .position(|frame| frame.sequence >= consumed_through)
            .unwrap_or(frames.len());
        let pending_bytes = frames[head..]
            .iter()
            .map(|frame| frame.payload_len as u64)
            .sum();
        Ok(Self {
            path,
            file,
            capacity,
            frames,
            head,
            next_sequence: sequence,
            pending_bytes,
            consumed_through,
            file_base: base_sequence,
            logical_end: truncate_to,
        })
    }

    /// Append a payload durably. Returns the assigned sequence number only after
    /// the record and updated length are flushed to stable storage.
    pub fn append(&mut self, payload: &[u8]) -> Result<u64, SpoolError> {
        if payload.is_empty() {
            return Err(SpoolError::InvalidPayload("payload must be non-empty"));
        }
        if payload.len() as u32 > MAX_RECORD_BYTES {
            return Err(SpoolError::InvalidPayload("payload exceeds record ceiling"));
        }
        let pending_records = (self.frames.len() - self.head) as u64;
        if pending_records >= self.capacity.max_records
            || self.pending_bytes.saturating_add(payload.len() as u64) > self.capacity.max_bytes
        {
            return Err(SpoolError::Full {
                max_records: self.capacity.max_records,
                max_bytes: self.capacity.max_bytes,
            });
        }
        let offset = self.logical_end;
        let mut prefix = [0u8; FRAME_PREFIX_LEN];
        prefix[0..4].copy_from_slice(&(payload.len() as u32).to_le_bytes());
        prefix[4..8].copy_from_slice(&crc32(payload).to_le_bytes());
        if let Err(error) = self.write_frame_at(offset, &prefix, payload) {
            // Repair the torn tail immediately: truncate back to the last
            // durable frame so a later successful append cannot land after
            // garbage and be discarded by recovery. This event itself fails
            // closed to the caller.
            self.repair_tail();
            return Err(error);
        }
        let sequence = self.next_sequence;
        self.frames.push(Frame {
            sequence,
            offset,
            payload_len: payload.len() as u32,
        });
        self.next_sequence += 1;
        self.pending_bytes += payload.len() as u64;
        self.logical_end = offset + FRAME_PREFIX_LEN as u64 + payload.len() as u64;
        Ok(sequence)
    }

    /// Write prefix+payload at `offset` and fsync; used by append and the
    /// consumed sentinel so both share the torn-tail repair discipline.
    fn write_frame_at(
        &mut self,
        offset: u64,
        prefix: &[u8; FRAME_PREFIX_LEN],
        payload: &[u8],
    ) -> Result<(), SpoolError> {
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(prefix)?;
        if !payload.is_empty() {
            self.file.write_all(payload)?;
        }
        // Flush-before-acknowledge: the caller must not treat this event as
        // captured until the bytes are on stable storage.
        self.file.sync_all()?;
        Ok(())
    }

    /// Best-effort truncation back to the last durable frame after a failed
    /// write. If the truncation itself fails the next `open` still recovers via
    /// torn-tail truncation — but only frames before the garbage, which is why
    /// this is attempted eagerly here.
    fn repair_tail(&mut self) {
        let _ = self.file.set_len(self.logical_end);
        let _ = self.file.sync_all();
    }

    /// The undelivered records, oldest first, up to `limit`.
    pub fn pending(&mut self, limit: usize) -> Result<Vec<SpooledRecord>, SpoolError> {
        let mut records = Vec::new();
        for frame in self.frames[self.head..].iter().take(limit) {
            let mut payload = vec![0u8; frame.payload_len as usize];
            self.file
                .seek(SeekFrom::Start(frame.offset + FRAME_PREFIX_LEN as u64))?;
            self.file.read_exact(&mut payload)?;
            if crc32(&payload) != read_frame_crc(&mut self.file, frame.offset)? {
                return Err(SpoolError::Corrupt("frame checksum drift on read"));
            }
            records.push(SpooledRecord {
                sequence: frame.sequence,
                payload,
            });
        }
        Ok(records)
    }

    /// Durably record that every record with sequence `<= through_sequence` has
    /// been delivered to the archive. Delivered records will not be replayed.
    pub fn ack(&mut self, through_sequence: u64) -> Result<(), SpoolError> {
        let new_head = self
            .frames
            .iter()
            .position(|frame| frame.sequence > through_sequence)
            .unwrap_or(self.frames.len());
        if new_head <= self.head {
            return Ok(());
        }
        let consumed_bytes: u64 = self.frames[self.head..new_head]
            .iter()
            .map(|frame| frame.payload_len as u64)
            .sum();
        self.head = new_head;
        self.pending_bytes -= consumed_bytes;
        self.consumed_through = through_sequence + 1;
        if self.head == self.frames.len() {
            // Fully drained: reset to an empty header so the file does not grow
            // without bound during steady-state operation.
            self.rewrite(&[])?;
        } else {
            // Persist the consumed cursor durably. Compact only when the
            // delivered prefix is large, to avoid O(n^2) rewrites while draining
            // in small batches.
            self.write_consumed_sentinel()?;
            if self.head >= 1024 || self.head * 2 >= self.frames.len() {
                let remaining: Vec<Vec<u8>> = self
                    .pending(usize::MAX)?
                    .into_iter()
                    .map(|record| record.payload)
                    .collect();
                self.rewrite(&remaining)?;
            }
        }
        Ok(())
    }

    /// Append a zero-length sentinel frame whose CRC slot carries the consumed
    /// sequence, then fsync. This makes cursor advancement crash-safe without a
    /// full rewrite.
    fn write_consumed_sentinel(&mut self) -> Result<(), SpoolError> {
        let mut prefix = [0u8; FRAME_PREFIX_LEN];
        // payload_len stays 0; encode the bounded consumed-delta in the CRC slot.
        let delta = self.consumed_through.saturating_sub(self.file_base);
        let marker = u32::try_from(delta).unwrap_or(u32::MAX);
        prefix[4..8].copy_from_slice(&marker.to_le_bytes());
        let offset = self.logical_end;
        if let Err(error) = self.write_frame_at(offset, &prefix, &[]) {
            self.repair_tail();
            return Err(error);
        }
        self.logical_end = offset + FRAME_PREFIX_LEN as u64;
        Ok(())
    }

    /// Rewrite the spool file with exactly `payloads`, assigning sequences from
    /// the current consumed cursor. Crash-safe via temp-file + atomic rename.
    fn rewrite(&mut self, payloads: &[Vec<u8>]) -> Result<(), SpoolError> {
        // The base sequence of the rewritten file is the sequence of the first
        // surviving record, or the next sequence when the file is now empty.
        let base_sequence = if payloads.is_empty() {
            self.next_sequence
        } else {
            self.frames[self.head].sequence
        };
        let temp_path = self.path.with_extension("compact");
        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true).truncate(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut temp = options.open(&temp_path)?;
        write_header_with_base(&mut temp, base_sequence)?;
        let mut offset = HEADER_LEN;
        let mut frames = Vec::with_capacity(payloads.len());
        for (index, payload) in payloads.iter().enumerate() {
            let mut prefix = [0u8; FRAME_PREFIX_LEN];
            prefix[0..4].copy_from_slice(&(payload.len() as u32).to_le_bytes());
            prefix[4..8].copy_from_slice(&crc32(payload).to_le_bytes());
            temp.write_all(&prefix)?;
            temp.write_all(payload)?;
            frames.push(Frame {
                sequence: base_sequence + index as u64,
                offset,
                payload_len: payload.len() as u32,
            });
            offset += FRAME_PREFIX_LEN as u64 + payload.len() as u64;
        }
        temp.sync_all()?;
        std::fs::rename(&temp_path, &self.path)?;
        sync_parent_dir(&self.path)?;
        self.file = options
            .read(true)
            .write(true)
            .create(false)
            .truncate(false)
            .open(&self.path)?;
        self.frames = frames;
        self.head = 0;
        self.pending_bytes = payloads.iter().map(|payload| payload.len() as u64).sum();
        self.consumed_through = base_sequence;
        self.file_base = base_sequence;
        self.logical_end = offset;
        Ok(())
    }

    pub fn health(&self) -> SpoolHealth {
        SpoolHealth {
            pending_records: (self.frames.len() - self.head) as u64,
            pending_bytes: self.pending_bytes,
            max_records: self.capacity.max_records,
            max_bytes: self.capacity.max_bytes,
            next_sequence: self.next_sequence,
        }
    }
}

fn write_header(file: &mut File) -> Result<(), SpoolError> {
    write_header_with_base(file, 0)
}

fn write_header_with_base(file: &mut File, base_sequence: u64) -> Result<(), SpoolError> {
    let mut header = [0u8; HEADER_LEN as usize];
    header[0..8].copy_from_slice(MAGIC);
    header[8] = FORMAT_VERSION;
    // bytes 9..16 hold the base sequence (little-endian, 7 bytes).
    let seq_bytes = base_sequence.to_le_bytes();
    header[9..16].copy_from_slice(&seq_bytes[0..7]);
    file.seek(SeekFrom::Start(0))?;
    file.write_all(&header)?;
    Ok(())
}

fn read_base_sequence(header: &[u8; HEADER_LEN as usize]) -> u64 {
    let mut seq_bytes = [0u8; 8];
    seq_bytes[0..7].copy_from_slice(&header[9..16]);
    u64::from_le_bytes(seq_bytes)
}

fn read_frame_crc(file: &mut File, offset: u64) -> Result<u32, SpoolError> {
    let mut prefix = [0u8; FRAME_PREFIX_LEN];
    file.seek(SeekFrom::Start(offset))?;
    file.read_exact(&mut prefix)?;
    Ok(u32::from_le_bytes(prefix[4..8].try_into().unwrap()))
}

#[cfg(unix)]
fn sync_parent_dir(path: &Path) -> Result<(), SpoolError> {
    if let Some(parent) = path.parent() {
        let dir = File::open(if parent.as_os_str().is_empty() {
            Path::new(".")
        } else {
            parent
        })?;
        dir.sync_all()?;
    }
    Ok(())
}

#[cfg(not(unix))]
fn sync_parent_dir(_path: &Path) -> Result<(), SpoolError> {
    Ok(())
}

/// IEEE 802.3 CRC32 (polynomial 0xEDB88320), computed without a table dependency.
fn crc32(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xffff_ffff;
    for &byte in data {
        crc ^= byte as u32;
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0xedb8_8320 & mask);
        }
    }
    !crc
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_path(name: &str) -> PathBuf {
        let mut dir = std::env::temp_dir();
        // Derive a unique-enough suffix without Date/rand (unavailable in tests).
        let unique = format!(
            "interlock65-spool-{}-{}-{}",
            std::process::id(),
            name,
            COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        );
        dir.push(unique);
        dir
    }

    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

    struct Cleanup(PathBuf);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
            let _ = std::fs::remove_file(self.0.with_extension("compact"));
        }
    }

    #[test]
    fn append_assigns_monotonic_sequences_and_preserves_order() {
        let path = temp_path("order");
        let _cleanup = Cleanup(path.clone());
        let mut spool = Spool::open(&path, SpoolCapacity::default()).unwrap();
        assert_eq!(spool.append(b"one").unwrap(), 0);
        assert_eq!(spool.append(b"two").unwrap(), 1);
        assert_eq!(spool.append(b"three").unwrap(), 2);
        let pending = spool.pending(10).unwrap();
        let payloads: Vec<_> = pending.iter().map(|r| r.payload.as_slice()).collect();
        assert_eq!(payloads, vec![b"one".as_slice(), b"two", b"three"]);
        assert_eq!(spool.health().pending_records, 3);
    }

    #[test]
    fn reopen_recovers_all_flushed_records() {
        let path = temp_path("recover");
        let _cleanup = Cleanup(path.clone());
        {
            let mut spool = Spool::open(&path, SpoolCapacity::default()).unwrap();
            spool.append(b"alpha").unwrap();
            spool.append(b"beta").unwrap();
        }
        let mut spool = Spool::open(&path, SpoolCapacity::default()).unwrap();
        let pending = spool.pending(10).unwrap();
        assert_eq!(pending.len(), 2);
        assert_eq!(pending[0].sequence, 0);
        assert_eq!(pending[1].payload, b"beta");
        // A new append continues the sequence, never reusing a number.
        assert_eq!(spool.append(b"gamma").unwrap(), 2);
    }

    #[test]
    fn torn_tail_is_truncated_on_recovery_but_prior_records_survive() {
        let path = temp_path("torn");
        let _cleanup = Cleanup(path.clone());
        {
            let mut spool = Spool::open(&path, SpoolCapacity::default()).unwrap();
            spool.append(b"durable").unwrap();
        }
        // Simulate an interrupted append: a valid frame prefix then a truncated
        // payload, exactly what a crash mid-write leaves behind.
        {
            let mut file = OpenOptions::new().write(true).open(&path).unwrap();
            file.seek(SeekFrom::End(0)).unwrap();
            let mut prefix = [0u8; FRAME_PREFIX_LEN];
            prefix[0..4].copy_from_slice(&(16u32).to_le_bytes());
            prefix[4..8].copy_from_slice(&crc32(&[0u8; 16]).to_le_bytes());
            file.write_all(&prefix).unwrap();
            file.write_all(b"only-part").unwrap(); // fewer than 16 bytes
            file.sync_all().unwrap();
        }
        let mut spool = Spool::open(&path, SpoolCapacity::default()).unwrap();
        let pending = spool.pending(10).unwrap();
        assert_eq!(pending.len(), 1, "torn record must be discarded");
        assert_eq!(pending[0].payload, b"durable");
        // The next append reuses the truncated space and stays consistent.
        assert_eq!(spool.append(b"after-crash").unwrap(), 1);
        let reopened = Spool::open(&path, SpoolCapacity::default())
            .unwrap()
            .pending(10)
            .unwrap();
        assert_eq!(reopened.len(), 2);
    }

    #[test]
    fn capacity_is_bounded_and_never_age_evicts() {
        let path = temp_path("bounded");
        let _cleanup = Cleanup(path.clone());
        let mut spool = Spool::open(&path, SpoolCapacity::new(3, 1_000_000).unwrap()).unwrap();
        spool.append(b"1").unwrap();
        spool.append(b"2").unwrap();
        spool.append(b"3").unwrap();
        let full = spool.append(b"4").unwrap_err();
        assert!(matches!(full, SpoolError::Full { .. }));
        // The oldest record is NOT evicted to make room; all three remain.
        assert_eq!(spool.pending(10).unwrap().len(), 3);
        assert_eq!(spool.pending(10).unwrap()[0].payload, b"1");
    }

    #[test]
    fn byte_capacity_is_enforced() {
        let path = temp_path("bytes");
        let _cleanup = Cleanup(path.clone());
        let mut spool = Spool::open(&path, SpoolCapacity::new(1000, 8).unwrap()).unwrap();
        spool.append(b"1234").unwrap();
        spool.append(b"5678").unwrap();
        assert!(matches!(
            spool.append(b"9").unwrap_err(),
            SpoolError::Full { .. }
        ));
    }

    #[test]
    fn ack_drains_and_frees_capacity() {
        let path = temp_path("ack");
        let _cleanup = Cleanup(path.clone());
        let mut spool = Spool::open(&path, SpoolCapacity::new(3, 1_000_000).unwrap()).unwrap();
        let a = spool.append(b"a").unwrap();
        let b = spool.append(b"b").unwrap();
        spool.append(b"c").unwrap();
        assert!(matches!(
            spool.append(b"d").unwrap_err(),
            SpoolError::Full { .. }
        ));
        spool.ack(a).unwrap();
        assert_eq!(spool.health().pending_records, 2);
        // Freed one slot, so one more append fits.
        spool.append(b"d").unwrap();
        spool.ack(b).unwrap();
        let remaining = spool.pending(10).unwrap();
        assert_eq!(remaining.len(), 2);
        assert_eq!(remaining[0].payload, b"c");
    }

    #[test]
    fn ack_cursor_survives_restart() {
        let path = temp_path("ack-restart");
        let _cleanup = Cleanup(path.clone());
        let (a, _b) = {
            let mut spool = Spool::open(&path, SpoolCapacity::default()).unwrap();
            let a = spool.append(b"first").unwrap();
            let b = spool.append(b"second").unwrap();
            spool.append(b"third").unwrap();
            spool.ack(a).unwrap();
            (a, b)
        };
        let mut spool = Spool::open(&path, SpoolCapacity::default()).unwrap();
        let pending = spool.pending(10).unwrap();
        assert_eq!(pending.len(), 2, "acked record must not reappear");
        assert_eq!(pending[0].payload, b"second");
        assert_eq!(pending[0].sequence, a + 1);
    }

    #[test]
    fn full_drain_resets_file_and_keeps_sequence() {
        let path = temp_path("drain");
        let _cleanup = Cleanup(path.clone());
        let mut spool = Spool::open(&path, SpoolCapacity::default()).unwrap();
        spool.append(b"x").unwrap();
        let last = spool.append(b"y").unwrap();
        spool.ack(last).unwrap();
        assert!(spool.health().is_empty());
        // Sequence numbers continue past the drained range after restart.
        let next = spool.append(b"z").unwrap();
        assert_eq!(next, last + 1);
        let reopened = Spool::open(&path, SpoolCapacity::default()).unwrap();
        assert_eq!(reopened.health().pending_records, 1);
        assert_eq!(reopened.health().next_sequence, last + 2);
    }
}
