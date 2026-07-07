//! Binary WAL file format.
//!
//! ```text
//! Header (16 bytes): magic b"EWAL" | version u8(2) | reserved [u8;3] | snapshot_seq u64 LE
//! Entry: len u32 LE | payload [u8;len] | xxh3 u64 LE
//! ```

use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;

use super::op::Op;
use crate::error::{Error, Result};

const MAGIC: &[u8; 4] = b"EWAL";
/// Current WAL format version written for new files. Legacy v3 remains
/// readable during the one-time upgrade path.
const VERSION: u8 = 4;
/// Lowest WAL version this build can still read. v3 held postcard values;
/// v4 holds per-value-versioned msgpack values.
const MIN_READABLE_VERSION: u8 = 3;
const HEADER_SIZE: u64 = 16;

/// WAL file writer — wraps a BufWriter for appends, raw File for reads.
pub struct WalFile {
    writer: BufWriter<File>,
    /// Current write offset (end of last written entry).
    offset: u64,
}

impl WalFile {
    /// Open or create a WAL file. Writes header if new.
    pub fn open(path: &Path) -> Result<Self> {
        let exists = path.exists() && std::fs::metadata(path)?.len() >= HEADER_SIZE;

        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)?;

        if !exists {
            let mut writer = BufWriter::new(file);
            write_header(&mut writer, 0)?;
            writer.flush()?;
            let offset = HEADER_SIZE;
            Ok(Self { writer, offset })
        } else {
            // Validate header.
            let mut reader = io::BufReader::new(&file);
            validate_header(&mut reader)?;

            let end = file.metadata()?.len();
            let mut writer = BufWriter::new(file);
            writer.seek(SeekFrom::End(0))?;

            Ok(Self {
                writer,
                offset: end,
            })
        }
    }

    /// Append a batch of ops as a single WAL entry. Does NOT fsync.
    pub fn append(&mut self, ops: &[Op]) -> Result<()> {
        if ops.is_empty() {
            return Ok(());
        }
        let payload = postcard::to_allocvec(ops)?;
        let len = payload.len() as u32;
        let hash = xxhash_rust::xxh3::xxh3_64(&payload);

        self.writer.write_all(&len.to_le_bytes())?;
        self.writer.write_all(&payload)?;
        #[cfg(test)]
        if crash_armed("wal_torn_entry") {
            // Model power loss mid-append: flush the buffered len+payload to
            // disk WITHOUT the trailing hash, then abort. The result is a torn
            // entry the next boot must reject (hash region missing), never
            // replay as valid. Compiles to nothing outside test builds.
            let _ = self.writer.flush();
            let _ = self.writer.get_ref().sync_all();
            std::process::abort();
        }
        self.writer.write_all(&hash.to_le_bytes())?;
        self.offset += 4 + payload.len() as u64 + 8;
        Ok(())
    }

    /// Flush the BufWriter and fsync the underlying file.
    pub fn sync(&mut self) -> Result<()> {
        self.writer.flush()?;
        self.writer.get_ref().sync_all()?;
        // Durability boundary: the bytes are fsync'd. A crash here (after the
        // OS has the data, before the caller's write returns) must still
        // recover this write on the next boot.
        maybe_crash("post_wal_sync");
        Ok(())
    }

    /// Iterate all entries from the start of the WAL.
    /// Stops at first corruption or EOF. Returns entries and the valid offset.
    pub fn iter_entries(path: &Path) -> Result<(Vec<Vec<Op>>, u64)> {
        let mut file = File::open(path)?;
        let file_len = file.metadata()?.len();

        if file_len < HEADER_SIZE {
            return Err(Error::WalCorrupted {
                offset: 0,
                reason: "file too short for header".into(),
            });
        }

        let mut reader = io::BufReader::new(&mut file);
        validate_header(&mut reader)?;

        let mut entries = Vec::new();
        let mut pos = HEADER_SIZE;

        loop {
            if pos >= file_len {
                break;
            }

            // Need at least 4 bytes for len.
            if pos + 4 > file_len {
                break; // Partial write — truncate here.
            }

            let mut len_buf = [0u8; 4];
            if reader.read_exact(&mut len_buf).is_err() {
                break;
            }
            let len = u32::from_le_bytes(len_buf) as u64;

            // Need len + 8 bytes for payload + hash.
            if pos + 4 + len + 8 > file_len {
                break; // Partial write.
            }

            let mut payload = vec![0u8; len as usize];
            if reader.read_exact(&mut payload).is_err() {
                break;
            }

            let mut hash_buf = [0u8; 8];
            if reader.read_exact(&mut hash_buf).is_err() {
                break;
            }
            let stored_hash = u64::from_le_bytes(hash_buf);
            let computed_hash = xxhash_rust::xxh3::xxh3_64(&payload);

            if stored_hash != computed_hash {
                // CRC mismatch — corruption at this entry.
                break;
            }

            match postcard::from_bytes::<Vec<Op>>(&payload) {
                Ok(ops) => {
                    entries.push(ops);
                    pos += 4 + len + 8;
                }
                Err(_) => break,
            }
        }

        Ok((entries, pos))
    }

    /// Truncate the WAL file at the given offset (for corruption recovery).
    ///
    /// Operates through an independent handle. Use this only when no live
    /// [`WalFile`] handle is open on `path`; if one is, use
    /// [`WalFile::truncate_to`] instead so the writer is repositioned.
    /// Now only used by tests — production recovery goes through
    /// [`WalFile::truncate_to`] on the live handle.
    #[cfg(test)]
    pub fn truncate_at(path: &Path, offset: u64) -> Result<()> {
        let file = OpenOptions::new().write(true).open(path)?;
        file.set_len(offset)?;
        file.sync_all()?;
        Ok(())
    }

    /// Truncate this open WAL to `offset` and reposition the writer so the
    /// next [`append`](Self::append) lands exactly at the new end.
    ///
    /// Unlike the static [`truncate_at`](Self::truncate_at), this repositions
    /// the live `BufWriter`. `truncate_at` shrinks the file through a separate
    /// handle and leaves this writer seeked to the old (larger) end — the next
    /// append would then write past a zero-filled sparse hole, which the next
    /// boot's hash check reads as corruption and truncates, silently dropping
    /// the acknowledged write.
    pub fn truncate_to(&mut self, offset: u64) -> Result<()> {
        self.writer.flush()?;
        self.writer.get_ref().set_len(offset)?;
        self.writer.get_ref().sync_all()?;
        self.writer.seek(SeekFrom::Start(offset))?;
        self.offset = offset;
        Ok(())
    }

    /// Reset the WAL (write fresh header, truncate everything after).
    pub fn reset(&mut self) -> Result<()> {
        self.writer.seek(SeekFrom::Start(0))?;
        write_header(&mut self.writer, 0)?;
        self.writer.flush()?;
        self.writer.get_ref().set_len(HEADER_SIZE)?;
        self.writer.get_ref().sync_all()?;
        self.offset = HEADER_SIZE;
        Ok(())
    }

    /// Copy current WAL contents to `backup_path`, then reset self to empty.
    ///
    /// Used at compaction: preserves a recoverable copy of the pre-compaction
    /// WAL (as `wal.prev`) before the new snapshot supersedes it. If the
    /// snapshot write crashes or the new snapshot turns out to be unreadable,
    /// the backup still holds every op that was compacted.
    ///
    /// The backup file is created atomically (overwriting any existing
    /// backup from a prior compaction), fsync'd, and its parent directory
    /// entry is made durable BEFORE the live WAL is reset — so a crash in the
    /// compaction window can never leave `wal.prev` without a directory entry.
    pub fn rotate_to_backup(&mut self, wal_path: &Path, backup_path: &Path) -> Result<()> {
        // Flush and sync the current WAL so every op we've seen in memory
        // is on disk before we copy it.
        self.writer.flush()?;
        self.writer.get_ref().sync_all()?;

        // Copy via std::io::copy — no rename/handle games, just bytes.
        // O(WAL size) which is bounded by the snapshot threshold.
        {
            let mut src = File::open(wal_path)?;
            let mut dst = OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(backup_path)?;
            std::io::copy(&mut src, &mut dst)?;
            dst.sync_all()?;
        }

        // Make wal.prev's directory entry durable BEFORE truncating the live
        // WAL. A crash after reset() but before the caller's post-rename dir
        // fsync must still find wal.prev on disk; otherwise the old snapshot is
        // superseded by a reset WAL and every post-snapshot op is lost. This
        // closes the compaction crash window.
        if let Some(parent) = backup_path.parent() {
            fsync_dir(parent)?;
        }

        maybe_crash("post_wal_prev_dir_fsync");

        // Reset the live WAL (header + truncate) so new writes start fresh.
        self.reset()?;
        Ok(())
    }

    /// Current number of bytes written (including header).
    #[cfg(test)]
    pub fn offset(&self) -> u64 {
        self.offset
    }

    /// Read just the header-version byte from a WAL file without opening
    /// it for writes. Used during load to decide between legacy and
    /// versioned replay formats.
    pub fn version_of(path: &Path) -> Result<u8> {
        let mut f = File::open(path)?;
        let mut header = [0u8; HEADER_SIZE as usize];
        use std::io::Read;
        f.read_exact(&mut header)?;
        if &header[..4] != MAGIC {
            return Err(Error::WalCorrupted {
                offset: 0,
                reason: format!("bad magic: got {:?}", &header[..4]),
            });
        }
        Ok(header[4])
    }
}

fn write_header(w: &mut BufWriter<File>, snapshot_seq: u64) -> Result<()> {
    w.write_all(MAGIC)?;
    w.write_all(&[VERSION])?;
    w.write_all(&[0u8; 3])?; // reserved
    w.write_all(&snapshot_seq.to_le_bytes())?;
    Ok(())
}

fn validate_header(r: &mut impl Read) -> Result<()> {
    let mut magic = [0u8; 4];
    r.read_exact(&mut magic)?;
    if &magic != MAGIC {
        return Err(Error::WalCorrupted {
            offset: 0,
            reason: format!("bad magic: expected EWAL, got {:?}", magic),
        });
    }

    let mut ver = [0u8; 1];
    r.read_exact(&mut ver)?;
    if ver[0] < MIN_READABLE_VERSION || ver[0] > VERSION {
        return Err(Error::WalCorrupted {
            offset: 4,
            reason: format!(
                "unsupported WAL version {}, this build reads v{}..v{}",
                ver[0], MIN_READABLE_VERSION, VERSION
            ),
        });
    }

    // Skip reserved + snapshot_seq.
    let mut skip = [0u8; 11];
    r.read_exact(&mut skip)?;
    Ok(())
}

/// Fsync a directory so recent create/rename/set_len operations on its
/// children are durable. POSIX guarantees directory fsync; on non-unix
/// platforms there is no portable equivalent, so this is a no-op.
#[cfg(unix)]
pub(crate) fn fsync_dir(dir: &Path) -> Result<()> {
    File::open(dir)?.sync_all()?;
    Ok(())
}

/// Non-unix stub — see the unix variant.
#[cfg(not(unix))]
pub(crate) fn fsync_dir(_dir: &Path) -> Result<()> {
    Ok(())
}

/// Deterministic crash-injection point for durability tests.
///
/// A child process sets `ETCHDB_CRASH_POINT=<label>`; when the matching
/// instrumentation point is reached, the process aborts WITHOUT unwinding —
/// modelling a hard kill (SIGKILL / power loss) that runs no destructors and
/// flushes no buffers. Compiles to nothing outside test builds.
#[inline]
pub(crate) fn maybe_crash(_point: &str) {
    #[cfg(test)]
    {
        if crash_armed(_point) {
            std::process::abort();
        }
    }
}

/// Test-only: whether crash point `point` is armed and its skip budget is
/// exhausted.
///
/// `ETCHDB_CRASH_POINT` selects the point. `ETCHDB_CRASH_SKIP` (default `0`)
/// lets the first N matching hits pass before the (N+1)th aborts — so a child
/// can acknowledge (append + fsync) several writes and then crash on a
/// *specific* later append/sync/save rather than the first one. With the
/// default skip of `0` the first matching hit aborts, preserving the original
/// single-shot behaviour. Only the currently-selected point ever increments
/// the counter, so a child that arms one point sees a per-point hit count.
#[cfg(test)]
fn crash_armed(point: &str) -> bool {
    use std::sync::atomic::{AtomicU64, Ordering};
    static HITS: AtomicU64 = AtomicU64::new(0);
    if std::env::var("ETCHDB_CRASH_POINT").as_deref() != Ok(point) {
        return false;
    }
    let skip = std::env::var("ETCHDB_CRASH_SKIP")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);
    HITS.fetch_add(1, Ordering::Relaxed) >= skip
}
