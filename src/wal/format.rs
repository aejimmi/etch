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
const VERSION: u8 = 2;
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
        self.writer.write_all(&hash.to_le_bytes())?;
        self.offset += 4 + payload.len() as u64 + 8;
        Ok(())
    }

    /// Flush the BufWriter and fsync the underlying file.
    pub fn sync(&mut self) -> Result<()> {
        self.writer.flush()?;
        self.writer.get_ref().sync_all()?;
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
    pub fn truncate_at(path: &Path, offset: u64) -> Result<()> {
        let file = OpenOptions::new().write(true).open(path)?;
        file.set_len(offset)?;
        file.sync_all()?;
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

    /// Current number of bytes written (including header).
    #[cfg(test)]
    pub fn offset(&self) -> u64 {
        self.offset
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
            reason: format!("bad magic: expected TWAL, got {:?}", magic),
        });
    }

    let mut ver = [0u8; 1];
    r.read_exact(&mut ver)?;
    if ver[0] != VERSION {
        return Err(Error::WalCorrupted {
            offset: 4,
            reason: format!("unsupported version: {}", ver[0]),
        });
    }

    // Skip reserved + snapshot_seq.
    let mut skip = [0u8; 11];
    r.read_exact(&mut skip)?;
    Ok(())
}
