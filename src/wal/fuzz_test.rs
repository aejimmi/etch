//! Bounded, deterministic property/fuzz tests for the WAL decode surfaces.
//!
//! No external fuzzing dependency (the crate is minimal-deps): a tiny
//! deterministic LCG seeded by a fixed constant drives randomized inputs, and
//! the byte sweeps (truncate-at-every-offset, single-bit-flip-at-every-bit) are
//! exhaustive over small known-good inputs. The invariant everywhere is the
//! same: an arbitrary byte string decodes to `Ok(valid prefix)` or a typed
//! `Err`, NEVER a panic and never a wrong-but-accepted value.
//!
//! Coverage:
//! - `iter_entries` framing (truncation + bit flips over a good WAL),
//! - `EtchKey` tuple decode (round-trip + malformed length-prefixed bytes),
//! - snapshot envelope decode (via `WalBackend::load` / `load_strict`).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::WalBackend;
use super::format::WalFile;
use super::key::EtchKey;
use super::op::Op;
use crate::backend::Backend;
use crate::wal::{ReplayFormat, Replayable};

/// Fixed seed — the whole suite is reproducible run to run.
const SEED: u64 = 0x5eed_1234_dead_beef;

/// Small deterministic PRNG (SplitMix64-style output over an LCG). Not
/// cryptographic — just a stable, dependency-free source of pseudo-random
/// bytes for the fuzz sweeps.
struct Lcg(u64);

impl Lcg {
    fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn next_u64(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
        z ^ (z >> 31)
    }

    fn next_byte(&mut self) -> u8 {
        (self.next_u64() & 0xff) as u8
    }

    /// A value in `0..n` (n must be non-zero).
    fn below(&mut self, n: usize) -> usize {
        (self.next_u64() % n as u64) as usize
    }
}

// -------------------------------------------------------------------------
// (a) WalFile::iter_entries framing.
// -------------------------------------------------------------------------

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
struct State {
    items: BTreeMap<String, String>,
}

impl Replayable for State {
    fn apply_with_format(&mut self, ops: &[Op], _format: ReplayFormat) -> crate::Result<()> {
        for op in ops {
            crate::wal::apply_op(&mut self.items, op)?;
        }
        Ok(())
    }
}

fn put(key: &str, value: &[u8]) -> Op {
    Op::Put {
        collection: 0,
        key: key.as_bytes().to_vec(),
        value: value.to_vec(),
    }
}

/// Build a known-good WAL and return its raw bytes plus the entries it holds.
fn good_wal() -> (Vec<u8>, Vec<Vec<Op>>) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("wal.bin");
    let entries = vec![
        vec![put("alpha", &[1, 2, 3])],
        vec![put("beta", &[4, 5]), put("gamma", &[6])],
        vec![Op::Delete {
            collection: 1,
            key: b"beta".to_vec(),
        }],
        vec![put("delta", &[7, 8, 9, 10, 11])],
    ];
    {
        let mut wal = WalFile::open(&path).unwrap();
        for e in &entries {
            wal.append(e).unwrap();
        }
        wal.sync().unwrap();
    }
    (std::fs::read(&path).unwrap(), entries)
}

/// Write `bytes` to a fresh WAL path and run `iter_entries` on it.
fn iter_bytes(bytes: &[u8]) -> crate::Result<(Vec<Vec<Op>>, u64)> {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("wal.bin");
    std::fs::write(&path, bytes).unwrap();
    WalFile::iter_entries(&path)
}

/// The returned entries must be an exact prefix of the good entries — hash
/// verification means a corrupted or truncated entry stops iteration; it can
/// never be silently altered into a different accepted value.
fn assert_valid_prefix(got: &[Vec<Op>], expected: &[Vec<Op>]) {
    assert!(
        got.len() <= expected.len(),
        "decoded {} entries from a {}-entry WAL — cannot exceed the original",
        got.len(),
        expected.len()
    );
    for (i, entry) in got.iter().enumerate() {
        assert_eq!(
            entry, &expected[i],
            "entry {i} decoded to a value that differs from the original — \
             corruption was accepted instead of rejected"
        );
    }
}

#[test]
fn test_iter_entries_truncated_at_every_offset_returns_valid_prefix() {
    let (bytes, entries) = good_wal();
    for cut in 0..=bytes.len() {
        let out = iter_bytes(&bytes[..cut]);
        match out {
            Ok((got, offset)) => {
                assert_valid_prefix(&got, &entries);
                assert!(offset as usize <= cut.max(16));
            }
            Err(_) => { /* typed error (e.g. short header) is acceptable */ }
        }
    }
}

#[test]
fn test_iter_entries_single_bit_flips_never_panic_and_stay_prefix() {
    let (bytes, entries) = good_wal();
    for byte_idx in 0..bytes.len() {
        for bit in 0..8u8 {
            let mut corrupt = bytes.clone();
            corrupt[byte_idx] ^= 1 << bit;
            match iter_bytes(&corrupt) {
                Ok((got, _)) => assert_valid_prefix(&got, &entries),
                Err(_) => { /* typed error (bad magic/version/header) is fine */ }
            }
        }
    }
}

#[test]
fn test_iter_entries_random_buffers_never_panic() {
    let mut rng = Lcg::new(SEED ^ 0xa5a5);
    for _ in 0..512 {
        let len = rng.below(96);
        let buf: Vec<u8> = (0..len).map(|_| rng.next_byte()).collect();
        // Must not panic; result is either a typed Err or a (possibly empty)
        // set of entries. We do not assert the contents — arbitrary bytes are
        // not a prefix of anything — only that no panic escapes.
        let _ = iter_bytes(&buf);
    }
}

// -------------------------------------------------------------------------
// (b) EtchKey tuple decode.
// -------------------------------------------------------------------------

#[test]
fn test_tuple_roundtrip_random_u32_string() {
    let mut rng = Lcg::new(SEED ^ 0x1111);
    for _ in 0..1000 {
        let a = rng.next_u64() as u32;
        let n = rng.below(24);
        let s: String = (0..n)
            .map(|_| (b'a' + rng.next_byte() % 26) as char)
            .collect();
        let key = (a, s.clone());
        let bytes = key.to_bytes();
        let restored = <(u32, String)>::from_bytes(&bytes).unwrap();
        assert_eq!(key, restored);
    }
}

#[test]
fn test_tuple_roundtrip_random_string_vec() {
    let mut rng = Lcg::new(SEED ^ 0x2222);
    for _ in 0..1000 {
        let na = rng.below(20);
        let a: String = (0..na)
            .map(|_| (b'A' + rng.next_byte() % 26) as char)
            .collect();
        let nb = rng.below(20);
        let b: Vec<u8> = (0..nb).map(|_| rng.next_byte()).collect();
        let key = (a.clone(), b.clone());
        let bytes = key.to_bytes();
        let restored = <(String, Vec<u8>)>::from_bytes(&bytes).unwrap();
        assert_eq!(key, restored);
    }
}

#[test]
fn test_tuple_malformed_bytes_are_err_not_panic() {
    let mut rng = Lcg::new(SEED ^ 0x3333);
    for _ in 0..4000 {
        let len = rng.below(40);
        let buf: Vec<u8> = (0..len).map(|_| rng.next_byte()).collect();
        // Whatever the bytes, decode must return Ok or Err — never panic.
        let _ = <(String, String)>::from_bytes(&buf);
        let _ = <(u32, u64)>::from_bytes(&buf);
        let _ = <(Vec<u8>, String)>::from_bytes(&buf);
    }
}

#[test]
fn test_tuple_huge_length_prefix_is_err_not_overflow_panic() {
    // Regression: `8 + a_len` must not overflow usize on a crafted prefix.
    let mut buf = vec![0xFFu8; 8]; // a_len = u64::MAX
    buf.extend_from_slice(b"payload");
    let result = <(String, String)>::from_bytes(&buf);
    assert!(
        result.is_err(),
        "an oversized length prefix must be a typed error, not a panic"
    );

    // A prefix that is merely larger than the buffer is also a clean error.
    let mut buf2 = (1_000_000u64).to_le_bytes().to_vec();
    buf2.extend_from_slice(b"short");
    assert!(<(String, Vec<u8>)>::from_bytes(&buf2).is_err());
}

// -------------------------------------------------------------------------
// (c) Snapshot envelope decode (via the public load paths).
// -------------------------------------------------------------------------

/// Write a valid snapshot for `State` and return the raw envelope bytes.
fn good_snapshot() -> Vec<u8> {
    let dir = tempfile::tempdir().unwrap();
    {
        let backend = WalBackend::<State>::open(dir.path()).unwrap();
        let mut state = State::default();
        state.items.insert("k".into(), "v".into());
        backend.save(&state).unwrap();
    }
    std::fs::read(dir.path().join("snapshot.postcard")).unwrap()
}

/// Open a fresh backend over a directory containing only `snap` as the
/// snapshot file, and run a lenient + strict load. Returns nothing — the point
/// is that neither call panics.
fn load_with_snapshot(snap: &[u8]) {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("snapshot.postcard"), snap).unwrap();
    {
        let backend = WalBackend::<State>::open(dir.path()).unwrap();
        // Lenient: a bad snapshot is discarded, not raised — so this is Ok.
        let _ = backend.load_with_report();
    }
    {
        let backend = WalBackend::<State>::open(dir.path()).unwrap();
        // Strict: a discarded snapshot surfaces as a typed error. Either way,
        // no panic.
        let _ = backend.load_strict();
    }
}

#[test]
fn test_snapshot_envelope_truncated_at_every_offset_never_panics() {
    let snap = good_snapshot();
    for cut in 0..=snap.len() {
        load_with_snapshot(&snap[..cut]);
    }
}

#[test]
fn test_snapshot_envelope_byte_mutations_never_panic() {
    let snap = good_snapshot();
    let mut rng = Lcg::new(SEED ^ 0x4444);
    for _ in 0..300 {
        let mut corrupt = snap.clone();
        if !corrupt.is_empty() {
            let idx = rng.below(corrupt.len());
            corrupt[idx] ^= rng.next_byte() | 1;
        }
        load_with_snapshot(&corrupt);
    }
}

#[test]
fn test_snapshot_random_buffers_never_panic() {
    let mut rng = Lcg::new(SEED ^ 0x5555);
    for _ in 0..300 {
        let len = rng.below(64);
        let buf: Vec<u8> = (0..len).map(|_| rng.next_byte()).collect();
        load_with_snapshot(&buf);
    }
}
