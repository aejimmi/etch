//! Replay observability — a structured account of what happened during load.
//!
//! Historically every recoverable problem hit during WAL/snapshot replay was
//! reported with a bare `eprintln!` and then swallowed, so `Backend::load`
//! returned `Ok(state)` no matter how much data was skipped or quarantined.
//! A downstream consumer that shipped a schema change could come up with a
//! silently partial state and no programmatic signal.
//!
//! [`ReplayReport`] replaces those prints. Every skip, quarantine, snapshot
//! discard, and drift detection becomes a counted, inspectable entry. The
//! lenient load path (`Store::open_wal`) still comes up — but the loss is now
//! *visible* via [`crate::Store::replay_report`]. The strict path
//! (`Store::open_wal_strict`) converts any loss into a typed error.

use std::collections::BTreeMap;

use crate::error::Error;

/// Upper bound on stored human-readable notes. Counts are always exact; only
/// the free-form note strings are capped so a pathologically corrupt WAL
/// cannot exhaust memory (the historical `eprintln!` path had no such bound,
/// but it also did not retain the strings).
const MAX_NOTES: usize = 256;

/// Status of the snapshot file observed during a load.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum SnapshotStatus {
    /// No snapshot file existed (or it was empty); load began from default state.
    #[default]
    Absent,
    /// The snapshot decoded successfully.
    Loaded,
    /// The snapshot was present but could not be decoded. In lenient mode it
    /// was preserved on disk and the load continued from default state,
    /// replaying the WAL on top. The `reason` explains why it was rejected.
    Discarded {
        /// Human-readable decode-failure reason (includes the preserved path).
        reason: String,
    },
}

/// Outcome of schema-fingerprint drift detection.
///
/// Drift is *informational only* — a fingerprint change means the schema
/// evolved since the snapshot was written, which is expected when a version
/// is bumped with a migration. It never aborts a load on its own (not even
/// in strict mode); real data loss surfaces independently as skips or
/// quarantine. See [`crate::Replayable::schema_fingerprint`].
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum SchemaDrift {
    /// Not evaluated — no decodable snapshot, or one side opted out (fp `0`).
    #[default]
    NotChecked,
    /// Stored and current fingerprints agree.
    Match,
    /// Fingerprints disagree — the schema changed since the snapshot.
    Detected {
        /// Fingerprint recorded in the snapshot.
        stored: u64,
        /// Fingerprint of the current binary.
        current: u64,
        /// Whether any migrations are registered (a bumped version is
        /// expected to carry one; the absence is the louder signal).
        has_migrations: bool,
    },
}

/// A structured account of a single load's replay outcomes.
///
/// Obtain one from [`crate::Store::replay_report`] (the report from the most
/// recent open) or [`crate::Store::open_wal_with_report`]. Counts are exact;
/// the free-form [`ReplayReport::notes`] are a bounded diagnostic sample.
#[derive(Debug, Clone, Default)]
#[must_use]
pub struct ReplayReport {
    applied_entries: u64,
    applied_ops: u64,
    value_decode_skipped: u64,
    key_decode_skipped: u64,
    entry_decode_skipped: u64,
    wal_prev_unreadable: u64,
    unknown_collections: BTreeMap<u8, u64>,
    quarantined: u64,
    quarantine_by_reason: BTreeMap<String, u64>,
    snapshot: SnapshotStatus,
    drift: SchemaDrift,
    future_version: Option<(u16, u16)>,
    notes: Vec<String>,
}

impl ReplayReport {
    /// WAL entries (batches of ops) applied without an entry-level error.
    #[must_use]
    pub fn applied_entries(&self) -> u64 {
        self.applied_entries
    }

    /// Individual ops merged into live state (puts and deletes).
    #[must_use]
    pub fn applied_ops(&self) -> u64 {
        self.applied_ops
    }

    /// Ops skipped because their value failed to decode or migrate. These
    /// values were also quarantined (see [`ReplayReport::quarantined`]).
    #[must_use]
    pub fn value_decode_skipped(&self) -> u64 {
        self.value_decode_skipped
    }

    /// Ops skipped because the value decoded but the key could not be
    /// converted from its stored bytes.
    #[must_use]
    pub fn key_decode_skipped(&self) -> u64 {
        self.key_decode_skipped
    }

    /// Ops skipped because they targeted a collection id the current schema
    /// does not declare (the collection was removed or renamed).
    #[must_use]
    pub fn unknown_collection_skipped(&self) -> u64 {
        self.unknown_collections.values().copied().sum()
    }

    /// Per-collection-id breakdown of unknown-collection skips.
    #[must_use]
    pub fn unknown_collections(&self) -> &BTreeMap<u8, u64> {
        &self.unknown_collections
    }

    /// Whole WAL entries skipped because a (manual) `Replayable` impl returned
    /// an entry-level error.
    #[must_use]
    pub fn entry_decode_skipped(&self) -> u64 {
        self.entry_decode_skipped
    }

    /// Count of `wal.prev` backup files that could not be read during replay.
    #[must_use]
    pub fn wal_prev_unreadable(&self) -> u64 {
        self.wal_prev_unreadable
    }

    /// Number of entries in quarantine after the load settled (post auto-retry).
    #[must_use]
    pub fn quarantined(&self) -> u64 {
        self.quarantined
    }

    /// Quarantine breakdown keyed by a human-readable reason label.
    #[must_use]
    pub fn quarantine_by_reason(&self) -> &BTreeMap<String, u64> {
        &self.quarantine_by_reason
    }

    /// Snapshot file status for this load.
    #[must_use]
    pub fn snapshot(&self) -> &SnapshotStatus {
        &self.snapshot
    }

    /// Schema-fingerprint drift result for this load.
    #[must_use]
    pub fn schema_drift(&self) -> &SchemaDrift {
        &self.drift
    }

    /// Bounded diagnostic notes gathered during replay.
    #[must_use]
    pub fn notes(&self) -> &[String] {
        &self.notes
    }

    /// Total ops/entries skipped across every skip class.
    #[must_use]
    pub fn total_skipped(&self) -> u64 {
        self.value_decode_skipped
            + self.key_decode_skipped
            + self.unknown_collection_skipped()
            + self.entry_decode_skipped
            + self.wal_prev_unreadable
    }

    /// Whether the load lost or deferred any data.
    ///
    /// True if anything was skipped, quarantined, or the snapshot was
    /// discarded. Schema drift alone does **not** count — it is informational
    /// and, in particular, an old pair-only fingerprint drifting against the
    /// newer shape-aware computation must not be treated as loss.
    #[must_use]
    pub fn has_loss(&self) -> bool {
        self.total_skipped() > 0
            || self.quarantined > 0
            || matches!(self.snapshot, SnapshotStatus::Discarded { .. })
    }

    /// A compact one-line summary of the load outcome.
    #[must_use]
    pub fn summary(&self) -> String {
        let mut parts = vec![format!(
            "applied {} entries / {} ops",
            self.applied_entries, self.applied_ops
        )];
        self.push_skip_summaries(&mut parts);
        if let SnapshotStatus::Discarded { reason } = &self.snapshot {
            parts.push(format!("snapshot discarded ({reason})"));
        }
        parts.join("; ")
    }

    fn push_skip_summaries(&self, parts: &mut Vec<String>) {
        let pairs = [
            (self.quarantined, "quarantined"),
            (self.value_decode_skipped, "value-decode skipped"),
            (self.key_decode_skipped, "key-decode skipped"),
            (
                self.unknown_collection_skipped(),
                "unknown-collection skipped",
            ),
            (self.entry_decode_skipped, "entries skipped"),
            (self.wal_prev_unreadable, "wal.prev unreadable"),
        ];
        for (count, label) in pairs {
            if count > 0 {
                parts.push(format!("{count} {label}"));
            }
        }
    }

    // ---- crate-internal recorders -----------------------------------------

    pub(crate) fn record_applied_entry(&mut self) {
        self.applied_entries += 1;
    }

    pub(crate) fn record_applied_op(&mut self) {
        self.applied_ops += 1;
    }

    pub(crate) fn record_value_decode(&mut self) {
        self.value_decode_skipped += 1;
    }

    pub(crate) fn record_key_decode(&mut self) {
        self.key_decode_skipped += 1;
    }

    pub(crate) fn record_unknown_collection(&mut self, id: u8) {
        *self.unknown_collections.entry(id).or_insert(0) += 1;
    }

    pub(crate) fn record_entry_decode(&mut self, note: impl Into<String>) {
        self.entry_decode_skipped += 1;
        self.push_note(note);
    }

    pub(crate) fn record_wal_prev_unreadable(&mut self, note: impl Into<String>) {
        self.wal_prev_unreadable += 1;
        self.push_note(note);
    }

    pub(crate) fn note_future_version(&mut self, stored: u16, current: u16) {
        // Keep the first observed pair — enough to produce a typed
        // SchemaVersionMismatch in strict mode.
        self.future_version.get_or_insert((stored, current));
    }

    pub(crate) fn set_snapshot(&mut self, status: SnapshotStatus) {
        self.snapshot = status;
    }

    pub(crate) fn set_drift(&mut self, drift: SchemaDrift) {
        self.drift = drift;
    }

    pub(crate) fn set_quarantine(&mut self, count: u64, by_reason: BTreeMap<String, u64>) {
        self.quarantined = count;
        self.quarantine_by_reason = by_reason;
    }

    pub(crate) fn push_note(&mut self, note: impl Into<String>) {
        if self.notes.len() < MAX_NOTES {
            self.notes.push(note.into());
        } else if self.notes.len() == MAX_NOTES {
            self.notes
                .push("… further notes elided (see counts for totals)".to_string());
        }
    }

    /// Fold the per-op counters from a phase `other` into `self`. Snapshot
    /// status, drift, and the settled quarantine total are owned by the
    /// top-level load and are *not* absorbed.
    pub(crate) fn absorb(&mut self, other: &ReplayReport) {
        self.applied_entries += other.applied_entries;
        self.applied_ops += other.applied_ops;
        self.value_decode_skipped += other.value_decode_skipped;
        self.key_decode_skipped += other.key_decode_skipped;
        self.entry_decode_skipped += other.entry_decode_skipped;
        self.wal_prev_unreadable += other.wal_prev_unreadable;
        for (id, n) in &other.unknown_collections {
            *self.unknown_collections.entry(*id).or_insert(0) += n;
        }
        if self.future_version.is_none() {
            self.future_version = other.future_version;
        }
        for note in &other.notes {
            self.push_note(note.clone());
        }
    }

    /// The typed error a strict load should surface, or `None` if the load
    /// was clean. Prefers a version-level [`Error::SchemaVersionMismatch`]
    /// when a future-version value was the cause; otherwise reports the loss
    /// as [`Error::ReplayLoss`].
    pub(crate) fn strict_error(&self) -> Option<Error> {
        if !self.has_loss() {
            return None;
        }
        if let Some((stored, current)) = self.future_version {
            return Some(Error::SchemaVersionMismatch { stored, current });
        }
        Some(Error::ReplayLoss {
            summary: self.summary(),
        })
    }
}

#[cfg(test)]
#[path = "report_test.rs"]
mod report_test;
