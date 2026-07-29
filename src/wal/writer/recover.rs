//! Explicit quarantine recovery: [`WalBackend::retry_quarantine`] and the
//! chain-walk that decides how far forward a stored value can be migrated.

use serde::{Serialize, de::DeserializeOwned};

use super::WalBackend;
use crate::error::Result;
use crate::wal::diff::Replayable;
use crate::wal::migration::MigrationSet;
use crate::wal::op::Op;
use crate::wal::quarantine::QuarantinedEntry;

impl<T: Replayable + Serialize + DeserializeOwned + Default> WalBackend<T> {
    /// Retry migration for all quarantined entries using the current
    /// migration registry. Entries that migrate successfully are returned
    /// so the caller can merge them into live state. Entries that still
    /// fail remain in quarantine with updated reason.
    ///
    /// The returned `Vec<Op>` is a set of synthetic Put ops — one per
    /// recovered entry, encoded in current-version format. Callers can
    /// feed these to `Store::apply_ops` to merge into state.
    pub fn retry_quarantine(&self) -> Result<Vec<Op>> {
        let migrations = T::migrations();
        let mut q = self.quarantine.lock();

        let mut recovered = Vec::new();
        let mut still_quarantined = Vec::<QuarantinedEntry>::with_capacity(q.len());

        for entry in q.entries().iter().cloned() {
            // Reconstruct the versioned envelope the decoder expects.
            let mut envelope = Vec::with_capacity(2 + entry.value.len());
            envelope.extend_from_slice(&entry.version.to_le_bytes());
            envelope.extend_from_slice(&entry.value);

            // We don't know the collection's V type at this layer, so we
            // can only attempt to run the migration chain. The result is
            // still msgpack bytes. A successful migration means the caller
            // should be able to apply the resulting Op through the normal
            // path on its next replay.
            let from = entry.version;
            let to = guess_current_version(&migrations, entry.collection, from);

            if from == to || to == 0 {
                // No forward path registered; keep the entry.
                still_quarantined.push(entry);
                continue;
            }

            match migrations.migrate_chain(entry.collection, from, to, &entry.value) {
                crate::wal::migration::ChainResult::Migrated(new_bytes) => {
                    let mut new_env = Vec::with_capacity(2 + new_bytes.len());
                    new_env.extend_from_slice(&to.to_le_bytes());
                    new_env.extend_from_slice(&new_bytes);
                    recovered.push(Op::Put {
                        collection: entry.collection,
                        key: entry.key.clone(),
                        value: new_env,
                    });
                }
                _ => {
                    still_quarantined.push(entry);
                }
            }
        }

        q.clear();
        for entry in still_quarantined {
            q.insert(entry);
        }
        q.save(&self.dir)?;
        Ok(recovered)
    }
}

/// Find the highest target version reachable in a chain from `from` for
/// `collection`. Returns `from` if no forward migration exists.
pub(super) fn guess_current_version(migrations: &MigrationSet, collection: u8, from: u16) -> u16 {
    let mut v = from;
    // Walk forward as long as a hop exists. Bounded by u16 range; in
    // practice chains are short.
    while migrations.has(collection, v) {
        v = match v.checked_add(1) {
            Some(next) => next,
            None => break,
        };
    }
    v
}
