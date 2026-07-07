//! Tests for WalBackend + IncrementalSave.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use super::diff::Replayable;
use super::op::Op;
use super::writer::{IncrementalSave, WalBackend};
use crate::backend::Backend;

#[path = "writer_backend_test.rs"]
mod backend;
#[path = "writer_snapshot_test.rs"]
mod snapshot;

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
struct State {
    items: BTreeMap<String, String>,
}

impl Replayable for State {
    fn apply_with_format(
        &mut self,
        ops: &[Op],
        _format: crate::wal::ReplayFormat,
    ) -> crate::Result<()> {
        for op in ops {
            crate::wal::apply_op(&mut self.items, op)?;
        }
        Ok(())
    }
}

fn put_op(key: &str, value: &str) -> Op {
    Op::Put {
        collection: 0,
        key: key.as_bytes().to_vec(),
        value: postcard::to_allocvec(&value.to_string()).unwrap(),
    }
}

fn del_op(key: &str) -> Op {
    Op::Delete {
        collection: 0,
        key: key.as_bytes().to_vec(),
    }
}
