//! Tests for AsyncStore.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::async_store::AsyncStore;
use crate::store::Store;
use crate::wal::{Op, Overlay, Replayable, Transactable, WalBackend, apply_overlay_btree};

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
struct TestState {
    items: BTreeMap<String, String>,
}

impl Replayable for TestState {
    fn apply(&mut self, ops: &[Op]) -> crate::Result<()> {
        for op in ops {
            crate::wal::apply_op(&mut self.items, op)?;
        }
        Ok(())
    }
}

struct TestTx<'a> {
    committed: &'a TestState,
    items: Overlay<String, String>,
    ops: Vec<Op>,
}

struct TestOverlay {
    items: Overlay<String, String>,
}

impl<'a> TestTx<'a> {
    fn insert(&mut self, key: &str, value: &str) {
        self.ops.push(Op::Put {
            collection: 0,
            key: key.as_bytes().to_vec(),
            value: postcard::to_allocvec(&value.to_string()).unwrap(),
        });
        self.items.put(key.to_string(), value.to_string());
    }

    #[allow(dead_code)]
    fn get(&self, key: &str) -> Option<&String> {
        self.items.get(&self.committed.items, &key.to_string())
    }
}

impl Transactable for TestState {
    type Tx<'a> = TestTx<'a>;
    type Overlay = TestOverlay;

    fn begin_tx(&self) -> TestTx<'_> {
        TestTx {
            committed: self,
            items: Overlay::new(),
            ops: Vec::new(),
        }
    }

    fn finish_tx(tx: TestTx<'_>) -> (Vec<Op>, TestOverlay) {
        (tx.ops, TestOverlay { items: tx.items })
    }

    fn apply_overlay(&mut self, overlay: TestOverlay) {
        apply_overlay_btree(&mut self.items, overlay.items);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_read_write_roundtrip() {
    let store = AsyncStore::from_store(Store::<TestState>::memory());

    store
        .write(|tx| {
            tx.insert("a", "1");
            tx.insert("b", "2");
            Ok(())
        })
        .await
        .unwrap();

    let state = store.read();
    assert_eq!(state.items.get("a").unwrap(), "1");
    assert_eq!(state.items.get("b").unwrap(), "2");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_write_durable() {
    let dir = tempfile::tempdir().unwrap();
    let inner = Store::<TestState, WalBackend<TestState>>::open_wal(dir.path().into()).unwrap();
    let store = AsyncStore::from_store(inner);

    store
        .write_durable(|tx| {
            tx.insert("critical", "yes");
            Ok(())
        })
        .await
        .unwrap();

    let state = store.read();
    assert_eq!(state.items.get("critical").unwrap(), "yes");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_flush() {
    let store = AsyncStore::from_store(Store::<TestState>::memory());
    store.flush().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_concurrent_writes() {
    let store = AsyncStore::from_store(Store::<TestState>::memory());

    let mut handles = Vec::new();
    for t in 0..10 {
        let s = store.clone();
        handles.push(tokio::spawn(async move {
            for i in 0..10 {
                s.write(|tx| {
                    tx.insert(&format!("t{t}_k{i}"), &format!("v{i}"));
                    Ok(())
                })
                .await
                .unwrap();
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    let state = store.read();
    assert_eq!(state.items.len(), 100);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_clone_shares_state() {
    let store = AsyncStore::from_store(Store::<TestState>::memory());
    let store2 = store.clone();

    store
        .write(|tx| {
            tx.insert("shared", "yes");
            Ok(())
        })
        .await
        .unwrap();

    assert_eq!(store2.read().items.get("shared").unwrap(), "yes");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_store_accessor() {
    let inner = Store::<TestState>::memory();
    let store = AsyncStore::from_store(inner);

    store
        .write(|tx| {
            tx.insert("k", "v");
            Ok(())
        })
        .await
        .unwrap();

    // store() returns a reference to the underlying Store.
    let state = store.store().read();
    assert_eq!(state.items.get("k").unwrap(), "v");
}
