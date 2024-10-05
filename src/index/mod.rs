mod checkpoint;
mod storage;
#[cfg(test)]
mod tests;

use self::checkpoint::CheckpointTrie;
use crate::index::storage::{Push, Storage};
use crate::Result;
use async_trait::async_trait;
use indexmap::IndexSet;
use log::{info, trace, warn};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use std::{cmp, collections::HashMap};
use storage::Block;
use tokio::sync::{Mutex, RwLock, RwLockReadGuard};

#[async_trait]
pub trait Indexed<T> {
    async fn len(&self) -> usize;
    async fn get(&self, index: usize) -> Result<Option<T>>;
    async fn index(&self, item: T) -> Result<Option<usize>>;
}

pub type SharedIndex<const N: usize, T> = Arc<IndexTable<N, T>>;

pub struct Counters {
    pub last_indexed_block: u64,
    pub last_committed_block: u64,
}

pub struct IndexTable<const N: usize, T> {
    counters: RwLock<Counters>,
    pending: RwLock<HashMap<u64, Vec<T>>>,
    storage: Storage<N, T>,
    lock: Mutex<()>,
}

impl<const N: usize, T> IndexTable<N, T>
where
    T: AsRef<[u8]> + From<[u8; N]> + cmp::PartialEq + std::hash::Hash + Eq + Copy + Send + Sync,
    [u8; N]: From<T>,
{
    pub async fn new(path: PathBuf, cache_size: usize) -> Self {
        let storage = Storage::new(path, cache_size);
        let last_block = storage.get_counters().await.last_block;
        let counters = Counters {
            last_indexed_block: last_block as u64,
            last_committed_block: last_block as u64,
        };
        Self {
            pending: RwLock::new(HashMap::new()),
            counters: RwLock::new(counters),
            storage,
            lock: Mutex::new(()),
        }
    }

    pub async fn get_counters(&self) -> RwLockReadGuard<Counters> {
        self.counters.read().await
    }

    pub async fn queue(&self, block_number: u64, addresses: Vec<T>) -> Result<usize> {
        trace!(
            "queueing {} addresses for block {}",
            addresses.len(),
            block_number
        );
        // TODO: if storage lookup gets too slow and blocks other operations, consider unblocking `pending` and `counters`
        // watch out for concurrency
        let mut pending = self.pending.write().await;
        let mut counters = self.counters.write().await;
        if block_number <= counters.last_indexed_block {
            warn!(
                "possible reorg detected: {} <= {} -- rolling back index",
                block_number, counters.last_indexed_block
            );
            for n in block_number..=counters.last_indexed_block {
                match pending.remove(&n) {
                    Some(a) => {
                        info!("removing {} addresses from block {}", a.len(), n);
                    }
                    None => {
                        info!("no addresses to remove from block {}", n);
                    }
                }
            }
        } else if block_number != counters.last_indexed_block + 1 {
            Err(format!(
                "queuing error: tried to skip block {} and queue block {}",
                counters.last_indexed_block + 1,
                block_number
            ))?;
        }
        let queue: Vec<&T> = pending.values().flatten().collect();
        let mut new_queue = IndexSet::with_capacity(addresses.len());
        for address in addresses {
            if queue.contains(&&address) {
                continue;
            }
            if self.storage.index(address.into()).await?.is_some() {
                continue;
            }
            new_queue.insert(address);
        }
        let len = new_queue.len();
        pending.insert(block_number, new_queue.into_iter().collect());
        counters.last_indexed_block = block_number;
        Ok(len)
    }

    pub async fn commit(&self, safe_block: u64) -> Result<usize> {
        trace!("committing up to block {}", safe_block);
        let _lock_guard = self.lock.try_lock()?; // Do not allow concurrent commits for now
        let start = Instant::now();
        let mut index = self.storage.len().await as u64;
        let start_index = index as usize;
        let (blocks, target) = {
            let mut blocks: Vec<Block<T>> = vec![];
            let mut pending_blocks = self.pending.write().await;
            let counters = self.get_counters().await;
            let last_block = pending_blocks.keys().max().cloned().unwrap_or(0);
            let target = cmp::min(safe_block, last_block);
            for number in counters.last_committed_block + 1..=target {
                if let Some(items) = pending_blocks.remove(&number) {
                    let mut checkpoint = CheckpointTrie::new(index);
                    let root_hash =
                        checkpoint.bulk_insert(items.iter().map(|a| a.as_ref()).collect())?;
                    index += items.len() as u64;
                    blocks.push(Block {
                        items,
                        root_hash,
                        number,
                    });
                } else {
                    panic!("commit: missed block {}", number);
                }
            }
            (blocks, target)
        };

        let prep_time = start.elapsed().as_micros();

        let len = index as usize - start_index;
        let start = Instant::now();
        self.storage.push(blocks).await?;
        self.counters.write().await.last_committed_block = target;
        let push_time = start.elapsed().as_micros();
        if len > 0 {
            info!(
                "Commit: addresses={len} prepare={prep_time}us push={push_time}us average={}",
                push_time / len as u128
            );
        }
        Ok(len)
    }
}

#[async_trait]
impl<const N: usize, T> Indexed<T> for IndexTable<N, T>
where
    T: AsRef<[u8]>
        + cmp::PartialEq
        + std::hash::Hash
        + Eq
        + Copy
        + std::convert::From<[u8; N]>
        + Send
        + Sync,
    [u8; N]: From<T>,
{
    async fn len(&self) -> usize {
        let stored_count = self.storage.len().await;
        let pending_count = self.pending.read().await.values().flatten().count();
        stored_count + pending_count
    }

    async fn get(&self, index: usize) -> Result<Option<T>> {
        trace!(
            "get index={}, storage.len={}",
            index,
            self.storage.len().await
        );
        if index > self.storage.len().await {
            // if the index is in the pending queue
            let pending = self.pending.read().await;
            let mut offset = self.storage.len().await;
            for (_, items) in pending.iter() {
                if index < offset + items.len() {
                    return Ok(Some(items[index - offset]));
                }
                offset += items.len();
            }
        } else {
            return Ok(Some(self.storage.get(index).await?.unwrap().into()));
        };
        Ok(None)
    }

    async fn index(&self, item: T) -> Result<Option<usize>> {
        // Check the pending queue
        let mut index = self.storage.len().await;
        for pending in self.pending.read().await.values().flatten() {
            if *pending == item {
                return Ok(Some(index));
            }
            index += 1;
        }
        // Get from the storage
        match self.storage.index(item.into()).await? {
            Some(v) => Ok(Some(v)),
            None => Ok(None),
        }
    }
}
