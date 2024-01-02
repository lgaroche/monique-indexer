mod flat_storage;
mod storage;
#[cfg(test)]
mod tests;

use crate::index::storage::{Push, Storage};
use crate::Result;
use indexmap::IndexSet;
use std::path::PathBuf;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::{cmp, collections::HashMap};

pub trait Indexed<T> {
    fn len(&self) -> usize;
    fn get(&self, index: usize) -> Result<Option<T>>;
    fn index(&self, item: T) -> Result<Option<usize>>;
}

#[derive(Clone)]
pub struct SharedIndex<const N: usize, T>(Arc<RwLock<IndexTable<N, T>>>);

impl<const N: usize, T> SharedIndex<N, T>
where
    T: AsRef<[u8]> + From<[u8; N]> + cmp::PartialEq + std::hash::Hash + Eq + Copy,
    [u8; N]: From<T>,
{
    pub fn new(path: PathBuf, cache_size: usize) -> Self {
        Self(Arc::new(RwLock::new(IndexTable::new(path, cache_size))))
    }
}

impl<const N: usize, T> SharedIndex<N, T> {
    pub fn read<'a>(&self) -> Result<RwLockReadGuard<IndexTable<N, T>>> {
        match self.0.read() {
            Ok(this) => Ok(this),
            Err(e) => Err(format!("could not acquire lock: {}", e.to_string()).into()),
        }
    }

    pub fn lock(&self) -> Result<RwLockWriteGuard<IndexTable<N, T>>> {
        match self.0.write() {
            Ok(this) => Ok(this),
            Err(e) => Err(format!("could not acquire lock: {}", e.to_string()).into()),
        }
    }
}

pub struct IndexTable<const N: usize, T> {
    pub last_indexed_block: u64,
    pub last_committed_block: u64,
    pending: HashMap<u64, Vec<T>>,
    storage: Storage<N, T>,
}

impl<const N: usize, T> IndexTable<N, T>
where
    T: AsRef<[u8]> + From<[u8; N]> + cmp::PartialEq + std::hash::Hash + Eq + Copy,
    [u8; N]: From<T>,
{
    pub fn new(path: PathBuf, cache_size: usize) -> Self {
        let storage = Storage::new(path, cache_size);
        Self {
            pending: HashMap::new(),
            last_indexed_block: storage.last_block,
            last_committed_block: storage.last_block,
            storage,
        }
    }

    pub fn queue(&mut self, block_number: u64, addresses: Vec<T>) -> Result<usize> {
        if block_number <= self.last_indexed_block {
            println!(
                "possible reorg detected: {} <= {} -- rolling back index",
                block_number, self.last_indexed_block
            );
            self.rollback(block_number)?;
        }
        let queue: Vec<&T> = self.pending.values().flatten().collect();
        let mut new_queue = IndexSet::with_capacity(addresses.len());
        for address in addresses {
            if queue.contains(&&address) {
                continue;
            }
            if self.storage.index(address.into())?.is_some() {
                continue;
            }
            new_queue.insert(address);
        }
        let len = new_queue.len();
        self.pending
            .insert(block_number, new_queue.into_iter().collect());
        self.last_indexed_block = block_number;
        Ok(len)
    }

    pub fn commit(&mut self, safe_block: u64) -> Result<usize> {
        let target = cmp::min(safe_block, self.last_indexed_block);
        let mut pending = vec![];
        for n in self.last_committed_block + 1..=target {
            if let Some(mut a) = self.pending.remove(&n) {
                pending.append(&mut a);
            }
        }
        let len = pending.len();
        if len > 0 {
            self.storage.push(pending, target)?;
            self.last_committed_block = target;
        }
        Ok(len)
    }

    fn rollback(&mut self, block_number: u64) -> Result<()> {
        for n in block_number..=self.last_indexed_block {
            match self.pending.remove(&n) {
                Some(a) => {
                    println!("removing {} addresses from block {}", a.len(), n);
                }
                None => {
                    println!("no addresses to remove from block {}", n);
                }
            }
        }
        Ok(())
    }
}

impl<const N: usize, T> Indexed<T> for IndexTable<N, T>
where
    T: AsRef<[u8]> + cmp::PartialEq + std::hash::Hash + Eq + Copy + std::convert::From<[u8; N]>,
    [u8; N]: From<T>,
{
    fn len(&self) -> usize {
        self.storage.len() + self.pending.values().flatten().count()
    }

    fn get(&self, index: usize) -> Result<Option<T>> {
        let item = if index > self.storage.len() {
            // if the index is in the pending queue
            let mut i = self.storage.len();
            for pending in self.pending.values().flatten() {
                if i == index {
                    Some(*pending);
                }
                i += 1;
            }
            None
        } else {
            Some(self.storage.get(index)?.unwrap().into())
        };
        Ok(item)
    }

    fn index(&self, item: T) -> Result<Option<usize>> {
        // Check the pending queue
        let mut index = self.storage.len();
        for pending in self.pending.values().flatten() {
            if *pending == item {
                return Ok(Some(index));
            }
            index += 1;
        }
        // Get from the storage
        match self.storage.index(item.into())? {
            Some(v) => Ok(Some(v)),
            None => Ok(None),
        }
    }
}
