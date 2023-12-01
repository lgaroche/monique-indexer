mod flat_storage;
mod storage;

use crate::index::storage::{Push, Storage};
use crate::Result;
use lru::LruCache;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::{cmp, collections::HashMap};

pub trait Indexed<T> {
    type Key;
    fn len(&self) -> usize;
    fn get(&mut self, index: usize) -> Result<Option<Self::Key>>;
    fn index(&mut self, item: Self::Key) -> Result<Option<usize>>;
}

#[derive(Clone)]
pub struct SharedIndex<T>(pub Arc<RwLock<IndexTable<T>>>);

impl<T> SharedIndex<T> {
    pub fn read<'a>(&self) -> Result<RwLockReadGuard<IndexTable<T>>> {
        match self.0.read() {
            Ok(this) => Ok(this),
            Err(e) => Err(format!("could not acquire lock: {}", e.to_string()).into()),
        }
    }

    pub fn lock(&self) -> Result<RwLockWriteGuard<IndexTable<T>>> {
        match self.0.write() {
            Ok(this) => Ok(this),
            Err(e) => Err(format!("could not acquire lock: {}", e.to_string()).into()),
        }
    }
}

pub struct IndexTable<T> {
    pub pending: HashMap<u64, Vec<T>>,
    pub last_indexed_block: u64,
    pub last_committed_block: u64,
    cache: LruCache<T, usize>,
    storage: Storage<20, T>,
}

impl<T> IndexTable<T>
where
    T: AsRef<[u8]> + cmp::PartialEq + std::hash::Hash + Eq + Copy,
    [u8; 20]: From<T>,
{
    pub fn new(path: PathBuf, cache_size: usize) -> Self {
        let storage = Storage::new(path);
        let cache = LruCache::new(NonZeroUsize::new(cache_size).unwrap());
        Self {
            pending: HashMap::new(),
            last_indexed_block: 0,
            last_committed_block: 0,
            cache,
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
        let mut new_queue: Vec<T> = Vec::new();
        for address in addresses {
            if queue.contains(&&address) {
                continue;
            }
            if self.cache.contains(&address) {
                continue;
            }
            if new_queue.contains(&address) {
                continue;
            }
            self.cache.put(address, block_number as usize);
            new_queue.push(address);
        }
        let len = new_queue.len();
        self.pending.insert(block_number, new_queue);
        self.last_indexed_block = block_number;
        Ok(len)
    }

    pub fn commit(&mut self, safe_block: u64) -> Result<usize> {
        let target = cmp::min(safe_block, self.last_indexed_block);
        for n in self.last_committed_block + 1..=target {
            if let Some(a) = self.pending.remove(&n) {
                self.storage.push(a.iter().map(|p| (*p).into()).collect())?;
            }
        }
        Ok(0)
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

impl<T> Indexed<T> for IndexTable<T>
where
    T: cmp::PartialEq + std::hash::Hash + Eq + Copy + std::convert::From<[u8; 20]>,
    [u8; 20]: From<T>,
{
    type Key = T;

    fn len(&self) -> usize {
        self.storage.len() + self.pending.len()
    }

    fn get(&mut self, index: usize) -> Result<Option<Self::Key>> {
        if index > self.storage.len() {
            // if the index is in the pending queue
            let mut i = self.storage.len();
            for n in self.last_committed_block + 1..=self.last_indexed_block {
                if let Some(list) = self.pending.get(&n) {
                    for address in list {
                        if i == index {
                            return Ok(Some(*address));
                        }
                        i += 1;
                    }
                }
            }
            Ok(None)
        } else {
            Ok(Some(self.storage.get(index).unwrap().unwrap().into()))
        }
    }

    fn index(&mut self, item: Self::Key) -> Result<Option<usize>> {
        let mut i = self.storage.len();
        for n in self.last_committed_block + 1..=self.last_indexed_block {
            if let Some(list) = self.pending.get(&n) {
                for address in list {
                    if *address == item {
                        return Ok(Some(i));
                    }
                    i += 1;
                }
            }
        }

        match self.cache.get(&item) {
            Some(cached) => Ok(Some(*cached)),
            None => self.storage.index(item.into()),
        }
    }
}
