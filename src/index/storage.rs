use async_trait::async_trait;
use core::panic;
use std::{cmp, hash::Hash, num::NonZeroUsize, path::PathBuf};

use ethers::types::H256;
use libmdbx::{
    Database, DatabaseOptions, Mode, NoWriteMap, PageSize, ReadWriteOptions, TableFlags, WriteFlags,
};
use log::{error, info, trace};
use lru::LruCache;
use tokio::sync::{RwLock, RwLockReadGuard};

use crate::Result;

use super::{
    flat_storage::{Flat, Store},
    Indexed,
};

pub struct Counters {
    pub counter: u32,
    pub last_block: u64,
}

pub struct Storage<const N: usize, T> {
    _data: std::marker::PhantomData<T>,
    db: Database<NoWriteMap>,
    table: RwLock<Flat<T, N>>,
    counters: RwLock<Counters>,
    cache: RwLock<LruCache<T, usize>>,
    checkpoints: RwLock<Flat<H256, 32>>,
}

pub trait Push<T> {
    async fn push(&self, item: Vec<T>, last_block: u64) -> Result<()>;
}

impl<const N: usize, T> Storage<N, T>
where
    T: Sized + AsRef<[u8]> + PartialEq + Hash + Eq + Copy + std::convert::From<[u8; N]>,
{
    pub fn new(path: PathBuf, cache_size: usize) -> Self {
        let db = Database::open_with_options(
            &path,
            DatabaseOptions {
                max_tables: Some(2),
                page_size: Some(PageSize::MinimalAcceptable),
                mode: Mode::ReadWrite(ReadWriteOptions {
                    min_size: Some(17179869184),
                    sync_mode: libmdbx::SyncMode::NoMetaSync,
                    ..Default::default()
                }),
                ..Default::default()
            },
        )
        .unwrap();
        let (counter, last_block) = {
            let tx = db.begin_ro_txn().unwrap();
            if let Ok(table) = tx.open_table(Some("stats")) {
                let counter = tx.get(&table, b"counter").unwrap();
                let last_block = tx.get(&table, b"last_block").unwrap();
                (
                    u32::from_be_bytes(counter.unwrap()),
                    u64::from_be_bytes(last_block.unwrap()),
                )
            } else {
                (0, 0)
            }
        };
        let flat_db = Flat::new(path.join("flat.db"), 50_000).unwrap();
        let metadata = flat_db.metadata();

        info!("counter: {}", counter);
        info!("last_block: {}", last_block);
        info!("flat db metadata: {:?}", metadata);

        if metadata.cursor != last_block {
            panic!("flat db cursor does not match last block");
        }

        if counter as usize != flat_db.len() {
            panic!("counter does not match flat db len");
        }

        let checkpoint_table = Flat::new(path.join("checkpoints.db"), 0).unwrap();
        let checkpoint_count = checkpoint_table.len();
        info!("checkpoints: {}", checkpoint_count);
        if checkpoint_count > last_block as usize {
            info!("there are more checkpoints than blocks in db: indexed blocks will be verified against the checkpoints");
        } else if checkpoint_count == last_block as usize {
            info!("checkpoints will be created during indexing");
        } else {
            panic!("checkpoints missing, aborting");
        }

        let table = RwLock::new(flat_db);
        let cache = RwLock::new(LruCache::new(NonZeroUsize::new(cache_size).unwrap()));

        Self {
            _data: std::marker::PhantomData,
            db,
            table,
            counters: RwLock::new(Counters {
                counter,
                last_block,
            }),
            cache,
            checkpoints: RwLock::new(checkpoint_table),
        }
    }

    pub async fn get_counters(&self) -> RwLockReadGuard<Counters> {
        self.counters.read().await
    }

    //TODO: tests!! there are probably many catastrophic edge cases here
    pub async fn push_checkpoints(&self, roots: Vec<H256>) -> Result<()> {
        let mut checkpoints = self.checkpoints.write().await;
        let last_block = self.counters.read().await.last_block;
        match checkpoints.len() {
            n if n > last_block as usize => {
                // verify
                let to_verify = std::cmp::min(n - last_block as usize, roots.len());
                if to_verify > 0 {
                    info!("verifying {} checkpoints", to_verify);
                    for i in 0..to_verify {
                        let root = checkpoints.get(last_block as usize + i)?;
                        if root != roots[i] {
                            panic!(
                                "checkpoint mismatch at block {}: {} != {}",
                                last_block + i as u64,
                                root,
                                roots[i]
                            );
                        }
                    }
                }
                let to_append = roots[to_verify..].to_vec();
                if to_append.len() > 0 {
                    info!("appending {} checkpoints", to_append.len());
                    checkpoints.append(to_append, None)?;
                }
            }
            n => {
                checkpoints.append(roots, Some(n as u64))?;
            }
        }
        Ok(())
    }
}

impl<const N: usize, T> Push<T> for Storage<N, T>
where
    T: AsRef<[u8]> + From<[u8; N]> + cmp::PartialEq + std::hash::Hash + Eq + Clone + Copy,
    [u8; N]: From<T>,
{
    async fn push(&self, items: Vec<T>, last_block: u64) -> Result<()> {
        let tx = self.db.begin_rw_txn()?;
        let table = tx.create_table(Some("table"), TableFlags::CREATE)?;
        let mut cursor = tx.cursor(&table)?;
        let mut inserted = vec![];
        let mut counter = self.counters.read().await.counter;
        for i in items {
            let item = <T as Into<[u8; N]>>::into(i.clone());
            self.cache.write().await.put(i, counter as usize);
            match cursor.put(&item[..], &counter.to_be_bytes(), WriteFlags::NO_OVERWRITE) {
                Ok(_) => {
                    counter += 1;
                    inserted.push(i.clone());
                }
                Err(e) => {
                    error!("{}", e);
                    return Err(e.into());
                }
            }
        }

        self.table
            .write()
            .await
            .append(inserted, Some(last_block))?;

        let stats_table = tx.create_table(Some("stats"), TableFlags::CREATE)?;
        tx.put(
            &stats_table,
            b"counter",
            &counter.to_be_bytes(),
            WriteFlags::UPSERT,
        )?;
        tx.put(
            &stats_table,
            b"last_block",
            last_block.to_be_bytes(),
            WriteFlags::UPSERT,
        )?;

        tx.commit()?;

        let mut counters = self.counters.write().await;
        counters.counter = counter;
        counters.last_block = last_block;

        Ok(())
    }
}

#[async_trait]
impl<const N: usize, T> Indexed<T> for Storage<N, T>
where
    T: AsRef<[u8]> + From<[u8; N]> + PartialEq + Hash + Eq + Copy + Send + Sync,
    [u8; N]: From<T>,
{
    async fn len(&self) -> usize {
        self.get_counters().await.counter as usize
    }

    async fn get(&self, index: usize) -> Result<Option<T>> {
        let item = self.table.write().await.get(index as usize)?;
        Ok(Some(item))
    }

    async fn index(&self, item: T) -> Result<Option<usize>> {
        let mut cache = self.cache.write().await;
        if let Some(index) = cache.get(&item.into()) {
            trace!("Storage::index: cache hit {index}");
            return Ok(Some(*index));
        }
        let tx = self.db.begin_ro_txn()?;
        if let Ok(table) = tx.open_table(Some("table")) {
            let slice = <T as Into<[u8; N]>>::into(item);
            if let Some(counter_be) = tx.get(&table, &slice)? {
                let counter = u32::from_be_bytes(counter_be) as usize;
                cache.put(item, counter);
                Ok(Some(counter))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }
}
