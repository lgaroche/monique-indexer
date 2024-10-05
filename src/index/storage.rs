use async_trait::async_trait;
use std::{cmp, hash::Hash, num::NonZeroUsize, path::PathBuf};
use tiny_keccak::{Hasher, Keccak};
use xxhash_rust::xxh3::xxh3_64;

use ethers::types::H256;
use libmdbx::{
    Database, DatabaseOptions, Mode, NoWriteMap, PageSize, ReadWriteOptions, TableFlags, WriteFlags,
};
use log::{info, trace, warn};
use lru::LruCache;
use tokio::sync::{RwLock, RwLockReadGuard};

use crate::Result;

use super::Indexed;

#[derive(Clone)]
pub struct Counters {
    pub counter: u32,
    pub last_block: u32,
}

pub struct Storage<const N: usize, T> {
    _data: std::marker::PhantomData<T>,
    db: Database<NoWriteMap>,
    counters: RwLock<Counters>,
    cache: RwLock<LruCache<T, usize>>,
    index_cache: RwLock<LruCache<usize, T>>,
}

#[derive(Clone)]
pub struct Block<T> {
    pub number: u64,
    pub items: Vec<T>,
    pub root_hash: H256,
}

impl<T> Block<T> {
    pub fn compute_hash(&self, previous_hash: H256) -> H256 {
        let mut hash = [0u8; 32];
        let mut keccak = Keccak::v256();
        keccak.update(previous_hash.as_bytes());
        keccak.update(self.root_hash.as_ref());
        keccak.finalize(&mut hash);
        let res = H256::from(hash);
        trace!(
            "computed hash for block {}: {} (previous: {}",
            self.number,
            res,
            previous_hash
        );
        res
    }
}

#[async_trait]
pub trait Push<T> {
    async fn push(&self, blocks: Vec<Block<T>>) -> Result<()>;
}

impl<const N: usize, T> Storage<N, T>
where
    T: Sized + AsRef<[u8]> + PartialEq + Hash + Eq + Copy + std::convert::From<[u8; N]>,
{
    pub fn new(path: PathBuf, cache_size: usize) -> Self {
        // table format:
        // stats: 'counter' -> u32, 'last_block' -> u32
        // table: xxhash32(address) -> [index, ...]
        // index: index -> address
        // blocks: block_number -> start_index | count | checkpoint_hash
        let db = Database::open_with_options(
            &path,
            DatabaseOptions {
                max_tables: Some(4),
                page_size: Some(PageSize::Set(16384)),
                mode: Mode::ReadWrite(ReadWriteOptions {
                    min_size: Some(17179869184),
                    sync_mode: libmdbx::SyncMode::Durable,
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
                    u32::from_le_bytes(counter.unwrap()),
                    u32::from_le_bytes(last_block.unwrap()),
                )
            } else {
                (0, 0)
            }
        };

        info!("counter: {}", counter);
        info!("last_block: {}", last_block);

        let cache = RwLock::new(LruCache::new(NonZeroUsize::new(cache_size).unwrap()));
        let index_cache = RwLock::new(LruCache::new(NonZeroUsize::new(cache_size).unwrap()));

        Self {
            _data: std::marker::PhantomData,
            db,
            counters: RwLock::new(Counters {
                counter,
                last_block,
            }),
            cache,
            index_cache,
        }
    }

    pub async fn get_counters(&self) -> RwLockReadGuard<Counters> {
        self.counters.read().await
    }

    fn get_block_hash(&self, number: u32) -> Result<H256> {
        if number == 0 {
            return Ok(H256::zero());
        }
        let tx = self.db.begin_ro_txn()?;
        let blocks_table = tx.open_table(Some("blocks"))?;
        let key = number.to_le_bytes();
        match tx.get::<Vec<u8>>(&blocks_table, &key)? {
            Some(v) => Ok(H256::from_slice(&v)),
            None => Err("storage get_block_hash: block not found".into()),
        }
    }
}

#[async_trait]
impl<const N: usize, T> Push<T> for Storage<N, T>
where
    T: AsRef<[u8]>
        + From<[u8; N]>
        + cmp::PartialEq
        + std::hash::Hash
        + Eq
        + Clone
        + Copy
        + Send
        + Sync,
    [u8; N]: From<T>,
{
    async fn push(&self, blocks: Vec<Block<T>>) -> Result<()> {
        let mut previous_block_hash = match blocks.first() {
            Some(block) => {
                if block.number == 0 {
                    return Err("storage push: unexpected block number 0".into());
                } else {
                    self.get_block_hash(block.number as u32 - 1)?
                }
            }
            None => return Ok(()),
        };

        let counters = self.get_counters().await.clone();
        let mut last_block = counters.last_block;
        let tx = self.db.begin_rw_txn()?;
        let flags = TableFlags::CREATE | TableFlags::INTEGER_KEY;
        let blocks_table = tx.create_table(Some("blocks"), flags)?;
        let index_table = tx.create_table(Some("index"), flags)?;
        let stats_table = tx.create_table(Some("stats"), TableFlags::CREATE)?;
        let table = tx.create_table(
            Some("table"),
            flags | TableFlags::DUP_SORT | TableFlags::DUP_FIXED | TableFlags::INTEGER_DUP,
        )?;
        let mut block_cursor = tx.cursor(&blocks_table)?;
        let mut index_cursor = tx.cursor(&index_table)?;
        let mut table_cursor = tx.cursor(&table)?;
        let mut index = counters.counter;
        for block in blocks.iter() {
            if block.number != last_block as u64 + 1 {
                return Err("storage push: unexpected block number".into());
            }
            last_block = block.number as u32;
            let block_hash = block.compute_hash(previous_block_hash);
            let key = (block.number as u32).to_le_bytes();
            if block.number % 10_000 == 0 {
                info!("checkpoint: {} {}", block.number, block_hash);
            }
            previous_block_hash = block_hash;
            block_cursor.put(
                &key,
                &block_hash.as_bytes(),
                WriteFlags::APPEND | WriteFlags::NO_OVERWRITE,
            )?;
            for i in block.items.iter() {
                let item = <T as Into<[u8; N]>>::into(i.clone());
                let key = index.to_le_bytes();
                index_cursor.put(&key, &item[..], WriteFlags::APPEND)?;

                let hash = (xxh3_64(&item[..]) as u32).to_le_bytes();
                let value = index.to_le_bytes();
                table_cursor.put(&hash, &value, WriteFlags::APPEND_DUP)?;

                self.cache.write().await.put(*i, index as usize);
                self.index_cache.write().await.put(index as usize, *i);

                index += 1;
            }
        }

        tx.put(
            &stats_table,
            b"counter",
            &index.to_le_bytes(),
            WriteFlags::UPSERT,
        )?;
        tx.put(
            &stats_table,
            b"last_block",
            last_block.to_le_bytes(),
            WriteFlags::UPSERT,
        )?;

        tx.commit()?;

        let mut counters = self.counters.write().await;
        counters.counter = index;
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
        if let Some(item) = self.index_cache.write().await.get(&index) {
            return Ok(Some(*item));
        }
        let tx = self.db.begin_ro_txn()?;
        if let Ok(index_table) = tx.open_table(Some("index")) {
            return match tx.get(&index_table, &(index as u32).to_le_bytes())? {
                Some(data) => {
                    let item = T::from(data);
                    self.index_cache.write().await.put(index, item);
                    Ok(Some(item))
                }
                None => Ok(None),
            };
        }
        Ok(None)
    }

    async fn index(&self, item: T) -> Result<Option<usize>> {
        trace!("index: {:?}", item.as_ref());
        if let Some(index) = self.cache.write().await.get(&item.into()) {
            trace!("cache hit");
            return Ok(Some(*index));
        }
        let tx = self.db.begin_ro_txn()?;
        if let Ok(table) = tx.open_table(Some("table")) {
            let mut cursor = tx.cursor(&table)?;
            let hash = (xxh3_64(item.as_ref()) as u32).to_le_bytes();
            for value in cursor.iter_from::<[u8; 4], [u8; 4]>(&hash) {
                match value {
                    Ok((k, v)) => {
                        if k != hash {
                            break;
                        }
                        let key = u32::from_le_bytes(v) as usize;
                        let item_test = self.get(key).await?;
                        if item_test == Some(item) {
                            self.cache.write().await.put(item, key);
                            return Ok(Some(key));
                        }
                    }
                    Err(e) => {
                        warn!("error: {:?}", e);
                        break;
                    }
                }
            }
            Ok(None)
        } else {
            Ok(None)
        }
    }
}
