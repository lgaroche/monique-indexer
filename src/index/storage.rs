use core::panic;
use std::{
    cmp,
    hash::Hash,
    num::NonZeroUsize,
    path::PathBuf,
    sync::{RwLock, RwLockWriteGuard},
};

use libmdbx::{
    Database, DatabaseOptions, Mode, NoWriteMap, PageSize, ReadWriteOptions, TableFlags, WriteFlags,
};
use lru::LruCache;

use crate::Result;

use super::{
    flat_storage::{Flat, Store},
    Indexed,
};

pub struct Storage<const N: usize, T> {
    _data: std::marker::PhantomData<T>,
    db: Database<NoWriteMap>,
    table: RwLock<Flat<T, N>>,
    counter: u32,
    pub last_block: u64,
    cache: RwLock<LruCache<T, usize>>,
}

pub trait Push<T> {
    fn push(&mut self, item: Vec<T>, last_block: u64) -> Result<()>;
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

        println!("counter: {}", counter);
        println!("last_block: {}", last_block);
        println!("flat db metadata: {:?}", metadata);

        if metadata.cursor != last_block {
            panic!("flat db cursor does not match last block");
        }

        if counter as usize != flat_db.len() {
            panic!("counter does not match flat db len");
        }

        let table = RwLock::new(flat_db);
        let cache = RwLock::new(LruCache::new(NonZeroUsize::new(cache_size).unwrap()));

        Self {
            _data: std::marker::PhantomData,
            db,
            table,
            counter,
            last_block,
            cache,
        }
    }

    fn get_cache(&self) -> Result<RwLockWriteGuard<LruCache<T, usize>>> {
        match self.cache.write() {
            Ok(this) => Ok(this),
            Err(e) => Err(format!("could not acquire lock: {}", e.to_string()).into()),
        }
    }

    fn get_table(&self) -> Result<RwLockWriteGuard<Flat<T, N>>> {
        match self.table.write() {
            Ok(this) => Ok(this),
            Err(e) => Err(format!("could not acquire lock: {}", e.to_string()).into()),
        }
    }
}

impl<const N: usize, T> Push<T> for Storage<N, T>
where
    T: AsRef<[u8]> + From<[u8; N]> + cmp::PartialEq + std::hash::Hash + Eq + Clone + Copy,
    [u8; N]: From<T>,
{
    fn push(&mut self, items: Vec<T>, last_block: u64) -> Result<()> {
        let tx = self.db.begin_rw_txn()?;
        let table = tx.create_table(Some("table"), TableFlags::CREATE)?;
        let mut cursor = tx.cursor(&table)?;
        let mut inserted = vec![];
        for i in items {
            let counter = u32::to_be_bytes(self.counter);
            let item = <T as Into<[u8; N]>>::into(i.clone());
            match cursor.put(&item[..], &counter, WriteFlags::NO_OVERWRITE) {
                Ok(_) => {
                    self.counter += 1;
                    inserted.push(i.clone());
                }
                Err(e) => {
                    println!("error: {}", e);
                    return Err(e.into());
                }
            }
            self.get_cache()?.put(i, self.counter as usize);
        }

        self.get_table()?.append(inserted, Some(last_block))?;

        let stats_table = tx.create_table(Some("stats"), TableFlags::CREATE)?;
        tx.put(
            &stats_table,
            b"counter",
            &self.counter.to_be_bytes(),
            WriteFlags::UPSERT,
        )?;
        tx.put(
            &stats_table,
            b"last_block",
            last_block.to_be_bytes(),
            WriteFlags::UPSERT,
        )?;
        self.last_block = last_block;
        tx.commit()?;

        Ok(())
    }
}

impl<const N: usize, T> Indexed<T> for Storage<N, T>
where
    T: AsRef<[u8]> + From<[u8; N]> + PartialEq + Hash + Eq + Copy,
    [u8; N]: From<T>,
{
    fn len(&self) -> usize {
        self.counter as usize
    }

    fn get(&self, index: usize) -> Result<Option<T>> {
        let item = self.get_table()?.get(index as usize)?;
        Ok(Some(item))
    }

    fn index(&self, item: T) -> Result<Option<usize>> {
        if let Some(index) = self.get_cache()?.get(&item.into()) {
            return Ok(Some(*index));
        }
        let tx = self.db.begin_ro_txn()?;
        if let Ok(table) = tx.open_table(Some("table")) {
            let slice = <T as Into<[u8; N]>>::into(item);
            if let Some(counter_be) = tx.get(&table, &slice)? {
                let counter = u32::from_be_bytes(counter_be) as usize;
                self.get_cache()?.put(item, counter);
                Ok(Some(counter))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }
}
