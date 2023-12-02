use std::{cmp, hash::Hash, num::NonZeroUsize, path::PathBuf};

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
    table: Flat<T, N>,
    counter: u32,
    pub last_block: u64,
    cache: LruCache<T, usize>,
}

pub trait Push<T> {
    fn push(&mut self, item: Vec<T>, last_block: u64) -> Result<()>;
}

impl<const N: usize, T> Storage<N, T>
where
    T: Sized + AsRef<[u8]> + PartialEq + Hash + Eq + Copy,
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
        println!("counter: {}", counter);
        println!("last_block: {}", last_block);
        let table = Flat::new(path.join("flat.db"), NonZeroUsize::new(50_000).unwrap());
        let cache = LruCache::new(NonZeroUsize::new(cache_size).unwrap());
        Self {
            _data: std::marker::PhantomData,
            db,
            table,
            counter,
            last_block,
            cache,
        }
    }
}

impl<const N: usize, T> Push<T> for Storage<N, T>
where
    T: AsRef<[u8]> + From<[u8; N]> + cmp::PartialEq + std::hash::Hash + Eq + Clone,
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
            self.cache.put(i, self.counter as usize);
        }

        self.table.append(inserted)?;

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

    fn get(&mut self, index: usize) -> Result<Option<T>> {
        let item = self.table.get(index as usize)?;
        Ok(Some(item))
    }

    fn index(&mut self, item: T) -> Result<Option<usize>> {
        if let Some(index) = self.cache.get(&item.into()) {
            return Ok(Some(*index));
        }
        let tx = self.db.begin_ro_txn()?;
        if let Ok(table) = tx.open_table(Some("table")) {
            let item = <T as Into<[u8; N]>>::into(item);
            let counter = tx.get(&table, &item[..])?;
            Ok(counter.map(|c| u32::from_be_bytes(c) as usize))
        } else {
            Ok(None)
        }
    }
}
