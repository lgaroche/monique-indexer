use std::{num::NonZeroUsize, path::PathBuf};

use libmdbx::{
    Database, DatabaseOptions, Mode, NoWriteMap, PageSize, ReadWriteOptions, TableFlags, WriteFlags,
};

use crate::Result;

use super::{
    flat_storage::{Flat, Store},
    Indexed,
};

pub struct Storage<const N: usize, T> {
    _data: std::marker::PhantomData<T>,
    db: Database<NoWriteMap>,
    table: Flat<[u8; N], N>,
    counter: u32,
}

pub trait Push {
    type Item;
    fn push(&mut self, item: Vec<Self::Item>) -> Result<()>;
}

impl<const N: usize, T> Storage<N, T>
where
    T: Sized + AsRef<[u8]>,
{
    pub fn new(path: PathBuf) -> Self {
        let db = Database::open_with_options(
            &path,
            DatabaseOptions {
                max_tables: Some(3),
                page_size: Some(PageSize::MinimalAcceptable),
                mode: Mode::ReadWrite(ReadWriteOptions {
                    min_size: Some(17179869184),
                    ..Default::default()
                }),
                ..Default::default()
            },
        )
        .unwrap();
        let counter = {
            let tx = db.begin_ro_txn().unwrap();
            if let Ok(table) = tx.open_table(Some("stats")) {
                let counter = tx.get(&table, b"counter").unwrap();
                u32::from_be_bytes(counter.unwrap())
            } else {
                0
            }
        };
        let table = Flat::new(path.join("flat.db"), NonZeroUsize::new(50_000).unwrap());
        Self {
            _data: std::marker::PhantomData,
            db,
            table,
            counter,
        }
    }
}

impl<const N: usize, T> Push for Storage<N, T> {
    type Item = [u8; N];

    fn push(&mut self, item: Vec<Self::Item>) -> Result<()> {
        let tx = self.db.begin_rw_txn()?;
        let table = tx.create_table(Some("table"), TableFlags::CREATE)?;
        let mut cursor = tx.cursor(&table)?;
        let mut inserted = vec![];
        for i in item {
            let counter = u32::to_be_bytes(self.counter);
            match cursor.put(&i[..], &counter, WriteFlags::NO_OVERWRITE) {
                Ok(_) => {
                    self.counter += 1;
                    inserted.push(i);
                }
                Err(e) => match e {
                    libmdbx::Error::KeyExist => (),
                    _ => {
                        println!("error: {}", e);
                        return Err(e.into());
                    }
                },
            }
        }

        self.table.append(inserted)?;

        let stats_table = tx.create_table(Some("stats"), TableFlags::CREATE)?;
        tx.put(
            &stats_table,
            b"counter",
            &self.counter.to_be_bytes(),
            WriteFlags::UPSERT,
        )?;
        tx.commit()?;

        Ok(())
    }
}

impl<const N: usize, T> Indexed<T> for Storage<N, T> {
    type Key = [u8; N];

    fn len(&self) -> usize {
        self.counter as usize
    }

    fn get(&mut self, index: usize) -> Result<Option<Self::Key>> {
        let item = self.table.get(index as usize)?;
        Ok(Some(item))
    }

    fn index(&mut self, item: Self::Key) -> Result<Option<usize>> {
        let tx = self.db.begin_ro_txn()?;
        let table = tx.open_table(Some("table")).expect("open table");
        let counter = tx.get(&table, &item[..])?;
        Ok(counter.map(|c| u32::from_be_bytes(c) as usize))
    }
}
