use crate::Result;
use lru::LruCache;
use std::{
    convert::From,
    fs::{File, OpenOptions},
    hash::Hash,
    io::{BufWriter, Read, Seek, SeekFrom, Write},
    mem::size_of,
    num::NonZeroUsize,
    path::PathBuf,
};

pub trait Store<T> {
    fn len(&self) -> usize;
    fn append(&mut self, item: Vec<T>) -> Result<()>;
    fn get(&mut self, index: usize) -> Result<T>;
}

pub struct Flat<T, const N: usize> {
    file: File,
    cache: LruCache<usize, T>,
}

impl<T, const N: usize> Flat<T, N>
where
    T: Hash + Eq,
{
    pub fn new(path: PathBuf, cache_size: NonZeroUsize) -> Self {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)
            .unwrap();
        Self {
            file,
            cache: LruCache::new(cache_size),
        }
    }
}

impl<T, const N: usize> Store<T> for Flat<T, N>
where
    T: Sized + AsRef<[u8]> + From<[u8; N]> + Hash + Eq + Clone,
{
    fn len(&self) -> usize {
        self.file.metadata().unwrap().len() as usize / size_of::<T>()
    }

    fn append(&mut self, item: Vec<T>) -> Result<()> {
        let mut index = self.file.seek(SeekFrom::End(0))? as usize / size_of::<T>();
        let mut buf = BufWriter::new(&mut self.file);
        for i in &item {
            buf.write_all(i.as_ref())?;
            self.cache.put(index, i.clone());
            index += 1;
        }
        buf.flush()?;
        Ok(())
    }

    fn get(&mut self, index: usize) -> Result<T> {
        let v = self.cache.try_get_or_insert(index, || {
            let offset = size_of::<T>() * index;
            self.file.seek(SeekFrom::Start(offset as u64))?;
            let mut buf = [0u8; N];
            self.file.read_exact(&mut buf)?;
            Ok::<T, Box<dyn std::error::Error>>(buf.into())
        })?;
        Ok(v.clone())
    }
}

#[cfg(test)]
mod test {
    use std::num::NonZeroUsize;
    use tempfile::tempdir;

    use crate::index::flat_storage::{Flat, Store};

    #[test]
    fn flat() {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("flat.db");
        let mut store = Flat::new(path, NonZeroUsize::new(30).unwrap());
        assert_eq!(store.len(), 0);

        let mut items = vec![];
        for i in 0..40u32 {
            items.push(i.to_be_bytes());
        }
        store.append(items.clone()).unwrap();
        assert_eq!(store.len(), 40);

        // test the cache
        for i in 10..40usize {
            let v = store.get(i).unwrap();
            assert_eq!(v, items[i]);
        }

        // test uncached
        for i in 0..40usize {
            let v = store.get(i).unwrap();
            assert_eq!(v, items[i]);
        }
    }
}
