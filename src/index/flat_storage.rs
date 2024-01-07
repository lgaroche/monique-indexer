use crate::Result;
use lru::LruCache;
use std::{
    convert::From,
    fs::{File, OpenOptions},
    hash::{Hash, Hasher},
    io::{BufWriter, Read, Seek, SeekFrom, Write},
    mem::size_of,
    num::NonZeroUsize,
    path::PathBuf,
};
use xxhash_rust::xxh3::{xxh3_64, Xxh3Builder};

pub trait Store<T> {
    fn len(&self) -> usize;
    fn append(&mut self, item: Vec<T>, cursor: Option<u64>) -> Result<()>;
    fn get(&mut self, index: usize) -> Result<T>;
    fn metadata(&self) -> Metadata;
}

#[derive(Default, Debug, Clone, Copy, PartialEq)]
pub struct Metadata {
    checksum: u64,
    last_batch_len: u64,
    pub cursor: u64,
}

const META_LEN: usize = size_of::<Metadata>();

impl Into<[u8; META_LEN]> for Metadata {
    fn into(self) -> [u8; META_LEN] {
        let mut buf = [0u8; META_LEN];
        buf[..8].copy_from_slice(&self.cursor.to_be_bytes());
        buf[8..16].copy_from_slice(&self.last_batch_len.to_be_bytes());
        buf[16..].copy_from_slice(&self.checksum.to_be_bytes());
        buf
    }
}

impl From<[u8; META_LEN]> for Metadata {
    fn from(buf: [u8; META_LEN]) -> Self {
        Self {
            cursor: u64::from_be_bytes(buf[..8].try_into().unwrap()),
            last_batch_len: u64::from_be_bytes(buf[8..16].try_into().unwrap()),
            checksum: u64::from_be_bytes(buf[16..].try_into().unwrap()),
        }
    }
}

pub struct Flat<T, const N: usize> {
    file: File,
    cache: Option<LruCache<usize, T>>,
    metadata: Metadata,
}

impl<T, const N: usize> Flat<T, N>
where
    T: Hash + Eq,
{
    pub fn new(path: PathBuf, cache_size: usize) -> Result<Self> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)
            .unwrap();

        let metadata = match file.metadata().unwrap().len() as usize {
            0 => {
                let meta = Metadata::default();
                file.write_all(&Into::<[u8; META_LEN]>::into(meta))?;
                Metadata::default()
            }
            n if n < META_LEN => Err("unexpected file size")?,
            n if (n - META_LEN) % size_of::<T>() != 0 => Err("unexpected file size")?,
            _ => {
                // read metadata at the end of the file
                let end_of_data = -(META_LEN as i64);
                file.seek(SeekFrom::End(end_of_data))?;
                let mut meta_buf = [0u8; META_LEN];
                file.read_exact(&mut meta_buf)?;
                let metadata: Metadata = meta_buf.into();

                // verify checksum
                let last = metadata.last_batch_len as usize;
                file.seek(SeekFrom::End(end_of_data - (last * N) as i64))?;
                let mut buf = vec![0u8; N * last];
                file.read_exact(&mut buf)?;
                if xxh3_64(&buf) != metadata.checksum {
                    Err("checksum mismatch")?;
                }
                metadata
            }
        };
        let cache = if cache_size > 0 {
            Some(LruCache::new(NonZeroUsize::new(cache_size).unwrap()))
        } else {
            None
        };
        Ok(Self {
            file,
            cache,
            metadata,
        })
    }
}

impl<T, const N: usize> Store<T> for Flat<T, N>
where
    T: Sized + AsRef<[u8]> + From<[u8; N]> + Hash + Eq + Clone,
{
    fn len(&self) -> usize {
        (self.file.metadata().unwrap().len() as usize - META_LEN) / size_of::<T>()
    }

    fn append(&mut self, items: Vec<T>, cursor: Option<u64>) -> Result<()> {
        let mut index = self.len();
        let mut buf = BufWriter::new(&mut self.file);
        let mut hasher = Xxh3Builder::new().build();
        buf.seek(SeekFrom::End(-(META_LEN as i64)))?;
        for i in &items {
            buf.write_all(i.as_ref())?;
            if self.cache.is_some() {
                self.cache.as_mut().unwrap().put(index, i.clone());
            }
            hasher.write(i.as_ref());
            index += 1;
        }
        let cursor = cursor.unwrap_or(self.metadata.cursor);
        self.metadata = Metadata {
            checksum: hasher.finish(),
            last_batch_len: items.len() as u64,
            cursor,
        };
        buf.write_all(&Into::<[u8; META_LEN]>::into(self.metadata))?;
        buf.flush()?;
        Ok(())
    }

    fn get(&mut self, index: usize) -> Result<T> {
        let mut get_inner = |index: usize| -> Result<T> {
            let offset = size_of::<T>() * index;
            self.file.seek(SeekFrom::Start(offset as u64))?;
            let mut buf = [0u8; N];
            self.file.read_exact(&mut buf)?;
            Ok::<T, Box<dyn std::error::Error>>(buf.into())
        };
        let v = match self.cache.as_mut() {
            Some(cache) => cache.try_get_or_insert(index, || get_inner(index))?.clone(),
            None => get_inner(index)?,
        };
        Ok(v.clone())
    }

    fn metadata(&self) -> Metadata {
        self.metadata
    }
}

#[cfg(test)]
mod test {
    use std::{hash::Hasher, io::Write};
    use tempfile::tempdir;

    use crate::index::flat_storage::{Flat, Metadata, Store, META_LEN};

    #[test]
    fn hash() {
        let mut hasher = xxhash_rust::xxh3::Xxh3Builder::new().build();
        hasher.write(&[1u8, 2u8, 3u8]);
        let hash = hasher.finish();
        println!("{}", hash);
        hasher.write(&[2u8]);
        let hash = hasher.finish();
        println!("{}", hash);

        let mut hasher = xxhash_rust::xxh3::Xxh3Builder::new().build();
        hasher.write(&[1u8, 2u8, 3u8, 2u8]);
        let hash = hasher.finish();
        println!("{}", hash);
    }

    #[test]
    fn metadata() {
        let metadata = Metadata {
            checksum: 123,
            last_batch_len: 456,
            cursor: 789,
        };
        let buf = Into::<[u8; META_LEN]>::into(metadata);
        let recovered: Metadata = buf.into();
        assert_eq!(metadata, recovered);
    }

    #[test]
    fn checksum() {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("checksum.db");
        {
            let mut store = Flat::new(path.clone(), 0).unwrap();
            assert_eq!(store.len(), 0);

            let mut items = vec![];
            for i in 0..40u32 {
                items.push(i.to_be_bytes());
            }
            store.append(items.clone(), None).unwrap();
            assert_eq!(store.len(), 40);
        }

        {
            let store: Flat<[u8; 4], 4> = Flat::new(path.clone(), 0).unwrap();
            assert_eq!(store.len(), 40);
        }

        {
            // corruption
            let mut file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(path.clone())
                .unwrap();
            file.write_all(&[5u8; 2]).unwrap();
        }

        {
            let store = Flat::<[u8; 4], 4>::new(path, 0);
            assert!(store.is_err());
        }
    }

    #[test]
    fn flat() {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("flat.db");
        {
            let mut store = Flat::new(path.clone(), 30).unwrap();
            assert_eq!(store.len(), 0);

            let mut items = vec![];
            for i in 0..40u32 {
                items.push(i.to_be_bytes());
            }
            store.append(items.clone(), None).unwrap();
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

            // append another batch
            let mut items = vec![];
            for i in 40..80u32 {
                items.push(i.to_be_bytes());
            }
            store.append(items.clone(), None).unwrap();
            assert_eq!(store.len(), 80);
        }

        {
            let mut store: Flat<[u8; 4], 4> = Flat::new(path.clone(), 0).unwrap();
            assert_eq!(store.len(), 80);
            for i in 0..80usize {
                let v = store.get(i).unwrap();
                assert_eq!(v, (i as u32).to_be_bytes());
            }
        }
    }
}
