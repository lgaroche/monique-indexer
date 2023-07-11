use std::{
    cmp,
    collections::HashMap,
    env,
    sync::{Arc, Mutex},
    time,
};

use ethers::types::Address;
use indexmap::IndexSet;
use rocksdb::{IteratorMode, WriteBatchWithTransaction, DB};

//type Result<T> = std::result::Result<T, Box<dyn std::error::Error + '_>>;

#[derive(Clone)]
pub struct SharedIndex(Arc<Mutex<IndexSet<Address>>>);

impl SharedIndex {
    pub fn lock(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, IndexSet<Address>>, Box<dyn std::error::Error>> {
        match self.0.lock() {
            Ok(this) => Ok(this),
            Err(e) => Err(format!("could not acquire lock: {}", e.to_string()).into()),
        }
    }
    pub fn len<'a>(&self) -> Result<usize, Box<dyn std::error::Error>> {
        Ok(self.lock()?.len())
    }
}

pub struct AddressDB {
    db: DB,
    pub index: SharedIndex,
    pub last_indexed_block: u64,
    pub last_committed_block: u64,
    pending: HashMap<u64, Vec<Address>>,
}

pub struct AddressDBIterator<'a> {
    inner: rocksdb::DBIterator<'a>,
}

impl<'a> Iterator for AddressDBIterator<'a> {
    type Item = (Address, u64);

    fn next(&mut self) -> Option<Self::Item> {
        match self.inner.next() {
            Some(Ok((key, value))) => {
                if key.len() != 20 {
                    return self.next();
                }
                Some((
                    Address::from_slice(&key),
                    u64::from_be_bytes(value[0..8].try_into().unwrap()),
                ))
            }
            _ => None,
        }
    }
}

impl AddressDB {
    pub fn new(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let db = DB::open_default(path)?;

        // For debug purposes, overrides the genesis block number
        let genesis_block = env::var("GENESIS_OVERRIDE")
            .unwrap_or_default()
            .parse::<u64>()
            .unwrap_or_default();

        let last_block = match db.get("last_block".as_bytes())? {
            Some(block) => u64::from_be_bytes(block[0..8].try_into().unwrap()),
            None => {
                db.put("last_block".as_bytes(), genesis_block.to_be_bytes())?;
                genesis_block
            }
        };

        let this = Self {
            db,
            last_indexed_block: last_block,
            last_committed_block: last_block,
            index: SharedIndex(Arc::new(Mutex::new(IndexSet::new()))),
            pending: HashMap::new(),
        };
        Ok(this)
    }

    pub fn build_index(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        println!("building index...");
        let start = time::Instant::now();

        let mut index: IndexSet<Address> = IndexSet::with_capacity(200_000_000);
        {
            let mut vec = Vec::with_capacity(200_000_000);
            for (address, index) in self.iterator() {
                if index >= vec.len() as u64 {
                    vec.resize(index as usize + 1, Address::from([0u8; 20]));
                }
                vec[index as usize] = address;
            }
            for i in 0..vec.len() {
                index.insert(vec[i]);
            }
            println!("index built in {} ms", start.elapsed().as_millis());
        }

        *self.index.lock()? = index;
        Ok(())
    }

    pub fn iterator(&self) -> AddressDBIterator {
        AddressDBIterator {
            inner: self.db.iterator(IteratorMode::Start),
        }
    }

    pub fn queue(
        &mut self,
        block_number: u64,
        addresses: Vec<Address>,
    ) -> Result<usize, Box<dyn std::error::Error>> {
        if block_number <= self.last_indexed_block {
            println!(
                "possible reorg detected: {} <= {} -- rolling back index",
                block_number, self.last_indexed_block
            );
            self.rollback(block_number)?;
        }
        let mut addr = Vec::new();
        let mut index = self.index.lock()?;
        for address in addresses {
            if index.insert(address) {
                addr.push(address);
            }
        }
        self.pending.insert(block_number, addr.clone());
        self.last_indexed_block = block_number;
        Ok(addr.len())
    }

    fn rollback(&mut self, block_number: u64) -> Result<(), Box<dyn std::error::Error>> {
        for n in block_number..=self.last_indexed_block {
            match self.pending.remove(&n) {
                Some(a) => {
                    println!("removing {} addresses from block {}", a.len(), n);
                    let mut index = self.index.lock()?;
                    let len = index.len();
                    index.truncate(len - a.len());
                }
                None => {
                    println!("no addresses to remove from block {}", n);
                }
            }
        }
        Ok(())
    }

    pub fn commit(&mut self, safe_block: u64) -> Result<usize, Box<dyn std::error::Error>> {
        let mut addr = Vec::new();
        let target = cmp::min(safe_block, self.last_indexed_block);
        for n in self.last_committed_block + 1..=target {
            match self.pending.remove(&n) {
                Some(mut a) => {
                    addr.append(&mut a);
                }
                None => {
                    println!("no addresses to commit for block {}", n);
                    break;
                }
            }
        }
        let len = addr.len();
        if len > 0 {
            self.write(safe_block, addr)?;
        }
        self.last_committed_block = target;
        Ok(len)
    }

    fn write(
        &mut self,
        block_number: u64,
        addresses: Vec<Address>,
    ) -> Result<usize, Box<dyn std::error::Error>> {
        if block_number <= self.last_committed_block {
            return Err(format!(
                "unexpected block number {} (last block indexed was {})",
                block_number, self.last_committed_block
            )
            .into());
        }
        let index_len = self.index.lock()?.len().to_be_bytes();
        let mut batch = WriteBatchWithTransaction::<false>::default();
        {
            for address in addresses {
                batch.put(address, index_len);
            }
        }
        batch.put("last_block".as_bytes(), block_number.to_be_bytes());
        let len = batch.len() - 1;
        self.db.write(batch)?;
        Ok(len)
    }
}
