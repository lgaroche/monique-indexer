use std::{
    sync::{Arc, Mutex},
    time,
};

use ethers::types::Address;
use indexmap::IndexSet;
use rocksdb::{IteratorMode, WriteBatchWithTransaction, DB};

pub struct AddressDB {
    db: DB,
    pub index: Arc<Mutex<IndexSet<Address>>>,
    pub last_block: u64,
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

        let last_block = match db.get("last_block".as_bytes())? {
            Some(block) => u64::from_be_bytes(block[0..8].try_into().unwrap()),
            None => {
                db.put("last_block".as_bytes(), 0u64.to_be_bytes())?;
                0
            }
        };

        let this = Self {
            db,
            last_block,
            index: Arc::new(Mutex::new(IndexSet::new())),
        };
        Ok(this)
    }

    pub fn build_index(&mut self) {
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

        *self.index.lock().unwrap() = index;
    }

    pub fn append(
        &mut self,
        block_number: u64,
        addresses: Vec<Address>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut index = self.index.lock().unwrap();
        if block_number <= self.last_block {
            return Err(format!(
                "block {} is before the last indexed block {}",
                block_number, self.last_block
            )
            .into());
        }

        let mut batch = WriteBatchWithTransaction::<false>::default();
        for address in addresses {
            if address == Address::zero() || index.contains(&address) {
                continue;
            }
            index.insert(address);
            batch.put(address, index.len().to_be_bytes());
        }
        batch.put("last_block".as_bytes(), block_number.to_be_bytes());
        self.db.write(batch)?;
        self.last_block = block_number;
        Ok(())
    }

    pub fn iterator(&self) -> AddressDBIterator {
        AddressDBIterator {
            inner: self.db.iterator(IteratorMode::Start),
        }
    }
}
