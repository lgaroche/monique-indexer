use std::collections::HashSet;

use ethers::types::Address;
use rocksdb::{IteratorMode, WriteBatchWithTransaction, DB};

pub struct AddressDB {
    db: DB,
    known_set: HashSet<Address>,
    pub counter: usize,
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
        let mut this = Self {
            db,
            last_block,
            known_set: HashSet::new(),
            counter: 0,
        };

        this.known_set = this.iterator().map(|r| r.0).collect();
        this.counter = this.count();

        Ok(this)
    }

    pub fn append(
        &mut self,
        block_number: u64,
        addresses: Vec<Address>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if block_number <= self.last_block {
            return Err(format!(
                "block {} is before the last indexed block {}",
                block_number, self.last_block
            )
            .into());
        }

        let mut batch = WriteBatchWithTransaction::<false>::default();
        for address in addresses {
            if address == Address::zero() || self.known_set.contains(&address) {
                continue;
            }
            self.known_set.insert(address);
            batch.put(address, self.counter.to_be_bytes());
            self.counter += 1;
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

    fn count(&self) -> usize {
        self.iterator().count()
    }
}
