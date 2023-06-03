use std::time;

use ethers::types::Address;
use indexmap::IndexSet;
use rocksdb::{IteratorMode, WriteBatchWithTransaction, DB};

pub struct AddressDB {
    db: DB,
    index: IndexSet<Address>,
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
            counter: 0,
            index: IndexSet::new(),
        };

        println!("reading database..."); // TODO: store address count in db
        let start = time::Instant::now();
        this.counter = this.count();
        println!(
            "loaded {} addresses in {} ms",
            this.counter,
            start.elapsed().as_millis()
        );

        let mut index: IndexSet<Address> = IndexSet::with_capacity(this.counter);

        {
            println!("building index...");
            let start = time::Instant::now();
            let mut vec = vec![Address::from([0u8; 20]); this.counter];
            for el in this.iterator() {
                vec[el.1 as usize] = el.0;
            }
            for i in 0..this.counter {
                index.insert(vec[i]);
            }
            println!("index built in {} ms", start.elapsed().as_millis());
        }

        this.index = index;

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
            if address == Address::zero() || self.index.contains(&address) {
                continue;
            }
            self.index.insert(address);
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
