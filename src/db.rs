use ethers::types::Address;
use rocksdb::{IteratorMode, WriteBatchWithTransaction, DB};

pub struct AddressDB {
    db: DB,
    counter: u64,
    pub last_block: u64,
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
        };
        this.counter = this.count();
        Ok(this)
    }

    pub fn append(
        &mut self,
        block_number: u64,
        addresses: Vec<Address>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut batch = WriteBatchWithTransaction::<false>::default();
        for address in addresses {
            batch.put(address, (self.counter - 1).to_be_bytes());
            self.counter += 1;
        }
        batch.put("last_block".as_bytes(), block_number.to_be_bytes());
        self.db.write(batch)?;
        Ok(())
    }

    // pub fn iterate(&self) {
    //     self.db.iterator(IteratorMode::Start).for_each(|r| {
    //         if let Ok((key, _)) = r {
    //             println!("key: {}, value: {}", Address::from_slice(&key), 0);
    //         }
    //     });
    // }

    pub fn count(&self) -> u64 {
        let mut count = 0;
        self.db.iterator(IteratorMode::Start).for_each(|r| {
            if let Ok((_, _)) = r {
                count += 1;
            }
        });
        return count;
    }
}
