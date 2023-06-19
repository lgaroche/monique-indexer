use crate::db::AddressDB;
use ethers::prelude::*;
use patricia_merkle_tree::PatriciaMerkleTree;
use sha3::Keccak256;
use std::time;

mod block;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

pub struct Indexer {
    pub db: AddressDB,
    provider: Provider<Http>,
}

#[derive(Debug)]
pub struct Info {
    pub last_node_block: u64,
    pub last_db_block: u64,
    pub unique_addresses: usize,
    pub root: Option<String>,
}

impl Indexer {
    pub fn new(db: AddressDB, provider: Provider<Http>) -> Self {
        Self { db, provider }
    }

    pub async fn info(&self, compute_root: bool) -> Result<Info> {
        let last_node_block = self.provider.get_block_number().await?;
        let last_db_block = self.db.last_block;
        let progress = (10_000 * last_db_block / last_node_block.as_u64()) as f64 / 100.0;
        let addr_count = self.db.index.len()?;
        println!("indexing stats: {last_db_block}/{last_node_block} [{progress}%] [{addr_count}]");
        let root = if compute_root {
            let root = self.compute_merkle_root()?;
            println!("merkle root: {}", root);
            Some(root)
        } else {
            None
        };
        Ok(Info {
            last_node_block: last_node_block.as_u64(),
            last_db_block,
            unique_addresses: addr_count,
            root,
        })
    }

    pub async fn run(&mut self) -> Result<()> {
        let start = self.db.last_block + 1;
        let mut log_time = time::Instant::now();
        let mut last_count = self.db.index.len()?;
        let mut last_block = start;
        let mut times = time::Instant::now();

        let info = self.info(false).await?;
        println!(
            "there are {} blocks to catch up",
            info.last_node_block - info.last_db_block
        );

        for block_number in (info.last_db_block + 1)..info.last_node_block {
            let set = block::process(&self.provider, block_number).await?;
            self.db.append(block_number, set)?;
            if log_time.elapsed().as_secs() > 3 {
                let processed = block_number - last_block;

                // blocks per second
                let speed = processed as f64 / log_time.elapsed().as_secs_f64();
                let counter = self.db.index.len()?;
                println!(
                    "Block: {} [{} new addresses] [{} blk/s] [{} ms]",
                    block_number,
                    counter - last_count,
                    speed.round(),
                    (times.elapsed().as_millis() as u64) / processed,
                );
                log_time = time::Instant::now();
                last_count = counter;
                last_block = block_number;
                times = time::Instant::now();
            }
        }
        self.info(false).await?;
        Ok(())
    }

    pub fn compute_merkle_root(&self) -> Result<String> {
        let mut tree = PatriciaMerkleTree::<&[u8], &[u8], Keccak256>::new();
        let size = self.db.index.len()? as usize;
        println!("computing merkle root for {} addresses", size);
        let mut v = Vec::with_capacity(size);
        {
            for (address, index) in self.db.iterator() {
                v.push((address.to_fixed_bytes(), index.to_be_bytes()));
            }
        }
        for i in 0..v.len() {
            tree.insert(&v[i].0, &v[i].1);
        }
        Ok(hex::encode(tree.compute_hash()))
    }
}
