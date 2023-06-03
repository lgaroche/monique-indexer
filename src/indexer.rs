use std::{cmp, time};

use crate::db::AddressDB;
use ethers::prelude::*;
use hex;

use patricia_merkle_tree::PatriciaMerkleTree;
use sha3::Keccak256;

pub struct Indexer {
    db: AddressDB,
    provider: Provider<Http>,
}

impl Indexer {
    pub fn new(db: AddressDB, provider: Provider<Http>) -> Self {
        Self { db, provider }
    }

    pub async fn print_info(&self, compute_root: bool) -> Result<(), Box<dyn std::error::Error>> {
        let last_block = self.provider.get_block_number().await?;
        println!("last block known by node: {}", last_block);
        println!(
            "last block in db: {} [{}%]",
            self.db.last_block,
            (10_000 * self.db.last_block / last_block.as_u64()) as f64 / 100.0
        );
        println!("unique address count: {}", self.db.counter);
        if compute_root {
            let root = self.compute_merkle_root();
            println!("merkle root: {}", root);
        }
        Ok(())
    }

    pub async fn run(&mut self, count: u64) -> Result<(), Box<dyn std::error::Error>> {
        let start = cmp::max(self.db.last_block + 1, 46147);
        let mut log_time = time::Instant::now();
        let mut last_count = self.db.counter;
        let mut last_block = start;
        let mut times = Vec::with_capacity(count as usize);

        for block_number in start..start + count {
            times.push(self.process_block(block_number).await?);
            if log_time.elapsed().as_secs() > 3 {
                // rpc call time and processing time
                let mut rpc_time = 0;
                let mut total_time = 0;
                for t in times.as_slice() {
                    rpc_time += t.0;
                    total_time += t.1;
                }
                times.clear();

                // blocks per second
                let speed = (block_number - last_block) as f64 / log_time.elapsed().as_secs_f64();
                println!(
                    "Block: {} [{} new addresses] [{} blk/s] [rpc: {} ms] [total: {} ms]",
                    block_number,
                    self.db.counter - last_count,
                    speed.round(),
                    rpc_time as u64 / 1000,
                    total_time as u64 / 1000
                );
                log_time = time::Instant::now();
                last_count = self.db.counter;
                last_block = block_number;
            }
        }
        self.print_info(false).await
    }

    pub async fn process_block(
        &mut self,
        number: u64,
    ) -> Result<(u128, u128), Box<dyn std::error::Error>> {
        let start = time::Instant::now();

        let block = self
            .provider
            .get_block(number)
            .await?
            .expect("block not found");

        // add the block miner
        let mut list = Vec::with_capacity(1 + block.transactions.len() * 2);
        list.push(block.author.unwrap());

        let elapsed = if block.transactions.len() > 0 {
            let receipts = self
                .provider
                .get_block_receipts(block.number.unwrap())
                .await?;
            let elapsed = start.elapsed().as_micros();

            for tx in receipts {
                // add the tx sender
                list.push(tx.from);
                if let Some(to) = tx.to {
                    // add the tx recipient
                    list.push(to);
                } else if let Some(to) = tx.contract_address {
                    // ad the created contract address
                    list.push(to);
                }
            }
            elapsed
        } else {
            start.elapsed().as_micros()
        };

        if let Some(withdrawals) = block.withdrawals {
            for withdrawal in withdrawals {
                // add the withdrawal recipient
                list.push(withdrawal.address);
            }
        }

        self.db.append(number, list)?;

        Ok((elapsed, start.elapsed().as_micros()))
    }

    pub fn compute_merkle_root(&self) -> String {
        println!("computing merkle root for {} addresses", self.db.counter);
        let mut tree = PatriciaMerkleTree::<&[u8], &[u8], Keccak256>::new();
        let mut v = Vec::with_capacity(self.db.counter as usize);
        for (address, index) in self.db.iterator() {
            v.push((address.to_fixed_bytes(), index.to_be_bytes()));
        }
        for i in 0..v.len() {
            tree.insert(&v[i].0, &v[i].1);
        }

        hex::encode(tree.compute_hash())
    }
}
