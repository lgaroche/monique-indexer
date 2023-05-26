use std::{cmp, time};

use crate::db::AddressDB;
use ethers::prelude::*;

pub struct Indexer {
    db: AddressDB,
    provider: Provider<Http>,
}

impl Indexer {
    pub fn new(db: AddressDB, provider: Provider<Http>) -> Self {
        Self { db, provider }
    }

    pub async fn print_info(&self) -> Result<(), Box<dyn std::error::Error>> {
        let last_block = self.provider.get_block_number().await?;
        println!("last block known by node: {}", last_block);
        println!(
            "last block in db: {} [{}%]",
            self.db.last_block,
            (10_000 * self.db.last_block / last_block.as_u64()) as f64 / 100.0
        );
        println!("unique address count: {}", self.db.count());
        Ok(())
    }

    pub async fn run(&mut self, count: u64) -> Result<(), Box<dyn std::error::Error>> {
        let start = cmp::max(self.db.last_block, 46147);
        let mut log_time = time::Instant::now();
        let mut last_count = self.db.count();
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
                    self.db.count() - last_count,
                    speed.round(),
                    rpc_time as u64 / 1000,
                    total_time as u64 / 1000
                );
                log_time = time::Instant::now();
                last_count = self.db.count();
                last_block = block_number;
            }
        }
        self.print_info().await
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

        // remove duplicates but keep order
        let mut unique = Vec::with_capacity(list.len());
        for address in list {
            if !unique.contains(&address) {
                unique.push(address);
            }
        }
        self.db.append(number, unique)?;

        Ok((elapsed, start.elapsed().as_micros()))
    }
}
