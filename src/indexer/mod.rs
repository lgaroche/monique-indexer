use crate::index::{Indexed, SharedIndex};
use ethers::{
    providers::{Middleware, Provider, StreamExt, Ws},
    types::{Address, BlockId, BlockNumber},
};
use log::{error, info};
use std::time;

mod block;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

pub struct Indexer {
    pub db: SharedIndex<20, Address>,
    provider: Provider<Ws>,
}

#[derive(Debug)]
pub struct Info {
    pub last_node_block: u64,
    pub safe_block: u64,
    pub last_db_block: u64,
    pub unique_addresses: usize,
}

impl Indexer {
    pub fn new(db: SharedIndex<20, Address>, provider: Provider<Ws>) -> Self {
        Self { db, provider }
    }

    pub async fn info(&self) -> Result<Info> {
        let safe_block = self
            .provider
            .get_block(BlockId::Number(BlockNumber::Safe))
            .await?
            .unwrap()
            .number
            .unwrap()
            .as_u64();

        let last_node_block = self.provider.get_block_number().await?;

        let db = self.db.read()?;
        let last_db_block = db.last_indexed_block;
        let progress = (10_000 * last_db_block / last_node_block.as_u64()) as f64 / 100.0;
        let addr_count = db.len();
        info!(
            "Indexing stats: [{last_db_block}/{last_node_block}] [{progress}%] [safe: {}] [index: {addr_count}]",
            safe_block,
        );
        Ok(Info {
            last_node_block: last_node_block.as_u64(),
            safe_block,
            last_db_block,
            unique_addresses: addr_count,
        })
    }

    pub async fn run(&mut self) -> Result<()> {
        let mut safe_block = loop {
            let info = self.catch_up().await?;
            if info.last_node_block == info.last_db_block {
                break info.safe_block;
            }
        };
        let provider = self.provider.to_owned();
        let mut stream = provider.subscribe_blocks().await?.boxed();
        while let Some(block) = stream.next().await {
            let queued = self.index_block(block.number.unwrap().as_u64()).await?;
            info!(
                "Processed block {} [{}] [{} new addresses]",
                block.number.unwrap(),
                block.hash.unwrap(),
                queued
            );
            let info = self.info().await?;
            if info.safe_block > safe_block {
                let len = self.db.lock()?.commit(info.safe_block)?;
                info!(
                    "Committed up to block {} [{} addresses]",
                    info.safe_block, len
                );
                safe_block = info.safe_block;
            }
        }

        error!("run loop exited");
        Err("run loop exited")?
    }

    pub async fn catch_up(&mut self) -> Result<Info> {
        let start = self.db.read()?.last_indexed_block + 1;
        let mut log_time = time::Instant::now();
        let mut last_count = self.db.read()?.len();
        let mut last_block = start;
        let mut times = time::Instant::now();

        let mut info = self.info().await?;
        info!(
            "There are {} blocks to catch up",
            info.last_node_block - info.last_db_block
        );

        for block_number in (info.last_db_block + 1)..=info.last_node_block {
            self.index_block(block_number).await?;
            let processed = block_number - last_block;
            if log_time.elapsed().as_secs() > 20 && processed > 0 {
                info = self.info().await?;
                let committed = if info.safe_block > self.db.read()?.last_committed_block {
                    self.db.lock()?.commit(info.safe_block)?
                } else {
                    0
                };

                // blocks per second
                let speed = processed as f64 / log_time.elapsed().as_secs_f64();
                let counter = self.db.read()?.len();
                info!(
                    "Block: {} [{} new addresses] [committed {}] [{} blk/s] [{} ms]",
                    block_number,
                    counter - last_count,
                    committed,
                    speed.round(),
                    (times.elapsed().as_millis() as u64) / processed,
                );
                log_time = time::Instant::now();
                last_count = counter;
                last_block = block_number;
                times = time::Instant::now();
            }
        }
        info = self.info().await?;
        let committed = if info.safe_block > self.db.read()?.last_committed_block {
            self.db.lock()?.commit(info.safe_block)?
        } else {
            0
        };
        info!("end of catch_up: committed {}", committed);
        Ok(info)
    }

    async fn index_block(&mut self, number: u64) -> Result<usize> {
        let id = BlockId::Number(number.into());
        let block = self.provider.get_block(id).await?.expect("block not found");
        let set = block::process(&self.provider, &block).await?;
        Ok(self.db.lock()?.queue(block.number.unwrap().as_u64(), set)?)
    }
}
