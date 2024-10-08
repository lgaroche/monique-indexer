use crate::index::{Indexed, SharedIndex};
use crate::Result;
use ethers::{
    providers::{Middleware, Provider, StreamExt, Ws},
    types::{Address, BlockId, BlockNumber},
};
use log::{error, info, trace};
use std::time;

mod block;

pub struct Indexer {
    db: SharedIndex<20, Address>,
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

        let last_db_block = self.db.get_counters().await.last_indexed_block;
        let progress = (10_000 * last_db_block / last_node_block.as_u64()) as f64 / 100.0;
        let addr_count = self.db.len().await;
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
            let (queued, _, _, _) = self.index_block(block.number.unwrap().as_u64()).await?;
            info!(
                "Processed block {} [{}] [{} new addresses]",
                block.number.unwrap(),
                block.hash.unwrap(),
                queued
            );
            let info = self.info().await?;
            if info.safe_block > safe_block {
                let len = self.db.commit(info.safe_block).await?;
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
        let mut log_time = time::Instant::now();
        let mut times = (0usize, 0u128, 0u128, 0u128);

        let mut info = self.info().await?;
        info!(
            "There are {} blocks to catch up",
            info.last_node_block - info.last_db_block
        );

        let mut last_block = info.last_db_block + 1;
        let mut last_count = self.db.len().await;
        for block_number in last_block..=info.last_node_block {
            let (count, get_block_time, process_time, queue_time) =
                self.index_block(block_number).await?;
            times.0 += count;
            times.1 += get_block_time;
            times.2 += process_time;
            times.3 += queue_time;

            let processed = block_number - last_block;
            if times.0 > 0 && (log_time.elapsed().as_secs() > 15) {
                info = self.info().await?;
                let committed =
                    if info.safe_block > self.db.get_counters().await.last_committed_block {
                        self.db.commit(info.safe_block).await?
                    } else {
                        0
                    };

                // blocks per second
                let speed = processed as f64 / log_time.elapsed().as_secs_f64();
                let counter = self.db.len().await;
                info!(
                    "Block: {} [{} new addresses] [committed {}] [{} blk/s] [{} ms per block]",
                    block_number,
                    counter - last_count,
                    committed,
                    speed.round(),
                    (log_time.elapsed().as_millis() as u64) / processed,
                );
                info!(
                    "  get_block={}us process={}us queue={}us",
                    times.1 / times.0 as u128,
                    times.2 / times.0 as u128,
                    times.3 / times.0 as u128
                );
                log_time = time::Instant::now();
                last_count = counter;
                last_block = block_number;
                times = (0, 0, 0, 0);
            }
        }
        info = self.info().await?;
        let committed = if info.safe_block > self.db.get_counters().await.last_committed_block {
            self.db.commit(info.safe_block).await?
        } else {
            0
        };
        info!("end of catch_up: committed {}", committed);
        Ok(info)
    }

    async fn index_block(&mut self, number: u64) -> Result<(usize, u128, u128, u128)> {
        trace!("indexing block {}", number);
        let id = BlockId::Number(number.into());

        // get block
        let start = time::Instant::now();
        let block = self.provider.get_block(id).await?.expect("block not found");
        let get_block_time = start.elapsed().as_micros();

        // process block
        let start = time::Instant::now();
        let set = block::process(&self.provider, &block).await?;
        let set_len = set.len() as u128;
        let process_time = start.elapsed().as_micros();

        // queue block
        let start = time::Instant::now();
        let result = self.db.queue(block.number.unwrap().as_u64(), set).await?;
        let queue_time = start.elapsed().as_micros();

        trace!(
            "index_block={} total={}us set={} get_block={}us process={}us queue={}us",
            block.number.unwrap(),
            get_block_time + process_time + queue_time,
            set_len,
            get_block_time,
            process_time / set_len,
            queue_time / set_len
        );
        Ok((result, get_block_time, process_time, queue_time))
    }
}
