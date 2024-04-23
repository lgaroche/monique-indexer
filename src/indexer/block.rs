use ethers::{
    providers::{Middleware, Provider, Ws},
    types::{Address, Block, TxHash},
};
use hex_literal::hex;
use indexmap::IndexSet;
use log::trace;

const TRANSFER_LOG: [u8; 32] =
    /* Transfer(address,address,uint256) */
    hex!("ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef");
const TRANSFERSINGLE_LOG: [u8; 32] =
    /* TransferSingle(address,address,address,uint256,uint256) */
    hex!("c3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62");
const TRANSFERBATCH_LOG: [u8; 32] =
    /* TransferBatch(address,address,address,uint256[],uint256[]) */
    hex!("4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb");

pub(crate) async fn process(
    provider: &Provider<Ws>,
    block: &Block<TxHash>,
) -> Result<Vec<Address>, Box<dyn std::error::Error + Send + Sync>> {
    let number = block.number.unwrap().as_u64();

    // add the block miner
    let mut list = IndexSet::with_capacity(500);
    list.insert(block.author.unwrap());

    if block.transactions.len() > 0 {
        let receipts = provider.get_block_receipts(number).await?;

        for tx in receipts {
            // add the tx sender
            list.insert(tx.from);
            if let Some(to) = tx.to {
                // add the tx recipient
                list.insert(to);
            } else if let Some(to) = tx.contract_address {
                // ad the created contract address
                list.insert(to);
            }
            for log in tx.logs {
                if log.topics.len() > 2 {
                    let signature = log.topics[0].to_fixed_bytes();
                    let addrs = match signature {
                        TRANSFER_LOG => vec![
                            Address::from_slice(&log.topics[1].as_bytes()[12..]), // from
                            Address::from_slice(&log.topics[2].as_bytes()[12..]), // to
                        ],
                        TRANSFERSINGLE_LOG | TRANSFERBATCH_LOG => vec![
                            Address::from_slice(&log.topics[1].as_bytes()[12..]), // operator
                            Address::from_slice(&log.topics[2].as_bytes()[12..]), // from
                            Address::from_slice(&log.topics[3].as_bytes()[12..]), // to
                        ],
                        _ => vec![],
                    };
                    for addr in addrs {
                        list.insert(addr);
                    }
                }
            }
        }
    } else {
        trace!("no transactions in block {}", number);
    }

    if let Some(withdrawals) = &block.withdrawals {
        for withdrawal in withdrawals {
            // add the withdrawal recipient
            list.insert(withdrawal.address);
        }
    }

    Ok(list.into_iter().collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ethers::{
        providers::{Middleware, Provider},
        types::{BlockId, BlockNumber},
    };
    use std::env;
    use tiny_keccak::{Hasher, Keccak};

    async fn provider() -> Result<Provider<Ws>, Box<dyn std::error::Error>> {
        let provider_env = env::var("PROVIDER_RPC_URL");
        let provider_url = match provider_env {
            Ok(provider_url) => provider_url,
            Err(_) => {
                println!(
                    "using default provider: http://localhost:8545 (set PROVIDER_RPC_URL to override)"
                );
                "http://localhost:8545".to_string()
            }
        };
        Ok(Provider::<Ws>::connect(provider_url).await?)
    }

    #[tokio::test]
    async fn test_genesis() {
        let provider = provider().await.unwrap();
        let genesis = BlockId::Number(BlockNumber::Number(0.into()));
        let block = provider.get_block(genesis).await.unwrap().unwrap();
        let addresses = process(&provider, &block).await.unwrap();
        assert_eq!(addresses.len(), 1);
        assert_eq!(addresses[0], Address::zero());
    }

    async fn multi_test(blocks: Vec<(u64, &str)>) {
        let provider = provider().await.unwrap();
        for (block, expected) in blocks {
            let block = provider
                .get_block(BlockId::Number(block.into()))
                .await
                .unwrap()
                .unwrap();
            let set = process(&provider, &block).await.unwrap();
            let mut h = Keccak::v256();
            for addr in &set {
                h.update(addr.as_bytes());
            }
            let mut hash = [0u8; 32];
            h.finalize(&mut hash);
            println!(
                "processed block {}: {} unique addresses, digest: {}",
                block.number.unwrap().as_u64(),
                set.len(),
                hex::encode(hash)
            );
            assert_eq!(&hash[..], hex::decode(expected).unwrap().as_slice());
        }
    }

    #[tokio::test]
    async fn test_multi() {
        multi_test(vec![
            (
                0,
                "5380c7b7ae81a58eb98d9c78de4a1fd7fd9535fc953ed2be602daaa41767312a",
            ),
            (
                123,
                "c627e342bba2807022514c2d522e22ba66f911653d1abcc74bd0a7868ad3cb36",
            ),
            (
                46147,
                "6f98c2dc68fbed0867534669f39046b52aae56b00b498efe5b7e7ee140cff127",
            ),
            (
                17464418,
                "6ea4a6eb22f833b1c60059c48861a49b4d71baa4bff8ffc644a69d21e2129324",
            ),
        ])
        .await;
    }
}
