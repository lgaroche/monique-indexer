use ethers::prelude::*;
use hex_literal::hex;
use indexmap::IndexSet;

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
) -> Result<Vec<Address>, Box<dyn std::error::Error>> {
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
        println!("no transactions in block {}", number);
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
    use ethers::providers::Provider;
    use sha3::{Digest, Sha3_256};
    use std::env;

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
            let mut h = Sha3_256::new();
            for addr in &set {
                h.update(addr.as_bytes());
            }
            let f = h.finalize();
            println!(
                "processed block {}: {} unique addresses, digest: {}",
                block.number.unwrap().as_u64(),
                set.len(),
                hex::encode(f)
            );
            assert_eq!(&f[..], hex::decode(expected).unwrap().as_slice());
        }
    }

    #[tokio::test]
    async fn test_multi() {
        multi_test(vec![
            (
                0,
                "f3683c9e3da9a7f90397767215345efe3be07565f14ab80d102f50644b98fbfa",
            ),
            (
                123,
                "959164b8c17c0bd3690c6f22ba0571bc30ecd41d12acc9ad074fd4416f094e78",
            ),
            (
                46147,
                "38b086cd03f4cc3c51aa18e36ee9a09a7a561cee6c573f1affb1b72529c65e04",
            ),
            (
                17464418,
                "6b1fab2f5e062dc53b27bd50b21c4001293b723def9e1b3f93e47649d91d2509",
            ),
        ])
        .await;
    }
}
