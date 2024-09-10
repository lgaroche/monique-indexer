#[cfg(test)]
mod tests {
    use ethers::core::rand;
    use ethers::core::rand::Rng;
    use tempfile::tempdir;

    use crate::index::{storage::Push, Indexed, Storage};

    const TARGET_DB_SIZE: u32 = 1_000_000;
    const BATCH_SIZE: u32 = 30_000;
    const GET_ITERATIONS: u32 = 400_000;

    #[tokio::test]
    #[ignore]
    async fn benchmark() {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("benchmark-test.db");
        let mut index = Storage::<20, [u8; 20]>::new(path, 1_000_000);
        println!("start: {}", index.len().await);
        let mut gen = rand::thread_rng();
        let mut block_num = 0;
        while index.len().await < TARGET_DB_SIZE as usize {
            let mut items = Vec::new();
            let t = std::time::Instant::now();
            for _ in 0..BATCH_SIZE {
                let v = gen.gen::<[u8; 20]>();
                items.push(v);
            }
            index.push(items, block_num).await.expect("push");
            println!(
                "items: {} - {} ns",
                index.len().await,
                t.elapsed().as_nanos() / (BATCH_SIZE as u128)
            );
            block_num += 1;
        }

        let t = std::time::Instant::now();
        let mut items = Vec::new();
        for _ in 0..GET_ITERATIONS {
            let key = gen.gen::<u32>() % index.len().await as u32;
            let v = index.get(key as usize).await.expect("get");
            assert!(v.is_some());
            items.push(v.unwrap());
        }
        println!(
            "get: {:?}",
            t.elapsed().as_nanos() / (GET_ITERATIONS as u128)
        );

        let t = std::time::Instant::now();
        for i in &items {
            let key = index.index(i.clone()).await.expect("index");
            assert!(key.is_some());
        }
        println!(
            "index: {:?}",
            t.elapsed().as_nanos() / (items.len() as u128)
        );
    }
}
