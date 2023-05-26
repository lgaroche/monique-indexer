mod db;
mod indexer;

use db::AddressDB;
use ethers::prelude::*;
use indexer::Indexer;
use std::env;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let command = env::args().nth(1).unwrap_or("help".to_string());

    const DEFAULT_COUNT: u64 = 5000u64;
    let count = match env::args().nth(2) {
        Some(count_arg) => count_arg.parse::<u64>().unwrap_or(DEFAULT_COUNT),
        None => DEFAULT_COUNT,
    };

    match command.as_str() {
        "help" => Ok(print_help()),
        "run" => {
            let mut provider = init()?;
            println!("start indexing {} blocks", count);
            provider.run(count).await
        }
        "info" => init().expect("failed to initialize").print_info().await,
        _ => Ok(print_help()),
    }
}

fn init() -> Result<Indexer, Box<dyn std::error::Error>> {
    let db = AddressDB::new("db")?;
    let provider_url = env::var("PROVIDER_RPC_URL").unwrap_or("http://localhost:8545".to_string());
    println!(
        "using provider: {} (use PROVIDER_RPC_URL environment variable to override)",
        provider_url
    );
    let provider = Provider::<Http>::try_from(provider_url)?;
    Ok(Indexer::new(db, provider))
}

fn print_help() {
    let arg0 = env::args().nth(0).unwrap_or("program".to_string());
    let program = arg0.split("/").last().unwrap();
    println!("Usage:");
    println!("  {} info", program);
    println!("  {} run [count]", program);
    println!("  {} help", program);
}
