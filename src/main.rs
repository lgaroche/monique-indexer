mod api;
mod db;
mod indexer;
mod words;

use api::{alias, index, resolve, stats};
use db::AddressDB;
use ethers::prelude::*;
use indexer::Indexer;
use rocket::routes;
use std::env;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[tokio::main]
async fn main() -> Result<()> {
    let command = env::args().nth(1).unwrap_or("help".to_string());

    const DEFAULT_COUNT: u64 = 5000u64;
    let count = match env::args().nth(2) {
        Some(count_arg) => count_arg.parse::<u64>().unwrap_or(DEFAULT_COUNT),
        None => DEFAULT_COUNT,
    };

    match command.as_str() {
        "help" => Ok(print_help()),
        "run" => {
            let mut indexer = init()?;
            let db = indexer.db.index.clone();

            tokio::spawn({
                async move {
                    indexer.db.build_index().expect("failed to build index");
                    loop {
                        if let Err(e) = indexer.run(count).await {
                            println!("error: {}", e);
                        }
                    }
                }
            });

            rocket::build()
                .manage(db)
                .mount("/", routes![index, resolve, stats, alias])
                .launch()
                .await?;
            Ok(())
        }
        "info" => init()?.print_info(false).await,
        "root" => init()?.print_info(true).await,
        _ => Ok(print_help()),
    }
}

fn init() -> Result<Indexer> {
    let db = AddressDB::new("db")?;
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
