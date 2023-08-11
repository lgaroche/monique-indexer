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

    match command.as_str() {
        "help" => Ok(print_help()),
        "run" => {
            let mut indexer = init().await?;
            let db = indexer.db.index.clone();

            tokio::spawn({
                async move {
                    indexer.db.build_index().expect("failed to build index");
                    loop {
                        if let Err(e) = indexer.run().await {
                            println!("error: {}", e);
                            break;
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
        "info" => {
            let mut indexer = init().await?;
            indexer.db.build_index()?;
            indexer.info(false).await?;
            Ok(())
        }
        "root" => {
            let mut indexer = init().await?;
            indexer.db.build_index()?;
            indexer.info(true).await?;
            Ok(())
        }
        _ => Ok(print_help()),
    }
}

async fn init() -> Result<Indexer> {
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
    let provider = Provider::<Ws>::connect(provider_url).await?;
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
