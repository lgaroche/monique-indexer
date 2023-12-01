mod api;
mod index;
mod indexer;
mod words;

use ethers::prelude::*;
use index::{IndexTable, SharedIndex};
use indexer::Indexer;
use rocket::routes;
use std::{
    clone::Clone,
    env,
    sync::{Arc, RwLock},
};

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[tokio::main]
async fn main() -> Result<()> {
    let command = env::args().nth(1).unwrap_or("help".to_string());

    match command.as_str() {
        "help" => Ok(print_help()),
        "run" => {
            let mut indexer = init().await?;
            let db = indexer.db.clone();

            tokio::spawn({
                async move {
                    //indexer.db.build_index().expect("failed to build index");
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
                .mount(
                    "/",
                    routes![api::index, api::resolve, api::stats, api::alias],
                )
                .launch()
                .await?;
            Ok(())
        }
        "info" => {
            let indexer = init().await?;
            //indexer.db.build_index()?;
            indexer.info(false).await?;
            Ok(())
        }
        "root" => {
            let indexer = init().await?;
            //indexer.db.build_index()?;
            indexer.info(true).await?;
            Ok(())
        }
        _ => Ok(print_help()),
    }
}

async fn init() -> Result<Indexer> {
    let db = IndexTable::new("db".into(), 1_000_000);
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
    Ok(Indexer::new(
        SharedIndex::<Address>(Arc::new(RwLock::new(db))),
        provider,
    ))
}

fn print_help() {
    let arg0 = env::args().nth(0).unwrap_or("program".to_string());
    let program = arg0.split("/").last().unwrap();
    println!("Usage:");
    println!("  {} info", program);
    println!("  {} run [count]", program);
    println!("  {} help", program);
}
