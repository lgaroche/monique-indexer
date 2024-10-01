use clap::{arg, command, Command};
use ethers::{
    providers::{Provider, Ws},
    types::Address,
};
use log::{error, warn};
use monique::index::SharedIndex;
use monique::indexer::Indexer;
use monique::Result;
use monique::{api, index::IndexTable};
use rocket::{catchers, routes, Config};
use std::{
    clone::Clone,
    env,
    net::{IpAddr, Ipv4Addr},
    path::PathBuf,
};

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let common_args = [
        arg!(-r --"rpc-url" <PROVIDER> "JSON-RPC Provider"),
        arg!(-d --datadir <DATADIR> "Data directory")
            .required(true)
            .value_parser(clap::value_parser!(PathBuf)),
    ];

    let cmd = Command::new("monique")
        .subcommand_required(true)
        .subcommand(
            command!("run").args(
                [
                    &common_args[..],
                    &[
                        arg!(--api "Enable API server"),
                        arg!(-p --port <PORT> "API server port")
                            .value_parser(clap::value_parser!(u16)),
                        arg!(--address <ADDRESS> "API server address")
                            .value_parser(clap::value_parser!(Ipv4Addr)),
                    ][..],
                ]
                .concat(),
            ),
        )
        .subcommand(command!("info").args(&common_args));

    let matches = cmd.get_matches();
    let (command, matches) = matches.subcommand().expect("no subcommand");

    let default_provider = "ws://localhost:8546".to_string();
    let provider_url = matches
        .get_one::<String>("rpc-url")
        .unwrap_or(&default_provider);
    let datadir = matches.get_one::<PathBuf>("datadir").unwrap();

    let index_table = IndexTable::<20, Address>::new(datadir.to_path_buf(), 1_000_000).await;
    let db = SharedIndex::<20, Address>::new(index_table);

    if command == "info" {
        let provider = Provider::<Ws>::connect(provider_url).await?;
        let indexer = Indexer::new(db, provider);
        indexer.info().await?;
        return Ok(());
    }

    let api = matches.get_flag("api");
    let port = *matches.get_one::<u16>("port").unwrap_or(&8000);
    let default_address = Ipv4Addr::LOCALHOST;
    let address = matches
        .get_one::<Ipv4Addr>("address")
        .unwrap_or(&default_address);

    let _db = db.clone();
    let _provider_url = provider_url.clone();
    let indexing_loop = tokio::spawn({
        async move {
            loop {
                let provider = Provider::<Ws>::connect(_provider_url.clone())
                    .await
                    .unwrap();
                let mut indexer = Indexer::new(_db.clone(), provider);
                if let Err(e) = indexer.run().await {
                    error!("Indexer failed with error: {}", e);
                    warn!("Indexer will restart in 5 seconds...");
                    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                }
            }
        }
    });

    if !api {
        indexing_loop.await?;
        return Ok(());
    }

    let config = Config {
        port,
        address: IpAddr::V4(*address),
        ..Default::default()
    };

    rocket::custom(config)
        .manage(db)
        .mount(
            "/",
            routes![api::index, api::resolve, api::stats, api::alias],
        )
        .register("/", catchers![api::not_found, api::internal_error])
        .launch()
        .await?;
    Ok(())
}
