mod api;
mod index;
mod indexer;
mod words;

use clap::{arg, command, Command};
use ethers::{
    providers::{Provider, Ws},
    types::Address,
};
use index::SharedIndex;
use indexer::Indexer;
use log::error;
use rocket::{catchers, routes, Config};
use simple_logger::SimpleLogger;
use std::{
    clone::Clone,
    env,
    net::{IpAddr, Ipv4Addr},
    path::PathBuf,
};

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[tokio::main]
async fn main() -> Result<()> {
    SimpleLogger::new()
        .env()
        .with_level(log::LevelFilter::Info)
        .init()?;

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
                        arg!(-p --port <PORT> "API server port"),
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

    let db = SharedIndex::<20, Address>::new(datadir.to_path_buf(), 1_000_000);
    let provider = Provider::<Ws>::connect(provider_url).await?;
    let mut indexer = Indexer::new(db.clone(), provider);

    if command == "info" {
        indexer.info().await?;
        return Ok(());
    }

    let api = matches.get_flag("api");
    let port = *matches.get_one::<u16>("port").unwrap_or(&8000);
    let default_address = Ipv4Addr::LOCALHOST;
    let address = matches
        .get_one::<Ipv4Addr>("address")
        .unwrap_or(&default_address);

    if !api {
        if let Err(e) = indexer.run().await {
            return Err(e)?;
        }
        return Ok(());
    }

    tokio::spawn({
        async move {
            loop {
                if let Err(e) = indexer.run().await {
                    error!("{}", e);
                    break;
                }
            }
        }
    });

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
