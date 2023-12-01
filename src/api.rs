use ethers::types::Address;
use rocket::{
    get,
    response::Responder,
    serde::{json::Json, Serialize},
    State,
};
use std::{error::Error, str::FromStr};

use crate::{
    index::{Indexed, SharedIndex},
    words,
};

#[derive(Serialize)]
#[serde(crate = "rocket::serde")]
pub struct AddressInfo {
    address: Address,
    index: usize,
    monic: String,
}

#[derive(Serialize)]
#[serde(crate = "rocket::serde")]
pub struct Stats {
    last_block: u64,
    unique_addresses: usize,
}

#[derive(Responder, Serialize)]
#[serde(crate = "rocket::serde")]
pub enum ResolveError {
    #[response(status = 400)]
    InvalidAlias(String),
    #[response(status = 400)]
    BadAddress(String),
    #[response(status = 400)]
    WrongChecksum(String),
}

impl From<Box<dyn Error>> for ResolveError {
    fn from(value: Box<dyn Error>) -> Self {
        Self::InvalidAlias(value.to_string())
    }
}

impl From<rustc_hex::FromHexError> for ResolveError {
    fn from(value: rustc_hex::FromHexError) -> Self {
        Self::BadAddress(value.to_string())
    }
}

type ApiResponse = Result<Option<Json<AddressInfo>>, ResolveError>;

#[get("/")]
pub fn stats() -> Result<Json<Stats>, ResolveError> {
    Ok(Json(Stats {
        last_block: 0,
        unique_addresses: 0,
    }))
}

#[get("/resolve/<alias>")]
pub fn resolve(alias: &str, set: &State<SharedIndex<Address>>) -> ApiResponse {
    let index = words::to_index(alias.to_string())?;
    let addr = set.lock()?.get(index.0)?;
    if let Some(addr) = addr {
        if words::checksum(addr) == index.1 {
            let res = AddressInfo {
                address: addr,
                index: index.0,
                monic: alias.to_string(),
            };
            Ok(Some(Json(res)))
        } else {
            Err(ResolveError::WrongChecksum(format!(
                "wrong checksum {}",
                index.1
            )))
        }
    } else {
        Ok(None)
    }
}

#[get("/index/<index>")]
pub fn index(index: usize, set: &State<SharedIndex<Address>>) -> ApiResponse {
    let res = set.lock()?.get(index)?;
    let info = res.map(|addr| AddressInfo {
        address: addr,
        index,
        monic: words::to_words(index as u64, words::checksum(addr)),
    });
    Ok(info.map(Json))
}

#[get("/alias/<address>")]
pub fn alias(address: String, set: &State<SharedIndex<Address>>) -> ApiResponse {
    let addr = Address::from_str(address.as_str())?;
    let index = set.lock()?.index(addr)?;
    let res = index.map(|index| AddressInfo {
        address: addr,
        index,
        monic: words::to_words(index as u64, words::checksum(addr)),
    });
    Ok(res.map(Json))
}
