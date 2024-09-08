use crate::index::{Indexed, SharedIndex};
use crate::words;
use ethers::types::Address;
use rocket::{
    catch, get,
    response::Responder,
    serde::{json::Json, Serialize},
    Request, State,
};
use std::{error::Error, str::FromStr};

const PIVOT: usize = 0x40000;

#[derive(Responder, Serialize)]
#[serde(crate = "rocket::serde")]
pub struct ErrorDescription {
    error: String,
}

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

#[derive(Responder)]
pub enum ResolveError {
    #[response(status = 400, content_type = "json")]
    InvalidAlias(Json<ErrorDescription>),
    #[response(status = 400, content_type = "json")]
    BadAddress(Json<ErrorDescription>),
    #[response(status = 400, content_type = "json")]
    WrongChecksum(Json<ErrorDescription>),
}

impl From<Box<dyn Error + Send + Sync>> for ResolveError {
    fn from(value: Box<dyn Error + Send + Sync>) -> Self {
        Self::InvalidAlias(Json(ErrorDescription {
            error: value.to_string(),
        }))
    }
}

impl From<rustc_hex::FromHexError> for ResolveError {
    fn from(value: rustc_hex::FromHexError) -> Self {
        Self::BadAddress(Json(ErrorDescription {
            error: value.to_string(),
        }))
    }
}

type ApiResponse = Result<Option<Json<AddressInfo>>, ResolveError>;

#[catch(404)]
pub fn not_found(_: &Request) -> Json<ErrorDescription> {
    Json(ErrorDescription {
        error: "not found".to_string(),
    })
}

#[catch(500)]
pub fn internal_error(_: &Request) -> Json<ErrorDescription> {
    Json(ErrorDescription {
        error: "internal error".to_string(),
    })
}

#[get("/")]
pub fn stats(set: &State<SharedIndex<20, Address>>) -> Result<Json<Stats>, ResolveError> {
    Ok(Json(Stats {
        last_block: set.read()?.last_indexed_block,
        unique_addresses: set.read()?.len(),
    }))
}

#[get("/resolve/<alias>")]
pub fn resolve(alias: &str, set: &State<SharedIndex<20, Address>>) -> ApiResponse {
    let (index, checksum) = words::to_index(alias.to_string())?;
    if index < PIVOT {
        return Ok(None); // TODO: get mutable monics from the contract
    }
    let stored_index = index - PIVOT;
    let addr = set.read()?.get(stored_index)?;
    if let Some(addr) = addr {
        if words::checksum(addr) == checksum {
            let res = AddressInfo {
                address: addr,
                index,
                monic: alias.to_string(),
            };
            Ok(Some(Json(res)))
        } else {
            Err(ResolveError::WrongChecksum(Json(ErrorDescription {
                error: "wrong checksum".to_string(),
            })))
        }
    } else {
        Ok(None)
    }
}

#[get("/index/<index>")]
pub fn index(index: usize, set: &State<SharedIndex<20, Address>>) -> ApiResponse {
    if index < PIVOT {
        return Ok(None);
    }
    let res = set.read()?.get(index - PIVOT)?;
    let info = res.map(|addr| AddressInfo {
        address: addr,
        index,
        monic: words::to_words(index as u64, words::checksum(addr)),
    });
    Ok(info.map(Json))
}

#[get("/alias/<address>")]
pub fn alias(address: String, set: &State<SharedIndex<20, Address>>) -> ApiResponse {
    let addr = Address::from_str(address.as_str())?;
    let index = set.read()?.index(addr)?;
    let res = index.map(|index| AddressInfo {
        address: addr,
        index: index + PIVOT,
        monic: words::to_words((index + PIVOT) as u64, words::checksum(addr)),
    });
    Ok(res.map(Json))
}
