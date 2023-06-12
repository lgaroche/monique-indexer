use ethers::types::Address;
use indexmap::IndexSet;
use rocket::{get, State};
use std::sync::{Arc, Mutex};

#[get("/")]
pub fn index() -> &'static str {
    "Hello, world!"
}

#[get("/count")]
pub fn count(set: &State<Arc<Mutex<IndexSet<Address>>>>) -> String {
    format!("{}", set.lock().unwrap().len())
}

#[get("/resolve/<index>")]
pub fn resolve(index: u64, set: &State<Arc<Mutex<IndexSet<Address>>>>) -> String {
    format!(
        "{:?}",
        set.lock().unwrap().get_index(index as usize).unwrap()
    )
}
