pub mod api;
pub mod index;
pub mod indexer;
pub mod words;

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
