use std::sync::Arc;

use eth_trie::{EthTrie, MemoryDB, Trie};
use log::trace;

pub struct CheckpointTrie {
    trie: EthTrie<MemoryDB>,
    index: u64,
}

impl CheckpointTrie {
    pub fn new(start_index: u64) -> Self {
        let mem_db = Arc::new(MemoryDB::new(false));
        let trie = EthTrie::new(mem_db.clone());
        Self {
            trie,
            index: start_index,
        }
    }

    pub fn bulk_insert(
        &mut self,
        keys: Vec<&[u8]>,
    ) -> Result<ethers::types::H256, eth_trie::TrieError> {
        trace!("inserting {} keys for block {}", keys.len(), self.index);
        for key in keys.iter() {
            self.trie
                .insert(key, &self.index.to_be_bytes()[..])
                .unwrap();
            self.index += 1;
        }
        self.trie.root_hash()
    }
}
