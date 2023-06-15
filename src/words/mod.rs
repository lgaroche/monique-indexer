mod list;

use bitvec::prelude::*;
use ethers::{types::Address, utils::keccak256};
use std::error::Error;

use crate::words::list::ENGLISH;

type Result<T> = std::result::Result<T, Box<dyn Error>>;

#[derive(Debug)]
pub struct WordError;

impl std::fmt::Display for WordError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "invalid word")
    }
}

impl Error for WordError {}

pub fn checksum(address: Address) -> u8 {
    // address checksum is the first 4 bits of the address hash
    let hash = keccak256(&address.as_bytes());
    hash[0] >> 4
}

pub fn to_words(index: u64, checksum: u8) -> String {
    // a 6-word index needs 66 bits, minus 4 bits for the checksum
    // so the maximum index for u64 is 2^62 - 1
    assert!(index < 4611686018427387904);

    let mut chunks = index
        .view_bits::<Msb0>()
        .rchunks(11)
        .map(|c| c.load::<u16>())
        .collect::<Vec<u16>>();
    let pos = chunks.len()
        - 1
        - chunks
            .iter()
            .rev()
            .position(|chunk| chunk > &0)
            .unwrap_or(0);
    let last = if chunks[pos] > 127 { pos + 1 } else { pos };
    chunks[last] = chunks[last] | (checksum as u16) << 7;
    let mut words = Vec::new();
    for i in 0..last + 1 {
        words.push(ENGLISH[chunks[last - i] as usize].to_string());
    }
    words.join(" ")
}

pub fn to_index(words: String) -> Result<(usize, u8)> {
    let mut index = 0usize;
    let mut checksum = 0u8;
    let val: Vec<Option<usize>> = words
        .split(" ")
        .map(|w| list::ENGLISH.iter().position(|&r| r == w))
        .collect();
    if val.iter().any(|&v| v.is_none()) {
        return Err(WordError.into());
    }

    for (p, value) in val.iter().rev().enumerate() {
        let mut value = value.unwrap();
        if p == val.len() - 1 {
            checksum = (value >> 7) as u8;
            value = value & 0x7f;
        }
        index += value << (11 * p);
    }
    Ok((index, checksum))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_max() {
        let address = Address::from_slice(&[0xff; 20]);
        let words = to_words(4611686018427387903, checksum(address));
        assert_eq!(words, "that zoo zoo zoo zoo zoo");

        let to_i = to_index(words).unwrap();
        assert_eq!(to_i.0, 4611686018427387903);
        assert_eq!(to_i.1, checksum(address));
    }

    #[test]
    fn test_some() {
        let address = Address::from_slice(&[0xff; 20]);
        let words = to_words(262_144, checksum(address));
        assert_eq!(words, "source avoid abandon");

        let to_i = to_index(words).unwrap();
        assert_eq!(to_i.0, 262_144);
        assert_eq!(to_i.1, checksum(address));

        let address = Address::from_slice(&[0xee; 20]);
        let words = to_words(127, checksum(address));
        assert_eq!(words, "paper");

        let to_i = to_index(words).unwrap();
        assert_eq!(to_i.0, 127);
        assert_eq!(to_i.1, checksum(address));
    }
}
