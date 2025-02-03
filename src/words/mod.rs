mod list;

use crate::words::list::ENGLISH;
use crate::Result;
use bitvec::{field::BitField, order::Msb0, view::BitView};
use log::trace;
use num_modular::ModularCoreOps;
use std::error::Error;

// Currently supporting up to 3 words. Closest prime under 2**33 is:
const MAX_INDEX: u64 = 8_589_934_583;
const LCG_A: u64 = 537_395_585;
const LCG_INVERSE: usize = 1_025_113_904;

#[derive(Debug)]
pub struct WordError;

impl std::fmt::Display for WordError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "invalid word")
    }
}

impl Error for WordError {}

pub fn to_words(index: u64) -> String {
    assert!(index < MAX_INDEX);

    // Apply a simple linear congruential generator to the index for pseudo-randomness
    let index = (index * LCG_A + 1) % MAX_INDEX;

    if index == 0 {
        return ENGLISH[0].to_string();
    }

    let words_count = if index > 0 {
        (index.ilog2() as f32 / 11.0).floor() as usize + 1
    } else {
        0
    };

    let chunks = index
        .view_bits::<Msb0>()
        .rchunks(11)
        .map(|c| c.load::<u16>())
        .collect::<Vec<u16>>();

    let mut words = Vec::new();
    for i in 0..words_count {
        words.push(ENGLISH[chunks[words_count - i - 1] as usize].to_string());
    }
    words.join(" ")
}

pub fn to_index(words: String) -> Result<usize> {
    trace!("to_index: {}", words);
    let mut index = 0usize;
    let val: Vec<Option<usize>> = words
        .split(" ")
        .map(|w| list::ENGLISH.iter().position(|&r| r == w))
        .collect();
    if val.iter().any(|&v| v.is_none()) {
        return Err(WordError.into());
    }

    for (p, value) in val.iter().rev().enumerate() {
        index += value.unwrap() << (11 * p);
    }

    trace!("to_index before reverse lcg: {}", index);
    // Reverse the linear congruential generator
    let modulus = MAX_INDEX as usize;
    let index = (index.subm(1, &modulus)).mulm(LCG_INVERSE, &modulus);
    trace!("to_index after reverse lcg: {}", index);

    Ok(index)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_match(index: u64, words: &str) {
        let w = to_words(index);
        assert_eq!(w, words);
        let i = to_index(w).unwrap();
        assert_eq!(i, index as usize);
    }

    #[test]
    fn test_start() {
        assert_match(0, "ability");
        assert_match(1, "avoid cactus core");
        assert_match(2, "cactus divorce gather");

        assert_match(7564820678, "useful that solution");
        assert_match(7564820679, "abandon");
        assert_match(7564820680, "avoid cactus coral");

        assert_match(8589934582, "useful that stove");
    }

    #[test]
    fn test_all() {
        println!("MAX_INDEX: {}", MAX_INDEX);
        for i in 0..MAX_INDEX {
            let words = to_words(i);
            let to_i = to_index(words.clone()).unwrap();
            assert_eq!(to_i, i as usize);
            if i % 1_000_000 == 0 {
                println!("{}: {}", i, words);
            }
        }
    }

    #[test]
    fn test_max() {
        let words = to_words(0);
        assert_eq!(words, "source");

        let words = to_words(4611686018427387903);
        assert_eq!(words, "that zoo zoo zoo zoo zoo");

        let to_i = to_index(words).unwrap();
        assert_eq!(to_i, 4611686018427387903);
    }

    #[test]
    fn test_some() {
        let words = to_words(262_144);
        assert_eq!(words, "source avoid abandon");

        let to_i = to_index(words).unwrap();
        assert_eq!(to_i, 262_144);

        let words = to_words(127);
        assert_eq!(words, "paper");

        let to_i = to_index(words).unwrap();
        assert_eq!(to_i, 127);
    }
}
