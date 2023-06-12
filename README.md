# Lookup

Lookup is an Ethereum address index.<br/>
Every address that is referenced in the chain (e.g. sent or received a transaction, a token...) is assigned a unique number. This number can be converted from and to a short word mnemonic for easy sharing and memorization.

There are two distinct ranges for the index:

1. <b>Immutable index</b>. Automatically calculated for every address witnessed on-chain, about 268 millions addresses can fit in 3 words, the next trillion addresses will fit in 4-words. There are currently 170 millions addresses in the index.
2. <b>Mutable index</b>. These indices are registered in a smart contract and transferrable as an NFT. They will cover the 1 and 2-word range and are more limited.

The blockchain is indexed according to the rules [defined below](#indexing-rules).

Conversion between mnemonic and index follows the BIP39 logic, with a few modifications:

- Indices are padded with 0s to fit in 7, 18, 28 or 40 bits.
- Then they are prefixed with a 4-bit checksum:
  - Up to index 2^18 (mutable index), the checksum is the last 4 bits of the hash of the index.
  - Above index 2^18 (immutable index), the checksum is the last 4 bits of the hash of associated address.

## Indexing rules

The immutable index is an <b>ordered set</b> of addresses.
To allow room for the mutable range, the first index is 262144 (2^18).<br/>
Addresses are indexed in the following order, for each block:

1. Block `author` (miner/producer)
2. For each transaction in the block:
   1. Sender (`from`)
   2. Receiver (`to`)
   3. For each log in the transaction:
      1. If the log is an ERC-20 or ERC-721 `Transfer`:
         1. Transfer `from`
         2. Transfer `to`
      2. If the log is an ERC-1155 `TransferSingle` or `TransferBatch`:
         1. Transfer `operator`
         1. Transfer `from`
         1. Transfer `to`
3. Block withdrawal recipients, in order
