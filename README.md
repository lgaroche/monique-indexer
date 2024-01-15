# Monique 👵

> :warning: **Work in progress**: See below for the current status

> :white_check_mark: Live demo: [https://monique.app](https://monique.app)

Monique is an Ethereum address index.<br/>
Every address that is referenced in the chain (e.g. sent or received a transaction, a token, also every contract created...) is assigned a unique number. This number can be converted from and to a short word mnemonic for easy sharing and memorization.<br/>
The API can be integrated in wallets, block explorers, and other tools where users read or write addresses.

There are two distinct ranges for the index:

1. <b>Immutable index</b>. Automatically calculated for every address witnessed on-chain, about 537 million addresses can fit in 3 words, the next trillion addresses will fit in 4-words. There are currently 255 million addresses in the index.
2. <b>Mutable index</b>. These indices will be registerable in a smart contract and transferrable as an ERC-721 NFT. They will cover the 1 and 2-word range. This is still in development.

The blockchain is indexed according to the rules [defined below](#indexing-rules).

Conversion between mnemonic and index follows the [BIP39](https://github.com/bitcoin/bips/blob/master/bip-0039.mediawiki) logic, with a few modifications:

- Indices are padded with 0s to fit in 7, 18, 28 or 40 bits.
- Then they are prefixed with a 4-bit checksum:
  - Up to index 2<sup>18</sup> (mutable index), the checksum is the last 4 bits of the hash of the index.
  - Above index 2<sup>18</sup> (immutable index), the checksum is the last 4 bits of the hash of associated address.
- The resulting number (whose bit length is a multiple of 11 bits) is split in 11-bit chunks, each chunk representing a word in the BIP39 english wordlist.

## Indexing rules

The immutable index is an <b>ordered set</b> of addresses.
To allow room for the mutable range, the first index is 262144 (2<sup>18</sup>).<br/>
Addresses are indexed in the following order, for each block:

1. Block `author` (miner/producer)
2. For each transaction in the block:
   1. Sender (`from`)
   2. Receiver (`to`)
   3. For each log in the transaction:
      1. If the log is an [ERC-20](https://eips.ethereum.org/EIPS/eip-20) or [ERC-721](https://eips.ethereum.org/EIPS/eip-721) `Transfer`:
         1. Transfer `from`
         2. Transfer `to`
      2. If the log is an [ERC-1155](https://eips.ethereum.org/EIPS/eip-1155) `TransferSingle` or `TransferBatch`:
         1. Transfer `operator`
         1. Transfer `from`
         1. Transfer `to`
3. Block `withdrawals` recipients, in order

## Things to do
- [ ] Mutable monics smart contract
- [x] Index integrity (e.g. use a trie with root hash as checkpoints)
- [ ] Rebuild index from flat db?
- [ ] Better code documentation
- [x] API docs
- [ ] Support multiple RPC endpoints type

## How to build the index

Building the index requires a full Ethereum node synced with all the block receipts history. I have tested with a local [Erigon](https://github.com/ledgerwatch/erigon) archive node and a [Reth](https://github.com/paradigmxyz/reth) node with the following pruning config:

```toml
# Reth pruning configuration
[prune.parts]
# senders_recovery pruning is optional, but may speed up the sync
# senders_recovery = { distance = 65_536 }
transaction_lookup = { distance = 65_536 }
account_history = { distance = 65_536 }
storage_history = { distance = 65_536 }
``````

It will take about 5 days to build the first index, depending on your hardware. <br />
The API will be available as soon as the indexer start but may be slow to respond during index commit to disk.

## Query the API

The indexer exposes the API on port 8000. The Monique API has 3 routes. Each route return a JSON object describing the Monic:

```json
{
  "index": "number",
  "monic": "string",
  "address": "string"
}
```

- `GET /index/:index`<br/>
   Query by index.
- `GET /alias/:address`<br/>
   Query by address.
- `GET /resolve/:monic`<br/>
   Resolve a monic.