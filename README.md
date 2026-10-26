# flashblocks

Decode [Base Flashblocks](https://docs.base.org/chain/flashblocks) in real time and get **Chainlink price updates
200–2000 ms before the block is sealed** — with on-the-spot confirmation from `logs_bloom`, no receipt round-trip.

Rust library, command-line tool and Python module (`pip install flashblocks`).

## Why

Base builds every 2-second block out of ten 200 ms slices and streams them publicly
(`wss://mainnet.flashblocks.base.org/ws`). Anything that reacts to an oracle price — liquidation bots, OEV solvers,
hedgers, dashboards — can act on a Chainlink update the moment its transaction lands in a slice, instead of waiting for
the block or polling an RPC. The stream is raw, though: brotli-compressed JSON with RLP-encoded signed transactions,
roughly 3.5 GB a day. This crate turns it into typed events:

```text
{"block":53118600,"index":4,"aggregator":"0x05c84a58…","answer":"229655000000","confirmed":true,"tx":"0x…","latency_ms":0.41}
```

* **Finds transmits.** Chainlink updates on Base arrive as `forward(aggregator, transmit(report))` through a forwarder,
  or as a direct `transmit(report)`. Both are decoded from the raw transaction bytes; the new answer is the median of
  the report's observations, exactly as the OCR2 aggregator computes it (verified against the aggregator on-chain).
* **Confirms without a receipt.** A slice carries the block's cumulative `logs_bloom`. If the transmit succeeded,
  `AnswerUpdated(aggregator, answer)` is in the bloom; a reverted transmit leaves nothing. Blooms have no false
  negatives, so `confirmed == false` is final. On a live stream 11 of 11 confirmations matched the receipts.
* **Fast.** Slices without a watched address are rejected by a byte search before any JSON parsing. On the recorded
  sample (135 real slices, Apple M-series): 0.55 ms for a slice with an update, 0.11 ms without. A Python
  reference implementation (web3.py + eth_abi) takes 3.0 ms and 0.9 ms on the same input.

## Command line

```bash
cargo install flashblocks
flashblocks-tail --feed 0x05c84a58fe042275b37db038baacd15f410c7bb0   # cbETH/USD aggregator on Base
```

Base documents the public `mainnet.flashblocks.base.org` endpoint as intended for infrastructure use; for anything
serious run your own [flashblocks-websocket-proxy](https://github.com/base/flashblocks-websocket-proxy) or use a node
provider that exposes the stream, and point `--url` at it. The message format is the same.

`--feed` is the **aggregator** address (what `EACAggregatorProxy.aggregator()` returns), not the proxy. Pass it as many
times as you like; `--url` overrides the stream endpoint. One JSON line per update goes to stdout, connection
statistics go to stderr when the stream is quiet.

## Rust

```rust
use flashblocks::{Listener, Watch};

let watch = Watch { aggregators: vec![cb_eth_usd], ..Watch::default() };
let listener = Listener::start("wss://mainnet.flashblocks.base.org/ws".into(), watch);
for update in listener.receiver.lock().unwrap().iter() {
    let f = update.update;
    println!("block {} slice {} feed 0x{} confirmed {:?} after {:.2} ms",
             f.block, f.index, hex::encode(f.aggregator), f.confirmed,
             update.received.elapsed().as_secs_f64() * 1000.0);
}
```

Decoding only, without the listener: `flashblocks::parse(&message, compressed, &watch)` takes one WebSocket frame
(binary = brotli) and returns `Vec<FeedUpdate>`. `Listener.watch` can be swapped at runtime.

## Python

```bash
pip install flashblocks
```

```python
import flashblocks

listener = flashblocks.Listener("wss://mainnet.flashblocks.base.org/ws",
                                aggregators=["0x05c84a58fe042275b37db038baacd15f410c7bb0"])
while True:
    got = listener.next(1000)            # (aggregator, answer, block, index, age_ms, tx_hash, confirmed) or None
    if got:
        print(got)
```

`flashblocks.parse(message, compressed, aggregators)` decodes one frame. Decoding and
waiting run with the GIL released.

## How Base orders transactions, and why slices matter

Transactions in a slice are ordered by arrival (priority fee breaks ties only among those arriving together). A bot
that reacts to the sealed block is always a full block late; a bot that reacts to the slice can land in the next
200 ms slice of the same block. The `received` timestamp in `Update` is the moment the frame arrived, so you can
measure your own end-to-end reaction time.

## Testing

`cargo test --release` replays `tests/fixtures/base_sample.ndjson` — 135 messages recorded from the live stream in
October 2026 — and checks every decoded update against results from an independent Python decoder. Selectors are
derived from the function signatures in a test, so a typo cannot silently break decoding.

## License

MIT.
