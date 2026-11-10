//! `flashblocks-tail`: print Chainlink feed updates on Base as they appear.
//!
//! ```text
//! flashblocks-tail --feed 0x… [--feed 0x…] [--rpc wss://base-rpc.publicnode.com]
//! ```
//! Since Denim (Nov 5, 2026) Base makes a full block every 200 ms and no longer streams Flashblocks: updates come from
//! block headers (`newHeads`) and the blocks whose bloom may hold one, over any WebSocket RPC with subscriptions.
//! `--url wss://…` still reads a Flashblocks stream (a node or proxy that serves one). Each update is one JSON line:
//! block, transaction (or slice) index, aggregator, answer, confirmed (from the bloom), tx hash and the time from the
//! header's arrival to the printed line in milliseconds.

use std::sync::Mutex;
use std::sync::mpsc::Receiver;
use std::time::Duration;

use flashblocks::{BlockListener, Listener, Stats, Update, Watch};

const DEFAULT_RPC: &str = "wss://base-rpc.publicnode.com";

enum Source {
    Blocks(BlockListener),
    Slices(Listener),
}

impl Source {
    fn receiver(&self) -> &Mutex<Receiver<Update>> {
        match self {
            Source::Blocks(l) => &l.receiver,
            Source::Slices(l) => &l.receiver,
        }
    }

    fn stats(&self) -> &Stats {
        match self {
            Source::Blocks(l) => &l.stats,
            Source::Slices(l) => &l.stats,
        }
    }
}

fn parse_address(text: &str) -> Result<[u8; 20], String> {
    let bytes = hex::decode(text.strip_prefix("0x").unwrap_or(text)).map_err(|e| format!("{text}: {e}"))?;
    bytes.try_into().map_err(|_| format!("{text}: not 20 bytes"))
}

fn answer_decimal(word: &[u8; 32]) -> String {
    // int256 two's complement → decimal string without a bigint dependency: answers fit i128 in practice
    let negative = word[0] & 0x80 != 0;
    let mut magnitude = [0u8; 32];
    for (i, b) in word.iter().enumerate() {
        magnitude[i] = if negative { !b } else { *b };
    }
    let mut value: u128 = 0;
    for b in &magnitude[16..] {
        value = (value << 8) | *b as u128;
    }
    if negative {
        format!("-{}", value + 1)
    } else {
        value.to_string()
    }
}

fn main() {
    let mut rpc = DEFAULT_RPC.to_string();
    let mut slices: Option<String> = None;
    let mut watch = Watch::default();
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--rpc" => rpc = args.next().expect("--rpc needs a value"),
            "--url" => slices = Some(args.next().expect("--url needs a value")),
            "--feed" => match parse_address(&args.next().expect("--feed needs an address")) {
                Ok(address) => watch.aggregators.push(address),
                Err(e) => {
                    eprintln!("{e}");
                    std::process::exit(2);
                }
            },
            "-h" | "--help" => {
                println!("flashblocks-tail --feed 0x<aggregator> [--feed …] [--rpc {DEFAULT_RPC}] [--url <flashblocks stream>]");
                return;
            }
            other => {
                eprintln!("unknown argument {other}");
                std::process::exit(2);
            }
        }
    }
    if watch.aggregators.is_empty() {
        eprintln!("pass at least one --feed 0x<aggregator address>");
        std::process::exit(2);
    }
    let source = match slices {
        Some(url) => {
            eprintln!("note: Base stopped streaming Flashblocks on Nov 5, 2026; --url needs a node or proxy that serves one");
            Source::Slices(Listener::start(url, watch))
        }
        None => Source::Blocks(BlockListener::start(rpc, watch)),
    };
    loop {
        let got = source.receiver().lock().unwrap().recv_timeout(Duration::from_secs(30));
        match got {
            Ok(u) => {
                let f = u.update;
                println!(
                    "{{\"block\":{},\"index\":{},\"aggregator\":\"0x{}\",\"answer\":\"{}\",\"confirmed\":{},\"tx\":\"0x{}\",\"latency_ms\":{:.2}}}",
                    f.block, f.index, hex::encode(f.aggregator), answer_decimal(&f.answer),
                    f.confirmed.map(|c| c.to_string()).unwrap_or_else(|| "null".into()), hex::encode(f.tx_hash),
                    u.received.elapsed().as_secs_f64() * 1000.0
                );
            }
            Err(_) => {
                let stats = source.stats();
                let load = |c: &std::sync::atomic::AtomicU64| c.load(std::sync::atomic::Ordering::Relaxed);
                eprintln!(
                    "connected={} messages={} blocks_filled={} blocks_read={} reconnects={} last_error={:?}",
                    stats.connected.load(std::sync::atomic::Ordering::Relaxed),
                    load(&stats.messages), load(&stats.blocks_filled), load(&stats.blocks_read),
                    load(&stats.reconnects), stats.last_error.lock().unwrap()
                );
            }
        }
    }
}
