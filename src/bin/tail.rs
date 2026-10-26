//! `flashblocks-tail`: print Chainlink feed updates from the Base Flashblocks stream as they appear.
//!
//! ```text
//! flashblocks-tail --feed 0x… [--feed 0x…] [--url wss://mainnet.flashblocks.base.org/ws]
//! ```
//! Each update is one JSON line: block, slice index, aggregator, answer, confirmed (from logs_bloom), tx hash and
//! the time from message arrival to the printed line in milliseconds.

use std::time::Duration;

use flashblocks::{Listener, Watch};

const DEFAULT_URL: &str = "wss://mainnet.flashblocks.base.org/ws";

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
    let mut url = DEFAULT_URL.to_string();
    let mut watch = Watch::default();
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--url" => url = args.next().expect("--url needs a value"),
            "--feed" => match parse_address(&args.next().expect("--feed needs an address")) {
                Ok(address) => watch.aggregators.push(address),
                Err(e) => {
                    eprintln!("{e}");
                    std::process::exit(2);
                }
            },
            "-h" | "--help" => {
                println!("flashblocks-tail --feed 0x<aggregator> [--feed …] [--url {DEFAULT_URL}]");
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
    let listener = Listener::start(url, watch);
    loop {
        let got = listener.receiver.lock().unwrap().recv_timeout(Duration::from_secs(30));
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
                let stats = &listener.stats;
                eprintln!(
                    "connected={} messages={} reconnects={} last_error={:?}",
                    stats.connected.load(std::sync::atomic::Ordering::Relaxed),
                    stats.messages.load(std::sync::atomic::Ordering::Relaxed),
                    stats.reconnects.load(std::sync::atomic::Ordering::Relaxed),
                    stats.last_error.lock().unwrap()
                );
            }
        }
    }
}
