//! Replays 135 messages recorded from the live Base stream (October 2026) and compares every decoded update with
//! the reference results produced by an independent Python implementation (web3.py + eth_abi).

use std::path::Path;
use std::time::Instant;

use base64::Engine;
use flashblocks::{Watch, parse};

fn address(text: &str) -> [u8; 20] {
    hex::decode(&text[2..]).unwrap().try_into().unwrap()
}

#[test]
fn recorded_stream_matches_reference_decoder() {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    let sample = std::fs::read_to_string(dir.join("base_sample.ndjson")).unwrap();
    let expected: Vec<Vec<serde_json::Value>> =
        serde_json::from_str(&std::fs::read_to_string(dir.join("base_sample_expected.json")).unwrap()).unwrap();
    let mut lines = sample.lines();
    let header: serde_json::Value = serde_json::from_str(lines.next().unwrap()).unwrap();
    let watch = Watch {
        aggregators: header["aggregators"].as_array().unwrap().iter().map(|a| address(a.as_str().unwrap())).collect(),
        ..Watch::default()
    };
    let (mut with_updates, mut without, mut updates) = ((0u32, 0f64), (0u32, 0f64), 0usize);
    for (line, want) in lines.zip(expected.iter()) {
        let row: serde_json::Value = serde_json::from_str(line).unwrap();
        let raw = base64::engine::general_purpose::STANDARD.decode(row["data"].as_str().unwrap()).unwrap();
        let started = Instant::now();
        let got = parse(&raw, row["compressed"].as_bool().unwrap(), &watch).unwrap();
        let ms = started.elapsed().as_secs_f64() * 1000.0;
        assert_eq!(got.len(), want.len(), "{row}");
        for (g, w) in got.iter().zip(want) {
            assert_eq!(format!("0x{}", hex::encode(g.aggregator)), w["aggregator"].as_str().unwrap());
            assert_eq!(format!("0x{}", hex::encode(g.tx_hash)), w["tx_hash"].as_str().unwrap());
            assert_eq!(g.block, w["block"].as_u64().unwrap());
            assert_eq!(g.index, w["index"].as_u64().unwrap());
            assert_eq!(g.confirmed, w["confirmed"].as_bool());
            assert_eq!(answer_decimal(&g.answer), w["answer"].as_str().unwrap());
        }
        updates += got.len();
        let bucket = if got.is_empty() { &mut without } else { &mut with_updates };
        bucket.0 += 1;
        bucket.1 += ms;
    }
    assert!(updates >= 9, "updates {updates}");
    eprintln!(
        "slices with updates: {} ({:.2} ms each), without: {} ({:.3} ms each)",
        with_updates.0, with_updates.1 / with_updates.0 as f64, without.0, without.1 / without.0 as f64
    );
}

fn answer_decimal(word: &[u8; 32]) -> String {
    let negative = word[0] & 0x80 != 0;
    let mut value: u128 = 0;
    for b in &word[16..] {
        value = (value << 8) | (if negative { !b } else { *b }) as u128;
    }
    if negative { format!("-{}", value + 1) } else { value.to_string() }
}
