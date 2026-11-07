//! Chainlink price updates from 200 ms canonical blocks.
//!
//! Base retired Flashblocks on Nov 5, 2026 (the Denim upgrade): every 200 ms there is now a complete canonical block
//! instead of a slice of one. A block header carries the block's `logsBloom`, so a header alone tells whether a watched
//! aggregator may have emitted `AnswerUpdated` in it; only then are the block's transactions read and decoded, the
//! same way slices were. The bloom then confirms each decoded answer (`AnswerUpdated(aggregator, answer)`), so no
//! receipt is needed either.

use serde_json::Value;

use crate::decode::{answer_updated_in_bloom, decode_feed_update, hex_decode, in_bloom, keccak256, FeedUpdate, Watch};

/// The block's `logsBloom` (256 bytes), if the JSON object has a well-formed one.
pub fn bloom_of(object: &Value) -> Option<[u8; 256]> {
    let bytes = hex_decode(object["logsBloom"].as_str()?)?;
    bytes.try_into().ok()
}

/// A `"0x…"` quantity as u64.
pub fn quantity(value: &Value) -> Option<u64> {
    u64::from_str_radix(value.as_str()?.strip_prefix("0x")?, 16).ok()
}

/// May this block (a header from `newHeads` or `eth_getBlockByNumber`) hold an update of a watched aggregator? The
/// `AnswerUpdated` topic and one of the aggregator addresses must both be in its bloom; blooms have no false
/// negatives, so `false` is final. A header without a bloom is worth reading.
pub fn may_have_update(header: &Value, watch: &Watch) -> bool {
    let Some(bloom) = bloom_of(header) else { return !watch.aggregators.is_empty() };
    let topic = keccak256(b"AnswerUpdated(int256,uint256,uint256)");
    in_bloom(&bloom, &topic) && watch.aggregators.iter().any(|a| in_bloom(&bloom, a))
}

/// The watched aggregators' price updates in a full block (`eth_getBlockByNumber(n, true)`), in transaction order.
pub fn block_updates(block: &Value, watch: &Watch) -> Result<Vec<FeedUpdate>, String> {
    let number = quantity(&block["number"]).ok_or("number")?;
    let bloom = bloom_of(block);
    let mut updates = Vec::new();
    for tx in block["transactions"].as_array().ok_or("transactions")? {
        let Some(input) = tx["input"].as_str().and_then(hex_decode) else { continue };
        let to = tx["to"].as_str().and_then(hex_decode).unwrap_or_default();
        let Some((aggregator, answer)) = decode_feed_update(&to, &input, &watch.aggregators) else { continue };
        let index = quantity(&tx["transactionIndex"]).unwrap_or(0);
        let tx_hash = tx["hash"].as_str().and_then(hex_decode).and_then(|h| h.try_into().ok()).unwrap_or([0u8; 32]);
        let confirmed = bloom.as_ref().map(|b| answer_updated_in_bloom(b, &aggregator, &answer));
        updates.push(FeedUpdate { aggregator, answer, block: number, index, tx_hash, confirmed });
    }
    Ok(updates)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Base mainnet block 53935243 (Nov 6, 2026): transaction 8 is forward(cbETH/USD aggregator, transmit(report))
    const FULL: &str = include_str!("../tests/fixtures/block_53935243_full.json");
    const HEADER: &str = include_str!("../tests/fixtures/block_53935243_header.json");
    const CBETH_USD: [u8; 20] = [
        0x71, 0xe0, 0x21, 0xbc, 0x2e, 0x8a, 0x70, 0x9b, 0x72, 0xac, 0x7b, 0x60, 0x36, 0xe5, 0xb2, 0xbf, 0x30, 0xf2, 0x63,
        0xd0,
    ];

    fn result(json: &str) -> Value {
        serde_json::from_str::<Value>(json).unwrap()["result"].clone()
    }

    fn watch(aggregators: Vec<[u8; 20]>) -> Watch {
        Watch { aggregators }
    }

    #[test]
    fn decodes_the_update_and_confirms_it_from_the_block_bloom() {
        let updates = block_updates(&result(FULL), &watch(vec![CBETH_USD])).unwrap();
        assert_eq!(updates.len(), 1);
        let update = &updates[0];
        assert_eq!((update.aggregator, update.block, update.index), (CBETH_USD, 53_935_243, 8));
        assert_eq!(update.confirmed, Some(true)); // AnswerUpdated(cbETH/USD, this exact answer) is in the bloom
        assert_ne!(update.answer, [0u8; 32]);
        assert_ne!(update.tx_hash, [0u8; 32]);
    }

    #[test]
    fn header_bloom_decides_whether_to_read_the_block() {
        assert!(may_have_update(&result(HEADER), &watch(vec![CBETH_USD])));
        assert!(!may_have_update(&result(HEADER), &watch(vec![[0x11; 20]])));
        assert!(!may_have_update(&result(HEADER), &Watch::default()));
    }

    #[test]
    fn unwatched_aggregators_give_nothing() {
        assert_eq!(block_updates(&result(FULL), &watch(vec![[0x11; 20]])).unwrap(), vec![]);
    }

    #[test]
    fn quantities_parse() {
        assert_eq!(quantity(&Value::String("0x336ae8b".into())), Some(53_915_275));
        assert_eq!(quantity(&Value::String("12".into())), None);
    }
}
