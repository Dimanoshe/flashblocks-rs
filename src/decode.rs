//! Flashblock message decoding: brotli → JSON → raw transactions → Chainlink OCR2 `transmit` reports.
//!
//! A message looks like `{"index": N, "metadata": {"block_number": B}, "diff": {"transactions": ["0x…"],
//! "logs_bloom": "0x…"}}`. A price update is a transaction `forward(aggregator, transmit(report))` through one of
//! Chainlink's forwarder contracts, or `transmit(report)` sent directly to the aggregator. The new answer is the
//! median of the report's observations, exactly as the aggregator computes it.

use std::io::Read;

use tiny_keccak::{Hasher, Keccak};

const FORWARD: [u8; 4] = [0x6f, 0xad, 0xcf, 0x72]; // forward(address,bytes)
const TRANSMIT: [u8; 4] = [0xb1, 0xdc, 0x65, 0xa4]; // transmit(bytes32[3],bytes,bytes32[],bytes32[],bytes32)

pub type Address = [u8; 20];
/// int192 answer as a 32-byte two's-complement big-endian word (sign and full precision preserved).
pub type Answer = [u8; 32];

/// What to look for in each slice.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Watch {
    /// Aggregator contracts whose `transmit` calls are decoded.
    pub aggregators: Vec<Address>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeedUpdate {
    pub aggregator: Address,
    pub answer: Answer,
    /// Block being built (`metadata.block_number`), 0 if absent.
    pub block: u64,
    /// Slice index inside the block (`index`).
    pub index: u64,
    /// Hash of the transmit transaction.
    pub tx_hash: [u8; 32],
    /// `Some(true)` — `AnswerUpdated(aggregator, answer)` is in the slice's `logs_bloom` (the transmit succeeded);
    /// `Some(false)` — it is not (the transmit reverted; blooms have no false negatives); `None` — the message had no
    /// `logs_bloom`, check the receipt.
    pub confirmed: Option<bool>,
}

pub fn keccak256(data: &[u8]) -> [u8; 32] {
    let mut hasher = Keccak::v256();
    let mut out = [0u8; 32];
    hasher.update(data);
    hasher.finalize(&mut out);
    out
}

/// Ethereum log bloom membership (2048 bits, three bits from the keccak of the item). No false negatives.
pub fn in_bloom(bloom: &[u8; 256], item: &[u8]) -> bool {
    let digest = keccak256(item);
    for i in [0usize, 2, 4] {
        let bit = (((digest[i] as usize) << 8) | digest[i + 1] as usize) & 2047;
        if bloom[255 - bit / 8] & (1 << (bit % 8)) == 0 {
            return false;
        }
    }
    true
}

/// Does the bloom contain `AnswerUpdated(int256 indexed current, uint256 indexed roundId, uint256)` from this
/// aggregator with this value? Address, event topic and the indexed answer are all bloom items.
pub fn answer_updated_in_bloom(bloom: &[u8; 256], aggregator: &Address, answer: &Answer) -> bool {
    let topic = keccak256(b"AnswerUpdated(int256,uint256,uint256)");
    in_bloom(bloom, aggregator) && in_bloom(bloom, &topic) && in_bloom(bloom, answer)
}

// --- RLP: just enough to split a top-level list into item slices

fn rlp_items(data: &[u8]) -> Option<Vec<&[u8]>> {
    let (payload, _) = rlp_payload(data)?;
    let mut items = Vec::new();
    let mut rest = payload;
    while !rest.is_empty() {
        let (item, consumed) = rlp_payload(rest)?;
        items.push(item);
        rest = &rest[consumed..];
    }
    Some(items)
}

/// (item payload without its header, total item length including the header)
fn rlp_payload(data: &[u8]) -> Option<(&[u8], usize)> {
    let first = *data.first()? as usize;
    let (offset, length) = match first {
        0x00..=0x7f => return Some((&data[..1], 1)),
        0x80..=0xb7 => (1, first - 0x80),
        0xb8..=0xbf => {
            let n = first - 0xb7;
            (1 + n, be_len(data.get(1..1 + n)?)?)
        }
        0xc0..=0xf7 => (1, first - 0xc0),
        _ => {
            let n = first - 0xf7;
            (1 + n, be_len(data.get(1..1 + n)?)?)
        }
    };
    let end = offset.checked_add(length)?;
    Some((data.get(offset..end)?, end))
}

fn be_len(bytes: &[u8]) -> Option<usize> {
    if bytes.len() > 8 {
        return None;
    }
    Some(bytes.iter().fold(0usize, |acc, b| (acc << 8) | *b as usize))
}

/// `to` and calldata of a signed transaction: legacy, EIP-2930 (0x01), EIP-1559 (0x02) and EIP-7702 (0x04).
/// `None` for L1 deposits (0x7e), blob transactions and anything malformed.
pub fn to_and_data(raw: &[u8]) -> Option<(&[u8], &[u8])> {
    let first = *raw.first()?;
    if first >= 0xc0 {
        let fields = rlp_items(raw)?; // legacy: [nonce, gasPrice, gas, to, value, data, v, r, s]
        return Some((fields.get(3)?, fields.get(5)?));
    }
    let (to, data) = match first {
        0x01 => (4, 6),
        0x02 | 0x04 => (5, 7),
        _ => return None,
    };
    let fields = rlp_items(&raw[1..])?;
    Some((fields.get(to)?, fields.get(data)?))
}

// --- ABI: exactly the shapes used by forward / transmit

fn word(data: &[u8], at: usize) -> Option<&[u8]> {
    data.get(at..at + 32)
}

fn word_usize(data: &[u8], at: usize) -> Option<usize> {
    let w = word(data, at)?;
    if w[..24].iter().any(|b| *b != 0) {
        return None;
    }
    Some(w[24..].iter().fold(0usize, |acc, b| (acc << 8) | *b as usize))
}

/// Dynamic `bytes` whose offset is stored in the head at `head_offset`.
fn dynamic_bytes(data: &[u8], head_offset: usize) -> Option<&[u8]> {
    let at = word_usize(data, head_offset)?;
    let len = word_usize(data, at)?;
    data.get(at + 32..at + 32 + len)
}

fn abi_address(data: &[u8], at: usize) -> Option<Address> {
    let w = word(data, at)?;
    if w[..12].iter().any(|b| *b != 0) {
        return None;
    }
    let mut out = [0u8; 20];
    out.copy_from_slice(&w[12..]);
    Some(out)
}

/// Median observation of an OCR2 report `(uint32, bytes32, int192[], int192)`: `observations[len / 2]`.
fn report_median(report: &[u8]) -> Option<Answer> {
    let at = word_usize(report, 64)?; // offset of int192[] (third head element)
    let len = word_usize(report, at)?;
    if len == 0 {
        return None;
    }
    let mut out = [0u8; 32];
    out.copy_from_slice(word(report, at + 32 + 32 * (len / 2))?);
    Some(out)
}

/// `(aggregator, new answer)` if the transaction is a transmit to one of the watched aggregators.
pub fn decode_feed_update(to: &[u8], data: &[u8], aggregators: &[Address]) -> Option<(Address, Answer)> {
    let (target, payload): (Address, &[u8]) = if data.get(..4)? == FORWARD {
        (abi_address(data, 4)?, dynamic_bytes(&data[4..], 32)?)
    } else if data.get(..4)? == TRANSMIT && to.len() == 20 {
        let mut t = [0u8; 20];
        t.copy_from_slice(to);
        (t, data)
    } else {
        return None;
    };
    if !aggregators.contains(&target) || payload.get(..4)? != TRANSMIT {
        return None;
    }
    // transmit(bytes32[3] ctx, bytes report, bytes32[] rs, bytes32[] ss, bytes32 raw): the static bytes32[3] takes
    // three head words, so the report offset is the fourth word.
    let report = dynamic_bytes(&payload[4..], 96)?;
    Some((target, report_median(report)?))
}

fn hex_nibble(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

fn hex_decode(text: &str) -> Option<Vec<u8>> {
    let bytes = text.strip_prefix("0x").unwrap_or(text).as_bytes();
    if !bytes.len().is_multiple_of(2) {
        return None;
    }
    bytes.chunks(2).map(|p| Some((hex_nibble(p[0])? << 4) | hex_nibble(p[1])?)).collect()
}

fn hex_lower(bytes: &[u8]) -> Vec<u8> {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = Vec::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(DIGITS[(b >> 4) as usize]);
        out.push(DIGITS[(b & 15) as usize]);
    }
    out
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    needle.is_empty() || memchr::memmem::find(haystack, needle).is_some()
}

/// All watched feed updates in one Flashblocks message. `compressed` — the message is a binary (brotli) frame.
///
/// Cheap path first: if none of the watched addresses occurs in the decompressed bytes, the JSON is never parsed.
/// Returns `Err` only when the message is not brotli/JSON at all.
pub fn parse(message: &[u8], compressed: bool, watch: &Watch) -> Result<Vec<FeedUpdate>, String> {
    let raw: Vec<u8> = if compressed {
        let mut out = Vec::with_capacity(message.len() * 8);
        brotli::Decompressor::new(message, 4096).read_to_end(&mut out).map_err(|e| format!("brotli: {e}"))?;
        out
    } else {
        message.to_vec()
    };
    let needles: Vec<Vec<u8>> = watch.aggregators.iter().map(|a| hex_lower(a)).collect();
    // the stream uses lower-case hex; upper-case is checked too so a formatting change cannot hide an update
    let uppers: Vec<Vec<u8>> = needles.iter().map(|n| n.to_ascii_uppercase()).collect();
    if !needles.iter().chain(uppers.iter()).any(|n| contains(&raw, n)) {
        return Ok(Vec::new());
    }
    let body: serde_json::Value = serde_json::from_slice(&raw).map_err(|e| format!("json: {e}"))?;
    let block = body["metadata"]["block_number"].as_u64().unwrap_or(0);
    let index = body["index"].as_u64().ok_or("index")?;
    let diff = &body["diff"];
    let bloom: Option<[u8; 256]> = diff["logs_bloom"].as_str().and_then(|h| {
        if h.len() != 514 {
            return None;
        }
        let bytes = hex_decode(h)?;
        let mut out = [0u8; 256];
        out.copy_from_slice(&bytes);
        Some(out)
    });
    let mut updates = Vec::new();
    for tx in diff["transactions"].as_array().ok_or("transactions")? {
        let Some(hex) = tx.as_str() else { continue };
        let tx_bytes = hex.as_bytes();
        if !needles.iter().chain(uppers.iter()).any(|n| contains(tx_bytes, n)) {
            continue;
        }
        let Some(raw_tx) = hex_decode(hex) else { continue };
        let Some((to, data)) = to_and_data(&raw_tx) else { continue };
        let Some((aggregator, answer)) = decode_feed_update(to, data, &watch.aggregators) else { continue };
        let confirmed = bloom.as_ref().map(|b| answer_updated_in_bloom(b, &aggregator, &answer));
        updates.push(FeedUpdate { aggregator, answer, block, index, tx_hash: keccak256(&raw_tx), confirmed });
    }
    Ok(updates)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rlp_list_of_strings() {
        // ["cat", "dog"] = c8 83 'c' 'a' 't' 83 'd' 'o' 'g'
        let data = [0xc8, 0x83, b'c', b'a', b't', 0x83, b'd', b'o', b'g'];
        let items = rlp_items(&data).unwrap();
        assert_eq!(items, vec![b"cat".as_slice(), b"dog".as_slice()]);
    }

    #[test]
    fn selectors_match_signatures() {
        assert_eq!(&keccak256(b"forward(address,bytes)")[..4], &FORWARD);
        assert_eq!(&keccak256(b"transmit(bytes32[3],bytes,bytes32[],bytes32[],bytes32)")[..4], &TRANSMIT);
    }

    #[test]
    fn bloom_rejects_absent_item() {
        let bloom = [0u8; 256];
        assert!(!in_bloom(&bloom, b"anything"));
    }

    #[test]
    fn empty_watch_matches_nothing() {
        let message = br#"{"index": 3, "metadata": {"block_number": 1}, "diff": {"transactions": []}}"#;
        assert_eq!(parse(message, false, &Watch::default()).unwrap(), vec![]);
    }
}
