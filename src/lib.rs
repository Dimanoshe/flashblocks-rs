//! Decode [Base Flashblocks](https://docs.base.org/chain/flashblocks) in real time.
//!
//! Base publishes a block in ten 200 ms slices ("flashblocks") on a public WebSocket
//! (`wss://mainnet.flashblocks.base.org/ws`) before the block is sealed. Each message is a brotli-compressed JSON
//! document with the raw signed transactions of the slice and the cumulative `logs_bloom` of the block so far.
//!
//! This crate extracts **Chainlink price updates** from those slices as soon as they appear:
//!
//! * finds `forward(aggregator, transmit(report))` and direct `transmit(report)` calls to the aggregators you watch,
//! * computes the new answer the way an OCR2 aggregator does (median of the report's observations),
//! * confirms that the transmit succeeded from the slice's `logs_bloom` (`AnswerUpdated(aggregator, answer)`), so no
//!   receipt round-trip is needed.
//!
//! Decoding a slice with a price update takes well under a millisecond; slices without any watched address are
//! rejected by a byte search before the JSON is even parsed.
//!
//! ```no_run
//! use flashblocks::{decode, Listener, Watch};
//!
//! let watch = Watch { aggregators: vec![/* 20-byte aggregator addresses */], ..Watch::default() };
//! let listener = Listener::start("wss://mainnet.flashblocks.base.org/ws".into(), watch);
//! while let Ok(update) = listener.receiver.lock().unwrap().recv() {
//!     println!("{:?}", update.update);
//! }
//! # let _ = decode::parse;
//! ```

pub mod decode;
pub mod listener;

#[cfg(feature = "python")]
mod python;

pub use decode::{FeedUpdate, Watch, parse};
pub use listener::{Listener, Stats, Update};
