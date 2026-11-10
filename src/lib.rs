//! Chainlink price updates on Base as they land.
//!
//! Since Denim (Nov 5, 2026) Base makes a complete canonical block every 200 ms and no longer streams Flashblocks.
//! [`BlockListener`] subscribes to block headers over any WebSocket RPC (`eth_subscribe("newHeads")`), reads a block
//! only when its `logsBloom` may hold an update of a watched aggregator, and reads blocks the subscription skipped by
//! number. Each update is decoded from the transaction itself:
//!
//! * `forward(aggregator, transmit(report))` through a Chainlink forwarder, or a direct `transmit(report)`,
//! * the new answer as an OCR2 aggregator computes it (median of the report's observations),
//! * confirmed from the bloom (`AnswerUpdated(aggregator, answer)`), so no receipt round-trip is needed.
//!
//! [`Listener`] and [`parse`] still decode a Flashblocks stream for chains or nodes that serve one.
//!
//! ```no_run
//! use flashblocks::{BlockListener, Watch};
//!
//! let watch = Watch { aggregators: vec![/* 20-byte aggregator addresses */] };
//! let listener = BlockListener::start("wss://base-rpc.publicnode.com".into(), watch);
//! while let Ok(update) = listener.receiver.lock().unwrap().recv() {
//!     println!("{:?}", update.update);
//! }
//! ```

pub mod block_listener;
pub mod blocks;
pub mod decode;
pub mod listener;

#[cfg(feature = "python")]
mod python;

pub use block_listener::BlockListener;
pub use decode::{FeedUpdate, Watch, parse};
pub use listener::{Listener, Stats, Update};
