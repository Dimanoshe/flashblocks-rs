//! Listener for 200 ms blocks: `eth_subscribe("newHeads")` over one WebSocket to any Base RPC that supports
//! subscriptions. A header whose bloom may hold a watched update is read in full over the same connection, and blocks
//! the subscription skipped are read by number: public endpoints drop headers (on Base after Denim one public stream
//! delivered 59% of the blocks), and a dropped header must not mean a missed price update.

use std::collections::VecDeque;
use std::net::TcpStream;
use std::sync::atomic::Ordering;
use std::sync::mpsc::{Sender, channel};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use tungstenite::stream::MaybeTlsStream;
use tungstenite::{Message, WebSocket};

use crate::blocks::{block_updates, may_have_update, quantity};
use crate::decode::Watch;
use crate::listener::{PING_SECONDS, Stats, Update, connect};

/// Read at most this many skipped blocks after a silence (60 s at 200 ms); an update older than that is history.
pub const MAX_GAP: u64 = 300;

type Socket = WebSocket<MaybeTlsStream<TcpStream>>;

pub struct BlockListener {
    /// Replace to change what is watched; takes effect from the next block.
    pub watch: Arc<Mutex<Watch>>,
    pub stats: Arc<Stats>,
    pub receiver: Mutex<std::sync::mpsc::Receiver<Update>>,
}

impl BlockListener {
    /// `url` — a WebSocket RPC endpoint (`wss://…`) that supports `eth_subscribe`.
    pub fn start(url: String, watch: Watch) -> Self {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let watch = Arc::new(Mutex::new(watch));
        let stats = Arc::new(Stats::default());
        let (sender, receiver) = channel();
        let (w, s) = (watch.clone(), stats.clone());
        thread::Builder::new().name("blocks".into()).spawn(move || run(url, w, s, sender)).expect("listener thread");
        Self { watch, stats, receiver: Mutex::new(receiver) }
    }
}

fn run(url: String, watch: Arc<Mutex<Watch>>, stats: Arc<Stats>, sender: Sender<Update>) {
    let mut delay = 5u64;
    let mut last: Option<u64> = None; // kept across reconnects: the blocks of the outage are read by number
    loop {
        match connect(&url) {
            Ok(mut ws) => {
                stats.connected.store(true, Ordering::Relaxed);
                delay = 5;
                let mut session = Session { ws: &mut ws, next_id: 1, pending: VecDeque::new(), idle_pings: 0 };
                let error = session.serve(&watch, &stats, &sender, &mut last);
                if error == "receiver dropped" {
                    return;
                }
                *stats.last_error.lock().unwrap() = error;
            }
            Err(e) => *stats.last_error.lock().unwrap() = e,
        }
        stats.connected.store(false, Ordering::Relaxed);
        stats.reconnects.fetch_add(1, Ordering::Relaxed);
        thread::sleep(Duration::from_secs(delay));
        delay = (delay * 2).min(120);
    }
}

struct Session<'a> {
    ws: &'a mut Socket,
    next_id: u64,
    pending: VecDeque<Value>, // headers that arrived while a request was waiting for its answer
    idle_pings: u32,
}

impl Session<'_> {
    fn serve(&mut self, watch: &Mutex<Watch>, stats: &Stats, sender: &Sender<Update>, last: &mut Option<u64>)
             -> String {
        if let Err(e) = self.request("eth_subscribe", json!(["newHeads"]), stats) {
            return format!("subscribe: {e}");
        }
        loop {
            let header = match self.pending.pop_front() {
                Some(header) => header,
                None => match self.next_header(stats) {
                    Ok(header) => header,
                    Err(e) => return e,
                },
            };
            let received = Instant::now();
            let Some(number) = quantity(&header["number"]) else { continue };
            let watch = watch.lock().unwrap().clone();
            if let Some(prev) = *last {
                if number <= prev {
                    continue; // a repeat after a reconnect
                }
                for skipped in (prev + 1).max(number.saturating_sub(MAX_GAP))..number {
                    let filled = match self.request("eth_getBlockByNumber", json!([hex(skipped), false]), stats) {
                        Ok(h) => h,
                        Err(e) => return format!("block {skipped}: {e}"),
                    };
                    stats.blocks_filled.fetch_add(1, Ordering::Relaxed);
                    if let Err(e) = self.emit(&filled, skipped, &watch, stats, sender, Instant::now()) {
                        return e;
                    }
                }
            }
            if let Err(e) = self.emit(&header, number, &watch, stats, sender, received) {
                return e;
            }
            *last = Some(number);
        }
    }

    /// Reads the block in full if its bloom may hold a watched update and hands its updates over.
    fn emit(&mut self, header: &Value, number: u64, watch: &Watch, stats: &Stats, sender: &Sender<Update>,
            received: Instant) -> Result<(), String> {
        if watch.aggregators.is_empty() || !may_have_update(header, watch) {
            return Ok(());
        }
        let block = self.request("eth_getBlockByNumber", json!([hex(number), true]), stats)
            .map_err(|e| format!("block {number}: {e}"))?;
        stats.blocks_read.fetch_add(1, Ordering::Relaxed);
        for update in block_updates(&block, watch).unwrap_or_default() {
            if sender.send(Update { update, received }).is_err() {
                return Err("receiver dropped".into());
            }
        }
        Ok(())
    }

    /// A JSON-RPC call over the socket; headers arriving meanwhile are queued.
    fn request(&mut self, method: &str, params: Value, stats: &Stats) -> Result<Value, String> {
        let id = self.next_id;
        self.next_id += 1;
        let body = json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params}).to_string();
        self.ws.send(Message::Text(body.into())).map_err(|e| format!("send: {e}"))?;
        loop {
            let message = self.read_json(stats)?;
            if message["id"].as_u64() == Some(id) {
                if !message["error"].is_null() {
                    return Err(format!("{}", message["error"]));
                }
                return Ok(message["result"].clone());
            }
            if let Some(header) = notification(&message) {
                self.pending.push_back(header);
            }
        }
    }

    fn next_header(&mut self, stats: &Stats) -> Result<Value, String> {
        loop {
            if let Some(header) = notification(&self.read_json(stats)?) {
                return Ok(header);
            }
        }
    }

    /// The next JSON text message; on a read timeout sends a ping (the node has been silent for PING_SECONDS).
    fn read_json(&mut self, stats: &Stats) -> Result<Value, String> {
        loop {
            match self.ws.read() {
                Ok(Message::Text(text)) => {
                    stats.messages.fetch_add(1, Ordering::Relaxed);
                    self.idle_pings = 0;
                    if let Ok(value) = serde_json::from_str::<Value>(&text) {
                        return Ok(value);
                    }
                }
                Ok(Message::Close(_)) => return Err("server closed the connection".into()),
                Ok(_) => {}
                Err(tungstenite::Error::Io(e))
                    if e.kind() == std::io::ErrorKind::WouldBlock || e.kind() == std::io::ErrorKind::TimedOut =>
                {
                    self.idle_pings += 1;
                    if self.idle_pings > 3 || self.ws.send(Message::Ping(Vec::new().into())).is_err() {
                        return Err(format!("no messages for {} s", PING_SECONDS * self.idle_pings as u64));
                    }
                }
                Err(e) => return Err(format!("{e}")),
            }
        }
    }
}

/// The header of an `eth_subscription` notification, if the message is one.
fn notification(message: &Value) -> Option<Value> {
    (message["method"] == "eth_subscription").then(|| message["params"]["result"].clone())
}

fn hex(number: u64) -> String {
    format!("0x{number:x}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn notifications_are_told_from_answers() {
        let header = json!({"jsonrpc": "2.0", "method": "eth_subscription",
                            "params": {"subscription": "0x1", "result": {"number": "0x10"}}});
        assert_eq!(notification(&header), Some(json!({"number": "0x10"})));
        assert_eq!(notification(&json!({"jsonrpc": "2.0", "id": 3, "result": "0x1"})), None);
        assert_eq!(hex(53_935_243), "0x336fc8b");
    }
}
