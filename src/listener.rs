//! WebSocket listener: one persistent connection, every slice decoded on the receiving thread, updates handed over
//! through a channel. Reconnects with exponential backoff (public endpoints rate-limit frequent connects).

use std::net::TcpStream;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, Sender, channel};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use tungstenite::stream::MaybeTlsStream;
use tungstenite::{Message, WebSocket};

use crate::decode::{FeedUpdate, Watch, parse};

/// A decoded update plus the moment the message arrived (latency measurement starts here).
pub struct Update {
    pub update: FeedUpdate,
    pub received: Instant,
}

#[derive(Default)]
pub struct Stats {
    pub connected: AtomicBool,
    pub messages: AtomicU64,
    pub reconnects: AtomicU64,
    pub last_error: Mutex<String>,
}

pub struct Listener {
    /// Replace to change what is watched; takes effect from the next message.
    pub watch: Arc<Mutex<Watch>>,
    pub stats: Arc<Stats>,
    pub receiver: Mutex<Receiver<Update>>,
}

const PING_SECONDS: u64 = 20;

impl Listener {
    pub fn start(url: String, watch: Watch) -> Self {
        // rustls needs a process-wide crypto provider; installing twice is fine (the error means one is already set)
        let _ = rustls::crypto::ring::default_provider().install_default();
        let watch = Arc::new(Mutex::new(watch));
        let stats = Arc::new(Stats::default());
        let (sender, receiver) = channel();
        let (w, s) = (watch.clone(), stats.clone());
        thread::Builder::new().name("flashblocks".into()).spawn(move || run(url, w, s, sender)).expect("listener thread");
        Self { watch, stats, receiver: Mutex::new(receiver) }
    }
}

fn run(url: String, watch: Arc<Mutex<Watch>>, stats: Arc<Stats>, sender: Sender<Update>) {
    let mut delay = 5u64;
    loop {
        match connect(&url) {
            Ok(mut ws) => {
                stats.connected.store(true, Ordering::Relaxed);
                delay = 5;
                let error = read_loop(&mut ws, &watch, &stats, &sender);
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

fn connect(url: &str) -> Result<WebSocket<MaybeTlsStream<TcpStream>>, String> {
    let (ws, _) = tungstenite::connect(url).map_err(|e| format!("connect: {e}"))?;
    if let MaybeTlsStream::Rustls(stream) = ws.get_ref() {
        let _ = stream.get_ref().set_read_timeout(Some(Duration::from_secs(PING_SECONDS)));
    } else if let MaybeTlsStream::Plain(stream) = ws.get_ref() {
        let _ = stream.set_read_timeout(Some(Duration::from_secs(PING_SECONDS)));
    }
    Ok(ws)
}

/// Reads until an error; on a read timeout sends a ping (the node has been silent for PING_SECONDS).
fn read_loop(ws: &mut WebSocket<MaybeTlsStream<TcpStream>>, watch: &Mutex<Watch>, stats: &Stats,
             sender: &Sender<Update>) -> String {
    let mut idle_pings = 0u32;
    loop {
        match ws.read() {
            Ok(Message::Binary(bytes)) => {
                let received = Instant::now();
                stats.messages.fetch_add(1, Ordering::Relaxed);
                idle_pings = 0;
                let watch = watch.lock().unwrap().clone();
                if watch.aggregators.is_empty() {
                    continue;
                }
                if let Ok(updates) = parse(&bytes, true, &watch) {
                    for update in updates {
                        if sender.send(Update { update, received }).is_err() {
                            return "receiver dropped".into();
                        }
                    }
                }
            }
            Ok(Message::Text(text)) => {
                let received = Instant::now();
                stats.messages.fetch_add(1, Ordering::Relaxed);
                let watch = watch.lock().unwrap().clone();
                if let Ok(updates) = parse(text.as_bytes(), false, &watch) {
                    for update in updates {
                        let _ = sender.send(Update { update, received });
                    }
                }
            }
            Ok(Message::Close(_)) => return "server closed the connection".into(),
            Ok(_) => {}
            Err(tungstenite::Error::Io(e)) if e.kind() == std::io::ErrorKind::WouldBlock || e.kind() == std::io::ErrorKind::TimedOut => {
                idle_pings += 1;
                if idle_pings > 3 || ws.send(Message::Ping(Vec::new().into())).is_err() {
                    return format!("no messages for {} s", PING_SECONDS * idle_pings as u64);
                }
            }
            Err(e) => return format!("{e}"),
        }
    }
}
