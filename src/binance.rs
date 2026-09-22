use crate::book::{parse_levels, Level, OrderBook};
use crate::state::{now_ms, Backoff, FeedStatus, Shared};
use anyhow::{anyhow, Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio_tungstenite::{connect_async, tungstenite::Message};

const STALE_AFTER: Duration = Duration::from_secs(5);
const DEAD_AFTER: Duration = Duration::from_secs(30);
const MAX_BUFFER: usize = 20_000;

#[derive(Debug, Deserialize, Clone)]
pub struct DepthUpdate {
    #[serde(rename = "E")]
    pub event_time: u64,
    #[serde(rename = "U")]
    pub first_id: u64,
    #[serde(rename = "u")]
    pub last_id: u64,
    #[serde(rename = "b")]
    pub bids: Vec<(String, String)>,
    #[serde(rename = "a")]
    pub asks: Vec<(String, String)>,
}

#[derive(Debug, Deserialize)]
pub struct Snapshot {
    #[serde(rename = "lastUpdateId")]
    pub last_update_id: u64,
    pub bids: Vec<(String, String)>,
    pub asks: Vec<(String, String)>,
}

#[derive(Debug, PartialEq)]
pub enum Outcome {
    Buffered,
    Ignored,
    Applied,
}

#[derive(Debug)]
pub struct Gap {
    pub expected: u64,
    pub got: u64,
}

impl std::fmt::Display for Gap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "sequence gap: expected U={} got U={}", self.expected, self.got)
    }
}

impl std::error::Error for Gap {}

pub enum Phase {
    Buffering(Vec<DepthUpdate>),
    AwaitFirst { last_update_id: u64 },
    Live { last_u: u64 },
}

pub struct Syncer {
    pub phase: Phase,
}

impl Syncer {
    pub fn new() -> Self {
        Self { phase: Phase::Buffering(Vec::new()) }
    }

    pub fn reset(&mut self) {
        self.phase = Phase::Buffering(Vec::new());
    }

    pub fn is_live(&self) -> bool {
        matches!(self.phase, Phase::Live { .. })
    }

    pub fn buffered(&self) -> usize {
        match &self.phase {
            Phase::Buffering(b) => b.len(),
            _ => 0,
        }
    }

    fn apply(ev: &DepthUpdate, book: &mut OrderBook) -> Result<()> {
        let bids: Vec<Level> = parse_levels(&ev.bids)?;
        let asks: Vec<Level> = parse_levels(&ev.asks)?;
        book.apply_delta(&bids, &asks);
        Ok(())
    }

    pub fn on_update(&mut self, ev: DepthUpdate, book: &mut OrderBook) -> Result<Outcome> {
        match &mut self.phase {
            Phase::Buffering(buf) => {
                if buf.len() >= MAX_BUFFER {
                    return Err(anyhow!("buffer overflow while waiting for snapshot"));
                }
                buf.push(ev);
                Ok(Outcome::Buffered)
            }
            Phase::AwaitFirst { last_update_id } => {
                let id = *last_update_id;
                if ev.last_id <= id {
                    return Ok(Outcome::Ignored);
                }
                if ev.first_id > id + 1 {
                    return Err(Gap { expected: id + 1, got: ev.first_id }.into());
                }
                Self::apply(&ev, book)?;
                self.phase = Phase::Live { last_u: ev.last_id };
                Ok(Outcome::Applied)
            }
            Phase::Live { last_u } => {
                if ev.last_id <= *last_u {
                    return Ok(Outcome::Ignored);
                }
                if ev.first_id != *last_u + 1 {
                    return Err(Gap { expected: *last_u + 1, got: ev.first_id }.into());
                }
                Self::apply(&ev, book)?;
                *last_u = ev.last_id;
                Ok(Outcome::Applied)
            }
        }
    }

    pub fn on_snapshot(&mut self, snap: Snapshot, book: &mut OrderBook) -> Result<usize> {
        let buffered = match std::mem::replace(&mut self.phase, Phase::Buffering(Vec::new())) {
            Phase::Buffering(b) => b,
            _ => Vec::new(),
        };
        book.replace(parse_levels(&snap.bids)?, parse_levels(&snap.asks)?);
        self.phase = Phase::AwaitFirst { last_update_id: snap.last_update_id };
        let mut applied = 0;
        for ev in buffered {
            if self.on_update(ev, book)? == Outcome::Applied {
                applied += 1;
            }
        }
        Ok(applied)
    }
}

async fn fetch_snapshot(client: reqwest::Client, symbol: String) -> Result<Snapshot> {
    let snap = client
        .get("https://api.binance.com/api/v3/depth")
        .query(&[("symbol", symbol.as_str()), ("limit", "1000")])
        .timeout(Duration::from_secs(10))
        .send()
        .await
        .context("snapshot request")?
        .error_for_status()
        .context("snapshot status")?
        .json::<Snapshot>()
        .await
        .context("snapshot decode")?;
    Ok(snap)
}

pub async fn run(shared: Arc<Shared>, symbol: String, client: reqwest::Client) {
    let mut backoff = Backoff::new(500, 30_000);
    let mut first = true;
    loop {
        {
            let mut st = shared.binance.write().unwrap();
            st.set_status(if first { FeedStatus::Connecting } else { FeedStatus::Reconnecting });
            st.book.clear();
            st.touch();
        }
        let started = now_ms();
        match run_session(&shared, &symbol, &client, &mut backoff).await {
            Ok(()) => shared.emit("binance", "warn", "server closed the connection; reconnecting"),
            Err(e) => {
                shared.binance.write().unwrap().stats.last_error = Some(e.to_string());
                shared.emit("binance", "error", format!("session ended: {e:#}"));
            }
        }
        first = false;
        {
            let mut st = shared.binance.write().unwrap();
            st.stats.reconnects += 1;
            st.set_status(FeedStatus::Reconnecting);
        }
        if now_ms().saturating_sub(started) > 60_000 {
            backoff.reset();
        }
        let wait = backoff.next();
        shared.emit("binance", "info", format!("reconnecting in {} ms", wait.as_millis()));
        tokio::time::sleep(wait).await;
    }
}

async fn run_session(shared: &Arc<Shared>, symbol: &str, client: &reqwest::Client, backoff: &mut Backoff) -> Result<()> {
    let url = format!("wss://stream.binance.com:9443/ws/{}@depth@100ms", symbol.to_lowercase());
    let (ws, _) = connect_async(url.as_str()).await.context("websocket connect")?;
    let (mut tx, mut rx) = ws.split();
    shared.emit("binance", "info", format!("connected to {url}"));
    shared.binance.write().unwrap().set_status(FeedStatus::Syncing);

    let mut syncer = Syncer::new();
    let mut snap_task: Option<JoinHandle<Result<Snapshot>>> = Some(tokio::spawn(fetch_snapshot(client.clone(), symbol.to_string())));
    let mut snap_retry = tokio::time::interval(Duration::from_secs(2));
    snap_retry.tick().await;
    let mut need_snapshot = false;
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    let mut last_frame = now_ms();
    let mut last_depth = now_ms();

    let start_resync = |shared: &Arc<Shared>, syncer: &mut Syncer, reason: &str| {
        syncer.reset();
        let mut st = shared.binance.write().unwrap();
        st.stats.resyncs += 1;
        st.set_status(FeedStatus::Syncing);
        drop(st);
        shared.emit("binance", "warn", format!("resync triggered: {reason}"));
    };

    loop {
        tokio::select! {
            msg = rx.next() => {
                let msg = match msg {
                    Some(Ok(m)) => m,
                    Some(Err(e)) => return Err(anyhow!("websocket read: {e}")),
                    None => return Ok(()),
                };
                last_frame = now_ms();
                match msg {
                    Message::Text(txt) => {
                        let ev: DepthUpdate = match serde_json::from_str(&txt) {
                            Ok(ev) => ev,
                            Err(e) => {
                                shared.emit("binance", "warn", format!("unparseable message ({e}): {}", &txt[..txt.len().min(120)]));
                                continue;
                            }
                        };
                        last_depth = now_ms();
                        let mut st = shared.binance.write().unwrap();
                        st.stats.messages += 1;
                        let seq = ev.last_id;
                        let ts = ev.event_time;
                        match syncer.on_update(ev, &mut st.book) {
                            Ok(Outcome::Applied) => {
                                st.last_seq = Some(seq);
                                st.last_update_ms = Some(ts);
                                st.last_local_ms = Some(now_ms());
                                if st.book.is_crossed() {
                                    drop(st);
                                    start_resync(shared, &mut syncer, "book crossed after update");
                                    need_snapshot = true;
                                } else {
                                    if st.status != FeedStatus::Live {
                                        let was_stale = st.status == FeedStatus::Stale;
                                        st.set_status(FeedStatus::Live);
                                        backoff.reset();
                                        let (b, a) = st.book.depth();
                                        drop(st);
                                        if was_stale {
                                            shared.emit("binance", "info", "depth updates resumed with sequence intact");
                                        } else {
                                            shared.emit("binance", "info", format!("live: book synced ({b} bids / {a} asks)"));
                                        }
                                    } else {
                                        st.touch();
                                    }
                                }
                            }
                            Ok(Outcome::Buffered) | Ok(Outcome::Ignored) => {}
                            Err(e) => {
                                if e.downcast_ref::<Gap>().is_some() {
                                    st.stats.gaps += 1;
                                }
                                drop(st);
                                start_resync(shared, &mut syncer, &e.to_string());
                                need_snapshot = true;
                            }
                        }
                    }
                    Message::Ping(data) => {
                        tx.send(Message::Pong(data)).await.context("pong")?;
                    }
                    Message::Close(frame) => {
                        shared.emit("binance", "warn", format!("close frame: {frame:?}"));
                        return Ok(());
                    }
                    _ => {}
                }
            }
            res = async { snap_task.as_mut().unwrap().await }, if snap_task.is_some() => {
                snap_task = None;
                match res {
                    Ok(Ok(snap)) => {
                        let id = snap.last_update_id;
                        let buffered = syncer.buffered();
                        let mut st = shared.binance.write().unwrap();
                        match syncer.on_snapshot(snap, &mut st.book) {
                            Ok(applied) => {
                                st.last_seq = Some(id);
                                st.last_local_ms = Some(now_ms());
                                st.touch();
                                let live = syncer.is_live();
                                if live {
                                    st.set_status(FeedStatus::Live);
                                    backoff.reset();
                                }
                                let (b, a) = st.book.depth();
                                drop(st);
                                shared.emit("binance", "info", format!(
                                    "snapshot lastUpdateId={id} installed ({b} bids / {a} asks); {buffered} buffered, {applied} replayed{}",
                                    if live { "; live" } else { "; waiting for first diff" }
                                ));
                            }
                            Err(e) => {
                                if e.downcast_ref::<Gap>().is_some() {
                                    st.stats.gaps += 1;
                                }
                                drop(st);
                                start_resync(shared, &mut syncer, &format!("replay after snapshot failed: {e}"));
                                need_snapshot = true;
                            }
                        }
                    }
                    Ok(Err(e)) => {
                        shared.binance.write().unwrap().stats.last_error = Some(e.to_string());
                        shared.emit("binance", "error", format!("snapshot fetch failed: {e:#}; retrying"));
                        need_snapshot = true;
                    }
                    Err(e) => {
                        shared.emit("binance", "error", format!("snapshot task panicked: {e}"));
                        need_snapshot = true;
                    }
                }
            }
            _ = snap_retry.tick(), if need_snapshot && snap_task.is_none() => {
                need_snapshot = false;
                shared.emit("binance", "info", "requesting REST snapshot");
                snap_task = Some(tokio::spawn(fetch_snapshot(client.clone(), symbol.to_string())));
            }
            _ = tick.tick() => {
                let now = now_ms();
                if now.saturating_sub(last_frame) > DEAD_AFTER.as_millis() as u64 {
                    return Err(anyhow!("no frames for {}s", DEAD_AFTER.as_secs()));
                }
                let mut st = shared.binance.write().unwrap();
                if st.status == FeedStatus::Live && now.saturating_sub(last_depth) > STALE_AFTER.as_millis() as u64 {
                    st.set_status(FeedStatus::Stale);
                    drop(st);
                    shared.emit("binance", "warn", format!("no depth updates for {}s; marked stale", STALE_AFTER.as_secs()));
                } else if st.status == FeedStatus::Stale && now.saturating_sub(last_depth) <= STALE_AFTER.as_millis() as u64 {
                    st.set_status(FeedStatus::Live);
                    drop(st);
                    shared.emit("binance", "info", "depth updates resumed");
                }
            }
        }
    }
}
