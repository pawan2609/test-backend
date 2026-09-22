use crate::book::{parse_levels, Level};
use crate::state::{now_ms, Backoff, FeedStatus, Shared};
use anyhow::{anyhow, Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio_tungstenite::{connect_async, tungstenite::Message};

const WS_URL: &str = "wss://stream.coindcx.com/socket.io/?EIO=4&transport=websocket";
const STALE_AFTER: Duration = Duration::from_secs(5);
const DEAD_AFTER: Duration = Duration::from_secs(20);
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Deserialize)]
struct OpenPacket {
    #[serde(rename = "pingInterval")]
    ping_interval: u64,
    #[serde(rename = "pingTimeout")]
    ping_timeout: u64,
}

#[derive(Debug, Deserialize)]
struct Envelope {
    event: Option<String>,
    data: serde_json::Value,
}

#[derive(Debug, Deserialize)]
struct DepthPayload {
    #[serde(rename = "E")]
    event_time: Option<u64>,
    #[serde(rename = "b", alias = "bids", default)]
    bids: Vec<(String, String)>,
    #[serde(rename = "a", alias = "asks", default)]
    asks: Vec<(String, String)>,
    #[serde(default)]
    replace: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct RestBook {
    bids: HashMap<String, String>,
    asks: HashMap<String, String>,
}

fn map_levels(m: &HashMap<String, String>) -> Result<Vec<Level>> {
    let raw: Vec<(String, String)> = m.iter().map(|(p, q)| (p.clone(), q.clone())).collect();
    parse_levels(&raw)
}

async fn fetch_rest_book(client: reqwest::Client, pair: String) -> Result<RestBook> {
    let b = client
        .get("https://public.coindcx.com/market_data/orderbook")
        .query(&[("pair", pair.as_str())])
        .timeout(Duration::from_secs(10))
        .send()
        .await
        .context("rest orderbook request")?
        .error_for_status()
        .context("rest orderbook status")?
        .json::<RestBook>()
        .await
        .context("rest orderbook decode")?;
    Ok(b)
}

pub async fn run(shared: Arc<Shared>, pair: String, client: reqwest::Client) {
    let mut backoff = Backoff::new(500, 30_000);
    let mut first = true;
    loop {
        {
            let mut st = shared.coindcx.write().unwrap();
            st.set_status(if first { FeedStatus::Connecting } else { FeedStatus::Reconnecting });
            st.touch();
        }
        let started = now_ms();
        match run_session(&shared, &pair, &client, &mut backoff).await {
            Ok(()) => shared.emit("coindcx", "warn", "server closed the connection; reconnecting"),
            Err(e) => {
                shared.coindcx.write().unwrap().stats.last_error = Some(e.to_string());
                shared.emit("coindcx", "error", format!("session ended: {e:#}"));
            }
        }
        first = false;
        {
            let mut st = shared.coindcx.write().unwrap();
            st.stats.reconnects += 1;
            st.set_status(FeedStatus::Reconnecting);
        }
        if now_ms().saturating_sub(started) > 60_000 {
            backoff.reset();
        }
        let wait = backoff.next();
        shared.emit("coindcx", "info", format!("reconnecting in {} ms", wait.as_millis()));
        tokio::time::sleep(wait).await;
    }
}

async fn run_session(shared: &Arc<Shared>, pair: &str, client: &reqwest::Client, backoff: &mut Backoff) -> Result<()> {
    let (ws, _) = connect_async(WS_URL).await.context("websocket connect")?;
    let (mut tx, mut rx) = ws.split();
    shared.emit("coindcx", "info", "transport connected; waiting for Engine.IO open packet");

    let open = tokio::time::timeout(HANDSHAKE_TIMEOUT, async {
        loop {
            match rx.next().await {
                Some(Ok(Message::Text(t))) if t.starts_with('0') => {
                    return serde_json::from_str::<OpenPacket>(&t[1..]).context("open packet");
                }
                Some(Ok(_)) => continue,
                Some(Err(e)) => return Err(anyhow!("read during handshake: {e}")),
                None => return Err(anyhow!("closed during handshake")),
            }
        }
    })
    .await
    .context("handshake timeout")??;

    tx.send(Message::Text("40".into())).await.context("send namespace connect")?;
    tokio::time::timeout(HANDSHAKE_TIMEOUT, async {
        loop {
            match rx.next().await {
                Some(Ok(Message::Text(t))) if t.starts_with("40") => return Ok(()),
                Some(Ok(Message::Text(t))) if t.starts_with("44") => return Err(anyhow!("namespace rejected: {t}")),
                Some(Ok(_)) => continue,
                Some(Err(e)) => return Err(anyhow!("read during namespace connect: {e}")),
                None => return Err(anyhow!("closed during namespace connect")),
            }
        }
    })
    .await
    .context("namespace connect timeout")??;

    let join = serde_json::json!(["join", { "channelName": pair }]);
    tx.send(Message::Text(format!("42{join}"))).await.context("send join")?;
    shared.emit("coindcx", "info", format!("joined channel {pair} (ping every {} ms)", open.ping_interval));
    shared.coindcx.write().unwrap().set_status(FeedStatus::Syncing);

    let mut seed_task: Option<JoinHandle<Result<RestBook>>> = Some(tokio::spawn(fetch_rest_book(client.clone(), pair.to_string())));
    let mut got_ws_depth = false;

    let ping_dead = Duration::from_millis(open.ping_interval + open.ping_timeout + 5_000);
    let mut last_frame = now_ms();
    let mut last_depth = now_ms();
    let mut tick = tokio::time::interval(Duration::from_secs(1));

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
                    Message::Text(t) => {
                        if t == "2" {
                            tx.send(Message::Text("3".into())).await.context("send pong")?;
                        } else if t == "1" {
                            return Ok(());
                        } else if t.starts_with("41") {
                            return Err(anyhow!("namespace disconnected by server"));
                        } else if let Some(body) = t.strip_prefix("42") {
                            let parsed: Result<(String, Envelope), _> = serde_json::from_str(body);
                            let (name, env) = match parsed {
                                Ok(v) => v,
                                Err(e) => {
                                    shared.emit("coindcx", "warn", format!("unparseable event ({e}): {}", &body[..body.len().min(120)]));
                                    continue;
                                }
                            };
                            let name = env.event.clone().unwrap_or(name);
                            if name != "depth-update" {
                                continue;
                            }
                            let payload: DepthPayload = match &env.data {
                                serde_json::Value::String(s) => serde_json::from_str(s),
                                other => serde_json::from_value(other.clone()),
                            }
                            .map_err(|e| anyhow!("depth payload decode: {e}"))?;

                            let bids = parse_levels(&payload.bids)?;
                            let asks = parse_levels(&payload.asks)?;
                            last_depth = now_ms();
                            got_ws_depth = true;
                            let mut st = shared.coindcx.write().unwrap();
                            st.stats.messages += 1;
                            if payload.replace.unwrap_or(true) {
                                st.book.replace(bids, asks);
                            } else {
                                st.book.apply_delta(&bids, &asks);
                            }
                            st.last_update_ms = payload.event_time;
                            st.last_local_ms = Some(now_ms());
                            if st.book.is_crossed() {
                                drop(st);
                                return Err(anyhow!("crossed book received from venue"));
                            }
                            if st.status != FeedStatus::Live {
                                st.set_status(FeedStatus::Live);
                                backoff.reset();
                                let (b, a) = st.book.depth();
                                drop(st);
                                shared.emit("coindcx", "info", format!("live: first socket depth applied ({b} bids / {a} asks)"));
                            } else {
                                st.touch();
                            }
                        }
                    }
                    Message::Ping(d) => {
                        tx.send(Message::Pong(d)).await.context("ws pong")?;
                    }
                    Message::Close(frame) => {
                        shared.emit("coindcx", "warn", format!("close frame: {frame:?}"));
                        return Ok(());
                    }
                    _ => {}
                }
            }
            res = async { seed_task.as_mut().unwrap().await }, if seed_task.is_some() => {
                seed_task = None;
                match res {
                    Ok(Ok(book)) => {
                        if got_ws_depth {
                            shared.emit("coindcx", "info", "REST seed arrived after socket data; ignored");
                        } else {
                            match (map_levels(&book.bids), map_levels(&book.asks)) {
                                (Ok(b), Ok(a)) => {
                                    let mut st = shared.coindcx.write().unwrap();
                                    st.book.replace(b, a);
                                    st.last_local_ms = Some(now_ms());
                                    st.touch();
                                    let (nb, na) = st.book.depth();
                                    drop(st);
                                    shared.emit("coindcx", "info", format!("seeded from REST ({nb} bids / {na} asks); waiting for socket depth"));
                                }
                                (Err(e), _) | (_, Err(e)) => shared.emit("coindcx", "warn", format!("REST seed unparsable: {e}")),
                            }
                        }
                    }
                    Ok(Err(e)) => shared.emit("coindcx", "warn", format!("REST seed failed: {e:#}")),
                    Err(e) => shared.emit("coindcx", "warn", format!("REST seed task panicked: {e}")),
                }
            }
            _ = tick.tick() => {
                let now = now_ms();
                if now.saturating_sub(last_frame) > ping_dead.as_millis() as u64 {
                    return Err(anyhow!("no frames (not even Engine.IO ping) for {} ms", ping_dead.as_millis()));
                }
                let since_depth = now.saturating_sub(last_depth);
                if since_depth > DEAD_AFTER.as_millis() as u64 {
                    return Err(anyhow!("no depth updates for {}s; forcing reconnect", DEAD_AFTER.as_secs()));
                }
                let mut st = shared.coindcx.write().unwrap();
                if st.status == FeedStatus::Live && since_depth > STALE_AFTER.as_millis() as u64 {
                    st.set_status(FeedStatus::Stale);
                    drop(st);
                    shared.emit("coindcx", "warn", format!("no depth updates for {}s; marked stale", STALE_AFTER.as_secs()));
                } else if st.status == FeedStatus::Stale && since_depth <= STALE_AFTER.as_millis() as u64 {
                    st.set_status(FeedStatus::Live);
                    drop(st);
                    shared.emit("coindcx", "info", "depth updates resumed");
                }
            }
        }
    }
}
