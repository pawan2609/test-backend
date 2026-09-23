use crate::matching::{OrderType, Side};
use crate::pricing;
use crate::state::{now_ms, FeedStats, FeedStatus, Shared, TradingEvent};
use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Path, State,
    },
    http::StatusCode,
    response::IntoResponse,
    routing::{delete, get, post},
    Json, Router,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::{net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};
use tokio::sync::broadcast::error::RecvError;
use tower_http::{
    cors::CorsLayer,
    services::{ServeDir, ServeFile},
};

#[derive(Clone)]
struct AppState {
    shared: Arc<Shared>,
    depth: usize,
}

#[derive(Serialize)]
struct BookMsg<'a> {
    #[serde(rename = "type")]
    kind: &'static str,
    exchange: &'static str,
    symbol: &'a str,
    status: FeedStatus,
    bids: Vec<(String, String)>,
    asks: Vec<(String, String)>,
    last_update_ms: Option<u64>,
    last_local_ms: Option<u64>,
    last_seq: Option<u64>,
    stats: FeedStats,
    version: u64,
    server_ms: u64,
}

fn book_msg(shared: &Shared, exchange: &str, depth: usize) -> (u64, String) {
    let st = shared.feed(exchange).read().unwrap();
    let (bids, asks) = st.book.top(depth);
    let fmt = |v: Vec<(rust_decimal::Decimal, rust_decimal::Decimal)>| {
        v.into_iter().map(|(p, q)| (p.normalize().to_string(), q.normalize().to_string())).collect::<Vec<_>>()
    };
    let msg = BookMsg {
        kind: "book",
        exchange: st.exchange,
        symbol: &st.symbol,
        status: st.status,
        bids: fmt(bids),
        asks: fmt(asks),
        last_update_ms: st.last_update_ms,
        last_local_ms: st.last_local_ms,
        last_seq: st.last_seq,
        stats: st.stats.clone(),
        version: st.version,
        server_ms: now_ms(),
    };
    (st.version, serde_json::to_string(&msg).unwrap())
}

async fn status(State(app): State<AppState>) -> impl IntoResponse {
    let mut out = serde_json::Map::new();
    for ex in ["binance", "coindcx"] {
        let (_, s) = book_msg(&app.shared, ex, app.depth);
        out.insert(ex.to_string(), serde_json::from_str(&s).unwrap());
    }
    Json(serde_json::Value::Object(out))
}

async fn get_pricing(State(app): State<AppState>) -> impl IntoResponse {
    let vwap_qty = Decimal::from(1);
    let now = now_ms();
    let binance_snap = {
        let st = app.shared.binance.read().unwrap();
        pricing::snapshot("binance", &st.book, vwap_qty, app.depth, now)
    };
    let coindcx_snap = {
        let st = app.shared.coindcx.read().unwrap();
        pricing::snapshot("coindcx", &st.book, vwap_qty, app.depth, now)
    };
    Json(serde_json::json!({
        "binance": binance_snap,
        "coindcx": coindcx_snap,
        "server_ms": now,
    }))
}

async fn get_portfolio(State(app): State<AppState>) -> impl IntoResponse {
    let ts = app.shared.trading.read().unwrap();
    Json(ts.portfolio_snapshot())
}

async fn get_orders(State(app): State<AppState>) -> impl IntoResponse {
    let ts = app.shared.trading.read().unwrap();
    Json(serde_json::json!({
        "open": ts.open_orders(),
        "history": ts.order_history(),
    }))
}

async fn get_fills(State(app): State<AppState>) -> impl IntoResponse {
    let ts = app.shared.trading.read().unwrap();
    Json(ts.all_fills())
}

#[derive(Deserialize)]
struct PlaceOrderReq {
    side: Side,
    #[serde(rename = "type")]
    order_type: OrderType,
    symbol: Option<String>,
    price: Option<String>,
    quantity: String,
}

async fn place_order(State(app): State<AppState>, Json(req): Json<PlaceOrderReq>) -> impl IntoResponse {
    let quantity: Decimal = match req.quantity.parse() {
        Ok(q) if q > Decimal::ZERO => q,
        _ => return (StatusCode::BAD_REQUEST, Json(serde_json::json!({"error": "invalid quantity"}))),
    };
    let limit_price: Option<Decimal> = match req.price {
        Some(ref p) if req.order_type == OrderType::Limit => match p.parse() {
            Ok(lp) if lp > Decimal::ZERO => Some(lp),
            _ => return (StatusCode::BAD_REQUEST, Json(serde_json::json!({"error": "invalid limit price"}))),
        },
        _ => None,
    };

    let symbol = req.symbol.unwrap_or_else(|| "BTCUSDT".to_string());
    let now = now_ms();

    let binance_book;
    let coindcx_book;
    {
        let bs = app.shared.binance.read().unwrap();
        binance_book = bs.book.clone();
    }
    {
        let cs = app.shared.coindcx.read().unwrap();
        coindcx_book = cs.book.clone();
    }

    let result = {
        let mut ts = app.shared.trading.write().unwrap();
        ts.place_order(
            req.side,
            req.order_type,
            symbol,
            limit_price,
            quantity,
            &binance_book,
            &coindcx_book,
            now,
        )
    };

    for fill in &result.new_fills {
        app.shared.emit_trading(TradingEvent::Fill { fill: fill.clone() });
    }
    app.shared.emit_trading(TradingEvent::OrderUpdate { order: result.order.clone() });

    (StatusCode::OK, Json(serde_json::to_value(&result.order).unwrap()))
}

async fn cancel_order(State(app): State<AppState>, Path(order_id): Path<String>) -> impl IntoResponse {
    let mut ts = app.shared.trading.write().unwrap();
    match ts.cancel_order(&order_id) {
        Some(order) => {
            drop(ts);
            app.shared.emit_trading(TradingEvent::OrderUpdate { order: order.clone() });
            (StatusCode::OK, Json(serde_json::to_value(&order).unwrap()))
        }
        None => (StatusCode::NOT_FOUND, Json(serde_json::json!({"error": "order not found"}))),
    }
}

async fn ws_upgrade(ws: WebSocketUpgrade, State(app): State<AppState>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_ws(socket, app))
}

async fn handle_ws(mut socket: WebSocket, app: AppState) {
    let shared = app.shared.clone();
    let hello = serde_json::json!({
        "type": "hello",
        "depth": app.depth,
        "exchanges": ["binance", "coindcx"],
        "server_ms": now_ms(),
    });
    if socket.send(Message::Text(hello.to_string())).await.is_err() {
        return;
    }

    let mut trading_events = shared.trading_events.subscribe();
    let mut tick = tokio::time::interval(Duration::from_millis(100));
    let mut last_sent: [(u64, u64); 2] = [(u64::MAX, 0); 2];
    let exchanges = ["binance", "coindcx"];

    loop {
        tokio::select! {
            _ = tick.tick() => {
                let now = now_ms();
                for (i, ex) in exchanges.iter().enumerate() {
                    let (version, json) = book_msg(&shared, ex, app.depth);
                    let (lv, lt) = last_sent[i];
                    if version != lv || now.saturating_sub(lt) >= 1000 {
                        if socket.send(Message::Text(json)).await.is_err() {
                            return;
                        }
                        last_sent[i] = (version, now);
                    }
                }

                let vwap_qty = Decimal::from(1);
                let pricing_msg = {
                    let bs = shared.binance.read().unwrap();
                    let cs = shared.coindcx.read().unwrap();
                    serde_json::json!({
                        "type": "pricing",
                        "binance": pricing::snapshot("binance", &bs.book, vwap_qty, app.depth, now),
                        "coindcx": pricing::snapshot("coindcx", &cs.book, vwap_qty, app.depth, now),
                        "server_ms": now,
                    })
                };
                if socket.send(Message::Text(pricing_msg.to_string())).await.is_err() {
                    return;
                }
            }
            tev = trading_events.recv() => {
                match tev {
                    Ok(tev) => {
                        let json = serde_json::to_string(&tev).unwrap();
                        if socket.send(Message::Text(json)).await.is_err() {
                            return;
                        }
                    }
                    Err(RecvError::Lagged(_)) => continue,
                    Err(RecvError::Closed) => return,
                }
            }
            incoming = socket.recv() => {
                match incoming {
                    Some(Ok(Message::Close(_))) | None | Some(Err(_)) => return,
                    _ => {}
                }
            }
        }
    }
}

pub async fn serve(shared: Arc<Shared>, port: u16, depth: usize, static_dir: Option<PathBuf>) -> anyhow::Result<()> {
    let app_state = AppState { shared, depth };
    let mut router = Router::new()
        .route("/api/status", get(status))
        .route("/api/pricing", get(get_pricing))
        .route("/api/portfolio", get(get_portfolio))
        .route("/api/orders", get(get_orders))
        .route("/api/fills", get(get_fills))
        .route("/api/order", post(place_order))
        .route("/api/order/:id", delete(cancel_order))
        .route("/ws", get(ws_upgrade));

    match static_dir {
        Some(dir) if dir.join("index.html").exists() => {
            tracing::info!("serving UI from {}", dir.display());
            let index = ServeFile::new(dir.join("index.html"));
            router = router.fallback_service(ServeDir::new(&dir).not_found_service(index));
        }
        Some(dir) => tracing::warn!("static dir {} has no index.html; run `npm run build` in frontend/", dir.display()),
        None => tracing::info!("no static dir configured; UI must be served separately (vite dev)"),
    }

    let router = router.layer(CorsLayer::permissive()).with_state(app_state);
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    tracing::info!("listening on http://{addr}");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, router).await?;
    Ok(())
}
