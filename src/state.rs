use crate::book::OrderBook;
use crate::matching::{Fill, Order, TradingState};
use serde::Serialize;
use std::sync::RwLock;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::broadcast;

pub fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FeedStatus {
    Connecting,
    Syncing,
    Live,
    Stale,
    Reconnecting,
}

#[derive(Default, Clone, Debug, Serialize)]
pub struct FeedStats {
    pub messages: u64,
    pub reconnects: u64,
    pub resyncs: u64,
    pub gaps: u64,
    pub last_error: Option<String>,
}

pub struct FeedState {
    pub exchange: &'static str,
    pub symbol: String,
    pub status: FeedStatus,
    pub book: OrderBook,
    pub stats: FeedStats,
    pub last_update_ms: Option<u64>,
    pub last_local_ms: Option<u64>,
    pub last_seq: Option<u64>,
    pub version: u64,
}

impl FeedState {
    pub fn new(exchange: &'static str, symbol: String) -> Self {
        Self {
            exchange,
            symbol,
            status: FeedStatus::Connecting,
            book: OrderBook::default(),
            stats: FeedStats::default(),
            last_update_ms: None,
            last_local_ms: None,
            last_seq: None,
            version: 0,
        }
    }

    pub fn touch(&mut self) {
        self.version = self.version.wrapping_add(1);
    }

    pub fn set_status(&mut self, s: FeedStatus) {
        if self.status != s {
            self.status = s;
            self.touch();
        }
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TradingEvent {
    OrderUpdate { order: Order },
    Fill { fill: Fill },
}

pub struct Shared {
    pub binance: RwLock<FeedState>,
    pub coindcx: RwLock<FeedState>,
    pub trading: RwLock<TradingState>,
    pub trading_events: broadcast::Sender<TradingEvent>,
}

impl Shared {
    pub fn new(binance_symbol: String, coindcx_pair: String, initial_cash: rust_decimal::Decimal) -> Self {
        let (trading_events, _) = broadcast::channel(256);
        Self {
            binance: RwLock::new(FeedState::new("binance", binance_symbol)),
            coindcx: RwLock::new(FeedState::new("coindcx", coindcx_pair)),
            trading: RwLock::new(TradingState::new(initial_cash)),
            trading_events,
        }
    }

    pub fn feed(&self, exchange: &str) -> &RwLock<FeedState> {
        match exchange {
            "binance" => &self.binance,
            _ => &self.coindcx,
        }
    }

    pub fn emit(&self, exchange: &'static str, level: &'static str, message: impl Into<String>) {
        let message = message.into();
        match level {
            "error" => tracing::error!(exchange, "{}", message),
            "warn" => tracing::warn!(exchange, "{}", message),
            _ => tracing::info!(exchange, "{}", message),
        }
    }

    pub fn emit_trading(&self, event: TradingEvent) {
        let _ = self.trading_events.send(event);
    }
}

pub struct Backoff {
    cur_ms: u64,
    base_ms: u64,
    max_ms: u64,
}

impl Backoff {
    pub fn new(base_ms: u64, max_ms: u64) -> Self {
        Self { cur_ms: base_ms, base_ms, max_ms }
    }
    pub fn next(&mut self) -> std::time::Duration {
        let d = std::time::Duration::from_millis(self.cur_ms);
        self.cur_ms = (self.cur_ms * 2).min(self.max_ms);
        d
    }
    pub fn reset(&mut self) {
        self.cur_ms = self.base_ms;
    }
}
