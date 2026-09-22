use crate::book::OrderBook;
use rust_decimal::Decimal;
use rust_decimal::prelude::Zero;
use serde::Serialize;

#[derive(Clone, Debug, Serialize)]
pub struct PricingSnapshot {
    pub exchange: String,
    pub mid_price: Option<String>,
    pub vwap_ask: Option<String>,
    pub vwap_bid: Option<String>,
    pub vwap_qty: String,
    pub spread: Option<String>,
    pub spread_bps: Option<String>,
    pub best_bid: Option<String>,
    pub best_ask: Option<String>,
    pub ts_ms: u64,
}

pub fn mid_price(book: &OrderBook) -> Option<Decimal> {
    let (bid, _) = book.best_bid()?;
    let (ask, _) = book.best_ask()?;
    Some((bid + ask) / Decimal::from(2))
}

pub fn spread(book: &OrderBook) -> Option<Decimal> {
    let (bid, _) = book.best_bid()?;
    let (ask, _) = book.best_ask()?;
    Some(ask - bid)
}

pub fn spread_bps(book: &OrderBook) -> Option<Decimal> {
    let mid = mid_price(book)?;
    let sp = spread(book)?;
    if mid.is_zero() {
        return None;
    }
    Some(sp / mid * Decimal::from(10_000))
}

fn vwap_walk(levels: &[(Decimal, Decimal)], target_qty: Decimal) -> Option<Decimal> {
    if levels.is_empty() || target_qty.is_zero() {
        return None;
    }
    let mut remaining = target_qty;
    let mut cost = Decimal::zero();
    for (price, qty) in levels {
        let fill = remaining.min(*qty);
        cost += *price * fill;
        remaining -= fill;
        if remaining.is_zero() {
            break;
        }
    }
    let filled = target_qty - remaining;
    if filled.is_zero() {
        return None;
    }
    Some(cost / filled)
}

pub fn vwap_ask(book: &OrderBook, qty: Decimal, depth: usize) -> Option<Decimal> {
    let (_, asks) = book.top(depth);
    vwap_walk(&asks, qty)
}

pub fn vwap_bid(book: &OrderBook, qty: Decimal, depth: usize) -> Option<Decimal> {
    let (bids, _) = book.top(depth);
    vwap_walk(&bids, qty)
}

pub fn snapshot(
    exchange: &str,
    book: &OrderBook,
    vwap_qty: Decimal,
    depth: usize,
    ts_ms: u64,
) -> PricingSnapshot {
    let fmt = |d: Decimal| d.normalize().to_string();
    PricingSnapshot {
        exchange: exchange.to_string(),
        mid_price: mid_price(book).map(&fmt),
        vwap_ask: vwap_ask(book, vwap_qty, depth).map(&fmt),
        vwap_bid: vwap_bid(book, vwap_qty, depth).map(&fmt),
        vwap_qty: fmt(vwap_qty),
        spread: spread(book).map(&fmt),
        spread_bps: spread_bps(book).map(|d| format!("{:.2}", d)),
        best_bid: book.best_bid().map(|(p, _)| fmt(p)),
        best_ask: book.best_ask().map(|(p, _)| fmt(p)),
        ts_ms,
    }
}

