use crate::book::OrderBook;
use rust_decimal::Decimal;
use rust_decimal::prelude::Zero;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Side {
    Buy,
    Sell,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderType {
    Market,
    Limit,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderStatus {
    Open,
    PartiallyFilled,
    Filled,
    Cancelled,
}

#[derive(Clone, Debug, Serialize)]
pub struct Order {
    pub id: String,
    pub side: Side,
    pub order_type: OrderType,
    pub symbol: String,
    pub price: Option<String>,
    pub quantity: String,
    pub filled_qty: String,
    pub avg_fill_price: Option<String>,
    pub status: OrderStatus,
    pub created_at: u64,
    pub updated_at: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct Fill {
    pub id: String,
    pub order_id: String,
    pub side: Side,
    pub price: String,
    pub quantity: String,
    pub exchange: String,
    pub ts_ms: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct Position {
    pub symbol: String,
    pub quantity: String,
    pub avg_cost: String,
    pub realized_pnl: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct PortfolioSnapshot {
    pub cash: String,
    pub positions: Vec<Position>,
    pub total_realized_pnl: String,
}

#[derive(Clone, Debug)]
struct PositionInner {
    qty: Decimal,
    avg_cost: Decimal,
    realized_pnl: Decimal,
}

pub struct TradingState {
    cash: Decimal,
    positions: HashMap<String, PositionInner>,
    open_orders: Vec<OrderInner>,
    order_history: Vec<Order>,
    fills: Vec<Fill>,
}

#[derive(Clone, Debug)]
struct OrderInner {
    id: String,
    side: Side,
    order_type: OrderType,
    symbol: String,
    limit_price: Option<Decimal>,
    total_qty: Decimal,
    filled_qty: Decimal,
    cost_accum: Decimal,
    status: OrderStatus,
    created_at: u64,
}

impl OrderInner {
    fn remaining(&self) -> Decimal {
        self.total_qty - self.filled_qty
    }

    fn avg_fill_price(&self) -> Option<Decimal> {
        if self.filled_qty.is_zero() {
            None
        } else {
            Some(self.cost_accum / self.filled_qty)
        }
    }

    fn to_api(&self, now: u64) -> Order {
        let fmt = |d: Decimal| d.normalize().to_string();
        Order {
            id: self.id.clone(),
            side: self.side,
            order_type: self.order_type,
            symbol: self.symbol.clone(),
            price: self.limit_price.map(&fmt),
            quantity: fmt(self.total_qty),
            filled_qty: fmt(self.filled_qty),
            avg_fill_price: self.avg_fill_price().map(&fmt),
            status: self.status,
            created_at: self.created_at,
            updated_at: now,
        }
    }
}

#[derive(Debug)]
pub struct MatchResult {
    pub order: Order,
    pub new_fills: Vec<Fill>,
}

impl TradingState {
    pub fn new(initial_cash: Decimal) -> Self {
        Self {
            cash: initial_cash,
            positions: HashMap::new(),
            open_orders: Vec::new(),
            order_history: Vec::new(),
            fills: Vec::new(),
        }
    }

    pub fn portfolio_snapshot(&self) -> PortfolioSnapshot {
        let fmt = |d: Decimal| d.normalize().to_string();
        let positions: Vec<Position> = self
            .positions
            .iter()
            .filter(|(_, p)| !p.qty.is_zero())
            .map(|(sym, p)| Position {
                symbol: sym.clone(),
                quantity: fmt(p.qty),
                avg_cost: fmt(p.avg_cost),
                realized_pnl: fmt(p.realized_pnl),
            })
            .collect();
        let total_realized: Decimal = self.positions.values().map(|p| p.realized_pnl).sum();
        PortfolioSnapshot {
            cash: fmt(self.cash),
            positions,
            total_realized_pnl: fmt(total_realized),
        }
    }

    pub fn open_orders(&self) -> Vec<Order> {
        let now = crate::state::now_ms();
        self.open_orders.iter().map(|o| o.to_api(now)).collect()
    }

    pub fn order_history(&self) -> Vec<Order> {
        self.order_history.clone()
    }

    pub fn all_fills(&self) -> Vec<Fill> {
        self.fills.clone()
    }

    pub fn place_order(
        &mut self,
        side: Side,
        order_type: OrderType,
        symbol: String,
        limit_price: Option<Decimal>,
        quantity: Decimal,
        binance_book: &OrderBook,
        coindcx_book: &OrderBook,
        now_ms: u64,
    ) -> MatchResult {
        let mut inner = OrderInner {
            id: Uuid::new_v4().to_string(),
            side,
            order_type,
            symbol,
            limit_price,
            total_qty: quantity,
            filled_qty: Decimal::zero(),
            cost_accum: Decimal::zero(),
            status: OrderStatus::Open,
            created_at: now_ms,
        };

        let new_fills = self.try_fill(&mut inner, binance_book, coindcx_book, now_ms);

        if inner.remaining().is_zero() {
            inner.status = OrderStatus::Filled;
            let api_order = inner.to_api(now_ms);
            self.order_history.push(api_order.clone());
            MatchResult { order: api_order, new_fills }
        } else if order_type == OrderType::Market {
            if inner.filled_qty > Decimal::zero() {
                inner.status = OrderStatus::Filled;
            } else {
                inner.status = OrderStatus::Cancelled;
            }
            let api_order = inner.to_api(now_ms);
            self.order_history.push(api_order.clone());
            MatchResult { order: api_order, new_fills }
        } else {
            if inner.filled_qty > Decimal::zero() {
                inner.status = OrderStatus::PartiallyFilled;
            }
            let api_order = inner.to_api(now_ms);
            self.open_orders.push(inner);
            MatchResult { order: api_order, new_fills }
        }
    }

    pub fn cancel_order(&mut self, order_id: &str) -> Option<Order> {
        let idx = self.open_orders.iter().position(|o| o.id == order_id)?;
        let mut inner = self.open_orders.remove(idx);
        inner.status = OrderStatus::Cancelled;
        let now = crate::state::now_ms();
        let api_order = inner.to_api(now);
        self.order_history.push(api_order.clone());
        Some(api_order)
    }

    pub fn check_limits(
        &mut self,
        binance_book: &OrderBook,
        coindcx_book: &OrderBook,
        now_ms: u64,
    ) -> Vec<MatchResult> {
        let mut results = Vec::new();
        let mut i = 0;
        while i < self.open_orders.len() {
            let mut order = self.open_orders.remove(i);
            let new_fills = self.try_fill(&mut order, binance_book, coindcx_book, now_ms);
            if !new_fills.is_empty() {
                if order.remaining().is_zero() {
                    order.status = OrderStatus::Filled;
                    let api_order = order.to_api(now_ms);
                    self.order_history.push(api_order.clone());
                    results.push(MatchResult { order: api_order, new_fills });
                } else {
                    order.status = OrderStatus::PartiallyFilled;
                    let api_order = order.to_api(now_ms);
                    self.open_orders.insert(i, order);
                    results.push(MatchResult { order: api_order, new_fills });
                    i += 1;
                }
            } else {
                self.open_orders.insert(i, order);
                i += 1;
            }
        }
        results
    }

    fn try_fill(
        &mut self,
        order: &mut OrderInner,
        binance_book: &OrderBook,
        coindcx_book: &OrderBook,
        now_ms: u64,
    ) -> Vec<Fill> {
        let depth = 50;
        let (bin_bids, bin_asks) = binance_book.top(depth);
        let (cdx_bids, cdx_asks) = coindcx_book.top(depth);

        let mut levels: Vec<(Decimal, Decimal, &str)> = Vec::new();

        match order.side {
            Side::Buy => {
                for (p, q) in &bin_asks {
                    levels.push((*p, *q, "binance"));
                }
                for (p, q) in &cdx_asks {
                    levels.push((*p, *q, "coindcx"));
                }
                levels.sort_by_key(|(p, _, _)| *p);
            }
            Side::Sell => {
                for (p, q) in &bin_bids {
                    levels.push((*p, *q, "binance"));
                }
                for (p, q) in &cdx_bids {
                    levels.push((*p, *q, "coindcx"));
                }
                levels.sort_by(|a, b| b.0.cmp(&a.0));
            }
        }

        let mut new_fills = Vec::new();
        let mut remaining = order.remaining();

        for (price, qty, exchange) in &levels {
            if remaining.is_zero() {
                break;
            }

            if order.order_type == OrderType::Limit {
                if let Some(limit) = order.limit_price {
                    match order.side {
                        Side::Buy if *price > limit => break,
                        Side::Sell if *price < limit => break,
                        _ => {}
                    }
                }
            }

            let fill_qty = remaining.min(*qty);
            let fill_cost = *price * fill_qty;

            order.filled_qty += fill_qty;
            order.cost_accum += fill_cost;
            remaining -= fill_qty;

            match order.side {
                Side::Buy => {
                    self.cash -= fill_cost;
                    self.apply_fill(&order.symbol, fill_qty, *price);
                }
                Side::Sell => {
                    self.cash += fill_cost;
                    self.apply_fill(&order.symbol, -fill_qty, *price);
                }
            }

            let fill = Fill {
                id: Uuid::new_v4().to_string(),
                order_id: order.id.clone(),
                side: order.side,
                price: price.normalize().to_string(),
                quantity: fill_qty.normalize().to_string(),
                exchange: exchange.to_string(),
                ts_ms: now_ms,
            };
            self.fills.push(fill.clone());
            new_fills.push(fill);
        }

        new_fills
    }

   
    fn apply_fill(&mut self, symbol: &str, signed_qty: Decimal, price: Decimal) {
        if signed_qty.is_zero() {
            return;
        }
        let pos = self.positions.entry(symbol.to_string()).or_insert(PositionInner {
            qty: Decimal::zero(),
            avg_cost: Decimal::zero(),
            realized_pnl: Decimal::zero(),
        });

        let same_direction = pos.qty.is_zero() || (pos.qty.is_sign_positive() == signed_qty.is_sign_positive());
        if same_direction {
            let total_cost = pos.avg_cost * pos.qty.abs() + price * signed_qty.abs();
            pos.qty += signed_qty;
            pos.avg_cost = total_cost / pos.qty.abs();
            return;
        }

        
        let close_qty = signed_qty.abs().min(pos.qty.abs());
        let direction = if pos.qty.is_sign_positive() { Decimal::ONE } else { -Decimal::ONE };
        pos.realized_pnl += (price - pos.avg_cost) * close_qty * direction;
        pos.qty -= close_qty * direction;

        
        let leftover = signed_qty.abs() - close_qty;
        if leftover.is_zero() {
            if pos.qty.is_zero() {
                pos.avg_cost = Decimal::zero();
            }
        } else {
            pos.qty = if signed_qty.is_sign_positive() { leftover } else { -leftover };
            pos.avg_cost = price;
        }
    }
}
