use rust_decimal::Decimal;
use std::collections::BTreeMap;
use std::str::FromStr;

pub type Level = (Decimal, Decimal);

#[derive(Default, Clone, Debug)]
pub struct OrderBook {
    bids: BTreeMap<Decimal, Decimal>,
    asks: BTreeMap<Decimal, Decimal>,
}

impl OrderBook {
    pub fn clear(&mut self) {
        self.bids.clear();
        self.asks.clear();
    }

    pub fn replace(&mut self, bids: Vec<Level>, asks: Vec<Level>) {
        self.bids = bids.into_iter().filter(|(_, q)| !q.is_zero()).collect();
        self.asks = asks.into_iter().filter(|(_, q)| !q.is_zero()).collect();
    }

    pub fn apply_delta(&mut self, bids: &[Level], asks: &[Level]) {
        for (p, q) in bids {
            if q.is_zero() {
                self.bids.remove(p);
            } else {
                self.bids.insert(*p, *q);
            }
        }
        for (p, q) in asks {
            if q.is_zero() {
                self.asks.remove(p);
            } else {
                self.asks.insert(*p, *q);
            }
        }
    }

    pub fn best_bid(&self) -> Option<Level> {
        self.bids.iter().next_back().map(|(p, q)| (*p, *q))
    }

    pub fn best_ask(&self) -> Option<Level> {
        self.asks.iter().next().map(|(p, q)| (*p, *q))
    }

    pub fn top(&self, n: usize) -> (Vec<Level>, Vec<Level>) {
        let bids = self.bids.iter().rev().take(n).map(|(p, q)| (*p, *q)).collect();
        let asks = self.asks.iter().take(n).map(|(p, q)| (*p, *q)).collect();
        (bids, asks)
    }

    pub fn is_crossed(&self) -> bool {
        match (self.best_bid(), self.best_ask()) {
            (Some((b, _)), Some((a, _))) => b >= a,
            _ => false,
        }
    }

    pub fn depth(&self) -> (usize, usize) {
        (self.bids.len(), self.asks.len())
    }
}

pub fn parse_levels(raw: &[(String, String)]) -> anyhow::Result<Vec<Level>> {
    raw.iter()
        .map(|(p, q)| {
            let p = Decimal::from_str(p).map_err(|e| anyhow::anyhow!("bad price {p:?}: {e}"))?;
            let q = Decimal::from_str(q).map_err(|e| anyhow::anyhow!("bad qty {q:?}: {e}"))?;
            Ok((p, q))
        })
        .collect()
}
