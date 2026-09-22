mod binance;
mod book;
mod coindcx;
mod matching;
mod pricing;
mod server;
mod state;

use clap::Parser;
use rust_decimal::Decimal;
use std::{path::PathBuf, sync::Arc};

#[derive(Parser, Debug)]
#[command(version)]
struct Args {
    #[arg(long, env = "BINANCE_SYMBOL", default_value = "BTCUSDT")]
    binance_symbol: String,
    #[arg(long, env = "COINDCX_PAIR", default_value = "B-BTC_USDT")]
    coindcx_pair: String,
    #[arg(long, env = "DEPTH", default_value_t = 20)]
    depth: usize,
    #[arg(long, env = "PORT", default_value_t = 8080)]
    port: u16,
    #[arg(long, env = "STATIC_DIR")]
    static_dir: Option<PathBuf>,
    #[arg(long, env = "INITIAL_BALANCE", default_value = "100000")]
    initial_balance: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();
    let args = Args::parse();

    let static_dir = args.static_dir.clone().or_else(|| {
        let candidates = [PathBuf::from("../frontend/dist"), PathBuf::from("frontend/dist")];
        candidates.into_iter().find(|p| p.join("index.html").exists())
    });

    let initial_balance: Decimal = args.initial_balance.parse()
        .expect("invalid --initial-balance value");

    let shared = Arc::new(state::Shared::new(
        args.binance_symbol.to_uppercase(),
        args.coindcx_pair.clone(),
        initial_balance,
    ));
    let client = reqwest::Client::builder().user_agent("orderbook-backend/0.1").build()?;

    tokio::spawn(binance::run(shared.clone(), args.binance_symbol.to_uppercase(), client.clone()));
    tokio::spawn(coindcx::run(shared.clone(), args.coindcx_pair.clone(), client.clone()));

    {
        let shared = shared.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_millis(200));
            loop {
                interval.tick().await;
                let binance_book;
                let coindcx_book;
                {
                    let bs = shared.binance.read().unwrap();
                    binance_book = bs.book.clone();
                }
                {
                    let cs = shared.coindcx.read().unwrap();
                    coindcx_book = cs.book.clone();
                }
                let now = state::now_ms();
                let results = {
                    let mut ts = shared.trading.write().unwrap();
                    ts.check_limits(&binance_book, &coindcx_book, now)
                };
                for result in results {
                    for fill in &result.new_fills {
                        shared.emit_trading(state::TradingEvent::Fill { fill: fill.clone() });
                    }
                    shared.emit_trading(state::TradingEvent::OrderUpdate { order: result.order });
                }
            }
        });
    }

    server::serve(shared, args.port, args.depth, static_dir).await
}
