use futures_util::StreamExt;
use serde_json::Value;
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{error, info};
use url::Url;

use crate::strategy::MarketTick;

pub async fn start_binance_ws(tx: mpsc::UnboundedSender<MarketTick>) {
    let market_type = std::env::var("MARKET_TYPE")
        .unwrap_or_else(|_| "futures".to_string())
        .to_lowercase();

    let is_futures = market_type == "futures" || market_type == "future";

    let (agg_url, book_url) = if is_futures {
        (
            "wss://fstream.binance.com/market/ws/btcusdt@aggTrade",
            "wss://fstream.binance.com/market/ws/btcusdt@bookTicker",
        )
    } else {
        (
            "wss://stream.binance.com:9443/ws/btcusdt@aggTrade",
            "wss://stream.binance.com:9443/ws/btcusdt@bookTicker",
        )
    };

    info!(
        "Starting {} streams: aggTrade={} bookTicker={}",
        if is_futures { "Futures" } else { "Spot" },
        agg_url,
        book_url,
    );

    let tx_book = tx.clone();
    let tx_agg = tx.clone();

    // Spawn aggTrade stream on its own task
    let agg_url_str = agg_url.to_string();
    tokio::spawn(async move {
        loop {
            match connect_async(Url::parse(&agg_url_str).unwrap()).await {
                Ok((mut ws, _)) => {
                    info!("aggTrade stream connected ({})", agg_url_str);
                    let mut count = 0u64;
                    while let Some(msg) = ws.next().await {
                        match msg {
                            Ok(Message::Text(text)) => {
                                if let Ok(payload) = serde_json::from_str::<Value>(&text) {
                                    count += 1;
                                    if count <= 3 {
                                        info!("[AGG_TRADE #{}] p={:?} q={:?} m={:?}", count,
                                            payload.get("p"), payload.get("q"), payload.get("m"));
                                    }
                                    let _ = tx_agg.send(MarketTick::AggTrade(payload));
                                }
                            }
                            Ok(Message::Close(_)) => { info!("aggTrade stream closed"); break; }
                            Err(e) => { error!("aggTrade stream error: {}", e); break; }
                            _ => {}
                        }
                    }
                }
                Err(e) => error!("aggTrade connect failed: {}. Retrying in 5s...", e),
            }
            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
        }
    });

    // Run bookTicker stream on this task
    let book_url_str = book_url.to_string();
    loop {
        match connect_async(Url::parse(&book_url_str).unwrap()).await {
            Ok((mut ws, _)) => {
                info!("bookTicker stream connected ({})", book_url_str);
                while let Some(msg) = ws.next().await {
                    match msg {
                        Ok(Message::Text(text)) => {
                            if let Ok(payload) = serde_json::from_str::<Value>(&text) {
                                let _ = tx_book.send(MarketTick::BookTicker(payload));
                            }
                        }
                        Ok(Message::Close(_)) => { info!("bookTicker stream closed"); break; }
                        Err(e) => { error!("bookTicker stream error: {}", e); break; }
                        _ => {}
                    }
                }
            }
            Err(e) => error!("bookTicker connect failed: {}. Retrying in 5s...", e),
        }
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
    }
}
