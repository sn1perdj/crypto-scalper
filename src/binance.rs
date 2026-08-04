use futures_util::stream::StreamExt;
use serde_json::Value;
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{error, info};
use url::Url;

use crate::strategy::MarketTick;

pub async fn start_binance_ws(tx: mpsc::UnboundedSender<MarketTick>) {
    let ws_url = "wss://stream.binance.com:9443/stream?streams=btcusdt@aggTrade/btcusdt@depth@100ms/btcusdt@bookTicker";
    let url = Url::parse(ws_url).unwrap();

    let client = reqwest::Client::new();

    loop {
        info!("Connecting to Binance Spot WebSocket (wss://stream.binance.com:9443)...");

        match connect_async(url.clone()).await {
            Ok((mut ws_stream, _)) => {
                info!("Connected to Binance Spot WebSocket!");

                // Fetch initial REST Order Book depth snapshot
                let tx_snapshot = tx.clone();
                let client_ref = client.clone();
                tokio::spawn(async move {
                    info!("Fetching REST Order Book Snapshot from api.binance.com...");
                    match client_ref
                        .get("https://api.binance.com/api/v3/depth?symbol=BTCUSDT&limit=1000")
                        .send()
                        .await
                    {
                        Ok(resp) => {
                            if let Ok(snapshot_json) = resp.json::<Value>().await {
                                info!("Fetched REST Order Book Snapshot successfully.");
                                let _ = tx_snapshot.send(MarketTick::Snapshot(snapshot_json));
                            } else {
                                error!("Failed to parse REST depth snapshot JSON.");
                            }
                        }
                        Err(e) => {
                            error!("Error requesting REST depth snapshot: {}", e);
                        }
                    }
                });

                while let Some(msg) = ws_stream.next().await {
                    match msg {
                        Ok(Message::Text(text)) => {
                            if let Ok(parsed) = serde_json::from_str::<Value>(&text) {
                                if let Some(stream) = parsed.get("stream").and_then(|s| s.as_str()) {
                                    if let Some(data) = parsed.get("data") {
                                        let tick = if stream.contains("@depth") {
                                            MarketTick::Depth(data.clone())
                                        } else if stream.contains("@aggTrade") {
                                            MarketTick::AggTrade(data.clone())
                                        } else if stream.contains("@bookTicker") {
                                            MarketTick::BookTicker(data.clone())
                                        } else {
                                            continue;
                                        };

                                        let _ = tx.send(tick);
                                    }
                                }
                            }
                        }
                        Ok(Message::Close(_)) => {
                            info!("WebSocket closed by server.");
                            break;
                        }
                        Err(e) => {
                            error!("WebSocket error: {}", e);
                            break;
                        }
                        _ => {}
                    }
                }
            }
            Err(e) => {
                error!("Failed to connect to Binance Spot WS: {}. Retrying in 5s...", e);
            }
        }

        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
    }
}
