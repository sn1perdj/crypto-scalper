use std::collections::BTreeMap;
use serde_json::Value;
use tracing::info;

pub struct LocalOrderBook {
    // Key: (price * 100.0).round() as u64. Value: quantity in BTC
    bids: BTreeMap<u64, f64>,
    asks: BTreeMap<u64, f64>,
    last_update_id: u64,
    synced: bool,
    buffered_events: Vec<Value>,
}

impl LocalOrderBook {
    pub fn new() -> Self {
        Self {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            last_update_id: 0,
            synced: false,
            buffered_events: Vec::new(),
        }
    }

    pub fn init_from_snapshot(&mut self, snapshot: &Value) {
        if let Some(last_id) = snapshot.get("lastUpdateId").and_then(|v| v.as_u64()) {
            self.last_update_id = last_id;
            self.bids.clear();
            self.asks.clear();

            if let Some(bids_arr) = snapshot.get("bids").and_then(|v| v.as_array()) {
                for level in bids_arr {
                    if let (Some(p_str), Some(q_str)) = (
                        level.get(0).and_then(|v| v.as_str()),
                        level.get(1).and_then(|v| v.as_str()),
                    ) {
                        let p: f64 = p_str.parse().unwrap_or(0.0);
                        let q: f64 = q_str.parse().unwrap_or(0.0);
                        if p > 0.0 && q > 0.0 {
                            let key = (p * 100.0).round() as u64;
                            self.bids.insert(key, q);
                        }
                    }
                }
            }

            if let Some(asks_arr) = snapshot.get("asks").and_then(|v| v.as_array()) {
                for level in asks_arr {
                    if let (Some(p_str), Some(q_str)) = (
                        level.get(0).and_then(|v| v.as_str()),
                        level.get(1).and_then(|v| v.as_str()),
                    ) {
                        let p: f64 = p_str.parse().unwrap_or(0.0);
                        let q: f64 = q_str.parse().unwrap_or(0.0);
                        if p > 0.0 && q > 0.0 {
                            let key = (p * 100.0).round() as u64;
                            self.asks.insert(key, q);
                        }
                    }
                }
            }

            self.synced = true;
            info!(
                "Local Order Book initialized with REST snapshot (lastUpdateId={}). Processing {} buffered diff events...",
                last_id,
                self.buffered_events.len()
            );

            // Replay buffered events
            let events = std::mem::take(&mut self.buffered_events);
            for evt in events {
                self.apply_depth_update(&evt);
            }
        }
    }

    pub fn apply_depth_update(&mut self, data: &Value) {
        if !self.synced {
            self.buffered_events.push(data.clone());
            return;
        }

        let final_update_id = match data.get("u").and_then(|v| v.as_u64()) {
            Some(u) => u,
            None => return,
        };

        // Drop events that are older than our snapshot
        if final_update_id <= self.last_update_id {
            return;
        }

        if let Some(bids) = data.get("b").and_then(|v| v.as_array()) {
            for level in bids {
                if let (Some(p_str), Some(q_str)) = (
                    level.get(0).and_then(|v| v.as_str()),
                    level.get(1).and_then(|v| v.as_str()),
                ) {
                    let p: f64 = p_str.parse().unwrap_or(0.0);
                    let q: f64 = q_str.parse().unwrap_or(0.0);
                    if p > 0.0 {
                        let key = (p * 100.0).round() as u64;
                        if q == 0.0 {
                            self.bids.remove(&key);
                        } else {
                            self.bids.insert(key, q);
                        }
                    }
                }
            }
        }

        if let Some(asks) = data.get("a").and_then(|v| v.as_array()) {
            for level in asks {
                if let (Some(p_str), Some(q_str)) = (
                    level.get(0).and_then(|v| v.as_str()),
                    level.get(1).and_then(|v| v.as_str()),
                ) {
                    let p: f64 = p_str.parse().unwrap_or(0.0);
                    let q: f64 = q_str.parse().unwrap_or(0.0);
                    if p > 0.0 {
                        let key = (p * 100.0).round() as u64;
                        if q == 0.0 {
                            self.asks.remove(&key);
                        } else {
                            self.asks.insert(key, q);
                        }
                    }
                }
            }
        }

        self.last_update_id = final_update_id;
    }

    pub fn calculate_imbalance(&self, levels: usize) -> (f64, f64, f64) {
        let bid_vol: f64 = self.bids.values().rev().take(levels).sum();
        let ask_vol: f64 = self.asks.values().take(levels).sum();
        let obi = if ask_vol > 0.0 {
            bid_vol / ask_vol
        } else {
            1.0
        };
        (bid_vol, ask_vol, obi)
    }
}
