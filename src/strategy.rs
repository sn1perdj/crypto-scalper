use serde::Serialize;
use serde_json::Value;
use std::collections::VecDeque;
use tokio::sync::watch;
use tracing::info;

use crate::orderbook::LocalOrderBook;

#[derive(Clone, Debug)]
pub enum MarketTick {
    Snapshot(Value),
    Depth(Value),
    AggTrade(Value),
    BookTicker(Value),
}

#[derive(Clone, Serialize, Default)]
pub struct Position {
    pub side: String,
    pub entry_price: f64,
    pub quantity: f64,
    pub unrealized_pnl: f64,
    pub elapsed_seconds: i64,
    pub remaining_seconds: i64,
}

#[derive(Clone, Serialize)]
pub struct ClosedTrade {
    pub side: String,
    pub entry_price: f64,
    pub exit_price: f64,
    pub invested: f64,
    pub pnl: f64,
    pub duration_seconds: i64,
}

#[derive(Clone, Serialize)]
pub struct MarketState {
    pub btc_price: f64,
    pub mark_price: f64,
    pub spread: f64,
    pub funding: f64,
    pub obi: f64,
    pub cvd: f64,
    pub cvd_slope: f64,
    pub bid_volume: f64,
    pub ask_volume: f64,
    pub position: Option<Position>,
    pub closed_trades: Vec<ClosedTrade>,
    pub total_pnl: f64,
}

impl Default for MarketState {
    fn default() -> Self {
        Self {
            btc_price: 0.0,
            mark_price: 0.0,
            spread: 0.0,
            funding: 0.0,
            obi: 1.0,
            cvd: 0.0,
            cvd_slope: 0.0,
            bid_volume: 0.0,
            ask_volume: 0.0,
            position: None,
            closed_trades: Vec::new(),
            total_pnl: 0.0,
        }
    }
}

pub struct StrategyEngine {
    state: MarketState,
    order_book: LocalOrderBook,
    cvd_history: VecDeque<f64>,
    pos_entry_time: Option<std::time::Instant>,
    tx_out: watch::Sender<MarketState>,
}

impl StrategyEngine {
    pub fn new(tx_out: watch::Sender<MarketState>) -> Self {
        Self {
            state: MarketState::default(),
            order_book: LocalOrderBook::new(),
            cvd_history: VecDeque::with_capacity(100),
            pos_entry_time: None,
            tx_out,
        }
    }

    pub fn process_tick(&mut self, tick: MarketTick) {
        match tick {
            MarketTick::Snapshot(snapshot) => {
                self.order_book.init_from_snapshot(&snapshot);
                let (b_vol, a_vol, obi) = self.order_book.calculate_imbalance(20);
                self.state.bid_volume = b_vol;
                self.state.ask_volume = a_vol;
                self.state.obi = obi;
            }
            MarketTick::Depth(data) => {
                self.order_book.apply_depth_update(&data);
                let (b_vol, a_vol, obi) = self.order_book.calculate_imbalance(20);
                self.state.bid_volume = b_vol;
                self.state.ask_volume = a_vol;
                self.state.obi = obi;
            }
            MarketTick::AggTrade(data) => {
                let price_str = data.get("p").and_then(|v| v.as_str()).unwrap_or("0");
                let qty_str = data.get("q").and_then(|v| v.as_str()).unwrap_or("0");
                let is_buyer_maker = data.get("m").and_then(|v| v.as_bool()).unwrap_or(false);

                let p = price_str.parse::<f64>().unwrap_or(0.0);
                if p > 0.0 {
                    self.state.btc_price = p;
                    self.state.mark_price = p;
                }

                let qty = qty_str.parse::<f64>().unwrap_or(0.0);

                if is_buyer_maker {
                    self.state.cvd -= qty;
                } else {
                    self.state.cvd += qty;
                }

                self.cvd_history.push_back(self.state.cvd);
                if self.cvd_history.len() > 100 {
                    self.cvd_history.pop_front();
                }

                if self.cvd_history.len() >= 2 {
                    let first = self.cvd_history.front().unwrap();
                    let last = self.cvd_history.back().unwrap();
                    self.state.cvd_slope = last - first;
                }
            }
            MarketTick::BookTicker(data) => {
                let bid_str = data.get("b").and_then(|v| v.as_str()).unwrap_or("0");
                let ask_str = data.get("a").and_then(|v| v.as_str()).unwrap_or("0");
                let bid = bid_str.parse::<f64>().unwrap_or(0.0);
                let ask = ask_str.parse::<f64>().unwrap_or(0.0);
                self.state.spread = ask - bid;
                if self.state.btc_price == 0.0 && ask > 0.0 {
                    let mid = (bid + ask) / 2.0;
                    self.state.btc_price = mid;
                    self.state.mark_price = mid;
                }
            }
        }

        self.update_position();
        self.check_rules();

        let _ = self.tx_out.send(self.state.clone());
    }

    fn update_position(&mut self) {
        if let Some(mut pos) = self.state.position.clone() {
            if let Some(entry_time) = self.pos_entry_time {
                let elapsed = entry_time.elapsed().as_secs() as i64;
                pos.elapsed_seconds = elapsed;
                pos.remaining_seconds = 10 - elapsed;

                if pos.side == "LONG" {
                    pos.unrealized_pnl = (self.state.btc_price - pos.entry_price) * pos.quantity;
                } else {
                    pos.unrealized_pnl = (pos.entry_price - self.state.btc_price) * pos.quantity;
                }

                if pos.remaining_seconds <= 0 {
                    info!("Closing virtual position after 10 seconds.");
                    let invested = (pos.quantity * pos.entry_price) / 5.0; // 5x leverage margin
                    self.state.total_pnl += pos.unrealized_pnl;
                    self.state.closed_trades.insert(
                        0,
                        ClosedTrade {
                            side: pos.side.clone(),
                            entry_price: pos.entry_price,
                            exit_price: self.state.btc_price,
                            invested,
                            pnl: pos.unrealized_pnl,
                            duration_seconds: pos.elapsed_seconds,
                        },
                    );
                    if self.state.closed_trades.len() > 10 {
                        self.state.closed_trades.truncate(10);
                    }
                    self.state.position = None;
                    self.pos_entry_time = None;
                } else {
                    self.state.position = Some(pos);
                }
            }
        }
    }

    fn check_rules(&mut self) {
        if self.state.position.is_some() || self.state.btc_price == 0.0 {
            return;
        }

        if self.state.obi > 1.50 && self.state.cvd_slope > 0.0 {
            info!(
                "Signal: LONG. Opening virtual position at {}",
                self.state.btc_price
            );
            let notional = 10.0 * 5.0; // $10 USDT margin at 5x leverage = 50 USDT position size
            self.state.position = Some(Position {
                side: "LONG".to_string(),
                entry_price: self.state.btc_price,
                quantity: notional / self.state.btc_price,
                unrealized_pnl: 0.0,
                elapsed_seconds: 0,
                remaining_seconds: 10,
            });
            self.pos_entry_time = Some(std::time::Instant::now());
        } else if self.state.obi < 0.67 && self.state.cvd_slope < 0.0 {
            info!(
                "Signal: SHORT. Opening virtual position at {}",
                self.state.btc_price
            );
            let notional = 10.0 * 5.0;
            self.state.position = Some(Position {
                side: "SHORT".to_string(),
                entry_price: self.state.btc_price,
                quantity: notional / self.state.btc_price,
                unrealized_pnl: 0.0,
                elapsed_seconds: 0,
                remaining_seconds: 10,
            });
            self.pos_entry_time = Some(std::time::Instant::now());
        }
    }
}
