use serde::Serialize;
use serde_json::Value;
use std::collections::VecDeque;
use tokio::sync::watch;
use tracing::info;

use crate::logger::{get_current_timestamp, ExcelLogger, TradeRecord};
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
    pub leverage: f64,
    pub margin_usdt: f64,
    pub entry_notional: f64,
    pub current_notional: f64,
    pub entry_price: f64,
    pub quantity: f64,
    pub price_change: f64,
    pub price_change_pct: f64,
    pub gross_unrealized_pnl: f64,
    pub est_fee: f64,
    pub net_unrealized_pnl: f64,
    pub unrealized_roe_pct: f64,
    pub target_exit_rule: String,
    pub elapsed_seconds: i64,
}

#[derive(Clone, Serialize)]
pub struct ClosedTrade {
    pub side: String,
    pub entry_price: f64,
    pub exit_price: f64,
    pub invested: f64,
    pub gross_pnl: f64,
    pub fee: f64,
    pub pnl: f64,
    pub pnl_percent: f64,
    pub leverage: f64,
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
    logger: ExcelLogger,
    leverage: f64,
    trade_size_usdt: f64,
    symbol: String,
}

impl StrategyEngine {
    pub fn new(tx_out: watch::Sender<MarketState>) -> Self {
        let leverage = std::env::var("LEVERAGE")
            .unwrap_or_else(|_| "10".to_string())
            .parse::<f64>()
            .unwrap_or(10.0);

        let trade_size_usdt = std::env::var("TRADE_SIZE_USDT")
            .unwrap_or_else(|_| "10".to_string())
            .parse::<f64>()
            .unwrap_or(10.0);

        let symbol = std::env::var("SYMBOL").unwrap_or_else(|_| "BTCUSDT".to_string());

        Self {
            state: MarketState::default(),
            order_book: LocalOrderBook::new(),
            cvd_history: VecDeque::with_capacity(100),
            pos_entry_time: None,
            tx_out,
            logger: ExcelLogger::new("trades_log.xlsx"),
            leverage,
            trade_size_usdt,
            symbol,
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

                let curr_price = self.state.btc_price;
                let entry_notional = pos.quantity * pos.entry_price;
                let current_notional = pos.quantity * curr_price;
                pos.entry_notional = entry_notional;
                pos.current_notional = current_notional;

                if pos.side == "LONG" {
                    pos.price_change = curr_price - pos.entry_price;
                    pos.gross_unrealized_pnl = (curr_price - pos.entry_price) * pos.quantity;
                    pos.target_exit_rule = format!("Exit when OBI < 1.20 (Current: {:.2})", self.state.obi);
                } else {
                    pos.price_change = pos.entry_price - curr_price;
                    pos.gross_unrealized_pnl = (pos.entry_price - curr_price) * pos.quantity;
                    pos.target_exit_rule = format!("Exit when OBI > 0.70 (Current: {:.2})", self.state.obi);
                }

                if pos.entry_price > 0.0 {
                    pos.price_change_pct = (pos.price_change / pos.entry_price) * 100.0;
                }

                pos.est_fee = (entry_notional + current_notional) * 0.001;
                pos.net_unrealized_pnl = pos.gross_unrealized_pnl - pos.est_fee;
                if pos.margin_usdt > 0.0 {
                    pos.unrealized_roe_pct = (pos.net_unrealized_pnl / pos.margin_usdt) * 100.0;
                }

                let should_exit = match pos.side.as_str() {
                    "LONG" => self.state.obi < 1.20,
                    "SHORT" => self.state.obi > 0.70,
                    _ => false,
                };

                if should_exit {
                    info!(
                        "Exit Signal for {}: OBI={:.2}. Closing virtual position.",
                        pos.side, self.state.obi
                    );

                    let gross_pnl = pos.gross_unrealized_pnl;
                    let fee = pos.est_fee;
                    let net_pnl = pos.net_unrealized_pnl;
                    let invested = self.trade_size_usdt;
                    let pnl_percent = pos.unrealized_roe_pct;

                    let exit_reason = match pos.side.as_str() {
                        "LONG" => format!("OBI ({:.2}) < 1.20", self.state.obi),
                        "SHORT" => format!("OBI ({:.2}) > 0.70", self.state.obi),
                        _ => "OBI Exit".to_string(),
                    };

                    // Log to Excel
                    self.logger.log_trade(TradeRecord {
                        timestamp: get_current_timestamp(),
                        symbol: self.symbol.clone(),
                        side: pos.side.clone(),
                        leverage: self.leverage,
                        invested_usdt: invested,
                        notional_usdt: entry_notional,
                        quantity: pos.quantity,
                        entry_price: pos.entry_price,
                        exit_price: self.state.btc_price,
                        duration_seconds: pos.elapsed_seconds,
                        gross_pnl_usdt: gross_pnl,
                        fees_usdt: fee,
                        net_pnl_usdt: net_pnl,
                        pnl_percent,
                        exit_reason,
                    });

                    self.state.total_pnl += net_pnl;
                    self.state.closed_trades.insert(
                        0,
                        ClosedTrade {
                            side: pos.side.clone(),
                            entry_price: pos.entry_price,
                            exit_price: self.state.btc_price,
                            invested,
                            gross_pnl,
                            fee,
                            pnl: net_pnl,
                            pnl_percent,
                            leverage: self.leverage,
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

        let notional = self.trade_size_usdt * self.leverage;

        if self.state.obi > 1.50 && self.state.cvd_slope > 0.0 {
            info!(
                "Signal: LONG. Opening virtual position at {}",
                self.state.btc_price
            );
            self.state.position = Some(Position {
                side: "LONG".to_string(),
                leverage: self.leverage,
                margin_usdt: self.trade_size_usdt,
                entry_notional: notional,
                current_notional: notional,
                entry_price: self.state.btc_price,
                quantity: notional / self.state.btc_price,
                price_change: 0.0,
                price_change_pct: 0.0,
                gross_unrealized_pnl: 0.0,
                est_fee: notional * 0.002,
                net_unrealized_pnl: -(notional * 0.002),
                unrealized_roe_pct: (-(notional * 0.002) / self.trade_size_usdt) * 100.0,
                target_exit_rule: format!("Exit when OBI < 1.20 (Current: {:.2})", self.state.obi),
                elapsed_seconds: 0,
            });
            self.pos_entry_time = Some(std::time::Instant::now());
        } else if self.state.obi < 0.67 && self.state.cvd_slope < 0.0 {
            info!(
                "Signal: SHORT. Opening virtual position at {}",
                self.state.btc_price
            );
            self.state.position = Some(Position {
                side: "SHORT".to_string(),
                leverage: self.leverage,
                margin_usdt: self.trade_size_usdt,
                entry_notional: notional,
                current_notional: notional,
                entry_price: self.state.btc_price,
                quantity: notional / self.state.btc_price,
                price_change: 0.0,
                price_change_pct: 0.0,
                gross_unrealized_pnl: 0.0,
                est_fee: notional * 0.002,
                net_unrealized_pnl: -(notional * 0.002),
                unrealized_roe_pct: (-(notional * 0.002) / self.trade_size_usdt) * 100.0,
                target_exit_rule: format!("Exit when OBI > 0.70 (Current: {:.2})", self.state.obi),
                elapsed_seconds: 0,
            });
            self.pos_entry_time = Some(std::time::Instant::now());
        }
    }
}
