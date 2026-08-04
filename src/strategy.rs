use serde::Serialize;
use serde_json::Value;
use std::collections::VecDeque;
use tokio::sync::watch;
use tracing::info;

use crate::logger::{get_current_timestamp, ExcelLogger, TradeRecord};

#[derive(Clone, Debug)]
pub enum MarketTick {
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
    pub elapsed_seconds: i64,
    pub peak_cvd: f64,
    pub trough_cvd: f64,
    pub entry_local_high: f64,
    pub entry_local_low: f64,
}

#[derive(Clone, Serialize)]
pub struct ClosedTrade {
    pub side: String,
    pub leverage: f64,
    pub entry_price: f64,
    pub exit_price: f64,
    pub invested: f64,
    pub gross_pnl: f64,
    pub fee: f64,
    pub pnl: f64,
    pub pnl_percent: f64,
    pub duration_seconds: i64,
    pub exit_reason: String,
}

#[derive(Clone, Serialize)]
pub struct MarketState {
    pub btc_price: f64,
    pub mark_price: f64,
    pub spread: f64,
    pub spread_pct: f64,
    pub cvd: f64,
    pub cvd_slope_5s: f64,
    pub volume_30s: f64,
    pub avg_volume_30s: f64,
    pub volume_ratio: f64,
    pub local_high_60s: f64,
    pub local_low_60s: f64,
    pub position: Option<Position>,
    pub closed_trades: Vec<ClosedTrade>,
    pub total_pnl: f64,
    pub today_pnl: f64,
    pub win_rate_pct: f64,
    pub profit_factor: f64,
    pub total_trades_count: usize,
    pub consecutive_losses: usize,
    pub daily_drawdown_pct: f64,
    pub system_status: String, // "WAITING", "LONG", "SHORT"
    pub market_type: String,   // "FUTURES" or "SPOT"
    pub fee_rate_pct: f64,     // 0.05% or 0.10%
}

impl Default for MarketState {
    fn default() -> Self {
        Self {
            btc_price: 0.0,
            mark_price: 0.0,
            spread: 0.0,
            spread_pct: 0.0,
            cvd: 0.0,
            cvd_slope_5s: 0.0,
            volume_30s: 0.0,
            avg_volume_30s: 0.0,
            volume_ratio: 0.0,
            local_high_60s: 0.0,
            local_low_60s: 0.0,
            position: None,
            closed_trades: Vec::new(),
            total_pnl: 0.0,
            today_pnl: 0.0,
            win_rate_pct: 0.0,
            profit_factor: 0.0,
            total_trades_count: 0,
            consecutive_losses: 0,
            daily_drawdown_pct: 0.0,
            system_status: "WAITING".to_string(),
            market_type: "FUTURES".to_string(),
            fee_rate_pct: 0.05,
        }
    }
}

pub struct TradeEvent {
    pub timestamp: std::time::Instant,
    pub price: f64,
    pub qty: f64,
}

pub struct CvdSnapshot {
    pub timestamp: std::time::Instant,
    pub cvd: f64,
}

pub struct StrategyEngine {
    state: MarketState,
    trade_events: VecDeque<TradeEvent>,
    cvd_history: VecDeque<CvdSnapshot>,
    trade_sizes: VecDeque<f64>,
    pos_entry_time: Option<std::time::Instant>,
    last_exit_time: Option<std::time::Instant>,
    tx_out: watch::Sender<MarketState>,
    logger: ExcelLogger,
    leverage: f64,
    trade_size_usdt: f64,
    symbol: String,
    vol_spike_ratio: f64,
    max_spread_pct: f64,
    stop_loss_pct: f64,
    take_profit_pct: f64,
    cooldown_seconds: u64,
    fee_rate: f64,
    last_trade_vol: f64,
    gross_profits: f64,
    gross_losses: f64,
    wins_count: usize,
    peak_equity: f64,
}

impl StrategyEngine {
    pub fn new(tx_out: watch::Sender<MarketState>) -> Self {
        let market_type_env = std::env::var("MARKET_TYPE")
            .unwrap_or_else(|_| "futures".to_string())
            .to_lowercase();

        let is_futures = market_type_env == "futures" || market_type_env == "future";
        let market_type = if is_futures { "FUTURES".to_string() } else { "SPOT".to_string() };
        let fee_rate = if is_futures { 0.0005 } else { 0.0010 }; // 0.05% Taker for Futures vs 0.10% Taker for Spot
        let fee_rate_pct = fee_rate * 100.0;

        let leverage = std::env::var("LEVERAGE")
            .unwrap_or_else(|_| "5".to_string())
            .parse::<f64>()
            .unwrap_or(5.0);

        let trade_size_usdt = std::env::var("TRADE_SIZE_USDT")
            .unwrap_or_else(|_| "100".to_string())
            .parse::<f64>()
            .unwrap_or(100.0);

        let symbol = std::env::var("SYMBOL").unwrap_or_else(|_| "BTCUSDT".to_string());

        let vol_spike_ratio = std::env::var("VOL_SPIKE_RATIO")
            .unwrap_or_else(|_| "3.0".to_string())
            .parse::<f64>()
            .unwrap_or(3.0);

        let max_spread_pct = std::env::var("MAX_SPREAD_PCT")
            .unwrap_or_else(|_| "0.02".to_string())
            .parse::<f64>()
            .unwrap_or(0.02);

        let stop_loss_pct = std::env::var("STOP_LOSS_PCT")
            .unwrap_or_else(|_| "0.07".to_string())
            .parse::<f64>()
            .unwrap_or(0.07);

        let take_profit_pct = std::env::var("TAKE_PROFIT_PCT")
            .unwrap_or_else(|_| "0.15".to_string())
            .parse::<f64>()
            .unwrap_or(0.15);

        let cooldown_seconds = std::env::var("COOLDOWN_SECONDS")
            .unwrap_or_else(|_| "5".to_string())
            .parse::<u64>()
            .unwrap_or(5);

        info!(
            "Strategy Engine initialized: Market = {}, Fee Rate = {:.2}%, Vol Spike = {:.1}x, Max Spread = {:.2}%, SL = -{:.2}%, TP = +{:.2}%",
            market_type, fee_rate_pct, vol_spike_ratio, max_spread_pct, stop_loss_pct, take_profit_pct
        );

        let mut state = MarketState::default();
        state.market_type = market_type;
        state.fee_rate_pct = fee_rate_pct;

        Self {
            state,
            trade_events: VecDeque::with_capacity(5000),
            cvd_history: VecDeque::with_capacity(300),
            trade_sizes: VecDeque::with_capacity(100),
            pos_entry_time: None,
            last_exit_time: None,
            tx_out,
            logger: ExcelLogger::new("trades_log.xlsx"),
            leverage,
            trade_size_usdt,
            symbol,
            vol_spike_ratio,
            max_spread_pct,
            stop_loss_pct,
            take_profit_pct,
            cooldown_seconds,
            fee_rate,
            last_trade_vol: 0.0,
            gross_profits: 0.0,
            gross_losses: 0.0,
            wins_count: 0,
            peak_equity: 0.0,
        }
    }

    pub fn process_tick(&mut self, tick: MarketTick) {
        match tick {
            MarketTick::AggTrade(data) => {
                let price_str = data.get("p").and_then(|v| v.as_str()).unwrap_or("0");
                let qty_str = data.get("q").and_then(|v| v.as_str()).unwrap_or("0");
                let is_buyer_maker = data.get("m").and_then(|v| v.as_bool()).unwrap_or(false);

                let p = price_str.parse::<f64>().unwrap_or(0.0);
                let qty = qty_str.parse::<f64>().unwrap_or(0.0);

                if p > 0.0 {
                    self.state.btc_price = p;
                    self.state.mark_price = p;

                    let now = std::time::Instant::now();
                    self.trade_events.push_back(TradeEvent {
                        timestamp: now,
                        price: p,
                        qty,
                    });
                    self.last_trade_vol = qty;

                    self.trade_sizes.push_back(qty);
                    if self.trade_sizes.len() > 100 {
                        self.trade_sizes.pop_front();
                    }

                    // Calculate CVD
                    if is_buyer_maker {
                        self.state.cvd -= qty;
                    } else {
                        self.state.cvd += qty;
                    }

                    // Store CVD snapshot for 5s slope calculation
                    self.cvd_history.push_back(CvdSnapshot {
                        timestamp: now,
                        cvd: self.state.cvd,
                    });

                    // Retain only last 300s of trade events and 10s of CVD snapshots
                    while let Some(front) = self.trade_events.front() {
                        if now.duration_since(front.timestamp).as_secs() > 300 {
                            self.trade_events.pop_front();
                        } else {
                            break;
                        }
                    }

                    while let Some(front) = self.cvd_history.front() {
                        if now.duration_since(front.timestamp).as_secs() > 10 {
                            self.cvd_history.pop_front();
                        } else {
                            break;
                        }
                    }

                    self.recalculate_metrics(now);
                }
            }
            MarketTick::BookTicker(data) => {
                let bid_str = data.get("b").and_then(|v| v.as_str()).unwrap_or("0");
                let ask_str = data.get("a").and_then(|v| v.as_str()).unwrap_or("0");
                let bid = bid_str.parse::<f64>().unwrap_or(0.0);
                let ask = ask_str.parse::<f64>().unwrap_or(0.0);

                if ask > 0.0 && bid > 0.0 {
                    self.state.spread = ask - bid;
                    let mid = (bid + ask) / 2.0;
                    self.state.spread_pct = (self.state.spread / mid) * 100.0;
                    // Always update price from bookTicker (it fires on every tick)
                    self.state.btc_price = mid;
                    self.state.mark_price = mid;
                }
            }
        }

        self.update_position();
        self.check_rules();

        let _ = self.tx_out.send(self.state.clone());
    }

    fn recalculate_metrics(&mut self, now: std::time::Instant) {
        // 1. CVD Slope over last 5s
        if let Some(snapshot_5s_ago) = self.cvd_history.iter().find(|s| now.duration_since(s.timestamp).as_secs_f64() >= 4.5) {
            self.state.cvd_slope_5s = self.state.cvd - snapshot_5s_ago.cvd;
        } else if let Some(first) = self.cvd_history.front() {
            self.state.cvd_slope_5s = self.state.cvd - first.cvd;
        }

        // 2. Rolling 30s Volume & Average 30s Volume (over 10 previous 30s windows = 300s)
        let mut vol_current_30s = 0.0;
        let mut window_vols = [0.0; 10]; // 10 rolling 30s windows

        for event in self.trade_events.iter() {
            let age_secs = now.duration_since(event.timestamp).as_secs_f64();
            if age_secs <= 30.0 {
                vol_current_30s += event.qty;
            }
            let window_idx = (age_secs / 30.0) as usize;
            if window_idx < 10 {
                window_vols[window_idx] += event.qty;
            }
        }

        self.state.volume_30s = vol_current_30s;
        let prev_windows_sum: f64 = window_vols[1..10].iter().sum();
        let prev_windows_count = window_vols[1..10].iter().filter(|&&v| v > 0.0).count().max(1);
        self.state.avg_volume_30s = prev_windows_sum / prev_windows_count as f64;

        if self.state.avg_volume_30s > 0.0 {
            self.state.volume_ratio = self.state.volume_30s / self.state.avg_volume_30s;
        } else {
            self.state.volume_ratio = 1.0;
        }

        // 3. Local High & Low (previous 60 seconds, excluding current trade)
        let len = self.trade_events.len();
        if len > 1 {
            let recent_60s_events = self.trade_events.iter().take(len - 1).filter(|e| now.duration_since(e.timestamp).as_secs_f64() <= 60.0);
            let mut max_p = 0.0f64;
            let mut min_p = f64::MAX;

            for e in recent_60s_events {
                if e.price > max_p {
                    max_p = e.price;
                }
                if e.price < min_p {
                    min_p = e.price;
                }
            }

            self.state.local_high_60s = max_p;
            self.state.local_low_60s = if min_p < f64::MAX { min_p } else { 0.0 };
        }
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

                // Update Peak / Trough CVD since entry
                if self.state.cvd > pos.peak_cvd {
                    pos.peak_cvd = self.state.cvd;
                }
                if self.state.cvd < pos.trough_cvd {
                    pos.trough_cvd = self.state.cvd;
                }

                if pos.side == "LONG" {
                    pos.price_change = curr_price - pos.entry_price;
                    pos.gross_unrealized_pnl = (curr_price - pos.entry_price) * pos.quantity;
                } else {
                    pos.price_change = pos.entry_price - curr_price;
                    pos.gross_unrealized_pnl = (pos.entry_price - curr_price) * pos.quantity;
                }

                if pos.entry_price > 0.0 {
                    pos.price_change_pct = (pos.price_change / pos.entry_price) * 100.0;
                }

                // Dynamic fee based on Market Type (Futures 0.05% per side vs Spot 0.10% per side)
                pos.est_fee = (entry_notional + current_notional) * self.fee_rate;
                pos.net_unrealized_pnl = pos.gross_unrealized_pnl - pos.est_fee;
                if pos.margin_usdt > 0.0 {
                    pos.unrealized_roe_pct = (pos.net_unrealized_pnl / pos.margin_usdt) * 100.0;
                }

                // Check Exit Rules
                let cvd_reversal_exit = if pos.side == "LONG" {
                    pos.peak_cvd > 0.0 && self.state.cvd < (pos.peak_cvd * 0.80)
                } else {
                    pos.trough_cvd > 0.0 && self.state.cvd > (pos.trough_cvd * 1.20)
                };

                let breakout_failed_exit = if pos.side == "LONG" {
                    curr_price < pos.entry_local_high
                } else {
                    curr_price > pos.entry_local_low
                };

                let stop_loss_exit = pos.price_change_pct <= -self.stop_loss_pct;
                let take_profit_exit = pos.price_change_pct >= self.take_profit_pct;

                let (should_exit, exit_reason) = if stop_loss_exit {
                    (true, format!("Stop Loss (-{:.2}%)", self.stop_loss_pct))
                } else if take_profit_exit {
                    (true, format!("Take Profit (+{:.2}%)", self.take_profit_pct))
                } else if breakout_failed_exit {
                    (true, "Price Returned Inside Breakout Range".to_string())
                } else if cvd_reversal_exit {
                    (true, "CVD Reversal (>20% Drop)".to_string())
                } else {
                    (false, "".to_string())
                };

                if should_exit {
                    info!(
                        "Exit Signal for {} ({}): {}. Closing position after {}s.",
                        pos.side, self.state.market_type, exit_reason, pos.elapsed_seconds
                    );

                    let gross_pnl = pos.gross_unrealized_pnl;
                    let fee = pos.est_fee;
                    let net_pnl = pos.net_unrealized_pnl;
                    let invested = self.trade_size_usdt;
                    let pnl_percent = pos.unrealized_roe_pct;

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
                        exit_reason: exit_reason.clone(),
                    });

                    self.state.total_pnl += net_pnl;
                    self.state.today_pnl += net_pnl;
                    self.state.total_trades_count += 1;

                    if net_pnl > 0.0 {
                        self.gross_profits += net_pnl;
                        self.wins_count += 1;
                        self.state.consecutive_losses = 0;
                    } else {
                        self.gross_losses += net_pnl.abs();
                        self.state.consecutive_losses += 1;
                    }

                    self.state.win_rate_pct = (self.wins_count as f64 / self.state.total_trades_count as f64) * 100.0;
                    self.state.profit_factor = if self.gross_losses > 0.0 {
                        self.gross_profits / self.gross_losses
                    } else {
                        self.gross_profits
                    };

                    if self.state.today_pnl > self.peak_equity {
                        self.peak_equity = self.state.today_pnl;
                    }

                    let drawdown = self.peak_equity - self.state.today_pnl;
                    self.state.daily_drawdown_pct = (drawdown / self.trade_size_usdt) * 100.0;
                    self.state.system_status = "WAITING".to_string();

                    self.state.closed_trades.insert(
                        0,
                        ClosedTrade {
                            side: pos.side.clone(),
                            leverage: self.leverage,
                            entry_price: pos.entry_price,
                            exit_price: self.state.btc_price,
                            invested,
                            gross_pnl,
                            fee,
                            pnl: net_pnl,
                            pnl_percent,
                            duration_seconds: pos.elapsed_seconds,
                            exit_reason,
                        },
                    );

                    if self.state.closed_trades.len() > 10 {
                        self.state.closed_trades.truncate(10);
                    }
                    self.state.position = None;
                    self.pos_entry_time = None;
                    self.last_exit_time = Some(std::time::Instant::now());
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

        if let Some(last_exit) = self.last_exit_time {
            if last_exit.elapsed().as_secs() < self.cooldown_seconds {
                return;
            }
        }

        // Calculate Rolling Average Trade Size over last 100 trades
        let avg_trade_size: f64 = if !self.trade_sizes.is_empty() {
            self.trade_sizes.iter().sum::<f64>() / self.trade_sizes.len() as f64
        } else {
            0.0
        };

        // Long Entry Conditions
        let cond1_vol = self.state.volume_ratio >= self.vol_spike_ratio;
        let cond2_cvd = self.state.cvd_slope_5s > 0.0;
        let cond3_high = self.state.local_high_60s > 0.0 && self.state.btc_price > self.state.local_high_60s;
        let cond4_trade_vol = self.last_trade_vol > avg_trade_size;
        let cond5_spread = self.state.spread_pct < self.max_spread_pct;

        // Short Entry Conditions
        let cond2_cvd_short = self.state.cvd_slope_5s < 0.0;
        let cond3_low = self.state.local_low_60s > 0.0 && self.state.btc_price < self.state.local_low_60s;

        let notional = self.trade_size_usdt * self.leverage;

        if cond1_vol && cond2_cvd && cond3_high && cond4_trade_vol && cond5_spread {
            info!(
                "LONG ENTRY Triggered ({})! VolRatio {:.2} >= {:.1}, CVD Slope 5s {:.2} > 0, Price {:.2} > Local High {:.2}, Spread {:.4}%",
                self.state.market_type, self.state.volume_ratio, self.vol_spike_ratio, self.state.cvd_slope_5s, self.state.btc_price, self.state.local_high_60s, self.state.spread_pct
            );

            self.state.system_status = "LONG".to_string();
            let initial_est_fee = notional * (self.fee_rate * 2.0);
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
                est_fee: initial_est_fee,
                net_unrealized_pnl: -initial_est_fee,
                unrealized_roe_pct: (-initial_est_fee / self.trade_size_usdt) * 100.0,
                elapsed_seconds: 0,
                peak_cvd: self.state.cvd,
                trough_cvd: self.state.cvd,
                entry_local_high: self.state.local_high_60s,
                entry_local_low: self.state.local_low_60s,
            });
            self.pos_entry_time = Some(std::time::Instant::now());
        } else if cond1_vol && cond2_cvd_short && cond3_low && cond4_trade_vol && cond5_spread {
            info!(
                "SHORT ENTRY Triggered ({})! VolRatio {:.2} >= {:.1}, CVD Slope 5s {:.2} < 0, Price {:.2} < Local Low {:.2}, Spread {:.4}%",
                self.state.market_type, self.state.volume_ratio, self.vol_spike_ratio, self.state.cvd_slope_5s, self.state.btc_price, self.state.local_low_60s, self.state.spread_pct
            );

            self.state.system_status = "SHORT".to_string();
            let initial_est_fee = notional * (self.fee_rate * 2.0);
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
                est_fee: initial_est_fee,
                net_unrealized_pnl: -initial_est_fee,
                unrealized_roe_pct: (-initial_est_fee / self.trade_size_usdt) * 100.0,
                elapsed_seconds: 0,
                peak_cvd: self.state.cvd,
                trough_cvd: self.state.cvd,
                entry_local_high: self.state.local_high_60s,
                entry_local_low: self.state.local_low_60s,
            });
            self.pos_entry_time = Some(std::time::Instant::now());
        }
    }
}
