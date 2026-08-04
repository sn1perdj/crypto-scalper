# BTCUSDT Spot Scalping System (Rust Demo)

A local-only automated BTCUSDT spot scalping system demo built in Rust. It features a completely local setup with a Tokio-based async runtime, connecting live to the Binance Spot WebSocket and synchronized Local Order Book. The dashboard is served via Axum and updates at 10Hz. Currently, trades are simulated locally as virtual positions.

## Architecture

- **Backend**: Rust (edition 2021), `tokio`, `axum`
- **Market Data**: `tokio-tungstenite` connecting to Binance Futures WebSocket (depth, aggTrade, bookTicker, markPrice)
- **Execution**: Simulated virtual execution based on live data
- **Dashboard**: Vanilla HTML/JS served via Axum

## Strategy Rules

1. **Trade Size**: Simulated $10 USDT at 5x leverage
2. **Long Entry**: OBI > 1.50 AND CVD slope > 0 AND no existing position.
3. **Short Entry**: OBI < 0.67 AND CVD slope < 0 AND no existing position.
4. **Exit Rule**: Positions are automatically closed exactly 10 seconds after entry.

## Setup Instructions

### 1. Installation
Requires the Rust toolchain (cargo).

```bash
# Navigate to the project directory
cd project_directory

# Build the project
cargo build
```

### 2. Configuration
Copy the `.env.example` file to `.env` (this is a placeholder for future live trading integration).

```bash
cp .env.example .env
```

### 3. Running Locally

Start the backend application:

```bash
cargo run
```

This will:
1. Connect to the real Binance Futures WebSockets.
2. Start the Axum web server on `http://127.0.0.1:3000`.

### 4. Accessing the Dashboard

Open your web browser and navigate to:
[http://127.0.0.1:3000](http://127.0.0.1:3000)

The dashboard will automatically connect via WebSocket and stream live market data, OBI/CVD indicators, and show virtual trades as they happen!
