# BTCUSDT Scalper Dashboard Documentation

The dashboard is served locally at `http://localhost:3000` via the Rust Axum web server.

## Features
- **Bloomberg Terminal Aesthetics**: Dark mode, grid layout, monospace font.
- **Real-Time Data**: Uses WebSockets to receive 10Hz updates directly from the Rust backend.
- **Market Data Panel**: Displays BTC Price, OBI, CVD, Spread, and Volumes based on live Binance WS feeds. Color-coded (Green/Red) for quick visual cues.
- **Position Panel**: Automatically reveals when an active (virtual) position exists. Shows unrealized PNL and a live countdown from 10 to 0.
- **System Status**: Displays WebSocket latency.

## Running
Once the Rust backend is running (`cargo run`), navigate to `http://localhost:3000` in any modern web browser.
