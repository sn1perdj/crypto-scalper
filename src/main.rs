mod binance;
mod orderbook;
mod strategy;

use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    response::IntoResponse,
    routing::get,
    Router,
};
use std::sync::Arc;
use tokio::sync::{mpsc, watch};
use tower_http::{cors::CorsLayer, services::ServeDir};
use tracing::info;

use strategy::MarketTick;
use strategy::{MarketState, StrategyEngine};

struct AppState {
    tx: watch::Sender<MarketState>,
}

#[tokio::main]
async fn main() {
    dotenv::dotenv().ok();
    tracing_subscriber::fmt::init();

    // Channel for receiving raw ticks from Binance WS (unbounded so it never lags)
    let (tick_tx, mut tick_rx) = mpsc::unbounded_channel::<MarketTick>();
    
    // Channel for broadcasting state to dashboard WS clients (watch channel always keeps latest)
    let (state_tx, _) = watch::channel::<MarketState>(MarketState::default());
    
    let app_state = Arc::new(AppState {
        tx: state_tx.clone(),
    });

    // Spawn Binance WS Client
    let binance_tx = tick_tx.clone();
    tokio::spawn(async move {
        binance::start_binance_ws(binance_tx).await;
    });

    // Spawn Strategy Engine
    let engine_state_tx = state_tx.clone();
    tokio::spawn(async move {
        let mut engine = StrategyEngine::new(engine_state_tx);
        while let Some(tick) = tick_rx.recv().await {
            engine.process_tick(tick);
        }
    });

    // Setup HTTP server
    let app = Router::new()
        .route("/ws", get(ws_handler))
        .fallback_service(ServeDir::new("static"))
        .layer(CorsLayer::permissive())
        .with_state(app_state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000")
        .await
        .unwrap();
        
    info!("Rust Demo server running on http://127.0.0.1:3000");
    axum::serve(listener, app).await.unwrap();
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    ws.on_upgrade(|socket| handle_socket(socket, state))
}

async fn handle_socket(mut socket: WebSocket, state: Arc<AppState>) {
    let rx = state.tx.subscribe();
    let mut interval = tokio::time::interval(std::time::Duration::from_millis(100));
    
    loop {
        interval.tick().await;
        let state_update = rx.borrow().clone();
        if let Ok(msg) = serde_json::to_string(&state_update) {
            if socket.send(Message::Text(msg)).await.is_err() {
                break;
            }
        }
    }
}
