use std::error::Error;
use futures_util::stream::StreamExt;
use log::{debug, error, info};
use reqwest::Url;
use serde::Deserialize;
use tokio::sync::mpsc;
use tokio_tungstenite::connect_async;

use crate::exchange::binance::is_testnet_mode;
use crate::exchange::binance_futures::get_futures_ws_url;
use crate::model::CandleData;
use chrono::{NaiveDateTime, TimeZone, Utc};
use tokio::sync::mpsc::Sender;

#[derive(Debug, Deserialize)]
pub struct FuturesKlineMessage {
    pub e: String,   // Event type ("kline")
    #[serde(rename = "E")]
    pub event_time: u64,      // Event time
    pub s: String,   // Symbol
    pub k: FuturesKlineData, // Kline data
}

#[derive(Debug, Deserialize)]
pub struct FuturesKlineData {
    pub t: i64,      // Kline start time
    #[serde(rename = "T")]
    pub close_time: i64,      // Kline close time
    pub s: String,   // Symbol
    pub i: String,   // Interval
    pub f: i64,      // First trade ID (-1 when no trades)
    #[serde(rename = "L")]
    pub last_trade_id: i64,   // Last trade ID (-1 when no trades)
    pub o: String,   // Open price
    pub c: String,   // Close price
    pub h: String,   // High price
    pub l: String,   // Low price
    pub v: String,   // Base asset volume
    pub n: u64,      // Number of trades
    pub x: bool,     // Is this kline closed?
    pub q: String,   // Quote asset volume
    #[serde(rename = "V")]
    pub taker_buy_base_volume: String,  // Taker buy base asset volume
    #[serde(rename = "Q")]
    pub taker_buy_quote_volume: String, // Taker buy quote asset volume
    #[serde(rename = "B")]
    pub ignore: String,                 // Ignore
}

pub async fn start_binance_futures_ws(symbol: &str, sender: mpsc::Sender<CandleData>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Get appropriate Futures WebSocket URL based on testnet mode
    let base_url = get_futures_ws_url();
    
    let url = format!(
        "{}/{}@kline_5m",
        base_url, symbol.to_lowercase()
    );
    
    info!("Connecting to Binance Futures WebSocket ({} mode): {}", 
          if is_testnet_mode() { "testnet" } else { "real" }, url);

    let url_parsed = Url::parse(&url)?;
    let (ws_stream, _) = connect_async(url_parsed).await?;

    let (mut _write, mut read) = ws_stream.split();
    while let Some(message) = read.next().await {
        match message {
            Ok(msg) => {
                let text = match msg.to_text() {
                    Ok(t) => t,
                    Err(e) => {
                        error!("Error converting message to text: {}", e);
                        continue;
                    }
                };
                
                // Check if it's a ping message
                if let Ok(number) = text.parse::<u64>() {
                    // Likely a ping, just ignore or log at debug level
                    debug!("🔄 Futures Ping received: {}", number);
                    continue;
                }
                
                // Try to parse as futures kline message
                match serde_json::from_str::<FuturesKlineMessage>(text) {
                    Ok(parsed) => {
                        // Only process closed candles to avoid partial data
                        if let Some(value) = build_and_send_candle(symbol, &sender, parsed).await {
                            return value;
                        }
                    },
                    Err(e) => {
                        error!("Error parsing futures message: {}, text: {}", e, text);
                        continue;
                    }
                }
            }
            Err(err) => {
                error!("❌ Futures WebSocket error: {:?}", err);
                // Consider reconnecting here instead of breaking
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
            }
        }
    }

    Ok(())
}

async fn build_and_send_candle(symbol: &str, sender: &Sender<CandleData>, parsed: FuturesKlineMessage) -> Option<Result<(), Box<dyn Error + Send + Sync>>> {
    if parsed.k.x {
        let candle = CandleData {
            open_time: Utc.timestamp_millis_opt(parsed.k.t).unwrap().naive_utc(),
            open: parsed.k.o.parse::<f64>().unwrap_or(0.0),
            high: parsed.k.h.parse::<f64>().unwrap_or(0.0),
            low: parsed.k.l.parse::<f64>().unwrap_or(0.0),
            close: parsed.k.c.parse::<f64>().unwrap_or(0.0),
            volume: parsed.k.v.parse::<f64>().unwrap_or(0.0),
            close_time: Utc.timestamp_millis_opt(parsed.k.close_time).unwrap().naive_utc(),
        };

        debug!("📊 {} Futures Candle: O:{:.2} H:{:.2} L:{:.2} C:{:.2} V:{:.2}", 
                                symbol, candle.open, candle.high, candle.low, candle.close, candle.volume);

        // Send candle update, but don't fail if receiver has dropped
        if let Err(e) = sender.send(candle).await {
            error!("Failed to send futures candle update: {}", e);
            // Channel closed, exit
            return Some(Ok(()));
        }
    } else {
        // Update current price from unclosed candle
        let price = parsed.k.c.parse::<f64>().unwrap_or(0.0);
        debug!("📈 {} Futures Price Update (unclosed): ${:.2}", symbol, price);
    }
    None
}