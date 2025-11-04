use futures_util::stream::StreamExt;
use log::{debug, error, info};
use reqwest::Url;
use serde::Deserialize;
use tokio::sync::mpsc;
use tokio_tungstenite::connect_async;

use crate::exchange::binance::{get_ws_base_url, is_testnet_mode};
use crate::model::CandleData;
use chrono::{NaiveDateTime, TimeZone, Utc};


#[derive(Debug, Deserialize)]
pub struct KlineMessage {
    pub e: String,   // Event type ("kline")
    #[serde(rename = "E")]
    pub event_time: u64,      // Event time
    pub s: String,   // Symbol
    pub k: KlineData, // Kline data
}

#[derive(Debug, Deserialize)]
pub struct KlineData {
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

pub async fn start_binance_ws(symbol: &str, sender: mpsc::Sender<CandleData>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut reconnect_count = 0;
    const MAX_RECONNECT_ATTEMPTS: u32 = 100; // Allow many reconnections for continuous trading
    const BASE_RETRY_DELAY: u64 = 5; // Base delay in seconds
    
    loop {
        match connect_and_stream(symbol, &sender, reconnect_count).await {
            Ok(_) => {
                info!("WebSocket connection ended normally");
                break;
            }
            Err(e) => {
                reconnect_count += 1;
                
                if reconnect_count >= MAX_RECONNECT_ATTEMPTS {
                    error!("Maximum reconnection attempts ({}) reached. Giving up.", MAX_RECONNECT_ATTEMPTS);
                    return Err(format!("WebSocket failed after {} attempts: {}", MAX_RECONNECT_ATTEMPTS, e).into());
                }
                
                // Exponential backoff (max 60 seconds)
                let delay = std::cmp::min(BASE_RETRY_DELAY * (1u64 << std::cmp::min(reconnect_count, 4)), 30);
                let total_delay = delay;
                
                error!("WebSocket connection failed (attempt {}): {}. Reconnecting in {} seconds...", 
                       reconnect_count, e, total_delay);
                
                tokio::time::sleep(tokio::time::Duration::from_secs(total_delay)).await;
            }
        }
    }
    
    Ok(())
}

async fn connect_and_stream(
    symbol: &str, 
    sender: &mpsc::Sender<CandleData>, 
    attempt: u32
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Get appropriate WebSocket URL based on testnet mode
    let base_url = get_ws_base_url();
    
    let url = format!(
        "{}/{}@kline_5m",
        base_url, symbol.to_lowercase()
    );
    
    if attempt == 0 {
        info!("Connecting to Binance WebSocket ({} mode): {}", 
              if is_testnet_mode() { "testnet" } else { "real" }, url);
    } else {
        info!("Reconnecting to Binance WebSocket (attempt {}): {}", attempt + 1, url);
    }

    let url_parsed = Url::parse(&url)?;
    let (ws_stream, _) = connect_async(url_parsed).await?;

    info!("✅ WebSocket connected successfully");
    let (mut _write, mut read) = ws_stream.split();
    
    // Keep track of last message time for heartbeat detection
    let mut last_message_time = std::time::Instant::now();
    const HEARTBEAT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60); // 60 seconds timeout
    
    loop {
        // Use timeout to detect stale connections
        let message_result = tokio::time::timeout(
            std::time::Duration::from_secs(30), // Check every 30 seconds
            read.next()
        ).await;
        
        match message_result {
            Ok(Some(message)) => {
                last_message_time = std::time::Instant::now();
                
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
                            debug!("🔄 Ping received: {}", number);
                            continue;
                        }
                        
                        // Try to parse as kline message
                        match serde_json::from_str::<KlineMessage>(text) {
                            Ok(parsed) => {
                                // Only process closed candles to avoid partial data
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
                                    
                                    debug!("📊 {} Candle: O:{:.2} H:{:.2} L:{:.2} C:{:.2} V:{:.2}", 
                                        symbol, candle.open, candle.high, candle.low, candle.close, candle.volume);
                                    
                                    // Send candle update, but don't fail if receiver has dropped
                                    if let Err(e) = sender.send(candle).await {
                                        error!("Candle receiver channel closed: {}", e);
                                        // Channel closed, exit gracefully
                                        return Ok(());
                                    }
                                } else {
                                    // Update current price from unclosed candle
                                    let price = parsed.k.c.parse::<f64>().unwrap_or(0.0);
                                    debug!("📈 {} Price Update (unclosed): ${:.2}", symbol, price);
                                }
                            },
                            Err(e) => {
                                error!("Error parsing WebSocket message: {}, text: {}", e, text);
                                continue;
                            }
                        }
                    }
                    Err(err) => {
                        return Err(format!("WebSocket stream error: {:?}", err).into());
                    }
                }
            }
            Ok(None) => {
                // Stream ended
                return Err("WebSocket stream ended unexpectedly".into());
            }
            Err(_timeout) => {
                // Timeout occurred, check if we've been without messages for too long
                if last_message_time.elapsed() > HEARTBEAT_TIMEOUT {
                    return Err("WebSocket heartbeat timeout - no messages received".into());
                }
                // Continue checking for messages
                debug!("🔄 WebSocket heartbeat check - connection still alive");
                continue;
            }
        }
    }
    
    // If we reach here, the stream ended without error
    info!("WebSocket stream ended");
    Ok(())
}

