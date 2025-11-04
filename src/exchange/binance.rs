use crate::model::CandleData;
use anyhow::Result;
use chrono::{DateTime, Duration, TimeZone, Utc};
use hex::encode;
use hmac::{Hmac, Mac};
use reqwest::Client;
use serde::Deserialize;
use sha2::Sha256;
use std::collections::HashMap;
use std::error::Error;
use std::fs::File;
use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};
use tracing::{info, error, debug};

// API Base URLs
const BINANCE_REAL_BASE_URL: &str = "https://api.binance.com/api/v3";
const BINANCE_TESTNET_BASE_URL: &str = "https://testnet.binance.vision/api/v3";

// WebSocket Base URLs
const BINANCE_REAL_WS_URL: &str = "wss://stream.binance.com:9443/ws";
const BINANCE_TESTNET_WS_URL: &str = "wss://testnet.binance.vision/ws";

// Testnet Credentials
const TESTNET_API_KEY: &str = "234b7ab563611699e333398915ecd231b3368b75ad10ace72a84579581eb5ed0";
const TESTNET_API_SECRET: &str = "7f800dc63c4f31ce2986e1632ebc103181890c179c0ede3b30f044ac53e9a434";

// Real Account Credentials - Replace with your real credentials when needed
// In a production environment, these should be stored in environment variables
const REAL_API_KEY: &str = "BT52e7nFW9gBcLOw2SajaoSYYGatSoMz6HEtLg29h6Hwzes5LlqjJBLqDkz5eDnN";
const REAL_API_SECRET: &str = "BjQWZXDCD2zoJdAO7O0rA0Krh81UTjceBEaIkkP0ct6OTEus4npKUI9nPavmg6UC";

// Global testnet flag
static USE_TESTNET: AtomicBool = AtomicBool::new(true);

// Sets the global testnet flag
pub fn set_testnet_mode(testnet: bool) {
    USE_TESTNET.store(testnet, Ordering::SeqCst);
}

// Gets the current testnet mode
pub fn is_testnet_mode() -> bool {
    USE_TESTNET.load(Ordering::SeqCst)
}

// Helper functions to get the appropriate API URLs and credentials
fn get_api_base_url() -> &'static str {
    if is_testnet_mode() {
        BINANCE_TESTNET_BASE_URL
    } else {
        BINANCE_REAL_BASE_URL
    }
}

fn get_api_order_url() -> String {
    format!("{}/order", get_api_base_url())
}

pub fn get_api_key() -> &'static str {
    if is_testnet_mode() {
        TESTNET_API_KEY
    } else {
        REAL_API_KEY
    }
}

pub fn get_api_secret() -> &'static str {
    if is_testnet_mode() {
        TESTNET_API_SECRET
    } else {
        REAL_API_SECRET
    }
}

pub fn get_ws_base_url() -> &'static str {
    if is_testnet_mode() {
        BINANCE_TESTNET_WS_URL
    } else {
        BINANCE_REAL_WS_URL
    }
}

type HmacSha256 = Hmac<Sha256>;
#[derive(Deserialize, Debug)]
pub struct PriceResponse {
    pub symbol: String,
    pub price: String,
}



pub async fn get_binance_price(symbol: &str) -> Result<PriceResponse> {
    let url = format!(
        "{}/ticker/price?symbol={}",
        get_api_base_url(),
        symbol
    );
    let client = reqwest::Client::new();
    let response = client.get(&url).send().await?.json().await?;
    Ok(response)
}

pub fn sign_request(params: &HashMap<&str, &str>) -> String {
    let secret = get_api_secret();
    // Build query string with consistent ordering
    let mut param_vec: Vec<(&str, &str)> = params.iter().map(|(k, v)| (*k, *v)).collect();
    param_vec.sort_by(|a, b| a.0.cmp(b.0));
    
    let query_string = param_vec
        .iter()
        .map(|(k, v)| format!("{}={}", k, v))
        .collect::<Vec<String>>()
        .join("&");
    
    let mut mac =
        HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC can take key of any size");

    mac.update(query_string.as_bytes());
    encode(&mac.finalize().into_bytes())
}

/// Sign request with ordered parameters (preserves insertion order)
pub fn sign_request_ordered(params: &[(&str, &str)]) -> String {
    let secret = get_api_secret();
    
    // Build query string preserving the exact order
    let query_string = params
        .iter()
        .map(|(k, v)| format!("{}={}", k, v))
        .collect::<Vec<String>>()
        .join("&");
    
    debug!("Query string for signature: {}", query_string);
    
    let mut mac =
        HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC can take key of any size");

    mac.update(query_string.as_bytes());
    let signature = encode(&mac.finalize().into_bytes());
    
    debug!("Generated signature: {}", signature);
    signature
}

/// Get spot account information
pub async fn get_spot_account_info() -> Result<serde_json::Value> {
    let client = reqwest::Client::new();
    let timestamp = chrono::Utc::now().timestamp_millis().to_string();
    
    let mut params = HashMap::new();
    params.insert("timestamp", timestamp.as_str());
    
    let signature = sign_request(&params);
    params.insert("signature", signature.as_str());
    
    let api_key = get_api_key();
    let url = format!("{}/account", get_api_base_url());
    
    let resp = client.get(&url)
        .header("X-MBX-APIKEY", api_key)
        .query(&params)
        .send().await?;
        
    let status = resp.status();
    let body = resp.text().await?;
    
    if status.is_success() {
        let account_info: serde_json::Value = serde_json::from_str(&body)?;
        Ok(account_info)
    } else {
        error!("Failed to get spot account info: {}", body);
        Err(anyhow::anyhow!("Failed to get spot account info: {}", body))
    }
}

/// Get available USDT balance for spot trading
pub async fn get_spot_usdt_balance() -> Result<f64> {
    let account_info = get_spot_account_info().await?;
    
    if let Some(balances) = account_info["balances"].as_array() {
        for balance in balances {
            if let Some(asset) = balance["asset"].as_str() {
                if asset == "USDT" {
                    if let Some(free_str) = balance["free"].as_str() {
                        return free_str.parse::<f64>()
                            .map_err(|e| anyhow::anyhow!("Failed to parse USDT balance '{}': {}", free_str, e));
                    }
                }
            }
        }
    }
    
    Err(anyhow::anyhow!("USDT balance not found in account info"))
}

pub async fn place_order(
                   symbol: &str,
                   side: &str,
                   quantity: f64) -> Result<()> {
    let client = reqwest::Client::new();
    let timestamp = chrono::Utc::now().timestamp_millis().to_string();
    let quant_param = format!("{}", quantity);
    let mut params = HashMap::new();
    params.insert("symbol", symbol);
    params.insert("side", side); // "BUY" or "SELL"
    params.insert("type", "MARKET"); // Market order
    params.insert("quantity", quant_param.as_str() );
    params.insert("timestamp", timestamp.as_str());

    let signature = sign_request(&params);
    params.insert("signature", signature.as_str());
    
    let api_key = get_api_key();
    let order_url = get_api_order_url();
    
    let resp = client.post(&order_url)
        .header("X-MBX-APIKEY", api_key)
        .form(&params)
        .send().await?;
    let body = resp.text().await?;
    debug!("API Response: {}", body);
    Ok(())
}



pub struct FetchParams {
    pub symbol: String,
    pub interval: String,
    pub start_time: Option<DateTime<Utc>>,
    pub end_time: Option<DateTime<Utc>>,
    pub limit: u32,
}

pub async fn download_klines_as_candles(params: &FetchParams) -> Result<Vec<CandleData>, Box<dyn Error + Send + Sync>> {
    let client = Client::new();
    let mut all_candles: Vec<CandleData> = vec![];
    
    let mut start_time = params.start_time.unwrap_or_else(|| Utc::now() - Duration::days(30 * 5));
    let end_time = params.end_time.unwrap_or_else(|| Utc::now());

    info!("🔄 Downloading klines from Binance ({} mode)...", 
          if is_testnet_mode() { "testnet" } else { "real" });

    while start_time < end_time {
        let mut url = format!(
            "{}/klines?symbol={}&interval={}&limit={}",
            get_api_base_url(),
            params.symbol,
            params.interval,
            params.limit
        );
        
        // Add start time parameter
        url.push_str(&format!("&startTime={}", start_time.timestamp_millis()));
        
        // Optionally add end time parameter
        if start_time + Duration::days(7) < end_time {
            // Fetch in 7-day chunks to avoid hitting limits
            let chunk_end = start_time + Duration::days(7);
            url.push_str(&format!("&endTime={}", chunk_end.timestamp_millis()));
        } else {
            url.push_str(&format!("&endTime={}", end_time.timestamp_millis()));
        }

        let resp = client.get(&url).send().await?.json::<Vec<Vec<serde_json::Value>>>().await?;

        if resp.is_empty() {
            break;
        }

        for entry in &resp {
            let open_time = entry[0].as_i64().unwrap_or_default();
            let open: f64 = entry[1].as_str().unwrap_or("0").parse().unwrap_or_default();
            let high: f64 = entry[2].as_str().unwrap_or("0").parse().unwrap_or_default();
            let low: f64 = entry[3].as_str().unwrap_or("0").parse().unwrap_or_default();
            let close: f64 = entry[4].as_str().unwrap_or("0").parse().unwrap_or_default();
            let volume: f64 = entry[5].as_str().unwrap_or("0").parse().unwrap_or_default();
            let close_time = entry[6].as_i64().unwrap_or_default();

            let candle = CandleData {
                open_time: Utc.timestamp_millis_opt(open_time).unwrap().naive_utc(),
                open,
                high,
                low,
                close,
                volume,
                close_time: Utc.timestamp_millis_opt(close_time).unwrap().naive_utc(),
            };
            
            all_candles.push(candle);
        }

        // Update start time for next iteration
        if let Some(last_entry) = resp.last() {
            start_time = Utc.timestamp_millis_opt(last_entry[6].as_i64().unwrap() + 1).unwrap();
        } else {
            break;
        }
        
        // Respect rate limits
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }

    info!("✅ Downloaded {} candles from Binance", all_candles.len());
    Ok(all_candles)
}

/// Download klines and save to CSV
pub async fn download_klines() -> Result<(), Box<dyn Error + Send + Sync>> {
    let params = FetchParams {
        symbol: "ETHUSDT".to_string(),
        interval: "5m".to_string(),
        start_time: Some(Utc::now() - Duration::days(365 * 3)),
        end_time: Some(Utc::now()),
        limit: 1000,
    };
    
    let candles = download_klines_as_candles(&params).await?;
    
    // Save to CSV file
    let mut file = File::create("data/eth_5m.csv")?;
    writeln!(file, "OpenTime,Open,High,Low,Close,Volume,CloseTime")?;
    
    for candle in &candles {
        writeln!(file, "{},{},{},{},{},{},{}",
            candle.open_time.to_string(),
            candle.open,
            candle.high,
            candle.low,
            candle.close,
            candle.volume,
            candle.close_time.to_string(),
        )?;
    }

    info!("✅ Saved {} candles to data/btc_5m.csv", candles.len());
    Ok(())
}

/// Download missing klines and save directly to database
pub async fn sync_missing_klines(
    symbol: &str, 
    interval: &str, 
    repository: &crate::repository::kline_repository::KlineRepository
) -> Result<usize, Box<dyn Error + Send + Sync>> {
    info!("🔄 Checking for missing klines in database...");
    
    // Get the most recent kline from the database
    let start_time = match repository.get_latest_candle(symbol, interval).await? {
        Some(candle) => {
            info!("📊 Latest kline in database is from: {}", candle.close_time);
            // Add 1 millisecond to avoid duplicating the last candle
            DateTime::<Utc>::from_naive_utc_and_offset(candle.close_time, Utc) + Duration::milliseconds(1)
        },
        None => {
            // If no klines exist, start from 6 months ago
            info!("📊 No klines found in database, starting from 6 months ago");
            Utc::now() - Duration::days(30 * 6)
        }
    };
    
    // Set the end time to now
    let end_time = Utc::now();
    
    if start_time >= end_time {
        info!("✅ Database is already up to date. No new klines needed.");
        return Ok(0);
    }
    
    info!("🔄 Downloading klines from {} to {}", start_time, end_time);
    
    // Setup fetch parameters
    let params = FetchParams {
        symbol: symbol.to_string(),
        interval: interval.to_string(),
        start_time: Some(start_time),
        end_time: Some(end_time),
        limit: 1000,
    };
    
    // Download the missing klines
    let candles = download_klines_as_candles(&params).await?;
    
    if candles.is_empty() {
        info!("✅ No new klines to add");
        return Ok(0);
    }
    
    info!("📥 Downloaded {} new klines from Binance", candles.len());
    
    // Save to database
    let saved_count = repository.save_klines(symbol, interval, &candles).await?;
    
    info!("✅ Successfully added {} new klines to database", saved_count);
    Ok(saved_count)
}

/// Sync historical klines from a specific date
pub async fn sync_historical_klines(
    symbol: &str,
    interval: &str,
    start_date: DateTime<Utc>,
    force: bool,
    repository: &crate::repository::kline_repository::KlineRepository
) -> Result<usize, Box<dyn Error + Send + Sync>> {
    info!("🔄 Starting historical klines sync from {}...", start_date.format("%Y-%m-%d"));
    
    // If not forcing, check existing data
    let actual_start = if !force {
        match repository.get_latest_candle(symbol, interval).await? {
            Some(candle) => {
                let latest_time = DateTime::<Utc>::from_naive_utc_and_offset(candle.close_time, Utc);
                if latest_time > start_date {
                    info!("📊 Found existing data up to {}. Syncing gaps only.", latest_time);
                    // We'll sync from the requested start date to fill any gaps
                    start_date
                } else {
                    info!("📊 Existing data ends at {}, which is before requested start date", latest_time);
                    start_date
                }
            },
            None => {
                info!("📊 No existing klines found, starting fresh from {}", start_date);
                start_date
            }
        }
    } else {
        info!("📊 Force mode enabled, syncing from {} regardless of existing data", start_date);
        start_date
    };
    
    let end_time = Utc::now();
    
    if actual_start >= end_time {
        info!("✅ No historical data to sync. Start date is in the future.");
        return Ok(0);
    }
    
    // Calculate total time span for progress reporting
    let total_days = (end_time - actual_start).num_days();
    info!("📊 Syncing {} days of {} data for {}", total_days, interval, symbol);
    
    // Setup fetch parameters
    let params = FetchParams {
        symbol: symbol.to_string(),
        interval: interval.to_string(),
        start_time: Some(actual_start),
        end_time: Some(end_time),
        limit: 1000,
    };
    
    // Download all historical klines
    info!("🔄 Downloading historical klines from Binance...");
    let candles = download_klines_as_candles(&params).await?;
    
    if candles.is_empty() {
        info!("✅ No historical klines found for the specified period");
        return Ok(0);
    }
    
    info!("📥 Downloaded {} historical klines", candles.len());
    
    // Save to database (duplicates will be handled by ON CONFLICT)
    info!("💾 Saving to database (duplicates will be automatically handled)...");
    let saved_count = repository.save_klines(symbol, interval, &candles).await?;
    
    info!("✅ Successfully processed {} klines ({} new/updated)", candles.len(), saved_count);
    Ok(saved_count)
}

