use anyhow::Result;
use chrono::Utc;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use log::{info, error, warn};

use crate::exchange::binance::{get_api_key, is_testnet_mode, sign_request_ordered};

// Futures API Base URLs
const BINANCE_FUTURES_BASE_URL: &str = "https://fapi.binance.com";
const BINANCE_FUTURES_TESTNET_BASE_URL: &str = "https://testnet.binancefuture.com";

// Futures WebSocket URLs
const BINANCE_FUTURES_WS_URL: &str = "wss://fstream.binance.com/ws";
const BINANCE_FUTURES_TESTNET_WS_URL: &str = "wss://stream.binancefuture.com/ws";

#[derive(Debug, Serialize, Deserialize)]
pub struct FuturesOrderRequest {
    pub symbol: String,
    pub side: String,  // BUY or SELL
    pub position_side: String,  // LONG or SHORT
    pub order_type: String,  // MARKET, LIMIT, etc.
    pub quantity: f64,
    pub timestamp: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub price: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stop_price: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reduce_only: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub new_client_order_id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct FuturesOrderResponse {
    #[serde(rename = "orderId")]
    pub order_id: i64,
    pub symbol: String,
    pub status: String,
    #[serde(rename = "clientOrderId")]
    pub client_order_id: String,
    pub price: String,
    #[serde(rename = "avgPrice")]
    pub avg_price: String,
    #[serde(rename = "origQty")]
    pub orig_qty: String,
    #[serde(rename = "executedQty")]
    pub executed_qty: String,
    #[serde(rename = "cumQty")]
    pub cum_qty: String,
    #[serde(rename = "cumQuote")]
    pub cum_quote: String,
    #[serde(rename = "timeInForce")]
    pub time_in_force: String,
    #[serde(rename = "type")]
    pub order_type: String,
    #[serde(rename = "reduceOnly")]
    pub reduce_only: bool,
    #[serde(rename = "closePosition")]
    pub close_position: bool,
    pub side: String,
    #[serde(rename = "positionSide")]
    pub position_side: String,
    #[serde(rename = "stopPrice")]
    pub stop_price: String,
    #[serde(rename = "workingType")]
    pub working_type: String,
    #[serde(rename = "priceProtect")]
    pub price_protect: bool,
    #[serde(rename = "origType")]
    pub orig_type: String,
    #[serde(rename = "updateTime")]
    pub update_time: i64,
}

#[derive(Debug, Deserialize)]
pub struct FuturesAccountInfo {
    pub total_initial_margin: String,
    pub total_maint_margin: String,
    pub total_wallet_balance: String,
    pub total_unrealized_profit: String,
    pub total_margin_balance: String,
    pub available_balance: String,
    pub max_withdraw_amount: String,
}

pub fn get_futures_api_base_url() -> &'static str {
    if is_testnet_mode() {
        BINANCE_FUTURES_TESTNET_BASE_URL
    } else {
        BINANCE_FUTURES_BASE_URL
    }
}

pub fn get_futures_ws_url() -> &'static str {
    if is_testnet_mode() {
        BINANCE_FUTURES_TESTNET_WS_URL
    } else {
        BINANCE_FUTURES_WS_URL
    }
}


/// Set leverage for a futures symbol
pub async fn set_leverage(symbol: &str, leverage: u8) -> Result<()> {
    let client = Client::new();
    let timestamp = Utc::now().timestamp_millis().to_string();
    let leverage_str = leverage.to_string();
    
    let params_vec = vec![
        ("symbol", symbol),
        ("leverage", leverage_str.as_str()),
        ("timestamp", timestamp.as_str()),
    ];
    
    let signature = sign_request_ordered(&params_vec);
    
    // Build ordered form data to match signature order
    let mut form_data = Vec::new();
    for (k, v) in &params_vec {
        form_data.push((*k, *v));
    }
    form_data.push(("signature", signature.as_str()));
    
    let url = format!("{}/fapi/v1/leverage", get_futures_api_base_url());
    let api_key = get_api_key();
    
    let resp = client.post(&url)
        .header("X-MBX-APIKEY", api_key)
        .form(&form_data)
        .send()
        .await?;
    
    let status = resp.status();
    let body = resp.text().await?;
    
    if status.is_success() {
        info!("Successfully set leverage to {}x for {}", leverage, symbol);
        Ok(())
    } else {
        error!("Failed to set leverage: {}", body);
        Err(anyhow::anyhow!("Failed to set leverage: {}", body))
    }
}

/// Set position mode (true for hedge mode, false for one-way mode)
pub async fn set_position_mode(dual_side_position: bool) -> Result<()> {
    let client = Client::new();
    let timestamp = Utc::now().timestamp_millis().to_string();
    let dual_side_str = if dual_side_position { "true" } else { "false" };
    
    let params_vec = vec![
        ("dualSidePosition", dual_side_str),
        ("timestamp", timestamp.as_str()),
    ];
    
    let signature = sign_request_ordered(&params_vec);
    
    // Build form data manually to preserve order
    let mut form_data = Vec::new();
    for (k, v) in &params_vec {
        form_data.push((*k, *v));
    }
    form_data.push(("signature", signature.as_str()));
    
    let url = format!("{}/fapi/v1/positionSide/dual", get_futures_api_base_url());
    let api_key = get_api_key();
    
    let resp = client.post(&url)
        .header("X-MBX-APIKEY", api_key)
        .form(&form_data)
        .send()
        .await?;
    
    let status = resp.status();
    let body = resp.text().await?;
    
    if status.is_success() {
        info!("Successfully set position mode to {} mode", 
              if dual_side_position { "hedge" } else { "one-way" });
        Ok(())
    } else if body.contains("No need to change position side") {
        info!("Position mode already set to {} mode", 
              if dual_side_position { "hedge" } else { "one-way" });
        Ok(())
    } else {
        error!("Failed to set position mode: {}", body);
        Err(anyhow::anyhow!("Failed to set position mode: {}", body))
    }
}

/// Set margin type for a futures symbol (ISOLATED or CROSSED)
pub async fn set_margin_type(symbol: &str, margin_type: &str) -> Result<()> {
    let client = Client::new();
    let timestamp = Utc::now().timestamp_millis().to_string();
    
    let params_vec = vec![
        ("symbol", symbol),
        ("marginType", margin_type),
        ("timestamp", timestamp.as_str()),
    ];
    
    let signature = sign_request_ordered(&params_vec);
    
    // Build ordered form data to match signature order
    let mut form_data = Vec::new();
    for (k, v) in &params_vec {
        form_data.push((*k, *v));
    }
    form_data.push(("signature", signature.as_str()));
    
    let url = format!("{}/fapi/v1/marginType", get_futures_api_base_url());
    let api_key = get_api_key();
    
    let resp = client.post(&url)
        .header("X-MBX-APIKEY", api_key)
        .form(&form_data)
        .send()
        .await?;
    
    let status = resp.status();
    let body = resp.text().await?;
    
    if status.is_success() {
        info!("Successfully set margin type to {} for {}", margin_type, symbol);
        Ok(())
    } else if body.contains("No need to change margin type") {
        info!("Margin type already set to {} for {}", margin_type, symbol);
        Ok(())
    } else {
        error!("Failed to set margin type: {}", body);
        Err(anyhow::anyhow!("Failed to set margin type: {}", body))
    }
}

/// Place a futures market order
pub async fn place_futures_order(
    symbol: &str,
    side: &str,  // BUY or SELL
    position_side: &str,  // LONG or SHORT
    quantity: f64,
) -> Result<FuturesOrderResponse> {
    let client = Client::new();
    let timestamp = Utc::now().timestamp_millis().to_string();
    let quantity_str = format!("{:.3}", quantity);  // Format to 3 decimal places
    
    // Build parameters in a specific order for consistent signature
    let params_vec = vec![
        ("symbol", symbol),
        ("side", side),
        ("positionSide", position_side),
        ("type", "MARKET"),
        ("quantity", quantity_str.as_str()),
        ("timestamp", timestamp.as_str()),
    ];
    
    // Generate signature using ordered parameters
    let signature = sign_request_ordered(&params_vec);
    
    // Build ordered form data to match signature order
    let mut form_data = Vec::new();
    for (k, v) in &params_vec {
        form_data.push((*k, *v));
    }
    form_data.push(("signature", signature.as_str()));
    
    let url = format!("{}/fapi/v1/order", get_futures_api_base_url());
    let api_key = get_api_key();
    
    info!("Placing futures {} order: {} {} at MARKET", 
         position_side, quantity, symbol);
    
    let resp = client.post(&url)
        .header("X-MBX-APIKEY", api_key)
        .form(&form_data)
        .send()
        .await?;
    
    let status = resp.status();
    let body = resp.text().await?;
    
    if status.is_success() {
        let order_response: FuturesOrderResponse = serde_json::from_str(&body)?;
        info!("Successfully placed futures order: {:?}", order_response);
        Ok(order_response)
    } else {
        error!("Failed to place futures order: {}", body);
        Err(anyhow::anyhow!("Failed to place futures order: {}", body))
    }
}

/// Close a futures position
pub async fn close_futures_position(
    symbol: &str,
    position_side: &str,  // LONG or SHORT
    quantity: f64,
) -> Result<FuturesOrderResponse> {
    // To close a position:
    // - For LONG position: SELL with positionSide=LONG
    // - For SHORT position: BUY with positionSide=SHORT
    let side = match position_side {
        "LONG" => "SELL",
        "SHORT" => "BUY",
        _ => return Err(anyhow::anyhow!("Invalid position side: {}", position_side)),
    };
    
    let client = Client::new();
    let timestamp = Utc::now().timestamp_millis().to_string();
    let quantity_str = format!("{:.3}", quantity);
    
    let params_vec = vec![
        ("symbol", symbol),
        ("side", side),
        ("positionSide", position_side),
        ("type", "MARKET"),
        ("quantity", quantity_str.as_str()),
        ("timestamp", timestamp.as_str()),
    ];
    
    let signature = sign_request_ordered(&params_vec);
    
    // Build ordered form data to match signature order
    let mut form_data = Vec::new();
    for (k, v) in &params_vec {
        form_data.push((*k, *v));
    }
    form_data.push(("signature", signature.as_str()));
    
    let url = format!("{}/fapi/v1/order", get_futures_api_base_url());
    let api_key = get_api_key();
    
    info!("Closing futures {} position: {} {} at MARKET", 
         position_side, quantity, symbol);
    
    let resp = client.post(&url)
        .header("X-MBX-APIKEY", api_key)
        .form(&form_data)
        .send()
        .await?;
    
    let status = resp.status();
    let body = resp.text().await?;
    
    if status.is_success() {
        let order_response: FuturesOrderResponse = serde_json::from_str(&body)?;
        info!("Successfully closed futures position: {:?}", order_response);
        Ok(order_response)
    } else {
        error!("Failed to close futures position: {}", body);
        Err(anyhow::anyhow!("Failed to close futures position: {}", body))
    }
}

/// Get available balance for futures trading
pub async fn get_futures_available_balance() -> Result<f64> {
    let account_info = get_futures_account_info().await?;
    let balance_str = account_info.available_balance;
    balance_str.parse::<f64>()
        .map_err(|e| anyhow::anyhow!("Failed to parse available balance '{}': {}", balance_str, e))
}

/// Get futures account information
pub async fn get_futures_account_info() -> Result<FuturesAccountInfo> {
    let client = Client::new();
    let timestamp = Utc::now().timestamp_millis().to_string();
    let recv_window = "5000"; // 5 second receive window
    
    let params_vec = vec![
        ("recvWindow", recv_window),
        ("timestamp", timestamp.as_str()),
    ];
    
    let signature = sign_request_ordered(&params_vec);
    
    // Build query string manually to preserve order
    let query_string = params_vec
        .iter()
        .map(|(k, v)| format!("{}={}", k, v))
        .collect::<Vec<String>>()
        .join("&");
    let full_query = format!("{}&signature={}", query_string, signature);
    
    let url = format!("{}/fapi/v2/account?{}", get_futures_api_base_url(), full_query);
    let api_key = get_api_key();
    
    let resp = client.get(&url)
        .header("X-MBX-APIKEY", api_key)
        .send()
        .await?;
    
    let status = resp.status();
    let body = resp.text().await?;
    
    if status.is_success() {
        // Parse only the fields we need from the larger response
        let json: serde_json::Value = serde_json::from_str(&body)?;
        let account_info = FuturesAccountInfo {
            total_initial_margin: json["totalInitialMargin"].as_str().unwrap_or("0").to_string(),
            total_maint_margin: json["totalMaintMargin"].as_str().unwrap_or("0").to_string(),
            total_wallet_balance: json["totalWalletBalance"].as_str().unwrap_or("0").to_string(),
            total_unrealized_profit: json["totalUnrealizedProfit"].as_str().unwrap_or("0").to_string(),
            total_margin_balance: json["totalMarginBalance"].as_str().unwrap_or("0").to_string(),
            available_balance: json["availableBalance"].as_str().unwrap_or("0").to_string(),
            max_withdraw_amount: json["maxWithdrawAmount"].as_str().unwrap_or("0").to_string(),
        };
        Ok(account_info)
    } else {
        error!("Failed to get futures account info: {}", body);
        Err(anyhow::anyhow!("Failed to get futures account info: {}", body))
    }
}

/// Get current futures positions
pub async fn get_futures_positions(symbol: Option<&str>) -> Result<Vec<FuturesPosition>> {
    let client = Client::new();
    let timestamp = Utc::now().timestamp_millis().to_string();
    let recv_window = "5000"; // 5 second receive window
    
    let mut params_vec = vec![
        ("recvWindow", recv_window),
        ("timestamp", timestamp.as_str()),
    ];
    
    // Add symbol if provided (must be before signature)
    if let Some(sym) = symbol {
        params_vec.insert(0, ("symbol", sym));
    }
    
    let signature = sign_request_ordered(&params_vec);
    
    // Build query string manually to preserve order
    let query_string = params_vec
        .iter()
        .map(|(k, v)| format!("{}={}", k, v))
        .collect::<Vec<String>>()
        .join("&");
    let full_query = format!("{}&signature={}", query_string, signature);
    
    let url = format!("{}/fapi/v2/positionRisk?{}", get_futures_api_base_url(), full_query);
    let api_key = get_api_key();
    
    let resp = client.get(&url)
        .header("X-MBX-APIKEY", api_key)
        .send()
        .await?;
    
    let status = resp.status();
    let body = resp.text().await?;
    
    if status.is_success() {
        let positions: Vec<FuturesPosition> = serde_json::from_str(&body)?;
        let open_positions: Vec<FuturesPosition> = positions
            .into_iter()
            .filter(|p| p.position_amt.parse::<f64>().unwrap_or(0.0) != 0.0)
            .collect();
        
        for pos in &open_positions {
            let amt = pos.position_amt.parse::<f64>().unwrap_or(0.0);
            let side = if amt > 0.0 { "LONG" } else { "SHORT" };
            let pnl = pos.unrealized_profit.parse::<f64>().unwrap_or(0.0);
            info!("Open {} position: {} {} @ {} (PnL: ${:.2})", 
                side, amt.abs(), pos.symbol, pos.entry_price, pnl);
        }
        
        Ok(open_positions)
    } else {
        error!("Failed to get futures positions: {}", body);
        Err(anyhow::anyhow!("Failed to get futures positions: {}", body))
    }
}

#[derive(Debug, Deserialize)]
pub struct FuturesPosition {
    pub symbol: String,
    #[serde(rename = "positionAmt")]
    pub position_amt: String,
    #[serde(rename = "entryPrice")]
    pub entry_price: String,
    #[serde(rename = "markPrice")]
    pub mark_price: String,
    #[serde(rename = "unRealizedProfit")]
    pub unrealized_profit: String,
    pub leverage: String,
    #[serde(rename = "marginType")]
    pub margin_type: String,
}

/// Initialize futures trading settings for a symbol
pub async fn initialize_futures_trading(symbol: &str, leverage: u8) -> Result<()> {
    // Set position mode to hedge mode for LONG/SHORT position sides
    set_position_mode(true).await?;
    
    // Set margin type to ISOLATED for better risk management
    set_margin_type(symbol, "ISOLATED").await?;
    
    // Set leverage
    set_leverage(symbol, leverage).await?;
    
    // Get and log account info
    match get_futures_account_info().await {
        Ok(info) => {
            info!("Futures account info - Available balance: {} USDT", info.available_balance);
        }
        Err(e) => {
            warn!("Failed to get account info: {}", e);
        }
    }
    
    // Check current positions
    match get_futures_positions(Some(symbol)).await {
        Ok(positions) => {
            if positions.is_empty() {
                info!("No open futures positions for {}", symbol);
            }
        }
        Err(e) => {
            warn!("Failed to get positions: {}", e);
        }
    }
    
    Ok(())
}

/// Download futures klines data and save to CSV
pub async fn download_futures_klines() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use chrono::{Duration, TimeZone, Utc};
    use std::fs::File;
    use std::io::Write;
    use crate::model::CandleData;

    let client = reqwest::Client::new();
    let mut all_candles: Vec<CandleData> = vec![];
    
    // Download 6 months of data
    let start_time = Utc::now() - Duration::days(365 * 3);
    let end_time = Utc::now();
    let mut current_time = start_time;
    
    println!("🔄 Downloading futures klines from Binance ({} mode)...", 
             if is_testnet_mode() { "testnet" } else { "real" });
    println!("📅 Date range: {} to {}", start_time.format("%Y-%m-%d"), end_time.format("%Y-%m-%d"));

    while current_time < end_time {
        let mut url = format!(
            "{}/fapi/v1/klines?symbol={}&interval={}&limit={}",
            get_futures_api_base_url(),
            "ETHUSDT",
            "15m",
            1000
        );
        
        // Add start time parameter
        url.push_str(&format!("&startTime={}", current_time.timestamp_millis()));
        
        // Fetch in 7-day chunks to avoid hitting limits
        let chunk_end = if current_time + Duration::days(7) < end_time {
            current_time + Duration::days(7)
        } else {
            end_time
        };
        url.push_str(&format!("&endTime={}", chunk_end.timestamp_millis()));

        println!("📥 Fetching chunk: {} to {}", 
                current_time.format("%Y-%m-%d"), chunk_end.format("%Y-%m-%d"));

        let resp = client.get(&url).send().await?;
        let status = resp.status();
        
        if !status.is_success() {
            let error_text = resp.text().await?;
            return Err(format!("Failed to fetch futures klines: {} - {}", status, error_text).into());
        }

        let klines: Vec<Vec<serde_json::Value>> = resp.json().await?;

        if klines.is_empty() {
            break;
        }

        for kline in &klines {
            let open_time = kline[0].as_i64().unwrap_or_default();
            let open: f64 = kline[1].as_str().unwrap_or("0").parse().unwrap_or_default();
            let high: f64 = kline[2].as_str().unwrap_or("0").parse().unwrap_or_default();
            let low: f64 = kline[3].as_str().unwrap_or("0").parse().unwrap_or_default();
            let close: f64 = kline[4].as_str().unwrap_or("0").parse().unwrap_or_default();
            let volume: f64 = kline[5].as_str().unwrap_or("0").parse().unwrap_or_default();
            let close_time = kline[6].as_i64().unwrap_or_default();

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
        if let Some(last_kline) = klines.last() {
            current_time = Utc.timestamp_millis_opt(last_kline[6].as_i64().unwrap() + 1).unwrap();
        } else {
            break;
        }
        
        // Respect rate limits
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }

    // Save to CSV file with futures prefix
    let filename = "data/eth_15m_futures.csv";
    let mut file = File::create(filename)?;
    writeln!(file, "OpenTime,Open,High,Low,Close,Volume,CloseTime")?;
    
    for candle in &all_candles {
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

    println!("✅ Downloaded {} futures candles to {}", all_candles.len(), filename);
    Ok(())
}