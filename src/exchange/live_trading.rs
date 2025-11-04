use anyhow::Result;
use chrono::{NaiveDateTime, Timelike, Utc};
use log::{debug, error, info, warn};
use std::collections::VecDeque;
use tokio::sync::{mpsc, Mutex};
use tokio::time::{self, Duration};

use crate::exchange::binance::{get_spot_usdt_balance, is_testnet_mode, place_order};
use crate::exchange::binance_futures::{
    close_futures_position, get_futures_available_balance, get_futures_positions,
    initialize_futures_trading, place_futures_order,
};
use crate::model::{ActiveTrade, CandleData, PositionType, StrategyConfig, TradeSignal};
use crate::strategy::exit::{check_exit_conditions, update_trade_management, ExitContext};
use crate::strategy::market_state::MarketState;
use crate::strategy::factory::TradingStrategyFactory;
use crate::strategy::traits::{TradingStrategy, StrategyFactory};

const CANDLE_INTERVAL: u32 = 5; // 5 minute candles

// Import required for the repositories
use crate::repository::kline_repository::KlineRepository;
use crate::repository::trade_repository::TradeRepository;
use std::sync::Arc;

pub struct LiveTrader {
    config: StrategyConfig,
    market_state: MarketState,
    active_trade: Option<ActiveTrade>,
    candles: VecDeque<CandleData>,
    exit_context: ExitContext,
    // Trading strategy instance
    strategy: Box<dyn TradingStrategy>,
    // Optional database repository for storing candles
    db_repo: Option<(String, String, Arc<Mutex<KlineRepository>>)>,
    // Optional database repository for storing trades
    trade_repo: Option<(String, Arc<Mutex<TradeRepository>>)>,
    // Track database ID of current active trade
    active_trade_db_id: Option<i32>,
    // Cooldown mechanism to prevent rapid consecutive trades
    last_trade_exit_time: Option<chrono::NaiveDateTime>,
    // Track when live trading started for warmup period
    live_trading_start_time: Option<chrono::DateTime<chrono::Utc>>,
    // Cache for balance logging to reduce frequency
    last_balance_log_time: Option<chrono::NaiveDateTime>,
    // Paper trading components
    paper_components: Option<(
        Arc<Mutex<crate::paper_trading::paper_account::PaperAccount>>,
        Arc<crate::paper_trading::PaperOrderExecutor>,
        Option<Arc<crate::paper_trading::PaperTradeLogger>>,
    )>,
}

impl LiveTrader {
    pub fn new(config: StrategyConfig, historical_candles: Vec<CandleData>) -> Self {
        let market_state = MarketState::new(&config);
        let mut candles = VecDeque::with_capacity(1000);

        // Clone historical candles for sharing
        let historical_candles_for_buffer = historical_candles.clone();
        
        // Add historical candles to our buffer
        for candle in historical_candles {
            candles.push_back(candle);
        }

        // Create strategy instance
        let factory = TradingStrategyFactory::new();
        let strategy = match factory.create_strategy(&config.strategy_name) {
            Ok(s) => {
                info!("Live trading using strategy: {} ({})", s.name(), s.description());
                
                // Preload historical data for MSP strategy
                if config.strategy_name == "msp" {
                    if let Some(msp_adapter) = s.as_any().downcast_ref::<crate::strategy::msp::MSPStrategyAdapter>() {
                        msp_adapter.preload_historical_data(historical_candles_for_buffer);
                        info!("MSP strategy buffer preloaded with historical data");
                    }
                }
                
                s
            },
            Err(e) => {
                error!("Failed to create strategy '{}': {}. Using default strategy.", config.strategy_name, e);
                factory.create_strategy("default").expect("Default strategy should always exist")
            }
        };

        LiveTrader {
            config,
            market_state,
            active_trade: None,
            candles,
            exit_context: ExitContext::default(),
            strategy,
            db_repo: None,
            trade_repo: None,
            active_trade_db_id: None,
            last_trade_exit_time: None,
            live_trading_start_time: None,
            last_balance_log_time: None,
            paper_components: None,
        }
    }

    pub fn with_db_repository(
        mut self,
        symbol: &str,
        interval: &str,
        repo: Arc<Mutex<KlineRepository>>,
    ) -> Self {
        self.db_repo = Some((symbol.to_string(), interval.to_string(), repo));
        self
    }

    pub fn with_trade_repository(
        mut self,
        symbol: &str,
        repo: Arc<Mutex<TradeRepository>>,
    ) -> Self {
        self.trade_repo = Some((symbol.to_string(), repo));
        self
    }

    pub fn with_paper_trading(
        mut self,
        account: Arc<Mutex<crate::paper_trading::paper_account::PaperAccount>>,
        executor: Arc<crate::paper_trading::PaperOrderExecutor>,
        logger: Option<Arc<crate::paper_trading::PaperTradeLogger>>,
    ) -> Self {
        self.paper_components = Some((account, executor, logger));
        self
    }

    /// Check if paper trading is enabled
    pub fn is_paper_trading(&self) -> bool {
        self.paper_components.is_some()
    }

    /// Execute order (paper or real based on configuration)
    async fn execute_order(
        &self,
        symbol: &str,
        side: &str,
        position_size: f64,
        is_futures: bool,
    ) -> Result<()> {
        if let Some((ref account, ref executor, ref logger)) = self.paper_components {
            // Paper trading execution
            info!(
                "📝 Paper Trading: Executing {} order for {} {}",
                side, position_size, symbol
            );

            // Extract ML metadata if using MSP strategy
            let (ml_regime, ml_probability) = if self.config.strategy_name == "msp" {
                // Try to get ML metadata from MSP strategy
                if let Some(msp_adapter) = self.strategy.as_any().downcast_ref::<crate::strategy::msp::MSPStrategyAdapter>() {
                    let regime = msp_adapter.get_last_ml_regime();
                    let prob = msp_adapter.get_last_ml_probability();
                    
                    if let (Some(ref r), Some(p)) = (&regime, &prob) {
                        info!("MSP ML metadata for trade: regime={}, probability={:.4}", r, p);
                    }
                    
                    (regime, prob)
                } else {
                    (None, None)
                }
            } else {
                (None, None)
            };

            let order = crate::model::Order {
                symbol: symbol.to_string(),
                side: match side {
                    "BUY" => crate::model::OrderSide::Buy,
                    "SELL" => crate::model::OrderSide::Sell,
                    _ => return Err(anyhow::anyhow!("Invalid order side")),
                },
                order_type: crate::model::OrderType::Market,
                quantity: position_size,
                price: 0.0, // Market order
                status: crate::model::OrderStatus::New,
                executed_qty: None,
                order_id: None,
                time_in_force: None,
                update_time: None,
            };

            let strategy_name = &self.config.strategy_name;
            executor
                .execute_order(
                    order,
                    strategy_name,
                    ml_regime,
                    ml_probability,
                )
                .await?;

            // Log trade to database if available
            if let Some(logger) = logger {
                let account_guard = account.lock().await;
                if let Some(trade) = account_guard.get_trades().last() {
                    let _ = logger.log_trade(trade).await;
                }
            }

            Ok(())
        } else {
            // Real trading execution
            if is_futures {
                info!("Placing FUTURES order...");
                place_futures_order(symbol, side, side, position_size)
                    .await
                    .map(|_| ())
            } else {
                info!("Placing SPOT order...");
                place_order(symbol, side, position_size).await
            }
        }
    }

    /// Close position (paper or real based on configuration)
    async fn close_position(
        &self,
        symbol: &str,
        side: &str,
        position_size: f64,
        is_futures: bool,
        price: f64,
    ) -> Result<()> {
        if let Some((ref _account, ref executor, ref _logger)) = self.paper_components {
            // Paper trading close
            info!("📝 Paper Trading: Closing position for {}", symbol);
            executor
                .close_position(symbol, price, "Exit signal")
                .await?;
            Ok(())
        } else {
            // Real trading close
            if is_futures {
                info!("Closing FUTURES position...");
                let position_side = if side == "BUY" { "SHORT" } else { "LONG" };
                close_futures_position(symbol, position_side, position_size)
                    .await
                    .map(|_| ())
            } else {
                info!("Closing SPOT position...");
                place_order(symbol, side, position_size).await
            }
        }
    }

    /// Get account balance (paper or real)
    async fn get_account_balance(&self) -> Result<f64> {
        if let Some((ref account, _, _)) = self.paper_components {
            // Paper trading balance
            let account_guard = account.lock().await;
            let state = account_guard.get_state();
            Ok(state.free_margin)
        } else if is_testnet_mode() {
            Ok(self.config.account_size)
        } else {
            // Fetch real account balance
            if self.config.use_futures {
                get_futures_available_balance().await
            } else {
                get_spot_usdt_balance().await
            }
        }
    }

    /// Create a new LiveTrader with database integration for both klines and trades
    pub fn new_with_database(
        config: StrategyConfig,
        _data_feed: Arc<crate::feeds::BroadcastDataFeed>,
        kline_repo: Option<Arc<Mutex<KlineRepository>>>,
        trade_repo: Option<(String, Arc<Mutex<TradeRepository>>)>,
    ) -> Self {
        let market_state = MarketState::new(&config);
        let candles = VecDeque::with_capacity(1000);

        let db_repo = kline_repo.map(|repo| ("BTCUSDT".to_string(), "5m".to_string(), repo));

        // Create strategy instance
        let factory = TradingStrategyFactory::new();
        let strategy = match factory.create_strategy(&config.strategy_name) {
            Ok(s) => {
                info!("LiveTrader with_paper_trading using strategy: {} ({})", s.name(), s.description());
                s
            },
            Err(e) => {
                error!("Failed to create strategy '{}': {}. Using default strategy.", config.strategy_name, e);
                factory.create_strategy("default").expect("Default strategy should always exist")
            }
        };

        LiveTrader {
            config,
            market_state,
            active_trade: None,
            candles,
            exit_context: ExitContext::default(),
            strategy,
            db_repo,
            trade_repo,
            active_trade_db_id: None,
            last_trade_exit_time: None,
            live_trading_start_time: None,
            last_balance_log_time: None,
            paper_components: None,
        }
    }

    pub fn update_market_state(&mut self) {
        let candles_vec: Vec<CandleData> = self.candles.iter().cloned().collect();
        self.market_state.update(&candles_vec, &self.config);
    }

    pub async fn add_candle_update(&mut self, candle: CandleData) -> Result<()> {
        // Check if this is a new candle or an update to the current one
        let is_new_candle = if let Some(last_candle) = self.candles.back() {
            // Compare by open time to determine if it's a new candle
            candle.open_time != last_candle.open_time
        } else {
            true // First candle
        };

        if is_new_candle {
            // Add the new candle to our data
            self.candles.push_back(candle.clone());

            // Keep only the last 1000 candles
            if self.candles.len() > 1000 {
                self.candles.pop_front();
            }

            // Update market state with the new data
            self.update_market_state();

            info!("📊 New candle: O:{:.2} H:{:.2} L:{:.2} C:{:.2} V:{:.2}", 
                candle.open, candle.high, candle.low, candle.close, candle.volume);
        } else {
            // Update the existing candle (shouldn't happen with closed candles from WebSocket)
            if let Some(last_candle) = self.candles.back_mut() {
                *last_candle = candle.clone();
            }
        }

        // Store the candle in the database if repository is available
        // This is a "fire and forget" operation - we don't await the result
        if let Some((ref symbol, ref interval, ref repo)) = self.db_repo {
            let symbol_clone = symbol.clone();
            let interval_clone = interval.clone();
            let repo_clone = Arc::clone(repo);
            let candle_clone = candle.clone();

            // Spawn a new task to save the candle without blocking
            tokio::spawn(async move {
                // tokio::sync::Mutex is not poisoned like std::sync::Mutex
                // so lock() only returns a MutexGuard, not a Result
                let repo_guard = repo_clone.lock().await;

                // Try to save the new candle
                if let Err(e) = repo_guard
                    .save_kline(&symbol_clone, &interval_clone, &candle_clone)
                    .await
                {
                    error!("Failed to save new candle to database: {}", e);
                } else {
                    // Remove debug log to reduce verbosity - new candle saves happen frequently
                }
            });
        }

        Ok(())
    }

    pub async fn check_and_execute_trading_logic(&mut self) -> Result<()> {
        // Skip if we don't have enough data yet
        if !self.has_enough_data_for_indicators() {
            return Ok(());
        }

        // First check if we have an active trade that needs to be managed
        let close_position_info = self.check_active_trade_for_exit().await?;

        // Handle position closing outside the mutable borrow scope
        if let Some(close_info) = close_position_info {
            self.handle_position_close(close_info).await?;
        }

        // If we still have an active trade, skip entry checks
        if self.active_trade.is_some() {
            return Ok(());
        }

        // Check if we should skip entry due to warmup or cooldown
        if self.should_skip_entry() {
            return Ok(());
        }

        // Get current candle and account balance
        let current_candle = self.candles.back().unwrap().clone();
        let account_balance = self.get_account_balance_with_logging().await;

        // Adjust config for account balance if needed
        let mut adjusted_config = self.config.clone();
        adjusted_config.account_size = account_balance;

        // Check entry conditions using strategy
        if let Some(signal) = self.strategy.check_entry_conditions(
            &self.market_state,
            &current_candle,
            &adjusted_config,
        ) {
            self.process_entry_signal(signal).await?;
        }

        Ok(())
    }

    // Helper method to check if we have enough data for indicators
    fn has_enough_data_for_indicators(&self) -> bool {
        let min_indicators = self
            .config
            .ema_long_period
            .max(self.config.rsi_period)
            .max(self.config.adx_period);
        
        if self.candles.len() < min_indicators as usize {
            info!(
                "Not enough data for indicators yet. Need {} candles, have {}",
                min_indicators,
                self.candles.len()
            );
            false
        } else {
            true
        }
    }

    // Check active trade for exit conditions
    async fn check_active_trade_for_exit(&mut self) -> Result<Option<(PositionType, f64, f64, String, String)>> {
        if let Some(trade) = &mut self.active_trade {
            let current_candle = self.candles.back().unwrap();
            
            // Update trade management (trailing stops, etc.)
            update_trade_management(
                &self.market_state,
                current_candle,
                trade,
                &self.config,
                &self.exit_context,
            );
            
            // Check for exit conditions
            let (should_exit, exit_reason) = check_exit_conditions(
                &self.market_state,
                current_candle,
                trade,
                &mut self.exit_context,
                &self.config,
            );
            
            if should_exit {
                info!("Exit signal: {}", exit_reason);
                // Extract necessary info before exiting the borrow scope
                let position_type = trade.position_type.clone();
                let position_size = trade.position_size;
                let close_price = current_candle.close;
                // Determine trade side (BUY to close a short, SELL to close a long)
                let side = match position_type {
                    PositionType::Long => "SELL",
                    PositionType::Short => "BUY",
                };
                Ok(Some((
                    position_type,
                    position_size,
                    close_price,
                    side.to_string(),
                    exit_reason,
                )))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    // Handle closing a position
    async fn handle_position_close(&mut self, close_info: (PositionType, f64, f64, String, String)) -> Result<()> {
        let (position_type, position_size, close_price, side, exit_reason) = close_info;
        
        // Execute the exit order
        let exit_result = self.execute_exit_order(&position_type, position_size, close_price, &side).await;
        
        match exit_result {
            Ok(()) => {
                self.handle_successful_exit(close_price, &side, &exit_reason).await?;
            }
            Err(e) => {
                self.handle_failed_exit(e, close_price).await?;
            }
        }
        
        Ok(())
    }

    // Execute exit order based on trading mode
    async fn execute_exit_order(&self, position_type: &PositionType, position_size: f64, close_price: f64, side: &str) -> Result<()> {
        let is_paper_trading = self.paper_components.is_some();
        
        if is_paper_trading {
            // Paper trading doesn't need position verification
            self.close_position(
                "BTCUSDT",
                side,
                position_size,
                self.config.use_futures,
                close_price,
            )
            .await
        } else if self.config.use_futures {
            // Verify and close futures position
            self.verify_and_close_futures_position(position_type, position_size, side).await
        } else {
            // Close spot position
            info!("Closing SPOT position...");
            place_order("BTCUSDT", side, position_size).await
        }
    }

    // Verify futures position exists before closing
    async fn verify_and_close_futures_position(&self, position_type: &PositionType, position_size: f64, side: &str) -> Result<()> {
        info!("Verifying FUTURES position before closing...");
        
        match get_futures_positions(Some("BTCUSDT")).await {
            Ok(positions) => {
                let position_side = match position_type {
                    PositionType::Long => "LONG",
                    PositionType::Short => "SHORT",
                };
                
                let position_exists = positions.iter().any(|pos| {
                    let amt = pos.position_amt.parse::<f64>().unwrap_or(0.0);
                    amt != 0.0
                        && ((amt > 0.0 && position_side == "LONG")
                            || (amt < 0.0 && position_side == "SHORT"))
                });
                
                if position_exists {
                    info!("Position confirmed on exchange. Closing FUTURES position...");
                    close_futures_position("BTCUSDT", position_side, position_size)
                        .await
                        .map(|_| ())
                } else {
                    warn!("Position not found on exchange. It may have been closed externally.");
                    Ok(()) // Consider it successfully closed
                }
            }
            Err(e) => {
                error!("Failed to verify position existence: {}", e);
                Err(anyhow::anyhow!("Failed to verify position: {}", e))
            }
        }
    }

    // Handle successful exit
    async fn handle_successful_exit(&mut self, close_price: f64, side: &str, exit_reason: &str) -> Result<()> {
        info!(
            "✅ Successfully closed {} {} position with reason: {}",
            if self.config.use_futures { "FUTURES" } else { "SPOT" },
            if side == "SELL" { "LONG" } else { "SHORT" },
            exit_reason
        );
        
        // Save trade exit to database
        self.save_trade_exit_to_db(close_price, exit_reason).await;
        
        // Clear the active trade and set cooldown
        self.clear_active_trade();
        
        Ok(())
    }

    // Save trade exit to database
    async fn save_trade_exit_to_db(&self, close_price: f64, exit_reason: &str) {
        if let Some((ref _symbol, ref trade_repo)) = self.trade_repo {
            if let Some(trade_id) = self.active_trade_db_id {
                let repo_guard = trade_repo.lock().await;
                
                if let Some(active_trade) = &self.active_trade {
                    match repo_guard
                        .save_trade_exit(
                            trade_id,
                            close_price,
                            exit_reason,
                            active_trade,
                            None, // TODO: Get ML regime from strategy if using MSP
                            None, // TODO: Get ML probability from strategy if using MSP
                        )
                        .await
                    {
                        Ok(()) => {
                            debug!("Trade exit saved to database for ID: {}", trade_id);
                        }
                        Err(e) => {
                            error!("Failed to save trade exit to database: {}", e);
                        }
                    }
                }
            } else {
                warn!("No trade ID available for database update");
            }
        }
    }

    // Handle failed exit
    async fn handle_failed_exit(&mut self, error: anyhow::Error, close_price: f64) -> Result<()> {
        error!("❌ Failed to place exit order: {}", error);
        
        // If it's a ReduceOnly error, check if position actually exists
        if error.to_string().contains("ReduceOnly Order is rejected") {
            self.handle_reduce_only_error(close_price).await?;
        }
        
        Ok(())
    }

    // Handle ReduceOnly order rejection
    async fn handle_reduce_only_error(&mut self, close_price: f64) -> Result<()> {
        warn!("ReduceOnly order rejected. Checking if position was already closed...");
        
        if self.config.use_futures {
            match get_futures_positions(Some("BTCUSDT")).await {
                Ok(positions) => {
                    if positions.is_empty() || positions.iter().all(|p| {
                        p.position_amt.parse::<f64>().unwrap_or(0.0) == 0.0
                    }) {
                        info!("No open positions found. Clearing active trade.");
                        
                        // Save as externally closed
                        self.save_trade_exit_to_db(close_price, "Position closed externally").await;
                        
                        // Clear the active trade
                        self.clear_active_trade();
                    }
                }
                Err(check_err) => {
                    error!("Failed to check positions after ReduceOnly error: {}", check_err);
                }
            }
        }
        
        Ok(())
    }

    // Clear active trade and set cooldown
    fn clear_active_trade(&mut self) {
        self.active_trade = None;
        self.active_trade_db_id = None;
        self.last_trade_exit_time = Some(Utc::now().naive_utc());
    }

    // Check if we should skip entry due to warmup or cooldown
    fn should_skip_entry(&self) -> bool {
        // Check warmup period
        if self.is_in_warmup_period() {
            return true;
        }
        
        // Check cooldown period
        if self.is_in_cooldown_period() {
            return true;
        }
        
        false
    }

    // Check if in warmup period
    fn is_in_warmup_period(&self) -> bool {
        if let Some(start_time) = self.live_trading_start_time {
            let now = Utc::now();
            let warmup_duration = chrono::Duration::seconds(self.config.live_trading_warmup_seconds as i64);
            
            if now < start_time + warmup_duration {
                let remaining_seconds = (start_time + warmup_duration - now).num_seconds();
                if remaining_seconds % 10 == 0 || remaining_seconds <= 5 {
                    info!("⏳ Live trading warmup: {} seconds remaining before entry signals are enabled", remaining_seconds);
                }
                return true;
            }
        }
        false
    }

    // Check if in cooldown period
    fn is_in_cooldown_period(&self) -> bool {
        if let Some(last_exit) = self.last_trade_exit_time {
            let now = Utc::now().naive_utc();
            let cooldown_minutes = 15; // 15-minute cooldown between trades
            let time_since_exit = now.signed_duration_since(last_exit);
            
            if time_since_exit.num_minutes() < cooldown_minutes {
                return true;
            }
        }
        false
    }

    // Get account balance with logging
    async fn get_account_balance_with_logging(&mut self) -> f64 {
        match self.get_account_balance().await {
            Ok(balance) => {
                self.log_balance_if_needed(balance);
                balance
            }
            Err(e) => {
                warn!("Failed to get account balance, using config value: {}", e);
                self.config.account_size
            }
        }
    }

    // Log balance if enough time has passed
    fn log_balance_if_needed(&mut self, balance: f64) {
        let now = Utc::now().naive_utc();
        let should_log = match self.last_balance_log_time {
            Some(last_log) => {
                let time_since_log = now.signed_duration_since(last_log);
                time_since_log.num_minutes() >= 1
            }
            None => true,
        };
        
        if should_log {
            if self.is_paper_trading() {
                info!("📝 Paper trading balance: ${:.2}", balance);
            } else {
                info!("💰 Using real account balance: ${:.2}", balance);
            }
            self.last_balance_log_time = Some(now);
        }
    }

    // Process entry signal
    async fn process_entry_signal(&mut self, signal: TradeSignal) -> Result<()> {
        info!(
            "🎯 ENTRY SIGNAL DETECTED: {:?} with score {} (threshold: {} for {:?})",
            signal.position_type,
            signal.entry_score,
            match signal.position_type {
                PositionType::Long => self.config.long_entry_score_threshold,
                PositionType::Short => self.config.short_entry_score_threshold,
            },
            signal.position_type
        );
        
        // Log signal details
        info!(
            "Signal details - Price: ${:.2}, Size: {:.8} BTC, SL: ${:.2}, TP: ${:.2}",
            signal.entry_price, signal.position_size, signal.stop_loss, signal.take_profit
        );
        
        // Create and execute trade
        let new_trade = self.create_active_trade_from_signal(&signal);
        
        // Determine order side
        let side = match signal.position_type {
            PositionType::Long => "BUY",
            PositionType::Short => "SELL",
        };
        
        // Execute the order
        let order_result = self
            .execute_order("BTCUSDT", side, signal.position_size, self.config.use_futures)
            .await;
        
        match order_result {
            Ok(()) => {
                self.handle_successful_entry(new_trade, side, &signal).await?;
            }
            Err(e) => {
                error!("❌ Failed to place entry order: {}", e);
            }
        }
        
        Ok(())
    }

    // Create active trade from signal
    fn create_active_trade_from_signal(&self, signal: &TradeSignal) -> ActiveTrade {
        ActiveTrade {
            entry_time: Utc::now().naive_utc(),
            entry_price: signal.entry_price,
            position_size: signal.position_size,
            position_type: signal.position_type,
            initial_stop_loss: signal.stop_loss,
            current_stop_loss: signal.stop_loss,
            take_profit: signal.take_profit,
            max_price_reached: signal.entry_price,
            min_price_reached: signal.entry_price,
            is_breakeven_set: false,
            is_trailing_active: false,
            risk_amount: signal.risk_amount,
            required_margin: signal.required_margin,
            leverage: signal.leverage,
        }
    }

    // Handle successful entry
    async fn handle_successful_entry(&mut self, new_trade: ActiveTrade, side: &str, signal: &TradeSignal) -> Result<()> {
        info!(
            "✅ Successfully opened {} {} position at ${:.2} with size {:.8} BTC",
            if self.config.use_futures { "FUTURES" } else { "SPOT" },
            if side == "BUY" { "LONG" } else { "SHORT" },
            signal.entry_price,
            signal.position_size
        );
        
        // Set the active trade
        self.active_trade = Some(new_trade.clone());
        
        // Save trade entry to database
        self.save_trade_entry_to_db(&new_trade).await;
        
        Ok(())
    }

    // Save trade entry to database
    async fn save_trade_entry_to_db(&mut self, new_trade: &ActiveTrade) {
        if let Some((ref symbol, ref trade_repo)) = self.trade_repo {
            let repo_guard = trade_repo.lock().await;
            
            // Determine trading mode
            let trading_mode = if self.is_paper_trading() {
                crate::model::TradingMode::Paper
            } else if is_testnet_mode() {
                crate::model::TradingMode::Testnet
            } else {
                crate::model::TradingMode::Real
            };
            
            // Try to save the trade entry
            match repo_guard
                .save_trade_entry(
                    symbol,
                    new_trade,
                    trading_mode,
                    &self.config.strategy_name,
                )
                .await
            {
                Ok(trade_id) => {
                    debug!(
                        "Trade entry saved to database with ID: {} (mode: {})",
                        trade_id, trading_mode
                    );
                    self.active_trade_db_id = Some(trade_id);
                }
                Err(e) => {
                    error!("Failed to save trade entry to database: {}", e);
                }
            }
        }
    }
}

// Helper function to normalize a datetime to the nearest candle interval
fn normalize_to_candle_interval(dt: NaiveDateTime, interval_minutes: u32) -> NaiveDateTime {
    let minutes = dt.minute();
    let normalized_minutes = (minutes / interval_minutes) * interval_minutes;

    // Chain the operations together and handle any potential failures
    dt.with_minute(normalized_minutes)
        .and_then(|dt| dt.with_second(0))
        .and_then(|dt| dt.with_nanosecond(0))
        .unwrap_or(dt) // Fallback to the original datetime if any operation fails
}

pub async fn run_live_trading(
    config: StrategyConfig,
    historical_candles: Vec<CandleData>,
    db_pool: Option<sqlx::Pool<sqlx::Postgres>>,
) -> Result<()> {
    run_live_trading_with_paper_mode(config, historical_candles, db_pool, false).await
}

pub async fn run_live_trading_with_paper_mode(
    mut config: StrategyConfig,
    historical_candles: Vec<CandleData>,
    db_pool: Option<sqlx::Pool<sqlx::Postgres>>,
    paper_mode: bool,
) -> Result<()> {
    // Create a channel for candle updates
    let (candle_sender, mut candle_receiver) = mpsc::channel::<CandleData>(100);

    // For live trading, we want real-time data from production WebSocket
    // but orders should still go to testnet for safety
    let original_testnet_mode = crate::exchange::binance::is_testnet_mode();

    // Start the appropriate WebSocket connection based on trading mode
    let use_futures = config.use_futures;
    let ws_task = tokio::spawn(async move {
        let result = if use_futures {
            // Use futures WebSocket for futures trading
            info!("🔗 Starting futures WebSocket connection...");
            crate::exchange::binance_futures_ws::start_binance_futures_ws("BTCUSDT", candle_sender)
                .await
        } else {
            // Use spot WebSocket for spot trading
            info!("🔗 Starting spot WebSocket connection...");
            // Temporarily use production WebSocket for real-time data
            crate::exchange::binance::set_testnet_mode(false);
            let result =
                crate::exchange::binance_ws::start_binance_ws("BTCUSDT", candle_sender).await;
            // Restore original testnet mode
            crate::exchange::binance::set_testnet_mode(original_testnet_mode);
            result
        };

        match result {
            Ok(_) => {
                info!("WebSocket connection closed normally");
            }
            Err(e) => {
                error!("WebSocket error: {}", e);
                // In a production environment, we might want to implement reconnection
                // logic here to restart the WebSocket if it fails
            }
        }
    });

    // Symbol and interval we're trading
    let symbol = "BTCUSDT";
    let interval = "5m";

    // Create the live trader
    let mut trader = LiveTrader::new(config.clone(), historical_candles);

    // Set up paper trading components if enabled
    if paper_mode {
        info!("📝 Initializing Paper Trading mode...");
        info!("📊 Starting with virtual balance: $10,000");

        // Create paper trading configuration
        let paper_config = crate::paper_trading::PaperTradingConfig {
            enabled: true,
            initial_balance: 10000.0, // Start with $10,000 virtual balance
            maker_fee: 0.0010,        // 0.10% maker fee
            taker_fee: 0.0010,        // 0.10% taker fee (Binance spot)
            slippage_bps: 10,         // 10 basis points slippage
            risk: crate::paper_trading::RiskConfig {
                max_position_size: 0.1,
                max_open_positions: 3,
                daily_loss_limit: 0.1, // 10% daily loss
                max_drawdown_percent: 20.0,
                max_consecutive_losses: 3,
                position_size_limit: 0.50, // Max 50% of balance per trade
            },
        };

        // Create paper trading components
        let paper_account = Arc::new(tokio::sync::Mutex::new(
            crate::paper_trading::paper_account::PaperAccount::new(paper_config),
        ));
        let paper_executor = Arc::new(crate::paper_trading::PaperOrderExecutor::new(
            paper_account.clone(),
            50, // 50ms execution delay
        ));

        let paper_logger = db_pool
            .as_ref()
            .map(|pool| Arc::new(crate::paper_trading::PaperTradeLogger::new(pool.clone())));

        // Initialize paper trading tables if DB is available
        if let Some(logger) = &paper_logger {
            if let Err(e) = logger.create_tables().await {
                warn!("Failed to create paper trading tables: {}", e);
            } else {
                info!("Paper trading tables initialized");
            }
        }

        // Set paper trading components on the trader
        trader = trader.with_paper_trading(paper_account, paper_executor, paper_logger);
    }

    // Create a ticker for checking trading logic (runs every 10 seconds)
    let mut trading_interval = time::interval(Duration::from_secs(10));

    // Create a ticker for data synchronization (runs every 5 minutes)
    let mut sync_interval = time::interval(Duration::from_secs(300)); // 5 minutes = 300 seconds

    // Create a ticker for position monitoring (runs every 30 seconds)
    let mut position_interval = time::interval(Duration::from_secs(30));

    // Track WebSocket task state
    let mut ws_task_handle = Some(ws_task);

    // Prepare repositories if DB is enabled
    let (kline_repo, trade_repo) = if let Some(pool) = db_pool.clone() {
        let kline_repo = crate::repository::kline_repository::KlineRepository::new(pool.clone());
        let trade_repo = crate::repository::trade_repository::TradeRepository::new(pool);
        (Some(kline_repo), Some(trade_repo))
    } else {
        (None, None)
    };

    // Set up the trader with DB repositories if DB is enabled
    if let Some(repo) = kline_repo {
        // Create a tokio::sync::Mutex-wrapped repository for thread-safe access
        // in an asynchronous context
        let repo_arc = Arc::new(Mutex::new(repo));
        trader = trader.with_db_repository(symbol, interval, repo_arc);
        info!("Configured real-time candle storage to database");
    }

    if let Some(repo) = trade_repo {
        // Create a tokio::sync::Mutex-wrapped repository for thread-safe access
        // in an asynchronous context
        let repo_arc = Arc::new(Mutex::new(repo));
        trader = trader.with_trade_repository(symbol, repo_arc);
        info!("Configured real-time trade storage to database");
    }

    if db_pool.is_some() {
        // Initial market state update
        trader.update_market_state();
        info!("Initial market state updated from historical data");
    }

    // Initialize futures trading settings if enabled
    if trader.config.use_futures {
        info!("Initializing futures trading settings...");
        match initialize_futures_trading("BTCUSDT", trader.config.futures_leverage).await {
            Ok(_) => info!(
                "Futures trading initialized successfully with {}x leverage",
                trader.config.futures_leverage
            ),
            Err(e) => {
                error!(
                    "Failed to initialize futures trading: {}. Continuing anyway...",
                    e
                );
                // Continue anyway - the error might be due to testnet limitations
            }
        }
    } else {
        info!("Futures trading disabled - using spot trading");
    }

    // Main loop
    loop {
        tokio::select! {
            // Handle price updates
            Some(candle) = candle_receiver.recv() => {
                if let Err(e) = trader.add_candle_update(candle).await {
                    error!("Error adding price update: {}", e);
                }
            }

            // Periodically check trading logic
            _ = trading_interval.tick() => {
                if let Err(e) = trader.check_and_execute_trading_logic().await {
                    error!("Error in trading logic: {}", e);
                }
            }

            // Periodically sync data with Binance if DB is enabled
            _ = sync_interval.tick() => {
                if let Some(pool) = &db_pool {
                    // Create a new repository instance for this operation
                    let sync_repo = crate::repository::kline_repository::KlineRepository::new(pool.clone());

                    info!("Performing regular data synchronization with Binance...");
                    match crate::exchange::binance::sync_missing_klines("BTCUSDT", "5m", &sync_repo).await {
                        Ok(count) => {
                            if count > 0 {
                                info!("Synchronized {} new candles", count);
                            } else {
                                debug!("No new candles to synchronize");
                            }
                        },
                        Err(e) => {
                            error!("Error during data synchronization: {}", e);
                        }
                    }
                }
            }

            // Periodically check futures positions if enabled
            _ = position_interval.tick() => {
                if trader.config.use_futures && !trader.is_paper_trading() {
                    debug!("Checking futures positions...");
                    match get_futures_positions(Some("BTCUSDT")).await {
                        Ok(positions) => {
                            if positions.is_empty() {
                                // No open positions on exchange, clear local state
                                if trader.active_trade.is_some() {
                                    info!("📊 No open positions found on exchange, clearing local active trade");

                                    // Save trade exit to database if repository is available
                                    if let Some((ref symbol, ref trade_repo)) = trader.trade_repo {
                                        if let Some(trade_id) = trader.active_trade_db_id {
                                            if let Some(ref trade) = trader.active_trade {
                                                let repo_guard = trade_repo.lock().await;

                                                // Use a reasonable exit price (we don't have current candle in this context)
                                                let exit_price = trade.entry_price; // Approximate - position was closed externally
                                                let exit_reason = "Position not found on exchange";

                                                match repo_guard.save_trade_exit(trade_id, exit_price, exit_reason, trade, None, None).await {
                                                    Ok(()) => {
                                                        debug!("Trade exit saved to database for missing position, ID: {}", trade_id);
                                                    },
                                                    Err(e) => {
                                                        error!("Failed to save missing position trade exit to database: {}", e);
                                                    }
                                                }
                                            }
                                        }
                                    }

                                    trader.active_trade = None;
                                    trader.active_trade_db_id = None;
                                    trader.last_trade_exit_time = Some(Utc::now().naive_utc());
                                }
                            } else {
                                // Check if our tracked position matches exchange positions
                                let mut position_found = false;
                                for pos in positions {
                                    let amt = pos.position_amt.parse::<f64>().unwrap_or(0.0);
                                    if amt != 0.0 {
                                        position_found = true;
                                        let pnl = pos.unrealized_profit.parse::<f64>().unwrap_or(0.0);
                                        info!("📊 Active position: {} {} @ {} (PnL: ${:.2})",
                                            if amt > 0.0 { "LONG" } else { "SHORT" },
                                            amt.abs(),
                                            pos.entry_price,
                                            pnl
                                        );

                                        // Sync position type with active trade if exists
                                        if let Some(ref mut trade) = trader.active_trade {
                                            let exchange_position_type = if amt > 0.0 { PositionType::Long } else { PositionType::Short };
                                            if trade.position_type != exchange_position_type || (trade.position_size - amt.abs()).abs() > 0.0001 {
                                                warn!("⚠️ Local position mismatch with exchange. Syncing...");
                                                trade.position_type = exchange_position_type;
                                                trade.position_size = amt.abs();
                                            }
                                        }
                                    }
                                }

                                if !position_found && trader.active_trade.is_some() {
                                    info!("📊 No matching position found on exchange, clearing local active trade");

                                    // Save trade exit to database if repository is available
                                    if let Some((ref symbol, ref trade_repo)) = trader.trade_repo {
                                        if let Some(trade_id) = trader.active_trade_db_id {
                                            if let Some(ref trade) = trader.active_trade {
                                                let repo_guard = trade_repo.lock().await;

                                                // Use a reasonable exit price (we don't have current candle in this context)
                                                let exit_price = trade.entry_price; // Approximate - position was closed externally
                                                let exit_reason = "No matching position on exchange";

                                                match repo_guard.save_trade_exit(trade_id, exit_price, exit_reason, trade, None, None).await {
                                                    Ok(()) => {
                                                        debug!("Trade exit saved to database for unmatched position, ID: {}", trade_id);
                                                    },
                                                    Err(e) => {
                                                        error!("Failed to save unmatched position trade exit to database: {}", e);
                                                    }
                                                }
                                            }
                                        }
                                    }

                                    trader.active_trade = None;
                                    trader.active_trade_db_id = None;
                                    trader.last_trade_exit_time = Some(Utc::now().naive_utc());
                                }
                            }
                        }
                        Err(e) => {
                            debug!("Failed to check positions: {}", e);
                        }
                    }
                }
            }

            // Check if WebSocket task is still active and if it has completed
            result = async {
                if let Some(handle) = &mut ws_task_handle {
                    handle.await
                } else {
                    // This branch should never execute since we break after moving the handle
                    Ok(())
                }
            }, if ws_task_handle.is_some() => {
                match result {
                    Ok(_) => {
                        warn!("WebSocket connection task ended. Attempting to restart...");
                        // Reset the WebSocket task handle so we can restart
                        ws_task_handle = None;

                        // Restart the WebSocket connection
                        let (new_candle_sender, new_candle_receiver) = mpsc::channel::<CandleData>(100);
                        candle_receiver = new_candle_receiver;

                        let use_futures = trader.config.use_futures;
                        let original_testnet_mode = crate::exchange::binance::is_testnet_mode();

                        let new_ws_task = tokio::spawn(async move {
                            let result = if use_futures {
                                info!("🔗 Restarting futures WebSocket connection...");
                                crate::exchange::binance_futures_ws::start_binance_futures_ws("BTCUSDT", new_candle_sender).await
                            } else {
                                info!("🔗 Restarting spot WebSocket connection...");
                                // Temporarily use production WebSocket for real-time data
                                crate::exchange::binance::set_testnet_mode(false);
                                let result = crate::exchange::binance_ws::start_binance_ws("BTCUSDT", new_candle_sender).await;
                                // Restore original testnet mode
                                crate::exchange::binance::set_testnet_mode(original_testnet_mode);
                                result
                            };

                            match result {
                                Ok(_) => info!("WebSocket connection restarted successfully"),
                                Err(e) => error!("WebSocket restart failed: {}", e),
                            }
                        });

                        ws_task_handle = Some(new_ws_task);
                        info!("✅ WebSocket connection restarted");
                    },
                    Err(e) => {
                        error!("WebSocket task error: {}. Attempting to restart...", e);
                        // Reset the WebSocket task handle so we can restart
                        ws_task_handle = None;

                        // Similar restart logic as above
                        let (new_candle_sender, new_candle_receiver) = mpsc::channel::<CandleData>(100);
                        candle_receiver = new_candle_receiver;

                        let use_futures = trader.config.use_futures;
                        let original_testnet_mode = crate::exchange::binance::is_testnet_mode();

                        let new_ws_task = tokio::spawn(async move {
                            let result = if use_futures {
                                info!("🔗 Restarting futures WebSocket connection after error...");
                                crate::exchange::binance_futures_ws::start_binance_futures_ws("BTCUSDT", new_candle_sender).await
                            } else {
                                info!("🔗 Restarting spot WebSocket connection after error...");
                                crate::exchange::binance::set_testnet_mode(false);
                                let result = crate::exchange::binance_ws::start_binance_ws("BTCUSDT", new_candle_sender).await;
                                crate::exchange::binance::set_testnet_mode(original_testnet_mode);
                                result
                            };

                            match result {
                                Ok(_) => info!("WebSocket connection restarted successfully after error"),
                                Err(e) => error!("WebSocket restart after error failed: {}", e),
                            }
                        });

                        ws_task_handle = Some(new_ws_task);
                        info!("✅ WebSocket connection restarted after error");
                    }
                }
                // Continue the loop instead of breaking - we've restarted the WebSocket
                continue;
            }
        }
    }

    info!("Live trading session ended. Cleaning up resources.");
    Ok(())
}

/// Run live trading with a pre-configured trader instance
pub async fn run_live_trading_with_trader(
    mut trader: LiveTrader,
    historical_candles: Vec<CandleData>,
    data_feed: Arc<crate::feeds::BroadcastDataFeed>,
) -> Result<()> {
    info!("Starting live trading with pre-configured trader");

    // Set the live trading start time for warmup period
    trader.live_trading_start_time = Some(Utc::now());
    info!(
        "Live trading warmup period: {} seconds - trades will be disabled during this time",
        trader.config.live_trading_warmup_seconds
    );

    // Add historical candles to trader
    for candle in historical_candles {
        trader.candles.push_back(candle);
    }

    // Update market state with historical data
    trader.update_market_state();

    // Set up WebSocket connection
    use crate::exchange::binance_ws::start_binance_ws;

    let (ws_sender, mut ws_receiver) = mpsc::channel::<CandleData>(100);

    // Start the WebSocket connection in a separate task
    let ws_task_handle = Some(tokio::spawn(async move {
        if let Err(e) = start_binance_ws("BTCUSDT", ws_sender).await {
            error!("WebSocket connection failed: {}", e);
        }
    }));

    let mut ws_task_handle = ws_task_handle;

    info!("🔗 WebSocket connection established, starting live trading loop");

    // Main trading loop
    loop {
        tokio::select! {
            // Process incoming price updates from WebSocket
            price_msg = ws_receiver.recv() => {
                if let Some(price) = price_msg {
                    let volume = 1000.0; // Default volume since WebSocket doesn't provide it
                    // Update trader with new price
                    // Convert price to a basic candle (without volume, since this old WebSocket only provides prices)
                    let now = Utc::now().naive_utc();
                    let open_time = normalize_to_candle_interval(now, CANDLE_INTERVAL);
                    let candle = CandleData {
                        open_time,
                        open: price.open.clone(),
                        high: price.high.clone(),
                        low: price.low.clone(),
                        close: price.close.clone(),
                        volume: price.volume.clone(),  
                        close_time: price.close_time,
                    };
                    
                    if let Err(e) = trader.add_candle_update(candle).await {
                        error!("Error processing price update: {}", e);
                        continue;
                    }

                    // Publish price update via data feed
                    let current_price = price.close.clone();
                    let price_event = crate::feeds::TradingEvent::PriceUpdate(
                        crate::feeds::PriceUpdate {
                            symbol: "BTCUSDT".to_string(),
                            price: current_price,
                            timestamp: Utc::now(),
                            volume,
                        }
                    );

                    if let Err(e) = data_feed.publish(price_event).await {
                        debug!("Failed to publish price update: {}", e);
                    }

                    // Check for trading opportunities
                    if let Err(e) = trader.check_and_execute_trading_logic().await {
                        error!("Error in trading logic: {}", e);
                    }
                } else {
                    warn!("WebSocket channel closed");
                    break;
                }
            }

            // Check WebSocket task status - only if task exists and we're not already handling the receiver
            result = async {
                if let Some(handle) = &mut ws_task_handle {
                    handle.await
                } else {
                    // Wait indefinitely if no task handle - this prevents immediate termination
                    tokio::time::sleep(tokio::time::Duration::from_secs(u64::MAX)).await;
                    Ok(())
                }
            }, if ws_task_handle.is_some() => {
                match result {
                    Ok(_) => {
                        warn!("WebSocket connection closed. Attempting to restart...");
                        // Wait a moment before restarting to avoid rapid reconnection attempts
                        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

                        // Restart WebSocket connection
                        let (new_ws_sender, new_ws_receiver) = mpsc::channel::<CandleData>(100);
                        ws_receiver = new_ws_receiver;

                        let new_task = tokio::spawn(async move {
                            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await; // Brief delay before connecting
                            if let Err(e) = start_binance_ws("BTCUSDT", new_ws_sender).await {
                                error!("WebSocket restart failed: {}", e);
                            }
                        });

                        ws_task_handle = Some(new_task);
                        info!("✅ WebSocket connection restart initiated");
                    },
                    Err(e) => {
                        error!("WebSocket task error: {}. Attempting to restart...", e);
                        // Wait a moment before restarting to avoid rapid reconnection attempts
                        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

                        // Restart WebSocket connection after error
                        let (new_ws_sender, new_ws_receiver) = mpsc::channel::<CandleData>(100);
                        ws_receiver = new_ws_receiver;

                        let new_task = tokio::spawn(async move {
                            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await; // Brief delay before connecting
                            if let Err(e) = start_binance_ws("BTCUSDT", new_ws_sender).await {
                                error!("WebSocket restart after error failed: {}", e);
                            }
                        });

                        ws_task_handle = Some(new_task);
                        info!("✅ WebSocket connection restart after error initiated");
                    }
                }
            }
        }
    }

    info!("Live trading session ended");
    Ok(())
}
