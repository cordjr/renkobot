//! Paper Trading Integration for Live Trading
//! 
//! Extends the LiveTrader with paper trading capabilities

use crate::exchange::live_trading::LiveTrader;
use crate::paper_trading::{
    PaperAccount, PaperOrderExecutor, PaperTradeLogger, PaperTradingConfig,
    SharedPaperAccount,
};
use crate::model::{Order, OrderSide, OrderType, OrderStatus, PositionType};
use crate::strategy::StrategyType;
use anyhow::Result;
use chrono::Utc;
use log::{info, warn};
use std::sync::Arc;
use tokio::sync::Mutex;
use sqlx::PgPool;

/// Extension trait for paper trading
pub trait PaperTradingExt {
    fn enable_paper_trading(
        self,
        config: PaperTradingConfig,
        db_pool: Option<PgPool>,
    ) -> PaperLiveTrader;
}

/// Paper trading wrapper for LiveTrader
pub struct PaperLiveTrader {
    inner: LiveTrader,
    paper_account: SharedPaperAccount,
    paper_executor: Arc<PaperOrderExecutor>,
    paper_logger: Option<Arc<PaperTradeLogger>>,
    paper_mode: bool,
    strategy_name: String,
    ml_regime: Option<String>,
    ml_probability: Option<f64>,
}

impl PaperTradingExt for LiveTrader {
    fn enable_paper_trading(
        self,
        config: PaperTradingConfig,
        db_pool: Option<PgPool>,
    ) -> PaperLiveTrader {
        let paper_account = Arc::new(Mutex::new(PaperAccount::new(config)));
        let paper_executor = Arc::new(PaperOrderExecutor::new(
            paper_account.clone(),
            50, // 50ms execution delay
        ));
        
        let paper_logger = db_pool.map(|pool| {
            Arc::new(PaperTradeLogger::new(pool))
        });
        
        PaperLiveTrader {
            inner: self,
            paper_account,
            paper_executor,
            paper_logger,
            paper_mode: true,
            strategy_name: "unknown".to_string(),
            ml_regime: None,
            ml_probability: None,
        }
    }
}

impl PaperLiveTrader {
    /// Set the strategy name for logging
    pub fn set_strategy(&mut self, strategy: &StrategyType) {
        self.strategy_name = match strategy {
            StrategyType::Simple => "simple",
            StrategyType::Doji => "doji",
            StrategyType::EntryOptimized => "entry_optimized",
            StrategyType::Combined => "combined",
            StrategyType::MSP => "msp",
            _ => "unknown",
        }.to_string();
    }
    
    /// Set ML predictions for the current trade
    pub fn set_ml_predictions(&mut self, regime: Option<String>, probability: Option<f64>) {
        self.ml_regime = regime;
        self.ml_probability = probability;
    }
    
    /// Place an order (paper or real based on mode)
    pub async fn place_order(&mut self, order: Order) -> Result<Order> {
        if self.paper_mode {
            info!("Paper Trading: Placing {} order", order.side);
            
            // Execute paper order
            let executed_order = self.paper_executor.execute_order(
                order,
                &self.strategy_name,
                self.ml_regime.clone(),
                self.ml_probability,
            ).await?;
            
            // Log to database if available
            if let Some(logger) = &self.paper_logger {
                let account = self.paper_account.lock().await;
                if let Some(trade) = account.get_trades().last() {
                    let _ = logger.log_trade(trade).await;
                }
            }
            
            Ok(executed_order)
        } else {
            // Real trading - delegate to inner trader
            // This would need to be implemented based on your existing order placement
            warn!("Real trading not implemented in paper trader wrapper");
            Ok(order)
        }
    }
    
    /// Close a position
    pub async fn close_position(&mut self, symbol: &str, price: f64, exit_reason: &str) -> Result<()> {
        if self.paper_mode {
            info!("Paper Trading: Closing position for {}", symbol);
            
            self.paper_executor.close_position(symbol, price, exit_reason).await?;
            
            // Log updated trade
            if let Some(logger) = &self.paper_logger {
                let account = self.paper_account.lock().await;
                if let Some(trade) = account.get_trades().last() {
                    let _ = logger.log_trade(trade).await;
                }
            }
            
            Ok(())
        } else {
            warn!("Real position closing not implemented in paper trader wrapper");
            Ok(())
        }
    }
    
    /// Update market data and check for signals
    pub async fn update(&mut self, price: f64) -> Result<()> {
        // Update inner trader
        self.inner.add_price_update(price).await?;
        
        // Update paper account positions with current price
        if self.paper_mode {
            let mut account = self.paper_account.lock().await;
            let mut prices = std::collections::HashMap::new();
            
            // Get symbol from inner trader config
            let symbol = "BTCUSDT"; // Or get from config
            prices.insert(symbol.to_string(), price);
            
            account.update_positions(&prices);
            
            // Log account state periodically (every 5 minutes)
            if let Some(logger) = &self.paper_logger {
                let now = Utc::now();
                if now.minute() % 5 == 0 && now.second() < 5 {
                    let _ = logger.log_account_state(account.get_state()).await;
                }
            }
        }
        
        Ok(())
    }
    
    /// Get paper trading metrics
    pub async fn get_metrics(&self) -> Result<crate::paper_trading::PaperTradingMetrics> {
        let account = self.paper_account.lock().await;
        Ok(account.calculate_metrics())
    }
    
    /// Get account state
    pub async fn get_account_state(&self) -> crate::paper_trading::PaperAccountState {
        let account = self.paper_account.lock().await;
        account.get_state().clone()
    }
    
    /// Initialize paper trading tables
    pub async fn initialize_tables(&self) -> Result<()> {
        if let Some(logger) = &self.paper_logger {
            logger.create_tables().await?;
        }
        Ok(())
    }
    
    /// Check if we should enter a trade
    pub async fn check_entry_signal(&mut self) -> Result<Option<Order>> {
        // This would integrate with the existing strategy system
        // For now, return None
        Ok(None)
    }
    
    /// Check if we should exit current position
    pub async fn check_exit_signal(&mut self) -> Result<Option<String>> {
        // This would integrate with the existing exit system
        // For now, return None
        Ok(None)
    }
}