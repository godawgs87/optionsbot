"""
Backtesting engine for options trading strategies.
"""
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Union

from tqdm import tqdm
import pandas as pd

from db.database import Database
from api.thetadata_client import ThetaDataClient
from strategies.strategy_base import StrategyBase
from backtesting.performance_metrics import calculate_performance_metrics

logger = logging.getLogger(__name__)


class BacktestEngine:
    """Engine for backtesting options trading strategies with historical data."""
    
    def __init__(
        self,
        db: Database,
        thetadata_client: ThetaDataClient,
        strategy: StrategyBase,
        symbols: List[str],
        start_date: datetime,
        end_date: datetime,
        initial_capital: float = 100000.0,
        max_positions: int = 5,
        position_size_pct: float = 0.1,
        commission_per_contract: float = 0.65,
        slippage_pct: float = 0.01
    ):
        """
        Initialize backtest engine.
        
        Args:
            db: Database connection
            thetadata_client: ThetaData API client
            strategy: Strategy to backtest
            symbols: List of symbols to trade
            start_date: Backtest start date
            end_date: Backtest end date
            initial_capital: Starting capital
            max_positions: Maximum number of concurrent positions
            position_size_pct: Position size as percentage of capital
            commission_per_contract: Commission per options contract
            slippage_pct: Slippage percentage to simulate real-world fills
        """
        self.db = db
        self.thetadata_client = thetadata_client
        self.strategy = strategy
        self.symbols = symbols
        self.start_date = start_date
        self.end_date = end_date
        self.initial_capital = initial_capital
        self.max_positions = max_positions
        self.position_size_pct = position_size_pct
        self.commission_per_contract = commission_per_contract
        self.slippage_pct = slippage_pct
        
        # Backtest state
        self.current_capital = initial_capital
        self.open_positions = []
        self.closed_trades = []
        self.equity_curve = []
        self.current_date = None
    
    async def run(self) -> Dict[str, Any]:
        """
        Run the backtest.
        
        Returns:
            Dictionary with backtest results
        """
        logger.info(f"Starting backtest for {self.strategy.name} from {self.start_date} to {self.end_date}")
        
        # Initialize state
        self.current_capital = self.initial_capital
        self.open_positions = []
        self.closed_trades = []
        self.equity_curve = []
        
        # Generate daily date range
        date_range = []
        current_date = self.start_date
        while current_date <= self.end_date:
            # Skip weekends
            if current_date.weekday() < 5:  # 0-4 are Monday to Friday
                date_range.append(current_date)
            current_date += timedelta(days=1)
        
        # Main simulation loop
        with tqdm(total=len(date_range), desc="Backtesting") as pbar:
            for date in date_range:
                self.current_date = date
                
                # Process this trading day
                await self.process_trading_day(date)
                
                # Track equity
                self.track_equity(date)
                
                pbar.update(1)
        
        # Close any remaining open positions at the end
        if self.open_positions:
            for position in self.open_positions:
                await self.close_position(position, self.end_date, "end_of_backtest")
        
        # Calculate performance metrics
        performance = calculate_performance_metrics(
            initial_capital=self.initial_capital,
            final_capital=self.current_capital,
            equity_curve=self.equity_curve,
            trades=self.closed_trades
        )
        
        # Store backtest results in database
        backtest_id = self.save_backtest_results(performance)
        
        logger.info(f"Backtest completed with final capital: ${self.current_capital:.2f}")
        
        return {
            "backtest_id": backtest_id,
            "strategy": self.strategy.name,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "initial_capital": self.initial_capital,
            "final_capital": self.current_capital,
            "performance": performance,
            "trades": self.closed_trades,
            "equity_curve": self.equity_curve
        }
    
    async def process_trading_day(self, date: datetime):
        """
        Process a single trading day in the backtest.
        
        Args:
            date: Trading date
        """
        try:
            # Update market data for the day
            market_data = await self.fetch_market_data(date)
            
            # First, manage existing positions
            await self.manage_existing_positions(date, market_data)
            
            # Then, look for new trades
            if len(self.open_positions) < self.max_positions:
                # Run strategy to get new signals
                signals = await self.strategy.generate_signals(market_data, date)
                
                # Process signals
                for signal in signals:
                    # Check if we've reached position limit
                    if len(self.open_positions) >= self.max_positions:
                        break
                    
                    # Open new position based on signal
                    await self.open_position(signal, date)
        except Exception as e:
            logger.error(f"Error processing trading day {date}: {e}")
    
    async def fetch_market_data(self, date: datetime) -> Dict[str, Any]:
        """
        Fetch market data for a specific date.
        
        Args:
            date: Trading date
            
        Returns:
            Dictionary with market data
        """
        market_data = {
            "date": date,
            "options_chains": {},
            "underlying_prices": {}
        }
        
        for symbol in self.symbols:
            try:
                # Get option chain for the symbol
                options_chain = await self.thetadata_client.get_option_chain(symbol, date)
                market_data["options_chains"][symbol] = options_chain
                
                # Get underlying price
                # In a real implementation, you would get this from the API
                # For this example, we'll assume it's included in the option chain data
                if options_chain:
                    underlying_price = options_chain[0].get("underlying_price", 0)
                    market_data["underlying_prices"][symbol] = underlying_price
            except Exception as e:
                logger.error(f"Error fetching market data for {symbol} on {date}: {e}")
        
        return market_data
    
    async def manage_existing_positions(self, date: datetime, market_data: Dict[str, Any]):
        """
        Manage existing positions for the current day.
        
        Args:
            date: Current trading date
            market_data: Market data for the current day
        """
        # Identify positions that need to be closed
        positions_to_close = []
        
        for position in self.open_positions:
            # Check exit criteria
            symbol = position["symbol"]
            option_type = position["option_type"]
            strike = position["strike"]
            expiration = position["expiration"]
            
            # Skip if this symbol has no market data today
            if symbol not in market_data["options_chains"]:
                continue
            
            # Find the specific option in today's data
            option_data = None
            for option in market_data["options_chains"][symbol]:
                if (option["option_type"] == option_type and 
                    option["strike"] == strike and 
                    option["expiration"] == expiration):
                    option_data = option
                    break
            
            if not option_data:
                continue
            
            # Check strategy exit signals
            exit_signal = await self.strategy.check_exit_criteria(position, option_data, date)
            
            if exit_signal or datetime.strptime(position["expiration"], "%Y-%m-%d") <= date:
                # Mark for closing
                positions_to_close.append((position, exit_signal or "expiration"))
        
        # Close positions identified for exit
        for position, reason in positions_to_close:
            await self.close_position(position, date, reason)
    
    async def open_position(self, signal: Dict[str, Any], date: datetime):
        """
        Open a new position based on a signal.
        
        Args:
            signal: Trading signal
            date: Current trading date
        """
        try:
            symbol = signal.get("symbol")
            option_type = signal.get("option_type")
            strike = signal.get("strike")
            expiration = signal.get("expiration")
            entry_price = signal.get("price")
            
            # Calculate position size
            position_size_dollar = self.current_capital * self.position_size_pct
            contracts = int(position_size_dollar / (entry_price * 100))  # 100 shares per contract
            
            if contracts < 1:
                logger.warning(f"Insufficient capital for position in {symbol} {option_type} {strike}")
                return
            
            # Apply slippage to entry price
            slippage_adjusted_price = entry_price * (1 + self.slippage_pct) if option_type == "call" else entry_price * (1 - self.slippage_pct)
            
            # Calculate cost with commissions
            position_cost = (slippage_adjusted_price * 100 * contracts) + (self.commission_per_contract * contracts)
            
            # Check if we have enough capital
            if position_cost > self.current_capital:
                contracts = int((self.current_capital - self.commission_per_contract) / (slippage_adjusted_price * 100))
                if contracts < 1:
                    logger.warning(f"Insufficient capital to open position")
                    return
                position_cost = (slippage_adjusted_price * 100 * contracts) + (self.commission_per_contract * contracts)
            
            # Create position object
            position = {
                "symbol": symbol,
                "option_type": option_type,
                "strike": strike,
                "expiration": expiration,
                "entry_date": date.strftime("%Y-%m-%d"),
                "entry_price": slippage_adjusted_price,
                "contracts": contracts,
                "position_cost": position_cost,
                "current_price": slippage_adjusted_price,
                "exit_date": None,
                "exit_price": None,
                "exit_reason": None,
                "profit_loss": 0,
                "profit_loss_pct": 0
            }
            
            # Add to open positions
            self.open_positions.append(position)
            
            # Deduct cost from capital
            self.current_capital -= position_cost
            
            logger.debug(
                f"Opened position: {symbol} {option_type} ${strike} {expiration} "
                f"x{contracts} @ ${slippage_adjusted_price:.2f}"
            )
        except Exception as e:
            logger.error(f"Error opening position: {e}")
    
    async def close_position(self, position: Dict[str, Any], date: datetime, reason: str):
        """
        Close an existing position.
        
        Args:
            position: Position to close
            date: Current trading date
            reason: Reason for closing
        """
        try:
            # Find current price
            symbol = position["symbol"]
            option_type = position["option_type"]
            strike = position["strike"]
            expiration = position["expiration"]
            contracts = position["contracts"]
            
            # Get latest price
            market_data = await self.fetch_market_data(date)
            exit_price = position["current_price"]  # Default to last known price
            
            if symbol in market_data["options_chains"]:
                for option in market_data["options_chains"][symbol]:
                    if (option["option_type"] == option_type and 
                        option["strike"] == strike and 
                        option["expiration"] == expiration):
                        exit_price = option.get("last", 0) or option.get("mid", 0)
                        break
            
            # Apply slippage to exit price
            slippage_adjusted_price = exit_price * (1 - self.slippage_pct) if option_type == "call" else exit_price * (1 + self.slippage_pct)
            
            # Calculate proceeds and profit/loss
            proceeds = (slippage_adjusted_price * 100 * contracts) - (self.commission_per_contract * contracts)
            profit_loss = proceeds - position["position_cost"]
            profit_loss_pct = (profit_loss / position["position_cost"]) * 100 if position["position_cost"] > 0 else 0
            
            # Update position with exit details
            position["exit_date"] = date.strftime("%Y-%m-%d")
            position["exit_price"] = slippage_adjusted_price
            position["exit_reason"] = reason
            position["profit_loss"] = profit_loss
            position["profit_loss_pct"] = profit_loss_pct
            
            # Add to closed trades
            self.closed_trades.append(position)
            
            # Remove from open positions
            self.open_positions = [p for p in self.open_positions if p != position]
            
            # Add proceeds to capital
            self.current_capital += proceeds
            
            logger.debug(
                f"Closed position: {symbol} {option_type} ${strike} {expiration} "
                f"x{contracts} @ ${slippage_adjusted_price:.2f} ({reason}), "
                f"P/L: ${profit_loss:.2f} ({profit_loss_pct:.2f}%)"
            )
        except Exception as e:
            logger.error(f"Error closing position: {e}")
    
    def track_equity(self, date: datetime):
        """
        Track equity for the current day.
        
        Args:
            date: Current trading date
        """
        # Calculate portfolio value (cash + positions)
        positions_value = sum(p["current_price"] * p["contracts"] * 100 for p in self.open_positions)
        total_equity = self.current_capital + positions_value
        
        # Record equity point
        equity_point = {
            "date": date.strftime("%Y-%m-%d"),
            "capital": self.current_capital,
            "positions_value": positions_value,
            "total_equity": total_equity
        }
        
        self.equity_curve.append(equity_point)
    
    def save_backtest_results(self, performance: Dict[str, Any]) -> int:
        """
        Save backtest results to database.
        
        Args:
            performance: Performance metrics
            
        Returns:
            Backtest ID in database
        """
        try:
            # Prepare backtest data
            backtest_data = {
                "strategy": self.strategy.name,
                "start_date": self.start_date.strftime("%Y-%m-%d"),
                "end_date": self.end_date.strftime("%Y-%m-%d"),
                "total_trades": len(self.closed_trades),
                "winning_trades": performance.get("winning_trades", 0),
                "losing_trades": performance.get("losing_trades", 0),
                "win_rate": performance.get("win_rate", 0),
                "avg_profit": performance.get("avg_profit_pct", 0),
                "max_profit": performance.get("max_profit_pct", 0),
                "max_loss": performance.get("max_loss_pct", 0),
                "sharpe_ratio": performance.get("sharpe_ratio", 0),
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            
            # Insert backtest results
            backtest_id = self.db.insert("backtest_results", backtest_data)
            
            if not backtest_id:
                logger.error("Failed to save backtest results")
                return 0
            
            # Insert trade details
            for trade in self.closed_trades:
                trade_data = {
                    "backtest_id": backtest_id,
                    "symbol": trade.get("symbol", ""),
                    "option_type": trade.get("option_type", ""),
                    "strike": trade.get("strike", 0),
                    "expiration": trade.get("expiration", ""),
                    "entry_price": trade.get("entry_price", 0),
                    "entry_time": trade.get("entry_date", ""),
                    "exit_price": trade.get("exit_price", 0),
                    "exit_time": trade.get("exit_date", ""),
                    "profit_pct": trade.get("profit_loss_pct", 0),
                    "profit_amount": trade.get("profit_loss", 0)
                }
                
                self.db.insert("backtest_trades", trade_data)
            
            logger.info(f"Backtest results saved with ID: {backtest_id}")
            return backtest_id
        except Exception as e:
            logger.error(f"Error saving backtest results: {e}")
            return 0