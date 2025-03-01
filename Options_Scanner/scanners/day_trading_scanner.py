"""
Scanner for day trading opportunities in options.
"""
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

from api.thetadata_client import ThetaDataClient
from db.models import OpportunityModel
from notifications.telegram_bot import TelegramBot
from analysis.trade_analyzer import TradeAnalyzer

logger = logging.getLogger(__name__)


class DayTradingScanner:
    """Scanner for day trading opportunities in options."""
    
    def __init__(
        self, 
        thetadata_client: ThetaDataClient,
        opportunity_model: OpportunityModel,
        watchlist: List[str],
        telegram_bot: Optional[TelegramBot] = None,
        trade_analyzer: Optional[TradeAnalyzer] = None,
        min_volume: int = 100,
        min_open_interest: int = 500,
        min_iv_percentile: float = 70,
        scan_interval_seconds: int = 60,
        profit_targets: List[float] = [5, 10, 15, 20, 30],
        stop_loss: float = -15
    ):
        """
        Initialize day trading scanner.
        
        Args:
            thetadata_client: ThetaData API client
            opportunity_model: Database model for opportunities
            watchlist: List of symbols to monitor
            telegram_bot: Telegram bot for sending alerts (optional)
            trade_analyzer: AI trade analyzer (optional)
            min_volume: Minimum option volume
            min_open_interest: Minimum open interest
            min_iv_percentile: Minimum IV percentile (0-100)
            scan_interval_seconds: How often to scan for opportunities
            profit_targets: List of profit percentage targets
            stop_loss: Stop loss percentage (negative)
        """
        self.thetadata_client = thetadata_client
        self.opportunity_model = opportunity_model
        self.watchlist = watchlist
        self.telegram_bot = telegram_bot
        self.trade_analyzer = trade_analyzer
        self.min_volume = min_volume
        self.min_open_interest = min_open_interest
        self.min_iv_percentile = min_iv_percentile
        self.scan_interval_seconds = scan_interval_seconds
        self.profit_targets = profit_targets
        self.stop_loss = stop_loss
        self.running = False
        self.tracked_opportunities = {}  # Dictionary of tracked opportunities
    
    async def run(self):
        """Run the scanner in a loop."""
        self.running = True
        logger.info("Starting DayTradingScanner")
        
        while self.running:
            try:
                # Scan for new opportunities
                await self.scan_for_opportunities()
                
                # Monitor existing opportunities
                await self.monitor_opportunities()
                
                # Wait for next scan interval
                await asyncio.sleep(self.scan_interval_seconds)
            except Exception as e:
                logger.error(f"Error in DayTradingScanner: {e}")
                await asyncio.sleep(10)  # Wait before retrying
    
    async def stop(self):
        """Stop the scanner."""
        logger.info("Stopping DayTradingScanner")
        self.running = False
    
    async def scan_for_opportunities(self):
        """Scan for new day trading opportunities."""
        logger.info("Scanning for day trading opportunities")
        
        # Process each symbol in the watchlist
        for symbol in self.watchlist:
            try:
                # Get option chain
                options = await self.thetadata_client.get_option_chain(symbol)
                
                # Filter for potential opportunities
                for option in options:
                    # Apply scanning criteria
                    if await self.meets_criteria(option):
                        # Record the opportunity
                        await self.process_opportunity(option)
            except Exception as e:
                logger.error(f"Error scanning {symbol}: {e}")
    
    async def meets_criteria(self, option: Dict[str, Any]) -> bool:
        """
        Check if an option meets the day trading criteria.
        
        Args:
            option: Option data dictionary
            
        Returns:
            True if option meets criteria, False otherwise
        """
        # Check volume
        volume = option.get("volume", 0)
        if volume < self.min_volume:
            return False
        
        # Check open interest
        open_interest = option.get("open_interest", 0)
        if open_interest < self.min_open_interest:
            return False
        
        # Check implied volatility
        iv = option.get("iv", 0)
        # In a real system, you would compare to historical IV for percentile
        # This is a simplified check
        if iv < (self.min_iv_percentile / 100):
            return False
        
        return True
    
    async def process_opportunity(self, option: Dict[str, Any]):
        """
        Process a new day trading opportunity.
        
        Args:
            option: Option data dictionary
        """
        try:
            # Calculate notional value
            price = option.get("last", 0) or option.get("mid", 0)
            volume = option.get("volume", 0)
            notional_value = price * volume * 100  # 100 shares per contract
            
            # Create opportunity data
            opportunity = {
                "symbol": option.get("symbol", ""),
                "option_type": option.get("option_type", ""),
                "strike": option.get("strike", 0),
                "expiration": option.get("expiration", ""),
                "price": price,
                "volume": volume,
                "open_interest": option.get("open_interest", 0),
                "iv": option.get("iv", 0),
                "delta": option.get("delta", 0),
                "gamma": option.get("gamma", 0),
                "theta": option.get("theta", 0),
                "vega": option.get("vega", 0),
                "notional_value": notional_value,
                "alert_type": "day_trading",
                "strategy": "momentum",
                "underlying_price": option.get("underlying_price", 0)
            }
            
            # Run AI analysis if available
            ai_analysis = None
            if self.trade_analyzer:
                ai_analysis = self.trade_analyzer.analyze_trade(opportunity)
                
                # Only proceed if AI score is good enough
                if ai_analysis.get("success_probability", 0) < 60:
                    logger.info(f"Skipping opportunity due to low AI score: {option.get('symbol')} {option.get('option_type')} {option.get('strike')}")
                    return
            
            # Add to database
            opportunity_id = self.opportunity_model.add_opportunity(opportunity)
            
            if not opportunity_id:
                logger.error("Failed to add opportunity to database")
                return
            
            # Store in tracked opportunities
            option_key = f"{option.get('symbol')}_{option.get('option_type')}_{option.get('strike')}_{option.get('expiration')}"
            self.tracked_opportunities[option_key] = {
                "id": opportunity_id,
                "entry_price": price,
                "entry_time": datetime.now(),
                "profit_targets": self.profit_targets,
                "stop_loss": self.stop_loss,
                "highest_price": price,
                "lowest_price": price,
                "option_symbol": option.get("option_symbol", "")
            }
            
            # Send alert via Telegram
            if self.telegram_bot:
                await self.telegram_bot.send_opportunity_alert(opportunity, ai_analysis)
            
            logger.info(f"Added new day trading opportunity: {option.get('symbol')} {option.get('option_type')} ${option.get('strike')} {option.get('expiration')}")
        except Exception as e:
            logger.error(f"Error processing opportunity: {e}")
    
    async def monitor_opportunities(self):
        """Monitor existing opportunities for profit targets or stop loss."""
        if not self.tracked_opportunities:
            return
        
        logger.info(f"Monitoring {len(self.tracked_opportunities)} opportunities")
        
        # Track opportunities to remove
        to_remove = []
        
        for option_key, opp_data in self.tracked_opportunities.items():
            try:
                # Get current price
                option_symbol = opp_data.get("option_symbol", "")
                current_price_data = await self.thetadata_client.get_current_option_price(option_symbol)
                
                if not current_price_data:
                    logger.warning(f"Could not get current price for {option_key}")
                    continue
                
                current_price = current_price_data.get("last", 0) or current_price_data.get("mid", 0)
                
                # Update price in database
                self.opportunity_model.update_price(
                    opportunity_id=opp_data["id"],
                    current_price=current_price
                )
                
                # Update highest and lowest price
                if current_price > opp_data["highest_price"]:
                    opp_data["highest_price"] = current_price
                
                if current_price < opp_data["lowest_price"]:
                    opp_data["lowest_price"] = current_price
                
                # Calculate profit percentage
                entry_price = opp_data["entry_price"]
                profit_pct = ((current_price - entry_price) / entry_price) * 100 if entry_price > 0 else 0
                
                # Check profit targets
                for target in opp_data["profit_targets"]:
                    if profit_pct >= target:
                        logger.info(f"Profit target {target}% reached for {option_key}: {profit_pct:.2f}%")
                        
                        # In a real system, you might close the position here
                        # For now, just remove from tracking
                        to_remove.append(option_key)
                        
                        # Close in database
                        self.opportunity_model.close_opportunity(
                            opportunity_id=opp_data["id"],
                            close_price=current_price
                        )
                        
                        # Send alert
                        if self.telegram_bot:
                            await self.telegram_bot.send_message(
                                f"🎯 <b>PROFIT TARGET REACHED</b> 🎯\n\n"
                                f"Option: {option_key.replace('_', ' ')}\n"
                                f"Target: {target}%\n"
                                f"Actual Profit: {profit_pct:.2f}%\n"
                                f"Entry: ${entry_price:.2f}\n"
                                f"Exit: ${current_price:.2f}"
                            )
                        
                        break
                
                # Check stop loss
                if profit_pct <= self.stop_loss:
                    logger.info(f"Stop loss triggered for {option_key}: {profit_pct:.2f}%")
                    
                    # Remove from tracking
                    to_remove.append(option_key)
                    
                    # Close in database
                    self.opportunity_model.close_opportunity(
                        opportunity_id=opp_data["id"],
                        close_price=current_price
                    )
                    
                    # Send alert
                    if self.telegram_bot:
                        await self.telegram_bot.send_message(
                            f"🛑 <b>STOP LOSS TRIGGERED</b> 🛑\n\n"
                            f"Option: {option_key.replace('_', ' ')}\n"
                            f"Loss: {profit_pct:.2f}%\n"
                            f"Entry: ${entry_price:.2f}\n"
                            f"Exit: ${current_price:.2f}"
                        )
            except Exception as e:
                logger.error(f"Error monitoring {option_key}: {e}")
        
        # Remove closed opportunities
        for key in to_remove:
            self.tracked_opportunities.pop(key, None)