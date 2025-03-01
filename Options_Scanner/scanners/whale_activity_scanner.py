"""
Scanner for detecting unusual options activity and large trades (whale activity).
"""
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

from api.thetadata_client import ThetaDataClient
from db.models import OpportunityModel
from .base_scanner import BaseScanner
from analysis.trade_analyzer import TradeAnalyzer
from notifications.telegram_bot import TelegramBot

logger = logging.getLogger(__name__)


class WhaleActivityScanner(BaseScanner):
    """Scanner for unusual options activity and large trades."""
    
    def __init__(
        self, 
        thetadata_client: ThetaDataClient,
        opportunity_model: OpportunityModel,
        watchlist: List[str],
        telegram_bot: Optional[TelegramBot] = None,
        trade_analyzer: Optional[TradeAnalyzer] = None,
        min_notional_value: float = 1000000,
        unusual_volume_multiplier: float = 3.0,
        min_trade_size: int = 100,
        scan_interval_seconds: int = 300
    ):
        """
        Initialize unusual activity scanner.
        
        Args:
            thetadata_client: ThetaData API client
            opportunity_model: Database model for opportunities
            watchlist: List of symbols to monitor
            telegram_bot: Telegram bot for sending alerts (optional)
            trade_analyzer: AI trade analyzer (optional)
            min_notional_value: Minimum dollar value for a trade to be considered large
            unusual_volume_multiplier: Multiplier compared to avg volume to be unusual
            min_trade_size: Minimum trade size in contracts
            scan_interval_seconds: How often to scan for unusual activity
        """
        super().__init__(
            thetadata_client=thetadata_client,
            opportunity_model=opportunity_model,
            watchlist=watchlist,
            telegram_bot=telegram_bot,
            trade_analyzer=trade_analyzer,
            scan_interval_seconds=scan_interval_seconds
        )
        self.min_notional_value = min_notional_value
        self.unusual_volume_multiplier = unusual_volume_multiplier
        self.min_trade_size = min_trade_size
        self.average_volumes = {}  # Cache for average volumes
    
    async def scan(self):
        """Scan for unusual options activity."""
        logger.info("Scanning for unusual options activity")
        await self.scan_for_unusual_activity()
    
    async def scan_for_unusual_activity(self):
        """Scan for unusual options activity."""
        # Monitor each symbol in the watchlist
        for symbol in self.watchlist:
            try:
                # Get option chain for the symbol
                options = await self.thetadata_client.get_option_chain(symbol)
                
                # Process each option in the chain
                for option in options:
                    # Check for unusual activity
                    unusual_activity = await self.check_unusual_activity(symbol, option)
                    
                    if unusual_activity:
                        # Run AI analysis if available
                        ai_analysis = await self.analyze_opportunity(unusual_activity)
                        
                        # Only proceed if AI score is good enough or no AI available
                        if not ai_analysis or ai_analysis.get("success_probability", 0) >= 60:
                            # Record the opportunity
                            opportunity_id = self.record_opportunity(unusual_activity)
                            
                            if opportunity_id:
                                # Send alert
                                await self.send_alert(unusual_activity, ai_analysis)
            except Exception as e:
                logger.error(f"Error processing {symbol}: {e}")
    
    async def check_unusual_activity(self, symbol: str, option_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Check if an option has unusual activity.
        
        Args:
            symbol: The ticker symbol
            option_data: Option data dictionary
            
        Returns:
            Dictionary with unusual activity details if found, None otherwise
        """
        try:
            # Calculate notional value
            price = option_data.get("last", 0) or option_data.get("mid", 0)
            volume = option_data.get("volume", 0)
            open_interest = option_data.get("open_interest", 0)
            
            # Skip if very low volume
            if volume < self.min_trade_size:
                return None
            
            # Calculate notional value ($ amount of the trade)
            notional_value = price * volume * 100  # 100 shares per contract
            
            # Check for large notional value (whale activity)
            if notional_value >= self.min_notional_value:
                # Get average volume for this option
                avg_volume = await self.get_average_volume(
                    symbol=symbol,
                    option_type=option_data.get("option_type", ""),
                    strike=option_data.get("strike", 0),
                    expiration=option_data.get("expiration", "")
                )
                
                # If volume is significantly higher than average, flag as unusual
                is_unusual_volume = avg_volume > 0 and (volume / avg_volume) >= self.unusual_volume_multiplier
                
                # If large notional value OR unusual volume
                if is_unusual_volume or notional_value >= self.min_notional_value:
                    # Create opportunity data
                    opportunity = {
                        "symbol": option_data.get("symbol", ""),
                        "option_type": option_data.get("option_type", ""),
                        "strike": option_data.get("strike", 0),
                        "expiration": option_data.get("expiration", ""),
                        "price": price,
                        "volume": volume,
                        "open_interest": open_interest,
                        "iv": option_data.get("iv", 0),
                        "delta": option_data.get("delta", 0),
                        "gamma": option_data.get("gamma", 0),
                        "theta": option_data.get("theta", 0),
                        "vega": option_data.get("vega", 0),
                        "notional_value": notional_value,
                        "alert_type": "whale_activity",
                        "strategy": "follow_smart_money",
                        "underlying_price": option_data.get("underlying_price", 0),
                        "is_unusual_volume": is_unusual_volume,
                        "avg_volume": avg_volume,
                        "volume_ratio": (volume / avg_volume) if avg_volume > 0 else 0
                    }
                    
                    return opportunity
            
            return None
        except Exception as e:
            logger.error(f"Error checking for unusual activity: {e}")
            return None
    
    async def get_average_volume(self, symbol: str, option_type: str, strike: float, expiration: str) -> float:
        """
        Get average daily volume for an option.
        
        Args:
            symbol: Ticker symbol
            option_type: Option type (call/put)
            strike: Strike price
            expiration: Expiration date
            
        Returns:
            Average daily volume for the option
        """
        # Create a unique key for this option
        option_key = f"{symbol}_{option_type}_{strike}_{expiration}"
        
        # If we've already calculated this, return cached value
        if option_key in self.average_volumes:
            return self.average_volumes[option_key]
        
        try:
            # Get historical option data for the past week
            now = datetime.now()
            start_date = now - timedelta(days=7)
            
            # Create option symbol
            option_symbol = f"{symbol}_{expiration}_{option_type}_{strike}"
            
            # Get historical data
            historical_data = await self.thetadata_client.get_historical_option_data(
                option_symbol=option_symbol,
                start_date=start_date,
                end_date=now,
                interval_minutes=1440  # Daily data
            )
            
            # Calculate average volume
            if historical_data:
                avg_volume = sum(day.get("volume", 0) for day in historical_data) / len(historical_data)
            else:
                # If no historical data, use a default value
                avg_volume = 50  # Default average volume
            
            # Cache the result
            self.average_volumes[option_key] = avg_volume
            
            return avg_volume
        except Exception as e:
            logger.error(f"Error getting average volume: {e}")
            return 0  # Default to 0 on error