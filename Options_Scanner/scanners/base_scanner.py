"""
Base scanner abstract class for options trading system.
"""
import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional

from api.thetadata_client import ThetaDataClient
from db.models import OpportunityModel
from notifications.telegram_bot import TelegramBot
from analysis.trade_analyzer import TradeAnalyzer

logger = logging.getLogger(__name__)


class BaseScanner(ABC):
    """Abstract base class for all options scanners."""
    
    def __init__(
        self, 
        thetadata_client: ThetaDataClient,
        opportunity_model: OpportunityModel,
        watchlist: List[str],
        telegram_bot: Optional[TelegramBot] = None,
        trade_analyzer: Optional[TradeAnalyzer] = None,
        scan_interval_seconds: int = 60
    ):
        """
        Initialize base scanner.
        
        Args:
            thetadata_client: ThetaData API client
            opportunity_model: Database model for opportunities
            watchlist: List of symbols to monitor
            telegram_bot: Telegram bot for sending alerts (optional)
            trade_analyzer: AI trade analyzer (optional)
            scan_interval_seconds: How often to scan for opportunities
        """
        self.thetadata_client = thetadata_client
        self.opportunity_model = opportunity_model
        self.watchlist = watchlist
        self.telegram_bot = telegram_bot
        self.trade_analyzer = trade_analyzer
        self.scan_interval_seconds = scan_interval_seconds
        self.running = False
    
    async def run(self):
        """Run the scanner in a loop."""
        self.running = True
        scanner_name = self.__class__.__name__
        logger.info(f"Starting {scanner_name}")
        
        while self.running:
            try:
                # Run scanner-specific scan logic
                await self.scan()
                
                # Wait for next scan interval
                await asyncio.sleep(self.scan_interval_seconds)
            except Exception as e:
                logger.error(f"Error in {scanner_name}: {e}")
                await asyncio.sleep(10)  # Wait before retrying
    
    async def stop(self):
        """Stop the scanner."""
        logger.info(f"Stopping {self.__class__.__name__}")
        self.running = False
    
    @abstractmethod
    async def scan(self):
        """
        Main scanning logic to be implemented by each scanner.
        This method should be implemented by all subclasses.
        """
        pass
    
    def record_opportunity(self, opportunity_data: Dict[str, Any]):
        """
        Record a trading opportunity in the database.
        
        Args:
            opportunity_data: Dictionary with opportunity details
        """
        try:
            # Add to database
            opportunity_id = self.opportunity_model.add_opportunity(opportunity_data)
            
            if not opportunity_id:
                logger.error("Failed to add opportunity to database")
                return None
            
            logger.info(
                f"Added new opportunity: {opportunity_data.get('symbol')} "
                f"{opportunity_data.get('option_type')} ${opportunity_data.get('strike')} "
                f"{opportunity_data.get('expiration')}"
            )
            
            return opportunity_id
        except Exception as e:
            logger.error(f"Error recording opportunity: {e}")
            return None
    
    async def analyze_opportunity(self, opportunity_data: Dict[str, Any]):
        """
        Analyze opportunity with AI if available.
        
        Args:
            opportunity_data: Dictionary with opportunity details
            
        Returns:
            AI analysis results if available, None otherwise
        """
        if not self.trade_analyzer:
            return None
            
        try:
            return self.trade_analyzer.analyze_trade(opportunity_data)
        except Exception as e:
            logger.error(f"Error analyzing opportunity: {e}")
            return None
    
    async def send_alert(self, opportunity_data: Dict[str, Any], ai_analysis: Optional[Dict[str, Any]] = None):
        """
        Send alert for a new opportunity.
        
        Args:
            opportunity_data: Dictionary with opportunity details
            ai_analysis: Optional AI analysis results
            
        Returns:
            True if alert was sent successfully, False otherwise
        """
        if not self.telegram_bot:
            return False
            
        try:
            return await self.telegram_bot.send_opportunity_alert(opportunity_data, ai_analysis)
        except Exception as e:
            logger.error(f"Error sending alert: {e}")
            return False