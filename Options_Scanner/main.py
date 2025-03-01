"""
Main entry point for the options trading system.

Usage:
    python main.py [--backtest] [--historical-date YYYY-MM-DD] [--reduced-watchlist]

Options:
    --backtest          Run in backtesting mode
    --historical-date   Use historical data from a specific date
    --reduced-watchlist Use a smaller watchlist to avoid rate limiting
"""
import asyncio
import logging
import os
import sys
import argparse
from datetime import datetime, timedelta

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import components
from config import (
    DB_PATH, THETADATA_USERNAME, THETADATA_API_KEY,
    TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, WATCHLIST,
    DAY_TRADING_SETTINGS, WHALE_SCANNER_SETTINGS, AI_ANALYSIS_SETTINGS
)
from db.models import Database, OpportunityModel
from api.simplified_thetadata_client import SimplifiedThetaDataClient
from scanners.day_trading_scanner import DayTradingScanner
from scanners.whale_activity_scanner import WhaleActivityScanner
from reporting.performance_tracker import PerformanceTracker
from notifications.telegram_bot import TelegramBot
from analysis.trade_analyzer import TradeAnalyzer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("options_scanner.log", mode="a")
    ]
)
logger = logging.getLogger(__name__)


class RateLimitedDayTradingScanner(RateLimitedScanner):
    """Rate-limited scanner for day trading opportunities."""
    
    def __init__(self, *args, **kwargs):
        """Initialize with standard scanner args plus day trading specifics."""
        # Extract day trading specific settings
        self.min_volume = kwargs.pop('min_volume', 100)
        self.min_open_interest = kwargs.pop('min_open_interest', 500)
        self.min_iv_percentile = kwargs.pop('min_iv_percentile', 70)
        self.profit_targets = kwargs.pop('profit_targets', [5, 10, 15, 20, 30])
        self.stop_loss = kwargs.pop('stop_loss', -15)
        
        # Initialize base scanner
        super().__init__(*args, name="DayTradingScanner", **kwargs)
        self.tracked_opportunities = {}
    
    async def scan(self):
        """Scan for day trading opportunities with rate limiting."""
        logger.info("Scanning for day trading opportunities")
        
        # Process a limited number of symbols per scan
        # to avoid hitting rate limits
        for symbol in self.watchlist[:3]:  # Process max 3 symbols per scan cycle
            try:
                # Get expirations with rate limiting
                expirations = await self.rate_limited_api_call(
                    self.thetadata_client.get_expirations, symbol
                )
                
                if not expirations:
                    logger.warning(f"No expirations found for {symbol}")
                    continue
                
                # Get option chain for nearest expiration
                options = await self.rate_limited_api_call(
                    self.thetadata_client.get_option_chain, symbol, expirations[0]
                )
                
                # Process the options that meet criteria
                for option in options:
                    if await self.meets_criteria(option):
                        await self.process_opportunity(option)
                        
                # Sleep after processing each symbol to prevent rate limiting
                await asyncio.sleep(2)
                
            except Exception as e:
                logger.error(f"Error processing {symbol}: {e}")
    
    async def meets_criteria(self, option: Dict[str, Any]) -> bool:
        """Check if an option meets day trading criteria."""
        volume = option.get("volume", 0)
        if volume < self.min_volume:
            return False
        
        open_interest = option.get("open_interest", 0)
        if open_interest < self.min_open_interest:
            return False
        
        iv = option.get("iv", 0)
        if iv < (self.min_iv_percentile / 100):
            return False
        
        return True
    
    async def process_opportunity(self, option: Dict[str, Any]):
        """Process a potential day trading opportunity."""
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
            ai_analysis = await self.analyze_opportunity(opportunity)
            
            # Only proceed if AI score is good enough
            if ai_analysis and ai_analysis.get("success_probability", 0) < 60:
                logger.info(f"Skipping opportunity due to low AI score: {option.get('symbol')} {option.get('option_type')} {option.get('strike')}")
                return
            
            # Add to database
            opportunity_id = self.record_opportunity(opportunity)
            
            if opportunity_id:
                # Send alert
                await self.send_alert(opportunity, ai_analysis)
        
        except Exception as e:
            logger.error(f"Error processing opportunity: {e}")


class RateLimitedWhaleScanner(RateLimitedScanner):
    """Rate-limited scanner for unusual whale activity."""
    
    def __init__(self, *args, **kwargs):
        """Initialize with standard scanner args plus whale scanner specifics."""
        # Extract whale scanner specific settings
        self.min_notional_value = kwargs.pop('min_notional_value', 1000000)
        self.unusual_volume_multiplier = kwargs.pop('unusual_volume_multiplier', 3.0)
        self.min_trade_size = kwargs.pop('min_trade_size', 100)
        
        # Initialize base scanner
        super().__init__(*args, name="WhaleActivityScanner", **kwargs)
        self.average_volumes = {}
    
    async def scan(self):
        """Scan for unusual options activity with rate limiting."""
        logger.info("Scanning for unusual options activity")
        
        # Process a limited number of symbols per scan
        # to avoid hitting rate limits
        for symbol in self.watchlist[:2]:  # Process max 2 symbols per whale scan
            try:
                # Get expirations with rate limiting
                expirations = await self.rate_limited_api_call(
                    self.thetadata_client.get_expirations, symbol
                )
                
                if not expirations:
                    logger.warning(f"No expirations found for {symbol}")
                    continue
                
                # Get option chain for nearest expiration
                options = await self.rate_limited_api_call(
                    self.thetadata_client.get_option_chain, symbol, expirations[0]
                )
                
                # Process options for unusual activity
                for option in options:
                    # Check for unusual activity
                    if await self.is_unusual_activity(option):
                        # Create opportunity data
                        price = option.get("last", 0) or option.get("mid", 0)
                        volume = option.get("volume", 0)
                        notional_value = price * volume * 100
                        
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
                            "alert_type": "whale_activity",
                            "strategy": "follow_smart_money",
                            "underlying_price": option.get("underlying_price", 0)
                        }
                        
                        # Run AI analysis if available
                        ai_analysis = await self.analyze_opportunity(opportunity)
                        
                        # Only proceed if AI score is good enough
                        if ai_analysis and ai_analysis.get("success_probability", 0) < 60:
                            continue
                        
                        # Add to database
                        opportunity_id = self.record_opportunity(opportunity)
                        
                        if opportunity_id:
                            # Send alert
                            await self.send_alert(opportunity, ai_analysis)
                
                # Sleep after processing each symbol to prevent rate limiting
                await asyncio.sleep(2)
                
            except Exception as e:
                logger.error(f"Error processing {symbol}: {e}")
    
    async def is_unusual_activity(self, option: Dict[str, Any]) -> bool:
        """Determine if an option has unusual activity."""
        # Calculate notional value
        price = option.get("last", 0) or option.get("mid", 0)
        volume = option.get("volume", 0)
        
        # Skip if very low volume
        if volume < self.min_trade_size:
            return False
        
        # Calculate notional value ($ amount of the trade)
        notional_value = price * volume * 100  # 100 shares per contract
        
        # Check if this is a large trade by notional value
        if notional_value >= self.min_notional_value:
            return True
        
        # Check volume ratio against open interest
        open_interest = option.get("open_interest", 0)
        if open_interest > 0 and (volume / open_interest) >= self.unusual_volume_multiplier:
            return True
            
        return False


async def main():
    """Main function to start the trading system."""
    logger.info("🚀 Starting Options Trading System")

    try:
        # Initialize database
        logger.info("📂 Initializing database")
        db = Database(DB_PATH)
        opportunity_model = OpportunityModel(db)

        # Initialize ThetaData client - Use simplified client to avoid connection issues
        logger.info("🔗 Connecting to ThetaData API")
        thetadata_client = SimplifiedThetaDataClient(THETADATA_USERNAME, THETADATA_API_KEY)
        connected = await thetadata_client.connect()
        if not connected:
            logger.error("❌ Failed to connect to ThetaData API. Exiting.")
            return

        # Initialize Telegram bot
        logger.info("📢 Setting up Telegram bot")
        telegram_bot = TelegramBot(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID)

        # Initialize AI analyzer
        logger.info("🧠 Initializing AI trade analyzer")
        trade_analyzer = TradeAnalyzer(DB_PATH)
        if AI_ANALYSIS_SETTINGS["enabled"]:
            trade_analyzer.train_model()

        # Initialize performance tracker
        logger.info("📊 Setting up performance tracker")
        performance_tracker = PerformanceTracker(db, opportunity_model, telegram_bot)

        # Initialize scanners - Using rate-limited versions to prevent API throttling
        logger.info("🔍 Setting up scanners")
        
        # Use a smaller subset of the watchlist to avoid overloading
        active_watchlist = WATCHLIST[:5]  # Start with just 5 symbols
        
        day_trading_scanner = DayTradingScanner(
    thetadata_client, opportunity_model, active_watchlist, 
    telegram_bot, trade_analyzer, **DAY_TRADING_SETTINGS
)

        whale_scanner = WhaleActivityScanner(
    thetadata_client, opportunity_model, active_watchlist,
    telegram_bot, trade_analyzer, **WHALE_SCANNER_SETTINGS
)

        # Send startup notification
        try:
            await telegram_bot.send_message(
                f"🚀 <b>Options Trading System Started</b>\n"
                f"📈 Monitoring {len(active_watchlist)} symbols\n"
                f"🤖 AI Analysis: {'Enabled' if AI_ANALYSIS_SETTINGS['enabled'] else 'Disabled'}\n"
                f"🕒 Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            )
        except Exception as e:
            logger.error(f"Failed to send startup notification: {e}")

        # Start scanners
        logger.info("▶️ Starting scanners")
        scanner_tasks = [
            asyncio.create_task(day_trading_scanner.run()),
            asyncio.create_task(whale_scanner.run()),
            asyncio.create_task(performance_tracker.run())
        ]

        # Run indefinitely
        await asyncio.gather(*scanner_tasks)

    except KeyboardInterrupt:
        logger.info("⚠️ Shutting down due to keyboard interrupt")
    except Exception as e:
        logger.error(f"❌ Error in main loop: {e}")
    finally:
        # Cleanup resources
        logger.info("🧹 Cleaning up resources")
        if 'thetadata_client' in locals() and thetadata_client:
            await thetadata_client.disconnect()
        if 'db' in locals() and db:
            db.close()
        if 'telegram_bot' in locals() and telegram_bot:
            try:
                await telegram_bot.send_message("⚠️ Options Trading System Shutting Down ⚠️")
            except:
                pass
        logger.info("✅ Shutdown complete")


async def run_backtest(historical_date=None):
    """Run the trading system in backtesting mode."""
    logger.info("⏳ Starting backtest")

    try:
        # Initialize database
        db = Database(DB_PATH)
        opportunity_model = OpportunityModel(db)

        # Initialize ThetaData client for historical data
        thetadata_client = SimplifiedThetaDataClient(THETADATA_USERNAME, THETADATA_API_KEY)
        connected = await thetadata_client.connect()
        if not connected:
            logger.error("❌ Failed to connect to ThetaData API. Exiting.")
            return

        # Initialize AI analyzer
        trade_analyzer = TradeAnalyzer(DB_PATH)

        # Determine historical date
        if not historical_date:
            historical_date = datetime.now() - timedelta(days=30)
        logger.info(f"📅 Running backtest with data from {historical_date.strftime('%Y-%m-%d')}")

        # Analyze historical data - use limited subset to avoid API throttling
        results = []
        for symbol in WATCHLIST[:3]:  # Only use 3 symbols for testing
            logger.info(f"📊 Processing historical data for {symbol}")

            options = await thetadata_client.get_option_chain(symbol, historical_date.strftime("%Y-%m-%d"))
            for option in options:
                if AI_ANALYSIS_SETTINGS["enabled"]:
                    option["ai_analysis"] = trade_analyzer.analyze_trade(option)

                if option.get("volume", 0) >= DAY_TRADING_SETTINGS["min_volume"]:
                    forward_date = historical_date + timedelta(hours=2)
                    forward_prices = await thetadata_client.get_historical_option_data(
                        option.get("option_symbol", ""), 
                        historical_date, 
                        forward_date
                    )

                    entry_price = option.get("last", 0) or option.get("mid", 0)
                    max_profit_pct = 0
                    if entry_price > 0 and forward_prices:
                        max_price = max((p.get("price", 0) for p in forward_prices), default=0)
                        max_profit_pct = ((max_price - entry_price) / entry_price) * 100

                    results.append({
                        "symbol": symbol,
                        "strike": option.get("strike", 0),
                        "expiration": option.get("expiration", ""),
                        "entry_price": entry_price,
                        "max_profit_pct": max_profit_pct,
                        "ai_prediction": option.get("ai_analysis", {}).get("success_probability", 0)
                    })
                
                # Sleep between options to avoid rate limiting
                await asyncio.sleep(0.5)
            
            # Sleep between symbols to avoid rate limiting
            await asyncio.sleep(2)

        # Log and save backtest results
        if results:
            import pandas as pd
            df = pd.DataFrame(results)
            df.to_csv("backtest_results.csv", index=False)
            logger.info(f"📄 Backtest results saved to 'backtest_results.csv'")
        else:
            logger.warning("⚠️ No viable trades found in backtest period")

    except Exception as e:
        logger.error(f"❌ Error in backtest: {e}")
    finally:
        if 'thetadata_client' in locals() and thetadata_client:
            await thetadata_client.disconnect()
        if 'db' in locals() and db:
            db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Options Trading System")
    parser.add_argument("--backtest", action="store_true", help="Run in backtesting mode")
    parser.add_argument("--historical-date", type=str, help="Use historical data from a specific date (YYYY-MM-DD)")
    parser.add_argument("--reduced-watchlist", action="store_true", help="Use a reduced watchlist (first 5 symbols)")
    args = parser.parse_args()

    historical_date = None
    if args.historical_date:
        try:
            historical_date = datetime.strptime(args.historical_date, "%Y-%m-%d")
            logger.info(f"📅 Using historical data from {args.historical_date}")
        except ValueError:
            logger.error(f"❌ Invalid date format: {args.historical_date}. Use YYYY-MM-DD.")
            sys.exit(1)

    if args.backtest:
        logger.info("▶️ Running in backtesting mode")
        asyncio.run(run_backtest(historical_date))
    else:
        asyncio.run(main())
