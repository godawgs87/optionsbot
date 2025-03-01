import asyncio
import json
import logging
import pandas as pd
from datetime import datetime, timedelta
import requests
import os
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ThetaDataHistoricalTester:
    def __init__(self, username, api_key):
        """Initialize historical data tester for ThetaData API"""
        self.username = username
        self.api_key = api_key
        self.base_url = "https://api.thetadata.net/v1"
        
        # Default settings
        self.symbols = ["SPY", "QQQ", "AAPL", "MSFT", "NVDA", "TSLA", "AMD", "META"]
        self.days_to_test = 5
        self.opportunities = []
        
    async def fetch_historical_data(self, symbol, start_date, end_date):
        """Fetch historical options data for a symbol"""
        try:
            # Get historical trade data
            url = f"{self.base_url}/options/trade"
            params = {
                "username": self.username,
                "key": self.api_key,
                "root": symbol,
                "date_start": start_date.strftime("%Y-%m-%d"),
                "date_end": end_date.strftime("%Y-%m-%d")
            }
            
            response = await asyncio.to_thread(requests.get, url, params=params)
            
            if response.status_code == 200:
                return response.json().get("data", [])
            else:
                logger.error(f"Failed to get historical data: {response.text}")
                return []
        except Exception as e:
            logger.error(f"Error fetching historical data: {e}")
            return []
    
    async def fetch_option_details(self, symbol, date, option_type, strike, expiration):
        """Fetch option details including quotes and Greeks"""
        try:
            url = f"{self.base_url}/options/greeks"
            params = {
                "username": self.username,
                "key": self.api_key,
                "root": symbol,
                "option_type": option_type,
                "strike": strike,
                "expiration": expiration,
                "date": date.strftime("%Y-%m-%d")
            }
            
            response = await asyncio.to_thread(requests.get, url, params=params)
            
            if response.status_code == 200:
                return response.json().get("data", {})
            else:
                logger.error(f"Failed to get option details: {response.text}")
                return {}
        except Exception as e:
            logger.error(f"Error fetching option details: {e}")
            return {}
    
    async def fetch_quote_history(self, symbol, date, option_type, strike, expiration):
        """Fetch intraday quotes to analyze price movement after unusual activity"""
        try:
            url = f"{self.base_url}/options/quote"
            params = {
                "username": self.username,
                "key": self.api_key,
                "root": symbol,
                "option_type": option_type,
                "strike": strike,
                "expiration": expiration,
                "date": date.strftime("%Y-%m-%d")
            }
            
            response = await asyncio.to_thread(requests.get, url, params=params)
            
            if response.status_code == 200:
                return response.json().get("data", [])
            else:
                logger.error(f"Failed to get quote history: {response.text}")
                return []
        except Exception as e:
            logger.error(f"Error fetching quote history: {e}")
            return []
    
    def is_unusual_activity(self, trade_data):
        """Determine if a historical trade represents unusual activity"""
        try:
            # Adjustable criteria
            min_notional_value = 500000  # $500K
            min_volume = 200
            
            # Extract trade details
            volume = trade_data.get("size", 0)
            price = trade_data.get("price", 0)
            
            # Calculate notional value
            notional_value = volume * price * 100  # Standard options contract is 100 shares
            
            # Check if criteria are met
            if notional_value >= min_notional_value and volume >= min_volume:
                return True
                
            return False
        except Exception as e:
            logger.error(f"Error checking for unusual activity: {e}")
            return False
    
    async def analyze_price_movement(self, trade_data):
        """Analyze subsequent price movement after unusual activity"""
        try:
            # Extract trade details
            symbol = trade_data.get("root", "")
            option_type = trade_data.get("option_type", "")
            strike = trade_data.get("strike", 0)
            expiration = trade_data.get("expiration", "")
            trade_price = trade_data.get("price", 0)
            trade_time = trade_data.get("timestamp", "")
            
            # Parse trade timestamp
            trade_datetime = datetime.strptime(trade_time, "%Y-%m-%d %H:%M:%S.%f")
            trade_date = trade_datetime.date()
            
            # Get quote history for the day
            quotes = await self.fetch_quote_history(symbol, trade_date, option_type, strike, expiration)
            
            # Filter quotes after the trade
            subsequent_quotes = [q for q in quotes if datetime.strptime(q.get("timestamp", ""), "%Y-%m-%d %H:%M:%S.%f") > trade_datetime]
            
            if not subsequent_quotes:
                return {
                    "max_profit_pct": 0,
                    "timestamps": {}
                }
            
            # Create timestamps for performance tracking
            timestamps = {}
            max_prices = {}
            
            # Track max price at various time intervals
            for minutes in [1, 5, 10, 15, 20]:
                cutoff_time = trade_datetime + timedelta(minutes=minutes)
                quotes_within_window = [q for q in subsequent_quotes 
                                        if datetime.strptime(q.get("timestamp", ""), "%Y-%m-%d %H:%M:%S.%f") <= cutoff_time]
                
                if quotes_within_window:
                    # Use ask price for potential exit
                    max_price = max(q.get("ask", 0) for q in quotes_within_window)
                    max_prices[f"{minutes}m"] = max_price
                    
                    # Calculate potential profit percentage
                    if trade_price > 0:
                        profit_pct = ((max_price - trade_price) / trade_price) * 100
                        timestamps[f"{minutes}m"] = profit_pct
            
            # Find overall max profit percentage
            max_profit_pct = max(timestamps.values()) if timestamps else 0
            
            return {
                "max_profit_pct": max_profit_pct,
                "timestamps": timestamps
            }
            
        except Exception as e:
            logger.error(f"Error analyzing price movement: {e}")
            return {
                "max_profit_pct": 0,
                "timestamps": {}
            }
    
    async def run_historical_test(self):
        """Run historical test across multiple symbols and days"""
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=self.days_to_test)
        
        logger.info(f"Running historical test from {start_date} to {end_date}")
        
        all_opportunities = []
        
        for symbol in self.symbols:
            logger.info(f"Analyzing historical data for {symbol}...")
            
            # Fetch historical trades
            trades = await self.fetch_historical_data(symbol, start_date, end_date)
            logger.info(f"Found {len(trades)} historical trades for {symbol}")
            
            # Identify unusual activity
            unusual_trades = [trade for trade in trades if self.is_unusual_activity(trade)]
            logger.info(f"Identified {len(unusual_trades)} instances of unusual activity")
            
            # Analyze price movement after unusual activity
            for trade in unusual_trades:
                price_analysis = await self.analyze_price_movement(trade)
                
                # Create opportunity record
                opportunity = {
                    "symbol": trade.get("root", ""),
                    "option_type": trade.get("option_type", ""),
                    "strike": trade.get("strike", 0),
                    "expiration": trade.get("expiration", ""),
                    "trade_price": trade.get("price", 0),
                    "volume": trade.get("size", 0),
                    "notional_value": trade.get("price", 0) * trade.get("size", 0) * 100,
                    "timestamp": trade.get("timestamp", ""),
                    "max_profit_pct": price_analysis["max_profit_pct"],
                    "profit_1m": price_analysis["timestamps"].get("1m", 0),
                    "profit_5m": price_analysis["timestamps"].get("5m", 0),
                    "profit_10m": price_analysis["timestamps"].get("10m", 0),
                    "profit_15m": price_analysis["timestamps"].get("15m", 0),
                    "profit_20m": price_analysis["timestamps"].get("20m", 0)
                }
                
                all_opportunities.append(opportunity)
        
        # Store results
        self.opportunities = all_opportunities
        
        # Generate performance report
        self.generate_performance_report()
        
        return all_opportunities
    
    def generate_performance_report(self):
        """Generate performance report based on historical opportunities"""
        if not self.opportunities:
            logger.info("No opportunities found for performance report")
            return
        
        # Convert to DataFrame for analysis
        df = pd.DataFrame(self.opportunities)
        
        # Calculate summary statistics
        summary = {
            "total_opportunities": len(df),
            "avg_profit_1m": df["profit_1m"].mean(),
            "avg_profit_5m": df["profit_5m"].mean(),
            "avg_profit_10m": df["profit_10m"].mean(),
            "avg_profit_15m": df["profit_15m"].mean(),
            "avg_profit_20m": df["profit_20m"].mean(),
            "max_profit_1m": df["profit_1m"].max(),
            "max_profit_5m": df["profit_5m"].max(),
            "max_profit_10m": df["profit_10m"].max(),
            "max_profit_15m": df["profit_15m"].max(),
            "max_profit_20m": df["profit_20m"].max(),
        }
        
        # Calculate profit probabilities
        for time_window in ["1m", "5m", "10m", "15m", "20m"]:
            profit_col = f"profit_{time_window}"
            summary[f"prob_profit_{time_window}"] = (df[profit_col] > 0).mean() * 100
            summary[f"prob_profit_10pct_{time_window}"] = (df[profit_col] > 10).mean() * 100
            summary[f"prob_profit_25pct_{time_window}"] = (df[profit_col] > 25).mean() * 100
            summary[f"prob_profit_50pct_{time_window}"] = (df[profit_col] > 50).mean() * 100
        
        # Print summary report
        logger.info("=== HISTORICAL PERFORMANCE REPORT ===")
        logger.info(f"Total Opportunities: {summary['total_opportunities']}")
        logger.info("\nAverage Profit by Time Window:")
        for time_window in ["1m", "5m", "10m", "15m", "20m"]:
            logger.info(f"  {time_window}: {summary[f'avg_profit_{time_window}']:.2f}%")
        
        logger.info("\nMaximum Profit by Time Window:")
        for time_window in ["1m", "5m", "10m", "15m", "20m"]:
            logger.info(f"  {time_window}: {summary[f'max_profit_{time_window}']:.2f}%")
        
        logger.info("\nProbability of Profit by Time Window:")
        for time_window in ["1m", "5m", "10m", "15m", "20m"]:
            logger.info(f"  {time_window}: {summary[f'prob_profit_{time_window}']:.1f}%")
            logger.info(f"    >10%: {summary[f'prob_profit_10pct_{time_window}']:.1f}%")
            logger.info(f"    >25%: {summary[f'prob_profit_25pct_{time_window}']:.1f}%")
            logger.info(f"    >50%: {summary[f'prob_profit_50pct_{time_window}']:.1f}%")
        
        # Save detailed results
        df.to_csv("historical_opportunities.csv", index=False)
        
        # Save performance summary
        with open("performance_summary.json", "w") as f:
            json.dump(summary, f, indent=2)
        
        logger.info("\nDetailed results saved to 'historical_opportunities.csv'")
        logger.info("Performance summary saved to 'performance_summary.json'")
        
        # Create leaderboard of best opportunities
        self.generate_leaderboard(df)
    
    def generate_leaderboard(self, df):
        """Generate leaderboard of best performing opportunities"""
        logger.info("\n=== PERFORMANCE LEADERBOARD ===")
        
        for time_window in ["1m", "5m", "10m", "15m", "20m"]:
            profit_col = f"profit_{time_window}"
            
            # Sort by profit for this time window
            top_performers = df.sort_values(profit_col, ascending=False).head(5)
            
            logger.info(f"\nTop Performers - {time_window} Window:")
            for i, (idx, row) in enumerate(top_performers.iterrows(), 1):
                logger.info(f"  #{i}: {row['symbol']} {row['option_type']} ${row['strike']} {row['expiration']}: {row[profit_col]:.2f}%")
        
        # Save leaderboard to file
        with open("leaderboard.txt", "w") as f:
            f.write("=== PERFORMANCE LEADERBOARD ===\n\n")
            
            for time_window in ["1m", "5m", "10m", "15m", "20m"]:
                profit_col = f"profit_{time_window}"
                top_performers = df.sort_values(profit_col, ascending=False).head(10)
                
                f.write(f"\nTop Performers - {time_window} Window:\n")
                for i, (idx, row) in enumerate(top_performers.iterrows(), 1):
                    f.write(f"  #{i}: {row['symbol']} {row['option_type']} ${row['strike']} {row['expiration']}: {row[profit_col]:.2f}%\n")
        
        logger.info("\nLeaderboard saved to 'leaderboard.txt'")

async def main():
    # Load environment variables
    load_dotenv()
    
    # Get ThetaData credentials
    theta_username = os.environ.get("THETA_USERNAME")
    theta_api_key = os.environ.get("THETA_API_KEY")
    
    if not all([theta_username, theta_api_key]):
        logger.error("Missing ThetaData credentials. Check your .env file.")
        return
    
    # Initialize tester
    tester = ThetaDataHistoricalTester(theta_username, theta_api_key)
    
    # Customize test parameters
    tester.symbols = ["SPY", "QQQ", "AAPL", "MSFT", "NVDA", "TSLA", "AMD", "META"]  # Symbols to test
    tester.days_to_test = 5  # Number of trading days to analyze
    
    # Run historical test
    await tester.run_historical_test()

if __name__ == "__main__":
    asyncio.run(main())