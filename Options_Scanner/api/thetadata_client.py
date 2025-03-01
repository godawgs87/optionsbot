"""
Simplified ThetaData API client for options trading system.
This uses direct HTTP requests instead of the ThetaData library to avoid conflicts.
"""
import logging
import aiohttp
import asyncio
from datetime import datetime
from typing import Dict, List, Optional, Any

logger = logging.getLogger(__name__)

class SimplifiedThetaDataClient:
    """Client for interacting with the ThetaData API using direct HTTP requests."""
    
    def __init__(self, username: str, api_key: str):
        """
        Initialize ThetaData client.

        Args:
            username: ThetaData username
            api_key: ThetaData API key
        """
        self.username = username
        self.api_key = api_key
        self.base_url = "https://api.thetadata.us/v1"
        self.session = None
        self.connected = False
        
    async def connect(self) -> bool:
        """
        Connect to ThetaData API by initializing HTTP session.
        
        Returns:
            bool: True if connected successfully, False otherwise.
        """
        try:
            self.session = aiohttp.ClientSession()
            
            # Test connection with a simple request
            async with self.session.get(
                f"{self.base_url}/test",
                params={"username": self.username, "key": self.api_key}
            ) as response:
                data = await response.json()
                if response.status == 200:
                    self.connected = True
                    logger.info("✅ Connected to ThetaData API")
                    return True
                else:
                    logger.error(f"❌ Failed to connect to ThetaData API: {data}")
                    self.connected = False
                    return False
        except Exception as e:
            logger.error(f"❌ Connection error: {e}")
            self.connected = False
            return False

    async def disconnect(self):
        """
        Disconnect from ThetaData API.
        """
        if self.session:
            try:
                await self.session.close()
                self.session = None
                self.connected = False
                logger.info("✅ Disconnected from ThetaData API")
            except Exception as e:
                logger.error(f"❌ Error disconnecting from ThetaData API: {e}")

    async def ensure_connected(self):
        """Ensure we have an active session."""
        if not self.session:
            await self.connect()

    async def get_expirations(self, symbol: str) -> List[str]:
        """
        Retrieve available expiration dates for an options contract.

        Args:
            symbol: Stock ticker symbol.

        Returns:
            A list of expiration dates as strings (YYYY-MM-DD format).
        """
        await self.ensure_connected()
        
        try:
            async with self.session.get(
                f"{self.base_url}/options/expirations",
                params={
                    "username": self.username,
                    "key": self.api_key,
                    "root": symbol
                }
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    return sorted(data.get("data", []))
                else:
                    err_text = await response.text()
                    logger.error(f"Error retrieving expirations: {err_text}")
                    return []
        except Exception as e:
            logger.error(f"Error retrieving expirations for {symbol}: {e}")
            return []

    async def get_option_chain(self, symbol: str, date: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get the option chain for a given stock symbol.

        Args:
            symbol: Stock ticker symbol.
            date: Expiration date (YYYY-MM-DD format, optional).

        Returns:
            A list of option contracts as dictionaries.
        """
        await self.ensure_connected()
        
        try:
            # Get the nearest expiration if no date is provided
            if not date:
                expirations = await self.get_expirations(symbol)
                if not expirations:
                    logger.error(f"No expirations found for {symbol}")
                    return []
                date = expirations[0]
            
            # Get option chain for this date
            async with self.session.get(
                f"{self.base_url}/options/chain",
                params={
                    "username": self.username,
                    "key": self.api_key,
                    "root": symbol,
                    "date": date
                }
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    chain_data = data.get("data", [])
                    
                    # Process the chain data
                    options = []
                    for option in chain_data:
                        option_data = {
                            "symbol": symbol,
                            "expiration": date,
                            "strike": option.get("strike", 0),
                            "option_type": option.get("option_type", "").lower(),
                            "option_symbol": option.get("option_symbol", ""),
                            "bid": option.get("bid", 0),
                            "ask": option.get("ask", 0),
                            "last": option.get("last", 0),
                            "volume": option.get("volume", 0),
                            "open_interest": option.get("open_interest", 0)
                        }
                        
                        # Add greeks if available
                        greeks = option.get("greeks", {})
                        if greeks:
                            option_data.update({
                                "iv": greeks.get("implied_volatility", 0),
                                "delta": greeks.get("delta", 0),
                                "gamma": greeks.get("gamma", 0),
                                "theta": greeks.get("theta", 0),
                                "vega": greeks.get("vega", 0)
                            })
                        
                        options.append(option_data)
                    
                    return options
                else:
                    err_text = await response.text()
                    logger.error(f"Error fetching option chain: {err_text}")
                    return []
        except Exception as e:
            logger.error(f"Error fetching option chain for {symbol}: {e}")
            return []

    async def get_current_option_price(self, option_symbol: str) -> Optional[Dict[str, float]]:
        """
        Retrieve the latest price quote for an option.

        Args:
            option_symbol: Option contract symbol.

        Returns:
            A dictionary containing bid, ask, last, and mid prices.
        """
        await self.ensure_connected()
        
        try:
            # Parse option_symbol to extract components
            parts = option_symbol.split("_")
            if len(parts) < 4:
                logger.error(f"Invalid option symbol format: {option_symbol}")
                return None
            
            root = parts[0]
            expiration = parts[1]
            option_type = parts[2]
            strike = parts[3]
            
            async with self.session.get(
                f"{self.base_url}/options/quote",
                params={
                    "username": self.username,
                    "key": self.api_key,
                    "root": root,
                    "expiration": expiration,
                    "option_type": option_type,
                    "strike": strike
                }
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    quotes = data.get("data", [])
                    
                    if quotes:
                        # Get the most recent quote
                        latest_quote = quotes[-1]
                        return {
                            "bid": latest_quote.get("bid", 0),
                            "ask": latest_quote.get("ask", 0),
                            "last": latest_quote.get("last", 0),
                            "mid": (latest_quote.get("bid", 0) + latest_quote.get("ask", 0)) / 2
                        }
                    return None
                else:
                    err_text = await response.text()
                    logger.error(f"Error retrieving option quote: {err_text}")
                    return None
        except Exception as e:
            logger.error(f"Error retrieving price for {option_symbol}: {e}")
            return None

    async def get_historical_option_data(
        self, option_symbol: str, start_date: datetime, end_date: datetime
    ) -> List[Dict[str, Any]]:
        """
        Retrieve historical price data for an option.

        Args:
            option_symbol: Option contract symbol.
            start_date: Start date for data retrieval.
            end_date: End date for data retrieval.

        Returns:
            A list of dictionaries containing historical price data.
        """
        await self.ensure_connected()
        
        try:
            # Parse option_symbol to extract components
            parts = option_symbol.split("_")
            if len(parts) < 4:
                logger.error(f"Invalid option symbol format: {option_symbol}")
                return []
            
            root = parts[0]
            expiration = parts[1]
            option_type = parts[2]
            strike = parts[3]
            
            # Format dates
            start_date_str = start_date.strftime("%Y-%m-%d")
            end_date_str = end_date.strftime("%Y-%m-%d")
            
            async with self.session.get(
                f"{self.base_url}/options/trade",
                params={
                    "username": self.username,
                    "key": self.api_key,
                    "root": root,
                    "expiration": expiration,
                    "option_type": option_type,
                    "strike": strike,
                    "date_start": start_date_str,
                    "date_end": end_date_str
                }
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    trades = data.get("data", [])
                    
                    result = []
                    for trade in trades:
                        result.append({
                            "timestamp": trade.get("timestamp", ""),
                            "price": trade.get("price", 0),
                            "size": trade.get("size", 0),
                            "exchange": trade.get("exchange", "")
                        })
                    
                    return result
                else:
                    err_text = await response.text()
                    logger.error(f"Error fetching historical option data: {err_text}")
                    return []
        except Exception as e:
            logger.error(f"Error fetching historical data for {option_symbol}: {e}")
            return []

    async def get_historical_stock_data(
        self, symbol: str, start_date: datetime, end_date: datetime
    ) -> List[Dict[str, Any]]:
        """
        Retrieve historical stock price data.

        Args:
            symbol: Stock ticker symbol.
            start_date: Start date for data retrieval.
            end_date: End date for data retrieval.

        Returns:
            A list of dictionaries containing historical price data.
        """
        await self.ensure_connected()
        
        try:
            # Format dates
            start_date_str = start_date.strftime("%Y-%m-%d")
            end_date_str = end_date.strftime("%Y-%m-%d")
            
            async with self.session.get(
                f"{self.base_url}/equity/trade",
                params={
                    "username": self.username,
                    "key": self.api_key,
                    "symbol": symbol,
                    "date_start": start_date_str,
                    "date_end": end_date_str
                }
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    trades = data.get("data", [])
                    
                    result = []
                    for trade in trades:
                        result.append({
                            "timestamp": trade.get("timestamp", ""),
                            "price": trade.get("price", 0),
                            "size": trade.get("size", 0),
                            "exchange": trade.get("exchange", "")
                        })
                    
                    return result
                else:
                    err_text = await response.text()
                    logger.error(f"Error fetching historical stock data: {err_text}")
                    return []
        except Exception as e:
            logger.error(f"Error retrieving historical stock data for {symbol}: {e}")
            return []