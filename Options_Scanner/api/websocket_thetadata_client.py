"""
WebSocket-based ThetaData API client for options trading system.
"""
import logging
import asyncio
import json
import websockets
from datetime import datetime
from typing import Dict, List, Optional, Any, Union

logger = logging.getLogger(__name__)

class WebSocketThetaDataClient:
    """Client for interacting with the ThetaData API using WebSockets."""
    
    def __init__(self, username: str, api_key: str):
        """
        Initialize ThetaData WebSocket client.

        Args:
            username: ThetaData username
            api_key: ThetaData API key
        """
        self.username = username
        self.api_key = api_key
        self.websocket_uri = "wss://stream.thetadata.us/v1/ws"
        self.ws = None
        self.connected = False
        self.request_id = 0
        self.response_futures = {}
        
    async def connect(self) -> bool:
        """
        Connect to ThetaData API via WebSocket.
        
        Returns:
            bool: True if connected successfully, False otherwise.
        """
        try:
            self.ws = await websockets.connect(self.websocket_uri)
            
            # Authenticate with the API
            auth_msg = {
                "type": "auth",
                "payload": {
                    "username": self.username,
                    "key": self.api_key
                }
            }
            
            await self.ws.send(json.dumps(auth_msg))
            response = await self.ws.recv()
            response_data = json.loads(response)
            
            if response_data.get("type") == "success":
                self.connected = True
                logger.info("✅ Connected to ThetaData API via WebSocket")
                
                # Start the message handling loop
                asyncio.create_task(self._message_handler())
                
                return True
            else:
                logger.error(f"❌ Authentication failed: {response_data}")
                self.connected = False
                return False
                
        except Exception as e:
            logger.error(f"❌ WebSocket connection error: {e}")
            self.connected = False
            return False

    async def disconnect(self):
        """
        Disconnect from ThetaData API WebSocket.
        """
        if self.ws:
            try:
                await self.ws.close()
                self.ws = None
                self.connected = False
                logger.info("✅ Disconnected from ThetaData API WebSocket")
            except Exception as e:
                logger.error(f"❌ Error disconnecting from ThetaData API: {e}")

    async def _message_handler(self):
        """Handle incoming WebSocket messages."""
        try:
            while self.connected and self.ws:
                try:
                    message = await self.ws.recv()
                    data = json.loads(message)
                    
                    # Handle response to a specific request
                    if "request_id" in data:
                        request_id = data["request_id"]
                        if request_id in self.response_futures:
                            future = self.response_futures[request_id]
                            if not future.done():
                                future.set_result(data)
                    
                    # Handle subscription updates
                    elif data.get("type") == "data":
                        # Process subscription data
                        pass
                        
                except websockets.exceptions.ConnectionClosed:
                    logger.error("WebSocket connection closed")
                    self.connected = False
                    break
                except Exception as e:
                    logger.error(f"Error handling message: {e}")
                    
        except Exception as e:
            logger.error(f"Message handler loop error: {e}")
            self.connected = False
        
        # Try to reconnect if connection was lost
        if not self.connected:
            try:
                await self.connect()
            except:
                pass

    async def _send_request(self, request_type: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Send a request to the ThetaData API via WebSocket.
        
        Args:
            request_type: Type of request
            params: Request parameters
            
        Returns:
            Response data
        """
        if not self.connected or not self.ws:
            await self.connect()
            if not self.connected:
                raise ConnectionError("Failed to connect to ThetaData API")
        
        # Create a new request ID
        self.request_id += 1
        request_id = self.request_id
        
        # Create a future for the response
        future = asyncio.get_event_loop().create_future()
        self.response_futures[request_id] = future
        
        # Create the request
        request = {
            "type": request_type,
            "request_id": request_id,
            "payload": params
        }
        
        # Send the request
        await self.ws.send(json.dumps(request))
        
        # Wait for the response
        try:
            response = await asyncio.wait_for(future, timeout=10)
            del self.response_futures[request_id]
            
            if response.get("type") == "error":
                raise Exception(f"API error: {response.get('payload', {}).get('message')}")
                
            return response.get("payload", {})
        except asyncio.TimeoutError:
            del self.response_futures[request_id]
            raise TimeoutError("Request timed out")

    async def get_expirations(self, symbol: str) -> List[str]:
        """
        Retrieve available expiration dates for an options contract.

        Args:
            symbol: Stock ticker symbol.

        Returns:
            A list of expiration dates as strings (YYYY-MM-DD format).
        """
        try:
            response = await self._send_request("expirations", {"root": symbol})
            expirations = response.get("expirations", [])
            return sorted(expirations)
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
        try:
            # Get the nearest expiration if no date is provided
            if not date:
                expirations = await self.get_expirations(symbol)
                if not expirations:
                    logger.error(f"No expirations found for {symbol}")
                    return []
                date = expirations[0]
            
            # Get option chain data
            response = await self._send_request("chain", {
                "root": symbol,
                "expiration": date
            })
            
            chain_data = response.get("options", [])
            
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
                
                # Add underlying price if available
                if "underlying_price" in option:
                    option_data["underlying_price"] = option.get("underlying_price", 0)
                
                options.append(option_data)
            
            return options
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
        try:
            # Parse option_symbol to extract components if necessary
            # This depends on the format expected by the API
            
            response = await self._send_request("quote", {
                "option_symbol": option_symbol
            })
            
            if not response:
                return None
                
            return {
                "bid": response.get("bid", 0),
                "ask": response.get("ask", 0),
                "last": response.get("last", 0),
                "mid": (response.get("bid", 0) + response.get("ask", 0)) / 2
            }
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
        try:
            # Format dates
            start_date_str = start_date.strftime("%Y-%m-%d")
            end_date_str = end_date.strftime("%Y-%m-%d")
            
            response = await self._send_request("historical", {
                "option_symbol": option_symbol,
                "start_date": start_date_str,
                "end_date": end_date_str
            })
            
            trades = response.get("trades", [])
            
            return [
                {
                    "timestamp": trade.get("timestamp", ""),
                    "price": trade.get("price", 0),
                    "size": trade.get("size", 0),
                    "exchange": trade.get("exchange", "")
                }
                for trade in trades
            ]
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
        try:
            # Format dates
            start_date_str = start_date.strftime("%Y-%m-%d")
            end_date_str = end_date.strftime("%Y-%m-%d")
            
            response = await self._send_request("stock_historical", {
                "symbol": symbol,
                "start_date": start_date_str,
                "end_date": end_date_str
            })
            
            trades = response.get("trades", [])
            
            return [
                {
                    "timestamp": trade.get("timestamp", ""),
                    "price": trade.get("price", 0),
                    "size": trade.get("size", 0),
                    "exchange": trade.get("exchange", "")
                }
                for trade in trades
            ]
        except Exception as e:
            logger.error(f"Error retrieving historical stock data for {symbol}: {e}")
            return []
            
    # Subscription methods for real-time data
    
    async def subscribe_option(self, option_symbol: str, callback):
        """
        Subscribe to real-time option data.
        
        Args:
            option_symbol: Option contract symbol
            callback: Function to call when data is received
        """
        try:
            subscription_id = f"option_{option_symbol}"
            
            # Send subscription request
            await self._send_request("subscribe", {
                "channel": "option",
                "symbol": option_symbol,
                "id": subscription_id
            })
            
            # TODO: Implement callback registration for subscription updates
            
            return subscription_id
        except Exception as e:
            logger.error(f"Error subscribing to option {option_symbol}: {e}")
            return None
            
    async def subscribe_stock(self, symbol: str, callback):
        """
        Subscribe to real-time stock data.
        
        Args:
            symbol: Stock symbol
            callback: Function to call when data is received
        """
        try:
            subscription_id = f"stock_{symbol}"
            
            # Send subscription request
            await self._send_request("subscribe", {
                "channel": "stock",
                "symbol": symbol,
                "id": subscription_id
            })
            
            # TODO: Implement callback registration for subscription updates
            
            return subscription_id
        except Exception as e:
            logger.error(f"Error subscribing to stock {symbol}: {e}")
            return None
            
    async def unsubscribe(self, subscription_id: str):
        """
        Unsubscribe from a data stream.
        
        Args:
            subscription_id: ID of the subscription
        """
        try:
            await self._send_request("unsubscribe", {
                "id": subscription_id
            })
            
            # TODO: Clean up callback registration
            
            return True
        except Exception as e:
            logger.error(f"Error unsubscribing from {subscription_id}: {e}")
            return False