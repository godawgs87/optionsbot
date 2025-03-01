"""
Telegram bot for sending options scanner notifications.
"""
import asyncio
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional, Union
import aiohttp

logger = logging.getLogger(__name__)

class TelegramBot:
    """Telegram bot for sending scanner alerts and updates."""
    
    def __init__(self, token: str, chat_id: str):
        """
        Initialize Telegram bot.
        
        Args:
            token: Telegram bot token
            chat_id: Chat ID to send messages to
        """
        self.token = token
        self.chat_id = chat_id
        self.api_url = f"https://api.telegram.org/bot{token}"
    
    async def send_message(self, text: str, parse_mode: str = "HTML") -> bool:
        """
        Send a message via Telegram.
        
        Args:
            text: Message text
            parse_mode: Text formatting mode (HTML or Markdown)
            
        Returns:
            True if message was sent successfully, False otherwise
        """
        try:
            url = f"{self.api_url}/sendMessage"
            payload = {
                "chat_id": self.chat_id,
                "text": text,
                "parse_mode": parse_mode
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload) as response:
                    if response.status == 200:
                        logger.debug("Telegram message sent successfully")
                        return True
                    else:
                        response_text = await response.text()
                        logger.error(f"Failed to send Telegram message: {response_text}")
                        return False
        except Exception as e:
            logger.error(f"Error sending Telegram message: {e}")
            return False
    
    async def send_opportunity_alert(self, opportunity: Dict[str, Any], 
                                     ai_analysis: Optional[Dict[str, Any]] = None) -> bool:
        """
        Send an alert about a new trading opportunity.
        
        Args:
            opportunity: Dictionary with opportunity details
            ai_analysis: Optional AI analysis results
            
        Returns:
            True if alert was sent successfully, False otherwise
        """
        try:
            # Format option details
            symbol = opportunity.get("symbol", "")
            option_type = opportunity.get("option_type", "").upper()
            strike = opportunity.get("strike", 0)
            expiration = opportunity.get("expiration", "")
            price = opportunity.get("price", 0)
            volume = opportunity.get("volume", 0)
            notional_value = opportunity.get("notional_value", 0)
            alert_type = opportunity.get("alert_type", "").upper()
            
            # Create message
            message = f"🚨 <b>{alert_type} ALERT</b> 🚨\n\n"
            message += f"<b>{symbol} {option_type} ${strike} {expiration}</b>\n\n"
            message += f"💰 Price: ${price:.2f}\n"
            message += f"📊 Volume: {volume:,}\n"
            message += f"💵 Notional Value: ${notional_value:,.2f}\n"
            
            # Add AI analysis if available
            if ai_analysis:
                probability = ai_analysis.get("success_probability", 0)
                confidence = ai_analysis.get("confidence", "").upper()
                reasoning = ai_analysis.get("reasoning", "")
                
                # Use emojis to represent probability
                if probability >= 70:
                    emoji = "🔥"  # Fire (hot)
                elif probability >= 60:
                    emoji = "✅"  # Check mark
                elif probability >= 40:
                    emoji = "⚠️"  # Warning
                else:
                    emoji = "❌"  # X mark
                
                message += f"\n<b>AI ANALYSIS</b> {emoji}\n"
                message += f"Success Probability: <b>{probability:.1f}%</b> ({confidence})\n"
                message += f"{reasoning}\n"
            
            message += f"\n⏰ Alert Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            
            # Send the message
            return await self.send_message(message)
        except Exception as e:
            logger.error(f"Error sending opportunity alert: {e}")
            return False
    
    async def send_leaderboard(self, leaderboard: Dict[str, Any]) -> bool:
        """
        Send performance leaderboard.
        
        Args:
            leaderboard: Dictionary with leaderboard data
            
        Returns:
            True if leaderboard was sent successfully, False otherwise
        """
        try:
            message = "📈 <b>PERFORMANCE LEADERBOARD</b> 📈\n\n"
            
            # Add summary statistics
            summary = leaderboard.get("summary", {})
            total_opportunities = summary.get("total_opportunities", 0)
            message += f"Total Opportunities: {total_opportunities}\n\n"
            
            # Time window averages
            message += "<b>Average Profit by Time Window:</b>\n"
            for time_window in ["1m", "5m", "10m", "15m", "20m"]:
                avg_profit = summary.get(f"avg_profit_{time_window}", 0)
                message += f"  {time_window}: {avg_profit:.2f}%\n"
            
            # Top performers (max 5)
            message += "\n<b>Top Performers:</b>\n"
            top_performers = leaderboard.get("overall", [])[:5]
            
            for i, performer in enumerate(top_performers, 1):
                symbol = performer.get("symbol", "")
                option_type = performer.get("type", "").upper()
                strike = performer.get("strike", "")
                max_profit = performer.get("max_profit", "")
                
                message += f"{i}. {symbol} {option_type} {strike} - {max_profit}\n"
            
            # Send the message
            return await self.send_message(message)
        except Exception as e:
            logger.error(f"Error sending leaderboard: {e}")
            return False
    
    async def send_performance_report(self, open_opportunities: List[Dict[str, Any]]) -> bool:
        """
        Send a performance report for current open opportunities.
        
        Args:
            open_opportunities: List of dictionaries with open opportunity details
            
        Returns:
            True if report was sent successfully, False otherwise
        """
        try:
            if not open_opportunities:
                message = "📊 <b>PERFORMANCE REPORT</b>\n\nNo open opportunities at this time."
                return await self.send_message(message)
            
            message = "📊 <b>CURRENT OPPORTUNITIES REPORT</b>\n\n"
            
            # Group opportunities by alert type
            alert_types = {}
            for opp in open_opportunities:
                alert_type = opp.get("alert_type", "unknown")
                if alert_type not in alert_types:
                    alert_types[alert_type] = []
                alert_types[alert_type].append(opp)
            
            # Report stats for each alert type
            for alert_type, opps in alert_types.items():
                message += f"<b>{alert_type.upper()} ({len(opps)})</b>\n"
                
                # Calculate average performance
                avg_profit = sum(opp.get("current_profit_pct", 0) for opp in opps) / len(opps)
                max_profit = max(opp.get("current_profit_pct", 0) for opp in opps)
                
                message += f"Average Profit: {avg_profit:.2f}%\n"
                message += f"Max Profit: {max_profit:.2f}%\n\n"
            
            # Overall stats
            total_opps = len(open_opportunities)
            overall_avg = sum(opp.get("current_profit_pct", 0) for opp in open_opportunities) / total_opps
            
            message += f"<b>OVERALL ({total_opps} opportunities)</b>\n"
            message += f"Average Profit: {overall_avg:.2f}%\n"
            
            # Send the message
            return await self.send_message(message)
        except Exception as e:
            logger.error(f"Error sending performance report: {e}")
            return False
    
    async def send_ai_insights(self, analysis_data: Dict[str, Any]) -> bool:
        """
        Send AI insights and trend analysis.
        
        Args:
            analysis_data: Dictionary with AI analysis data
            
        Returns:
            True if insights were sent successfully, False otherwise
        """
        try:
            message = "🧠 <b>AI MARKET INSIGHTS</b> 🧠\n\n"
            
            # Market trend analysis
            market_trend = analysis_data.get("market_trend", {})
            trend_direction = market_trend.get("direction", "neutral").upper()
            trend_strength = market_trend.get("strength", 0)
            
            # Emoji based on trend
            if trend_direction == "BULLISH":
                emoji = "🐂"  # Bull
            elif trend_direction == "BEARISH":
                emoji = "🐻"  # Bear
            else:
                emoji = "⚖️"  # Balance scale
            
            message += f"<b>Market Trend:</b> {emoji} {trend_direction} (Strength: {trend_strength:.1f}/10)\n\n"
            
            # Add top picks
            top_picks = analysis_data.get("top_picks", [])
            if top_picks:
                message += "<b>Today's Top AI Picks:</b>\n"
                for i, pick in enumerate(top_picks[:3], 1):
                    symbol = pick.get("symbol", "")
                    option_type = pick.get("option_type", "").upper()
                    strike = pick.get("strike", 0)
                    expiry = pick.get("expiry", "")
                    probability = pick.get("success_probability", 0)
                    
                    message += f"{i}. {symbol} {option_type} ${strike} {expiry} - {probability:.1f}% probability\n"
            
            # Add strategy recommendations
            strategies = analysis_data.get("strategies", [])
            if strategies:
                message += "\n<b>Recommended Strategies:</b>\n"
                for strategy in strategies[:3]:
                    message += f"• {strategy}\n"
            
            # Send the message
            return await self.send_message(message)
        except Exception as e:
            logger.error(f"Error sending AI insights: {e}")
            return False