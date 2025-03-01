"""
Performance tracker for options trading opportunities.
"""
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import pandas as pd

from db.database import Database
from db.models import OpportunityModel
from notifications.telegram_bot import TelegramBot

logger = logging.getLogger(__name__)

class PerformanceTracker:
    """Track and report performance of options trading opportunities."""
    
    def __init__(
        self,
        db: Database,
        opportunity_model: OpportunityModel,
        telegram_bot: Optional[TelegramBot] = None,
        update_interval_seconds: int = 3600,  # Default to hourly updates
        performance_windows: List[str] = ["1m", "5m", "10m", "15m", "20m", "30m", "1h"]
    ):
        """
        Initialize performance tracker.
        
        Args:
            db: Database connection
            opportunity_model: Opportunity database model
            telegram_bot: Telegram bot for sending reports (optional)
            update_interval_seconds: How often to generate performance reports
            performance_windows: Time windows to track performance
        """
        self.db = db
        self.opportunity_model = opportunity_model
        self.telegram_bot = telegram_bot
        self.update_interval_seconds = update_interval_seconds
        self.performance_windows = performance_windows
        self.running = False
    
    async def run(self):
        """Run the performance tracker in a loop."""
        self.running = True
        logger.info("Starting PerformanceTracker")
        
        # Generate initial report
        await self.generate_performance_report()
        
        while self.running:
            try:
                # Wait for next update interval
                await asyncio.sleep(self.update_interval_seconds)
                
                # Generate and send performance report
                await self.generate_performance_report()
            except Exception as e:
                logger.error(f"Error in PerformanceTracker: {e}")
                await asyncio.sleep(10)  # Wait before retrying
    
    async def stop(self):
        """Stop the performance tracker."""
        logger.info("Stopping PerformanceTracker")
        self.running = False
    
    async def generate_performance_report(self):
        """Generate performance report for current opportunities."""
        logger.info("Generating performance report")
        
        try:
            # Get all open opportunities
            open_opportunities = self.opportunity_model.get_open_opportunities()
            
            if not open_opportunities:
                logger.info("No open opportunities to report")
                return
            
            # Get current prices for open opportunities
            opportunities_with_performance = await self.calculate_current_performance(open_opportunities)
            
            # Generate performance summary
            summary = self.generate_summary(opportunities_with_performance)
            
            # Send report via Telegram if available
            if self.telegram_bot:
                await self.telegram_bot.send_performance_report(opportunities_with_performance)
            
            # Record performance data in database
            self.record_performance_data(summary)
            
            # Generate leaderboard
            await self.generate_leaderboard()
            
            logger.info("Performance report generated successfully")
        except Exception as e:
            logger.error(f"Error generating performance report: {e}")
    
    async def calculate_current_performance(self, opportunities: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Calculate current performance metrics for open opportunities.
        
        Args:
            opportunities: List of opportunity dictionaries
            
        Returns:
            List of opportunities with performance data added
        """
        result = []
        
        for opp in opportunities:
            try:
                # Get latest price data
                latest_price_data = self.db.fetch_one(
                    """
                    SELECT price, price_change_pct, timestamp 
                    FROM price_updates 
                    WHERE opportunity_id = ? 
                    ORDER BY timestamp DESC 
                    LIMIT 1
                    """, 
                    (opp["id"],)
                )
                
                if not latest_price_data:
                    continue
                
                # Add performance data to opportunity
                opp_with_perf = opp.copy()
                opp_with_perf["current_price"] = latest_price_data["price"]
                opp_with_perf["current_profit_pct"] = latest_price_data["price_change_pct"]
                opp_with_perf["last_update"] = latest_price_data["timestamp"]
                
                # Calculate time-based performance (1m, 5m, etc.)
                time_performance = await self.calculate_time_performance(opp["id"])
                opp_with_perf.update(time_performance)
                
                result.append(opp_with_perf)
            except Exception as e:
                logger.error(f"Error calculating performance for opportunity {opp.get('id')}: {e}")
        
        return result
    
    async def calculate_time_performance(self, opportunity_id: int) -> Dict[str, float]:
        """
        Calculate performance over different time windows.
        
        Args:
            opportunity_id: ID of the opportunity
            
        Returns:
            Dictionary with performance metrics for each time window
        """
        result = {}
        now = datetime.now()
        
        for window in self.performance_windows:
            # Parse time window
            if window.endswith("m"):
                minutes = int(window[:-1])
                start_time = now - timedelta(minutes=minutes)
            elif window.endswith("h"):
                hours = int(window[:-1])
                start_time = now - timedelta(hours=hours)
            else:
                continue
            
            # Format start time for SQL query
            start_time_str = start_time.strftime("%Y-%m-%d %H:%M:%S")
            
            # Get price at start of window
            start_price_data = self.db.fetch_one(
                """
                SELECT price
                FROM price_updates
                WHERE opportunity_id = ? AND timestamp >= ?
                ORDER BY timestamp ASC
                LIMIT 1
                """,
                (opportunity_id, start_time_str)
            )
            
            # Get current price
            current_price_data = self.db.fetch_one(
                """
                SELECT price
                FROM price_updates
                WHERE opportunity_id = ?
                ORDER BY timestamp DESC
                LIMIT 1
                """,
                (opportunity_id,)
            )
            
            if start_price_data and current_price_data:
                start_price = start_price_data["price"]
                current_price = current_price_data["price"]
                
                if start_price > 0:
                    profit_pct = ((current_price - start_price) / start_price) * 100
                    result[f"profit_{window}"] = round(profit_pct, 2)
            
        return result
    
    def generate_summary(self, opportunities: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Generate performance summary statistics.
        
        Args:
            opportunities: List of opportunities with performance data
            
        Returns:
            Dictionary with summary statistics
        """
        if not opportunities:
            return {
                "total_opportunities": 0,
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
        
        # Convert to DataFrame for easier analysis
        df = pd.DataFrame(opportunities)
        
        # Calculate summary statistics
        summary = {
            "total_opportunities": len(df),
            "total_notional_value": df["notional_value"].sum(),
            "avg_profit_pct": df["current_profit_pct"].mean(),
            "max_profit_pct": df["current_profit_pct"].max(),
            "min_profit_pct": df["current_profit_pct"].min(),
            "profitable_count": (df["current_profit_pct"] > 0).sum(),
            "profitable_percentage": (df["current_profit_pct"] > 0).mean() * 100,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        
        # Add time window statistics
        for window in self.performance_windows:
            profit_col = f"profit_{window}"
            if profit_col in df.columns:
                summary[f"avg_{profit_col}"] = df[profit_col].mean()
                summary[f"win_rate_{window}"] = (df[profit_col] > 0).mean() * 100
        
        # Group by strategy
        strategy_groups = df.groupby("strategy")
        strategy_performance = {}
        
        for strategy, group in strategy_groups:
            strategy_performance[strategy] = {
                "count": len(group),
                "avg_profit": group["current_profit_pct"].mean(),
                "win_rate": (group["current_profit_pct"] > 0).mean() * 100
            }
        
        summary["strategy_performance"] = strategy_performance
        
        # Group by alert type
        alert_groups = df.groupby("alert_type")
        alert_performance = {}
        
        for alert_type, group in alert_groups:
            alert_performance[alert_type] = {
                "count": len(group),
                "avg_profit": group["current_profit_pct"].mean(),
                "win_rate": (group["current_profit_pct"] > 0).mean() * 100
            }
        
        summary["alert_performance"] = alert_performance
        
        return summary
    
    def record_performance_data(self, summary: Dict[str, Any]):
        """
        Record performance data in database.
        
        Args:
            summary: Dictionary with performance summary
        """
        try:
            # Convert complex data to JSON for storage
            import json
            
            # Prepare data for storage
            performance_data = {
                "timestamp": summary["timestamp"],
                "total_opportunities": summary["total_opportunities"],
                "avg_profit_pct": summary.get("avg_profit_pct", 0),
                "win_rate": summary.get("profitable_percentage", 0),
                "strategy_data": json.dumps(summary.get("strategy_performance", {})),
                "alert_data": json.dumps(summary.get("alert_performance", {})),
                "time_window_data": json.dumps({k: v for k, v in summary.items() if k.startswith("avg_profit_") or k.startswith("win_rate_")})
            }
            
            # Create performance_history table if it doesn't exist
            self.db.execute('''
            CREATE TABLE IF NOT EXISTS performance_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT,
                total_opportunities INTEGER,
                avg_profit_pct REAL,
                win_rate REAL,
                strategy_data TEXT,
                alert_data TEXT,
                time_window_data TEXT
            )
            ''')
            
            # Insert performance data
            self.db.insert("performance_history", performance_data)
            
            logger.info("Performance data recorded in database")
        except Exception as e:
            logger.error(f"Error recording performance data: {e}")
    
    async def generate_leaderboard(self):
        """Generate performance leaderboard for top performing opportunities."""
        try:
            # Get data for both open and closed opportunities
            all_opps = self.db.fetch_all("""
                SELECT o.*, 
                       CASE WHEN o.closed = 1 
                            THEN ((o.close_price - o.entry_price) / o.entry_price) * 100
                            ELSE (SELECT price_change_pct FROM price_updates 
                                  WHERE opportunity_id = o.id 
                                  ORDER BY timestamp DESC LIMIT 1)
                       END as profit_pct
                FROM opportunities o
                WHERE o.tracked = 1 AND (o.closed = 1 OR o.id IN (
                    SELECT opportunity_id FROM price_updates 
                    GROUP BY opportunity_id
                ))
                ORDER BY profit_pct DESC
            """)
            
            if not all_opps:
                logger.info("No opportunities for leaderboard")
                return
            
            # Generate leaderboard data
            leaderboard = {
                "overall": [],
                "by_strategy": {},
                "by_alert_type": {},
                "summary": {
                    "total_opportunities": len(all_opps),
                    "avg_profit": sum(opp.get("profit_pct", 0) for opp in all_opps) / len(all_opps)
                }
            }
            
            # Overall top performers (top 10)
            for i, opp in enumerate(all_opps[:10]):
                leaderboard["overall"].append({
                    "rank": i + 1,
                    "symbol": opp.get("symbol", ""),
                    "type": opp.get("option_type", ""),
                    "strike": f"${opp.get('strike', 0)}",
                    "max_profit": f"{opp.get('profit_pct', 0):.2f}%",
                    "status": "Closed" if opp.get("closed", 0) else "Open"
                })
            
            # Group by strategy
            strategy_groups = {}
            for opp in all_opps:
                strategy = opp.get("strategy", "unknown")
                if strategy not in strategy_groups:
                    strategy_groups[strategy] = []
                strategy_groups[strategy].append(opp)
            
            # Top performers by strategy
            for strategy, opps in strategy_groups.items():
                # Sort by profit
                sorted_opps = sorted(opps, key=lambda x: x.get("profit_pct", 0), reverse=True)
                
                # Add top 5
                leaderboard["by_strategy"][strategy] = []
                for i, opp in enumerate(sorted_opps[:5]):
                    leaderboard["by_strategy"][strategy].append({
                        "rank": i + 1,
                        "symbol": opp.get("symbol", ""),
                        "type": opp.get("option_type", ""),
                        "strike": f"${opp.get('strike', 0)}",
                        "max_profit": f"{opp.get('profit_pct', 0):.2f}%"
                    })
            
            # Group by alert type
            alert_groups = {}
            for opp in all_opps:
                alert_type = opp.get("alert_type", "unknown")
                if alert_type not in alert_groups:
                    alert_groups[alert_type] = []
                alert_groups[alert_type].append(opp)
            
            # Top performers by alert type
            for alert_type, opps in alert_groups.items():
                # Sort by profit
                sorted_opps = sorted(opps, key=lambda x: x.get("profit_pct", 0), reverse=True)
                
                # Add top 5
                leaderboard["by_alert_type"][alert_type] = []
                for i, opp in enumerate(sorted_opps[:5]):
                    leaderboard["by_alert_type"][alert_type].append({
                        "rank": i + 1,
                        "symbol": opp.get("symbol", ""),
                        "type": opp.get("option_type", ""),
                        "strike": f"${opp.get('strike', 0)}",
                        "max_profit": f"{opp.get('profit_pct', 0):.2f}%"
                    })
            
            # Send leaderboard via Telegram if available
            if self.telegram_bot:
                await self.telegram_bot.send_leaderboard(leaderboard)
            
            logger.info("Leaderboard generated successfully")
        except Exception as e:
            logger.error(f"Error generating leaderboard: {e}")