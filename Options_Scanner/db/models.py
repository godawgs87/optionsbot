"""
Database models for the options trading system.
"""
import sqlite3
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple


class Database:
    """SQLite database wrapper for options trading system."""
    
    def __init__(self, db_path: str):
        """Initialize database connection."""
        self.db_path = db_path
        self.conn = None
        self.setup_database()
    
    def setup_database(self):
        """Create database and tables if they don't exist."""
        try:
            self.conn = sqlite3.connect(self.db_path)
            cursor = self.conn.cursor()
            
            # Opportunities table
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS opportunities (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT,
                option_type TEXT,
                strike REAL,
                expiration TEXT,
                entry_price REAL,
                entry_time TEXT,
                volume INTEGER,
                open_interest INTEGER,
                iv REAL,
                delta REAL,
                gamma REAL,
                theta REAL,
                vega REAL,
                notional_value REAL,
                alert_type TEXT,
                strategy TEXT,
                tracked BOOLEAN DEFAULT 1,
                closed BOOLEAN DEFAULT 0,
                close_price REAL,
                close_time TEXT
            )
            ''')
            
            # Price updates table
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS price_updates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                opportunity_id INTEGER,
                timestamp TEXT,
                price REAL,
                underlying_price REAL,
                price_change_pct REAL,
                FOREIGN KEY (opportunity_id) REFERENCES opportunities (id)
            )
            ''')
            
            # Historical backtest table
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS backtest_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                strategy TEXT,
                start_date TEXT,
                end_date TEXT,
                total_trades INTEGER,
                winning_trades INTEGER,
                losing_trades INTEGER,
                win_rate REAL,
                avg_profit REAL,
                max_profit REAL,
                max_loss REAL,
                sharpe_ratio REAL,
                timestamp TEXT
            )
            ''')
            
            # Backtest trade details
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS backtest_trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                backtest_id INTEGER,
                symbol TEXT,
                option_type TEXT,
                strike REAL,
                expiration TEXT,
                entry_price REAL,
                entry_time TEXT,
                exit_price REAL,
                exit_time TEXT,
                profit_pct REAL,
                profit_amount REAL,
                FOREIGN KEY (backtest_id) REFERENCES backtest_results (id)
            )
            ''')
            
            self.conn.commit()
            return True
        except Exception as e:
            print(f"Error setting up database: {e}")
            return False
    
    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()


class OpportunityModel:
    """Model for trading opportunities."""
    
    def __init__(self, db: Database):
        """Initialize with database connection."""
        self.db = db
    
    def add_opportunity(self, data: Dict[str, Any]) -> Optional[int]:
        """
        Add a new trading opportunity to the database.
        
        Args:
            data: Dictionary containing opportunity details
            
        Returns:
            Opportunity ID if successful, None otherwise
        """
        try:
            cursor = self.db.conn.cursor()
            
            # Extract data fields
            symbol = data.get("symbol", "")
            option_type = data.get("option_type", "")
            strike = data.get("strike", 0)
            expiration = data.get("expiration", "")
            entry_price = data.get("price", 0)
            volume = data.get("volume", 0)
            open_interest = data.get("open_interest", 0)
            iv = data.get("iv", 0)
            delta = data.get("delta", 0)
            gamma = data.get("gamma", 0)
            theta = data.get("theta", 0)
            vega = data.get("vega", 0)
            notional_value = data.get("notional_value", 0)
            alert_type = data.get("alert_type", "unknown")
            strategy = data.get("strategy", "unknown")
            
            # Current timestamp
            entry_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # Insert opportunity
            cursor.execute('''
            INSERT INTO opportunities
            (symbol, option_type, strike, expiration, entry_price, entry_time, 
             volume, open_interest, iv, delta, gamma, theta, vega, 
             notional_value, alert_type, strategy)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                symbol, option_type, strike, expiration, entry_price, entry_time,
                volume, open_interest, iv, delta, gamma, theta, vega,
                notional_value, alert_type, strategy
            ))
            
            self.db.conn.commit()
            opportunity_id = cursor.lastrowid
            
            # Add initial price update
            cursor.execute('''
            INSERT INTO price_updates
            (opportunity_id, timestamp, price, underlying_price, price_change_pct)
            VALUES (?, ?, ?, ?, ?)
            ''', (
                opportunity_id, entry_time, entry_price, 
                data.get("underlying_price", 0), 0.0
            ))
            
            self.db.conn.commit()
            return opportunity_id
        except Exception as e:
            print(f"Error adding opportunity: {e}")
            return None
    
    def update_price(self, opportunity_id: int, current_price: float, 
                     underlying_price: Optional[float] = None) -> bool:
        """
        Update price for an opportunity.
        
        Args:
            opportunity_id: ID of the opportunity
            current_price: Current option price
            underlying_price: Current underlying asset price (optional)
            
        Returns:
            True if update successful, False otherwise
        """
        try:
            cursor = self.db.conn.cursor()
            
            # Get entry price
            cursor.execute(
                "SELECT entry_price FROM opportunities WHERE id = ?", 
                (opportunity_id,)
            )
            result = cursor.fetchone()
            
            if not result:
                print(f"Opportunity ID {opportunity_id} not found")
                return False
            
            entry_price = result[0]
            
            # Calculate percentage change
            price_change_pct = 0
            if entry_price > 0:
                price_change_pct = ((current_price - entry_price) / entry_price) * 100
            
            # Current timestamp
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # Insert price update
            cursor.execute('''
            INSERT INTO price_updates
            (opportunity_id, timestamp, price, underlying_price, price_change_pct)
            VALUES (?, ?, ?, ?, ?)
            ''', (
                opportunity_id, timestamp, current_price, 
                underlying_price or 0, price_change_pct
            ))
            
            self.db.conn.commit()
            return True
        except Exception as e:
            print(f"Error updating price: {e}")
            return False
    
    def close_opportunity(self, opportunity_id: int, 
                          close_price: float) -> bool:
        """
        Mark an opportunity as closed.
        
        Args:
            opportunity_id: ID of the opportunity
            close_price: Price at which the opportunity was closed
            
        Returns:
            True if update successful, False otherwise
        """
        try:
            cursor = self.db.conn.cursor()
            close_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            cursor.execute('''
            UPDATE opportunities 
            SET closed = 1, close_price = ?, close_time = ? 
            WHERE id = ?
            ''', (close_price, close_time, opportunity_id))
            
            self.db.conn.commit()
            return True
        except Exception as e:
            print(f"Error closing opportunity: {e}")
            return False
    
    def get_opportunity(self, opportunity_id: int) -> Optional[Dict[str, Any]]:
        """
        Get details for a specific opportunity.
        
        Args:
            opportunity_id: ID of the opportunity
            
        Returns:
            Dictionary with opportunity details if found, None otherwise
        """
        try:
            cursor = self.db.conn.cursor()
            cursor.execute("SELECT * FROM opportunities WHERE id = ?", (opportunity_id,))
            row = cursor.fetchone()
            
            if not row:
                return None
            
            # Get column names
            column_names = [description[0] for description in cursor.description]
            
            # Create dictionary from row
            opportunity = dict(zip(column_names, row))
            return opportunity
        except Exception as e:
            print(f"Error getting opportunity: {e}")
            return None
    
    def get_open_opportunities(self) -> List[Dict[str, Any]]:
        """
        Get all open (not closed) opportunities.
        
        Returns:
            List of dictionaries containing opportunity details
        """
        try:
            cursor = self.db.conn.cursor()
            cursor.execute("SELECT * FROM opportunities WHERE closed = 0")
            rows = cursor.fetchall()
            
            # Get column names
            column_names = [description[0] for description in cursor.description]
            
            # Create list of dictionaries
            opportunities = []
            for row in rows:
                opportunity = dict(zip(column_names, row))
                opportunities.append(opportunity)
            
            return opportunities
        except Exception as e:
            print(f"Error getting open opportunities: {e}")
            return []