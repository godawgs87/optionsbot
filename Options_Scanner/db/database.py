"""
Database connection module with enhanced error handling for options trading system.
"""
import os
import sqlite3
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple, Union
from contextlib import contextmanager
import threading
import time
import json

from utils.error_handler import handle_errors, async_handle_errors, DatabaseError, register_error

logger = logging.getLogger(__name__)


class Database:
    """Enhanced SQLite database wrapper for options trading system with improved error handling."""
    
    def __init__(self, db_path: str, max_retries: int = 3, retry_delay: float = 0.5):
        """
        Initialize database connection manager.
        
        Args:
            db_path: Path to SQLite database file
            max_retries: Maximum number of retry attempts for database operations
            retry_delay: Base delay between retries (will be exponentially increased)
        """
        self.db_path = db_path
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self._conn = None
        self._connections = {}  # Thread-local connections
        self._lock = threading.RLock()  # Reentrant lock for thread safety
        
        # Ensure directory exists
        db_dir = os.path.dirname(db_path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir)
            
        # Initialize database
        self.setup_database()
    
    @handle_errors(default_return=False, handled_exceptions=(sqlite3.Error, OSError))
    def setup_database(self) -> bool:
        """
        Create database and tables if they don't exist.
        
        Returns:
            True if setup successful, False otherwise
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Create tables
            self._create_tables(cursor)
            
            conn.commit()
            logger.info("Database setup complete")
            return True
    
    def _create_tables(self, cursor):
        """Create all required database tables."""
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
        
        # AI model performance
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS ai_model_performance (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            model_version TEXT,
            accuracy REAL,
            precision REAL,
            recall REAL,
            f1_score REAL,
            training_date TEXT,
            feature_importances TEXT,
            notes TEXT
        )
        ''')
        
        # System settings
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS system_settings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            setting_name TEXT UNIQUE,
            setting_value TEXT,
            setting_type TEXT,
            description TEXT,
            last_updated TEXT
        )
        ''')
        
        # Error tracking table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS error_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            error_id TEXT UNIQUE,
            error_type TEXT,
            message TEXT,
            timestamp TEXT,
            traceback TEXT,
            context TEXT,
            resolved BOOLEAN DEFAULT 0
        )
        ''')
        
        # Database status table for health monitoring
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS db_status (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            check_time TEXT,
            status TEXT,
            size_bytes INTEGER,
            tables_count INTEGER,
            last_vacuum TEXT,
            details TEXT
        )
        ''')
    
    @contextmanager
    def get_connection(self):
        """
        Get a database connection from the pool with retry logic.
        
        Yields:
            SQLite connection object
        
        Raises:
            DatabaseError: If connection cannot be established after retries
        """
        # Get current thread ID
        thread_id = threading.get_ident()
        
        # Get or create connection with retry logic
        conn = None
        retry_count = 0
        last_error = None
        
        while retry_count < self.max_retries:
            try:
                with self._lock:
                    # Check if this thread already has a connection
                    if thread_id in self._connections:
                        conn = self._connections[thread_id]
                        
                        # Test if connection is valid
                        try:
                            conn.execute("SELECT 1")
                        except sqlite3.Error:
                            # Connection is invalid, create a new one
                            try:
                                conn.close()
                            except:
                                pass
                            conn = None
                    
                    # Create a new connection if needed
                    if conn is None:
                        conn = sqlite3.connect(self.db_path, timeout=20.0)
                        
                        # Enable foreign keys
                        conn.execute("PRAGMA foreign_keys = ON")
                        # Enable WAL mode for better concurrency
                        conn.execute("PRAGMA journal_mode = WAL")
                        
                        # Configure connection
                        conn.row_factory = sqlite3.Row
                        
                        # Store connection
                        self._connections[thread_id] = conn
                
                # If we got here, connection is successful
                break
            
            except sqlite3.Error as e:
                last_error = e
                logger.warning(f"Database connection attempt {retry_count + 1} failed: {e}")
                
                # Exponential backoff
                retry_delay = self.retry_delay * (2 ** retry_count)
                time.sleep(retry_delay)
                retry_count += 1
        
        # If we couldn't establish a connection after all retries
        if conn is None:
            error_id = register_error(DatabaseError(f"Failed to connect to database after {self.max_retries} attempts: {last_error}"))
            raise DatabaseError(f"Database connection failed. Error ID: {error_id}")
        
        try:
            yield conn
            # Successful operation - implicit commit happens from context manager
        except Exception as e:
            # Try to rollback on any exception
            try:
                conn.rollback()
            except sqlite3.Error:
                pass
            
            # Register and re-raise the original error
            if isinstance(e, sqlite3.Error):
                error_id = register_error(DatabaseError(f"Database operation failed: {str(e)}"))
                raise DatabaseError(f"Database operation failed. Error ID: {error_id}") from e
            else:
                raise
    
    @property
    def conn(self):
        """Get the default connection (for backward compatibility)."""
        if self._conn is None:
            try:
                self._conn = sqlite3.connect(self.db_path)
                self._conn.row_factory = sqlite3.Row
                self._conn.execute("PRAGMA foreign_keys = ON")
            except sqlite3.Error as e:
                error_id = register_error(DatabaseError(f"Failed to create default connection: {str(e)}"))
                raise DatabaseError(f"Database connection failed. Error ID: {error_id}") from e
        return self._conn
    
    @handle_errors(default_return=None, handled_exceptions=sqlite3.Error)
    def execute(self, query: str, params: tuple = ()) -> sqlite3.Cursor:
        """
        Execute a SQL query with retry logic.
        
        Args:
            query: SQL query string
            params: Query parameters
            
        Returns:
            SQLite cursor object
        """
        retry_count = 0
        last_error = None
        
        while retry_count < self.max_retries:
            try:
                with self.get_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute(query, params)
                    conn.commit()
                    return cursor
            except sqlite3.Error as e:
                last_error = e
                logger.warning(f"Database execute attempt {retry_count + 1} failed: {e}")
                
                # Only retry on certain errors
                if "database is locked" in str(e).lower() or "busy" in str(e).lower():
                    # Exponential backoff
                    retry_delay = self.retry_delay * (2 ** retry_count)
                    time.sleep(retry_delay)
                    retry_count += 1
                else:
                    # Don't retry on syntax errors, etc.
                    break
        
        # If we get here, all retries failed
        error_id = register_error(DatabaseError(f"Database execute failed after {retry_count} attempts: {last_error}"))
        raise DatabaseError(f"Database execute failed. Error ID: {error_id}")
    
    @handle_errors(default_return=None, handled_exceptions=sqlite3.Error)
    def executemany(self, query: str, params_list: List[tuple]) -> sqlite3.Cursor:
        """
        Execute a SQL query with multiple parameter sets and retry logic.
        
        Args:
            query: SQL query string
            params_list: List of parameter tuples
            
        Returns:
            SQLite cursor object
        """
        retry_count = 0
        last_error = None
        
        while retry_count < self.max_retries:
            try:
                with self.get_connection() as conn:
                    cursor = conn.cursor()
                    cursor.executemany(query, params_list)
                    conn.commit()
                    return cursor
            except sqlite3.Error as e:
                last_error = e
                logger.warning(f"Database executemany attempt {retry_count + 1} failed: {e}")
                
                # Only retry on certain errors
                if "database is locked" in str(e).lower() or "busy" in str(e).lower():
                    # Exponential backoff
                    retry_delay = self.retry_delay * (2 ** retry_count)
                    time.sleep(retry_delay)
                    retry_count += 1
                else:
                    # Don't retry on syntax errors, etc.
                    break
        
        # If we get here, all retries failed
        error_id = register_error(DatabaseError(f"Database executemany failed after {retry_count} attempts: {last_error}"))
        raise DatabaseError(f"Database executemany failed. Error ID: {error_id}")
    
    @handle_errors(default_return=None, handled_exceptions=sqlite3.Error)
    def fetch_one(self, query: str, params: tuple = ()) -> Optional[Dict]:
        """
        Execute a query and fetch a single result with retry logic.
        
        Args:
            query: SQL query string
            params: Query parameters
            
        Returns:
            Dictionary with result or None if no results
        """
        retry_count = 0
        last_error = None
        
        while retry_count < self.max_retries:
            try:
                with self.get_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute(query, params)
                    row = cursor.fetchone()
                    
                    if not row:
                        return None
                    
                    return dict(row)
            except sqlite3.Error as e:
                last_error = e
                logger.warning(f"Database fetch_one attempt {retry_count + 1} failed: {e}")
                
                # Only retry on certain errors
                if "database is locked" in str(e).lower() or "busy" in str(e).lower():
                    # Exponential backoff
                    retry_delay = self.retry_delay * (2 ** retry_count)
                    time.sleep(retry_delay)
                    retry_count += 1
                else:
                    # Don't retry on syntax errors, etc.
                    break
        
        # If we get here, all retries failed
        error_id = register_error(DatabaseError(f"Database fetch_one failed after {retry_count} attempts: {last_error}"))
        raise DatabaseError(f"Database fetch_one failed. Error ID: {error_id}")
    
    @handle_errors(default_return=[], handled_exceptions=sqlite3.Error)
    def fetch_all(self, query: str, params: tuple = ()) -> List[Dict]:
        """
        Execute a query and fetch all results with retry logic.
        
        Args:
            query: SQL query string
            params: Query parameters
            
        Returns:
            List of dictionaries with results
        """
        retry_count = 0
        last_error = None
        
        while retry_count < self.max_retries:
            try:
                with self.get_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute(query, params)
                    rows = cursor.fetchall()
                    
                    return [dict(row) for row in rows]
            except sqlite3.Error as e:
                last_error = e
                logger.warning(f"Database fetch_all attempt {retry_count + 1} failed: {e}")
                
                # Only retry on certain errors
                if "database is locked" in str(e).lower() or "busy" in str(e).lower():
                    # Exponential backoff
                    retry_delay = self.retry_delay * (2 ** retry_count)
                    time.sleep(retry_delay)
                    retry_count += 1
                else:
                    # Don't retry on syntax errors, etc.
                    break
        
        # If we get here, all retries failed
        error_id = register_error(DatabaseError(f"Database fetch_all failed after {retry_count} attempts: {last_error}"))
        raise DatabaseError(f"Database fetch_all failed. Error ID: {error_id}")
    
    @handle_errors(default_return=None, handled_exceptions=sqlite3.Error)
    def insert(self, table: str, data: Dict) -> Optional[int]:
        """
        Insert data into a table with retry logic.
        
        Args:
            table: Table name
            data: Dictionary mapping column names to values
            
        Returns:
            ID of inserted row or None on failure
        """
        # Validate input
        if not table or not data:
            error_id = register_error(DatabaseError("Invalid table name or empty data for insert"))
            logger.error(f"Invalid insert parameters. Error ID: {error_id}")
            return None
        
        columns = ', '.join(data.keys())
        placeholders = ', '.join(['?'] * len(data))
        values = tuple(data.values())
        
        retry_count = 0
        last_error = None
        
        while retry_count < self.max_retries:
            try:
                with self.get_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute(
                        f"INSERT INTO {table} ({columns}) VALUES ({placeholders})",
                        values
                    )
                    conn.commit()
                    return cursor.lastrowid
            except sqlite3.Error as e:
                last_error = e
                logger.warning(f"Database insert attempt {retry_count + 1} failed: {e}")
                
                # Only retry on certain errors
                if "database is locked" in str(e).lower() or "busy" in str(e).lower():
                    # Exponential backoff
                    retry_delay = self.retry_delay * (2 ** retry_count)
                    time.sleep(retry_delay)
                    retry_count += 1
                else:
                    # Don't retry on syntax errors, constraint violations, etc.
                    break
        
        # If we get here, all retries failed
        error_id = register_error(DatabaseError(f"Insert into {table} failed after {retry_count} attempts: {last_error}"))
        logger.error(f"Database insert failed. Error ID: {error_id}")
        return None
    
    @handle_errors(default_return=False, handled_exceptions=sqlite3.Error)
    def update(self, table: str, data: Dict, where: str, where_params: tuple) -> bool:
        """
        Update data in a table with retry logic.
        
        Args:
            table: Table name
            data: Dictionary mapping column names to values
            where: WHERE clause
            where_params: Parameters for WHERE clause
            
        Returns:
            True if successful, False otherwise
        """
        # Validate input
        if not table or not data:
            error_id = register_error(DatabaseError("Invalid table name or empty data for update"))
            logger.error(f"Invalid update parameters. Error ID: {error_id}")
            return False
        
        set_clause = ', '.join([f"{key} = ?" for key in data.keys()])
        values = tuple(data.values()) + where_params
        
        retry_count = 0
        last_error = None
        
        while retry_count < self.max_retries:
            try:
                with self.get_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute(
                        f"UPDATE {table} SET {set_clause} WHERE {where}",
                        values
                    )
                    conn.commit()
                    return True
            except sqlite3.Error as e:
                last_error = e
                logger.warning(f"Database update attempt {retry_count + 1} failed: {e}")
                
                # Only retry on certain errors
                if "database is locked" in str(e).lower() or "busy" in str(e).lower():
                    # Exponential backoff
                    retry_delay = self.retry_delay * (2 ** retry_count)
                    time.sleep(retry_delay)
                    retry_count += 1
                else:
                    # Don't retry on syntax errors, etc.
                    break
        
        # If we get here, all retries failed
        error_id = register_error(DatabaseError(f"Update {table} failed after {retry_count} attempts: {last_error}"))
        logger.error(f"Database update failed. Error ID: {error_id}")
        return False
    
    @handle_errors(default_return=False, handled_exceptions=sqlite3.Error)
    def delete(self, table: str, where: str, where_params: tuple) -> bool:
        """
        Delete data from a table with retry logic.
        
        Args:
            table: Table name
            where: WHERE clause
            where_params: Parameters for WHERE clause
            
        Returns:
            True if successful, False otherwise
        """
        retry_count = 0
        last_error = None
        
        while retry_count < self.max_retries:
            try:
                with self.get_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute(
                        f"DELETE FROM {table} WHERE {where}",
                        where_params
                    )
                    conn.commit()
                    return True
            except sqlite3.Error as e:
                last_error = e
                logger.warning(f"Database delete attempt {retry_count + 1} failed: {e}")
                
                # Only retry on certain errors
                if "database is locked" in str(e).lower() or "busy" in str(e).lower():
                    # Exponential backoff
                    retry_delay = self.retry_delay * (2 ** retry_count)
                    time.sleep(retry_delay)
                    retry_count += 1
                else:
                    # Don't retry on syntax errors, etc.
                    break
        
        # If we get here, all retries failed
        error_id = register_error(DatabaseError(f"Delete from {table} failed after {retry_count} attempts: {last_error}"))
        logger.error(f"Database delete failed. Error ID: {error_id}")
        return False
    
    @handle_errors(default_return=False, handled_exceptions=sqlite3.Error)
    def vacuum(self) -> bool:
        """
        Perform VACUUM operation to optimize the database.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            with self.get_connection() as conn:
                conn.execute("VACUUM")
                conn.commit()
                
                # Record vacuum time
                vacuum_time = time.strftime("%Y-%m-%d %H:%M:%S")
                self.update_db_status({"last_vacuum": vacuum_time})
                
                return True
        except sqlite3.Error as e:
            error_id = register_error(DatabaseError(f"VACUUM operation failed: {e}"))
            logger.error(f"Database optimization failed. Error ID: {error_id}")
            return False
    
    @handle_errors(default_return=False, handled_exceptions=sqlite3.Error)
    def update_db_status(self, status_data: Dict = None) -> bool:
        """
        Update database status for health monitoring.
        
        Args:
            status_data: Additional status data to record
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Gather basic stats
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Count tables
                cursor.execute("SELECT count(*) FROM sqlite_master WHERE type='table'")
                tables_count = cursor.fetchone()[0]
                
                # Get database size
                if os.path.exists(self.db_path):
                    size_bytes = os.path.getsize(self.db_path)
                else:
                    size_bytes = 0
                
                # Prepare status data
                status = {
                    "check_time": time.strftime("%Y-%m-%d %H:%M:%S"),
                    "status": "healthy",
                    "size_bytes": size_bytes,
                    "tables_count": tables_count,
                    "details": json.dumps({
                        "connections": len(self._connections),
                        "path": self.db_path
                    })
                }
                
                # Add custom status data if provided
                if status_data:
                    status.update(status_data)
                
                # Insert status record
                self.insert("db_status", status)
                
                return True
        except Exception as e:
            error_id = register_error(DatabaseError(f"Failed to update DB status: {e}"))
            logger.error(f"Database status update failed. Error ID: {error_id}")
            return False
    
    def log_error(self, error_id: str, error_data: Dict) -> bool:
        """
        Log an error to the database.
        
        Args:
            error_id: Unique error ID
            error_data: Error details
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Convert traceback to string if it's a list
            traceback = error_data.get("traceback", "")
            if isinstance(traceback, list):
                traceback = "".join(traceback)
            
            # Convert context to JSON if it's a dict
            context = error_data.get("context", {})
            if isinstance(context, dict):
                context = json.dumps(context)
            
            # Prepare error record
            error_record = {
                "error_id": error_id,
                "error_type": error_data.get("error_type", ""),
                "message": error_data.get("message", ""),
                "timestamp": error_data.get("timestamp", time.strftime("%Y-%m-%d %H:%M:%S")),
                "traceback": traceback,
                "context": context,
                "resolved": 0
            }
            
            # Insert error record
            return bool(self.insert("error_logs", error_record))
        except Exception as e:
            # Don't use error handler here to avoid infinite recursion
            logger.error(f"Failed to log error to database: {e}")
            return False
    
    def close(self):
        """Close all database connections."""
        # Close thread-specific connections
        for conn in self._connections.values():
            if conn:
                try:
                    conn.close()
                except sqlite3.Error:
                    pass
        
        # Clear connection cache
        self._connections.clear()
        
        # Close default connection
        if self._conn:
            try:
                self._conn.close()
            except sqlite3.Error:
                pass
            self._conn = None
        
        logger.info("All database connections closed")
    
    def __del__(self):
        """Ensure connections are closed when object is deleted."""
        self.close()