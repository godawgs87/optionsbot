"""
Configuration settings for the options trading system.
"""
import os
from pathlib import Path

# Base paths
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
REPORTS_DIR = BASE_DIR / "reports"

# Create directories if they don't exist
DATA_DIR.mkdir(exist_ok=True)
REPORTS_DIR.mkdir(exist_ok=True)

# Database settings
DB_PATH = BASE_DIR / "db" / "options_trading.db"

# ThetaData API credentials
THETADATA_API_KEY = os.environ.get("THETADATA_API_KEY", "")
THETADATA_USERNAME = os.environ.get("THETADATA_USERNAME", "")

# Telegram bot settings
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

# Scanner settings
WATCHLIST = [
    "SPY", "QQQ", "AAPL", "MSFT", "AMZN", "GOOGL", "TSLA", "META", "NVDA", "AMD",
    "KR", "WMT", "LCID", "RIVN", "GME", "AMC", "PLTR", "NIO"
]

# Day trading scanner settings
DAY_TRADING_SETTINGS = {
    "min_volume": 100,
    "min_open_interest": 500, 
    "min_iv_percentile": 70,
    "scan_interval_seconds": 60,
    "profit_targets": [5, 10, 15, 20, 30],  # Percentage targets
    "stop_loss": -15  # Percentage stop loss
}

# Whale activity settings
WHALE_SCANNER_SETTINGS = {
    "min_notional_value": 1000000,  # $1M minimum
    "unusual_volume_multiplier": 3,  # 3x normal volume
    "min_trade_size": 100,  # Minimum contracts
    "scan_interval_seconds": 300
}

# Performance tracking settings
PERFORMANCE_WINDOWS = ["1m", "5m", "10m", "15m", "20m", "30m", "1h"]

# AI Analysis settings
AI_ANALYSIS_SETTINGS = {
    "enabled": True,
    "min_confidence_threshold": 60,  # Minimum confidence for alerts
    "model_update_frequency_hours": 24,  # How often to retrain the model
    "backtesting_lookback_days": 30,  # Days of historical data for backtesting
    "feature_importance_threshold": 0.05,  # Minimum threshold for feature importance
    "prediction_score_range": {
        "very_high": (85, 100),  # Range for very high confidence
        "high": (70, 85),        # Range for high confidence
        "medium": (55, 70),      # Range for medium confidence
        "low": (40, 55),         # Range for low confidence
        "very_low": (0, 40)      # Range for very low confidence
    }
}