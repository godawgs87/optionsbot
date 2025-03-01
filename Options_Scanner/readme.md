options_trading_system/
├── config.py                      # Configuration settings
├── db/
│   ├── __init__.py
│   ├── database.py                # Database connection and setup
│   └── models.py                  # Database models
├── api/
│   ├── __init__.py
│   ├── thetadata_client.py        # ThetaData API client
│   └── market_data.py             # Market data fetching utilities
├── scanners/
│   ├── __init__.py
│   ├── base_scanner.py            # Base scanner class
│   ├── day_trading_scanner.py     # Day trading opportunities scanner
│   ├── whale_activity_scanner.py  # Unusual/whale activity scanner
│   └── custom_scanner.py          # Template for custom scanners
├── strategies/
│   ├── __init__.py
│   ├── strategy_base.py           # Base strategy class
│   ├── momentum_strategy.py       # Momentum-based strategies
│   └── volatility_strategy.py     # Volatility-based strategies
├── backtesting/
│   ├── __init__.py
│   ├── backtest_engine.py         # Backtesting engine
│   └── performance_metrics.py     # Performance calculation utilities
├── reporting/
│   ├── __init__.py
│   ├── performance_tracker.py     # Track and store performance
│   ├── leaderboard.py             # Generate performance leaderboards
│   └── visualizations.py          # Charts and visualizations
├── notifications/
│   ├── __init__.py
│   ├── telegram_bot.py            # Telegram notification bot
│   └── email_alerts.py            # Email notification system
├── utils/
│   ├── __init__.py
│   ├── logging_utils.py           # Logging configuration
│   └── helpers.py                 # Helper functions
└── main.py                        # Main entry point