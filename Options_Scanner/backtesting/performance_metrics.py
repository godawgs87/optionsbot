"""
Performance metrics calculation for backtesting.
"""
import numpy as np
import pandas as pd
from typing import Dict, List, Any, Optional


def calculate_performance_metrics(
    initial_capital: float,
    final_capital: float,
    equity_curve: List[Dict[str, Any]],
    trades: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Calculate performance metrics for a backtest.
    
    Args:
        initial_capital: Starting capital
        final_capital: Ending capital
        equity_curve: List of daily equity values
        trades: List of completed trades
        
    Returns:
        Dictionary with performance metrics
    """
    # Basic return metrics
    total_return = (final_capital - initial_capital) / initial_capital
    total_return_pct = total_return * 100
    
    # Convert equity curve to DataFrame
    if equity_curve:
        df_equity = pd.DataFrame(equity_curve)
        df_equity['date'] = pd.to_datetime(df_equity['date'])
        df_equity.set_index('date', inplace=True)
        
        # Trading days per year (approx)
        trading_days_per_year = 252
        
        # Duration in years
        duration_days = (df_equity.index[-1] - df_equity.index[0]).days
        duration_years = duration_days / 365
        
        # Annualized return
        annualized_return = (1 + total_return) ** (1 / duration_years) - 1 if duration_years > 0 else 0
        annualized_return_pct = annualized_return * 100
        
        # Daily returns
        df_equity['daily_return'] = df_equity['total_equity'].pct_change()
        
        # Calculate Sharpe Ratio (assuming risk-free rate of 0% for simplicity)
        sharpe_ratio = 0
        if len(df_equity) > 1:
            daily_returns = df_equity['daily_return'].dropna()
            if len(daily_returns) > 0:
                sharpe_ratio = np.sqrt(trading_days_per_year) * (daily_returns.mean() / daily_returns.std())
        
        # Calculate Sortino Ratio (downside deviation)
        sortino_ratio = 0
        if len(df_equity) > 1:
            daily_returns = df_equity['daily_return'].dropna()
            if len(daily_returns) > 0:
                negative_returns = daily_returns[daily_returns < 0]
                downside_deviation = negative_returns.std() if len(negative_returns) > 0 else 0
                if downside_deviation > 0:
                    sortino_ratio = np.sqrt(trading_days_per_year) * (daily_returns.mean() / downside_deviation)
        
        # Maximum Drawdown
        df_equity['cummax'] = df_equity['total_equity'].cummax()
        df_equity['drawdown'] = (df_equity['total_equity'] - df_equity['cummax']) / df_equity['cummax']
        max_drawdown = df_equity['drawdown'].min() * 100  # Convert to percentage
        
        # Calculate drawdown duration
        is_in_drawdown = df_equity['total_equity'] < df_equity['cummax']
        drawdown_periods = []
        current_period = None
        
        for date, in_drawdown in zip(df_equity.index, is_in_drawdown):
            if in_drawdown and current_period is None:
                current_period = {'start': date, 'end': None}
            elif not in_drawdown and current_period is not None:
                current_period['end'] = date
                drawdown_periods.append(current_period)
                current_period = None
        
        # Handle if still in drawdown at end of period
        if current_period is not None:
            current_period['end'] = df_equity.index[-1]
            drawdown_periods.append(current_period)
        
        # Calculate max drawdown duration in days
        max_drawdown_duration = 0
        for period in drawdown_periods:
            duration = (period['end'] - period['start']).days
            max_drawdown_duration = max(max_drawdown_duration, duration)
    else:
        # Default values if equity curve is empty
        annualized_return_pct = 0
        sharpe_ratio = 0
        sortino_ratio = 0
        max_drawdown = 0
        max_drawdown_duration = 0
    
    # Trade metrics
    total_trades = len(trades)
    
    if total_trades > 0:
        # Winning/losing trades
        winning_trades = sum(1 for t in trades if t.get('profit_loss', 0) > 0)
        losing_trades = sum(1 for t in trades if t.get('profit_loss', 0) <= 0)
        
        # Win rate
        win_rate = (winning_trades / total_trades) * 100 if total_trades > 0 else 0
        
        # Average profit/loss
        avg_profit_pct = sum(t.get('profit_loss_pct', 0) for t in trades) / total_trades if total_trades > 0 else 0
        avg_profit_amount = sum(t.get('profit_loss', 0) for t in trades) / total_trades if total_trades > 0 else 0
        
        # Profit factor
        gross_profit = sum(t.get('profit_loss', 0) for t in trades if t.get('profit_loss', 0) > 0)
        gross_loss = abs(sum(t.get('profit_loss', 0) for t in trades if t.get('profit_loss', 0) <= 0))
        profit_factor = gross_profit / gross_loss if gross_loss > 0 else float('inf')
        
        # Maximum consecutive wins/losses
        results = [1 if t.get('profit_loss', 0) > 0 else 0 for t in trades]
        
        max_consecutive_wins = max_consecutive_count(results, 1)
        max_consecutive_losses = max_consecutive_count(results, 0)
        
        # Best and worst trades
        best_trade_pct = max(t.get('profit_loss_pct', 0) for t in trades) if trades else 0
        worst_trade_pct = min(t.get('profit_loss_pct', 0) for t in trades) if trades else 0
        
        # Average trade duration in days
        trade_durations = []
        for trade in trades:
            if trade.get('entry_date') and trade.get('exit_date'):
                entry_date = pd.to_datetime(trade['entry_date'])
                exit_date = pd.to_datetime(trade['exit_date'])
                duration_days = (exit_date - entry_date).days
                trade_durations.append(duration_days)
        
        avg_trade_duration = sum(trade_durations) / len(trade_durations) if trade_durations else 0
    else:
        # Default values if no trades
        winning_trades = 0
        losing_trades = 0
        win_rate = 0
        avg_profit_pct = 0
        avg_profit_amount = 0
        profit_factor = 0
        max_consecutive_wins = 0
        max_consecutive_losses = 0
        best_trade_pct = 0
        worst_trade_pct = 0
        avg_trade_duration = 0
    
    # Compile metrics
    return {
        # Overall performance
        "total_return_pct": total_return_pct,
        "annualized_return_pct": annualized_return_pct,
        "sharpe_ratio": sharpe_ratio,
        "sortino_ratio": sortino_ratio,
        "max_drawdown_pct": max_drawdown,
        "max_drawdown_duration": max_drawdown_duration,
        
        # Trade metrics
        "total_trades": total_trades,
        "winning_trades": winning_trades,
        "losing_trades": losing_trades,
        "win_rate": win_rate,
        "avg_profit_pct": avg_profit_pct,
        "avg_profit_amount": avg_profit_amount,
        "profit_factor": profit_factor,
        "max_consecutive_wins": max_consecutive_wins,
        "max_consecutive_losses": max_consecutive_losses,
        "max_profit_pct": best_trade_pct,
        "max_loss_pct": worst_trade_pct,
        "avg_trade_duration": avg_trade_duration
    }


def max_consecutive_count(results: List[int], value: int) -> int:
    """
    Calculate maximum consecutive occurrences of a value in a list.
    
    Args:
        results: List of binary values (0 or 1)
        value: Value to count consecutive occurrences of
        
    Returns:
        Maximum number of consecutive occurrences
    """
    max_count = 0
    current_count = 0
    
    for result in results:
        if result == value:
            current_count += 1
            max_count = max(max_count, current_count)
        else:
            current_count = 0
    
    return max_count