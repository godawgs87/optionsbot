"""
AI-powered trade analysis module.
"""
import logging
import numpy as np
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import pandas as pd
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
import os
from joblib import load, dump

logger = logging.getLogger(__name__)

class TradeAnalyzer:
    """AI-powered analyzer for determining trade success probability."""
    
    def __init__(self, db_path: str = "performance.db"):
        """
        Initialize the trade analyzer.
        
        Args:
            db_path: Path to SQLite database file
        """
        self.db_path = db_path
        self.model = None
        self.scaler = StandardScaler()
        self.feature_columns = [
            'volume', 'open_interest', 'iv', 'delta', 'gamma', 
            'theta', 'vega', 'time_to_expiry', 'atm_ratio',
            'relative_volume', 'rsi', 'macd', 'bollinger_band_position'  # Added technical indicators
        ]
        self.trained = False
        self.model_path = "models/trade_analyzer.joblib"
        self.scaler_path = "models/scaler.joblib"
        self._load_model()  # Try to load existing model
    
    def _load_model(self) -> None:
        """Load trained model and scaler if they exist."""
        try:
            if os.path.exists(self.model_path) and os.path.exists(self.scaler_path):
                self.model = load(self.model_path)
                self.scaler = load(self.scaler_path)
                self.trained = True
                logger.info("Loaded existing model and scaler")
        except Exception as e:
            logger.warning(f"Could not load existing model: {e}")
    
    def _save_model(self) -> None:
        """Save trained model and scaler."""
        try:
            os.makedirs(os.path.dirname(self.model_path), exist_ok=True)
            dump(self.model, self.model_path)
            dump(self.scaler, self.scaler_path)
            logger.info("Saved model and scaler")
        except Exception as e:
            logger.error(f"Could not save model: {e}")
    
    def train_model(self) -> bool:
        """
        Train the AI model based on historical data.
        
        Returns:
            True if training was successful, False otherwise
        """
        try:
            # Load historical data from database
            historical_data = self._load_historical_data()
            
            if len(historical_data) < 100:
                logger.warning("Not enough historical data to train model effectively")
                return False
            
            # Prepare features and target
            features = historical_data[self.feature_columns]
            target = (historical_data['profit_pct'] > 0).astype(int)  # 1 if profitable, 0 if not
            
            # Split data
            X_train, X_test, y_train, y_test = train_test_split(
                features, target, test_size=0.2, random_state=42
            )
            
            # Scale features
            X_train_scaled = self.scaler.fit_transform(X_train)
            X_test_scaled = self.scaler.transform(X_test)
            
            # Train model (Gradient Boosting)
            self.model = GradientBoostingClassifier(
                n_estimators=100, 
                learning_rate=0.1,
                max_depth=5,
                random_state=42
            )
            self.model.fit(X_train_scaled, y_train)
            
            # Evaluate model
            y_pred = self.model.predict(X_test_scaled)
            accuracy = accuracy_score(y_test, y_pred)
            logger.info(f"Model trained with accuracy: {accuracy:.2f}")
            
            self.trained = True
            
            # Save model after successful training
            self._save_model()
            return True
        except Exception as e:
            logger.error(f"Error training model: {str(e)}", exc_info=True)
            return False
    
    def analyze_trade(self, trade_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Analyze a potential trade and return success probability score.
        
        Args:
            trade_data: Dictionary containing trade details
            
        Returns:
            Dictionary with analysis results including success probability
        """
        try:
            if not self.trained or not self.model:
                # If model not trained, use rule-based scoring
                return self._rule_based_scoring(trade_data)
            
            # Prepare features
            features = self._prepare_features(trade_data)
            
            if features is None:
                return {
                    "success_probability": 50.0,
                    "confidence": "low",
                    "reasoning": "Insufficient data to make a prediction"
                }
            
            # Scale features
            features_scaled = self.scaler.transform(features.reshape(1, -1))
            
            # Get probability of success
            probability = self.model.predict_proba(features_scaled)[0][1] * 100
            
            # Get feature importances for this prediction
            importance_dict = self._get_importance_explanation(trade_data, features)
            
            result = {
                "success_probability": round(probability, 2),
                "confidence": self._determine_confidence(probability),
                "key_factors": importance_dict,
                "reasoning": self._generate_reasoning(trade_data, probability, importance_dict)
            }
            
            return result
        except Exception as e:
            logger.error(f"Error analyzing trade: {e}")
            return {
                "success_probability": 50.0,
                "confidence": "very low",
                "reasoning": f"Error in analysis: {str(e)}"
            }
    
    def _load_historical_data(self) -> pd.DataFrame:
        """
        Load historical trade data from database.
        
        Returns:
            DataFrame containing historical trades
        """
        import sqlite3
        
        conn = sqlite3.connect(self.db_path)
        
        # Get closed opportunities
        query = """
        SELECT o.*, 
               ((o.close_price - o.entry_price) / o.entry_price) * 100 as profit_pct,
               julianday(o.expiration) - julianday(o.entry_time) as time_to_expiry,
                o.strike / (SELECT price FROM price_updates 
                WHERE opportunity_id = o.id 
                ORDER BY timestamp ASC LIMIT 1) as atm_ratio

        FROM opportunities o
        WHERE o.closed = 1
        """
        
        df = pd.read_sql_query(query, conn)
        
        # Add relative volume (compared to open interest)
        df['relative_volume'] = df['volume'] / df['open_interest'].where(df['open_interest'] > 0, 1)
        
        conn.close()
        return df
    
    def _prepare_features(self, trade_data: Dict[str, Any]) -> Optional[np.ndarray]:
        """
        Prepare feature array from trade data with validation.
        
        Args:
            trade_data: Dictionary containing trade details
            
        Returns:
            NumPy array of features or None if insufficient data
        
        Raises:
            ValueError: If critical data is missing
        """
        # Validate required fields
        required_fields = ['volume', 'open_interest', 'iv', 'delta']
        missing_fields = [field for field in required_fields if field not in trade_data]
        
        if missing_fields:
            raise ValueError(f"Missing required fields: {', '.join(missing_fields)}")
        
        # Calculate time to expiry with validation
        if 'expiration' in trade_data:
            try:
                expiry_date = datetime.strptime(trade_data['expiration'], "%Y-%m-%d")
                current_date = datetime.now()
                time_to_expiry = (expiry_date - current_date).days
                if time_to_expiry < 0:
                    raise ValueError("Expiration date is in the past")
            except ValueError as e:
                logger.error(f"Invalid expiration date: {e}")
                return None
        else:
            return None  # Don't use default value for critical fields
        
        # Calculate technical indicators
        rsi = self._calculate_rsi(trade_data)
        macd = self._calculate_macd(trade_data)
        bb_position = self._calculate_bollinger_position(trade_data)
        
        features = np.array([
            trade_data['volume'],
            trade_data['open_interest'],
            trade_data['iv'],
            trade_data['delta'],
            trade_data['gamma'],
            trade_data['theta'],
            trade_data['vega'],
            time_to_expiry,
            trade_data.get('strike', 0) / trade_data.get('underlying_price', 1),
            trade_data['volume'] / trade_data['open_interest'],
            rsi,
            macd,
            bb_position
        ])
        
        return features
    
    def _rule_based_scoring(self, trade_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Use rule-based scoring when ML model is not available.
        
        Args:
            trade_data: Dictionary containing trade details
            
        Returns:
            Dictionary with analysis results
        """
        score = 50.0  # Start with neutral score
        reasons = []
        
        # Volume factors
        volume = trade_data.get('volume', 0)
        open_interest = trade_data.get('open_interest', 0)
        
        if volume > 1000:
            score += 5
            reasons.append("High volume")
        
        if open_interest > 0 and volume / open_interest > 0.5:
            score += 10
            reasons.append("Volume/OI ratio > 0.5")
        
        # Option greeks
        iv = trade_data.get('iv', 0)
        if iv > 0.5:
            score += 5
            reasons.append("High implied volatility")
        
        # Strike vs underlying
        strike = trade_data.get('strike', 0)
        underlying_price = trade_data.get('underlying_price', 0)
        
        if underlying_price > 0:
            # For calls
            if trade_data.get('option_type') == 'call' and strike < underlying_price * 1.05:
                score += 5
                reasons.append("Call strike near or in-the-money")
            
            # For puts
            if trade_data.get('option_type') == 'put' and strike > underlying_price * 0.95:
                score += 5
                reasons.append("Put strike near or in-the-money")
        
        # Notional value (large trades)
        notional_value = trade_data.get('notional_value', 0)
        if notional_value > 1000000:
            score += 10
            reasons.append("Large notional value (whale activity)")
        
        # Cap score at 100
        score = min(score, 100)
        
        return {
            "success_probability": round(score, 2),
            "confidence": "medium",
            "reasoning": f"Based on rule scoring: {', '.join(reasons)}"
        }
    
    def _determine_confidence(self, probability: float) -> str:
        """
        Determine confidence level based on probability.
        
        Args:
            probability: Success probability (0-100)
            
        Returns:
            Confidence level string
        """
        if probability > 85 or probability < 15:
            return "very high"
        elif probability > 75 or probability < 25:
            return "high"
        elif probability > 65 or probability < 35:
            return "medium"
        else:
            return "low"
    
    def _get_importance_explanation(self, trade_data: Dict[str, Any], features: np.ndarray) -> Dict[str, float]:
        """
        Get feature importance explanation.
        
        Args:
            trade_data: Dictionary containing trade details
            features: Feature array
            
        Returns:
            Dictionary mapping factors to their importance
        """
        if not self.trained or not self.model:
            return {}
        
        # Get base feature importances from model
        importances = self.model.feature_importances_
        
        # Create dictionary of factors and their importance
        factor_importance = {}
        for i, feature_name in enumerate(self.feature_columns):
            if importances[i] >= 0.05:  # Only include significant factors
                if feature_name == 'atm_ratio':
                    display_name = 'Strike Price Positioning'
                elif feature_name == 'time_to_expiry':
                    display_name = 'Time to Expiration'
                elif feature_name == 'relative_volume':
                    display_name = 'Volume/OI Ratio'
                else:
                    display_name = feature_name.replace('_', ' ').title()
                
                factor_importance[display_name] = round(importances[i] * 100, 2)
        
        return factor_importance
    
    def _generate_reasoning(self, trade_data: Dict[str, Any], probability: float, 
                            importance_dict: Dict[str, float]) -> str:
        """
        Generate human-readable reasoning for the prediction.
        
        Args:
            trade_data: Dictionary containing trade details
            probability: Success probability
            importance_dict: Dictionary of important factors
            
        Returns:
            String explaining the reasoning
        """
        option_type = trade_data.get('option_type', 'option').upper()
        symbol = trade_data.get('symbol', 'stock')
        
        if probability >= 70:
            sentiment = "favorable"
        elif probability <= 30:
            sentiment = "unfavorable"
        else:
            sentiment = "neutral"
        
        reasoning = f"Analysis indicates a {sentiment} outlook for this {symbol} {option_type} trade with a {probability:.1f}% probability of success. "
        
        if importance_dict:
            reasoning += "Key factors influencing this prediction include: "
            factor_texts = [f"{factor} ({importance}%)" for factor, importance in importance_dict.items()]
            reasoning += ", ".join(factor_texts) + "."
        
        return reasoning