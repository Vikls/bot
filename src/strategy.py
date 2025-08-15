# -*- coding: utf-8 -*-
"""
Торгова стратегія на основі PineScript алгоритму
"""

import logging
import pandas as pd
import numpy as np
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple, Any
from config.settings import (
    STRATEGY_CONFIG, TRADING_CONFIG, INDICATORS_CONFIG,
    DISPLAY_CONFIG, PRECISION_CONFIG
)

logger = logging.getLogger(__name__)

class LightningVolumeStrategy:
    """Стратегія Lightning Volume на основі PineScript"""
    
    def __init__(self):
        self.strategy_config = STRATEGY_CONFIG
        self.trading_config = TRADING_CONFIG
        self.indicators_config = INDICATORS_CONFIG
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}") # Додано логер для класу
        
        # Стан стратегії
        self.consecutive_losses = 0
        self.current_risk_multiplier = 1.0
        
        # Позиції та TP стани (цей словник тут не використовується для зберігання активних позицій,
        # він залишений для можливого майбутнього розширення, якщо стратегія буде вести власний стан)
        self.positions = {} 
        
        self.logger.info("Lightning Volume Strategy ініціалізовано")
    
    def _check_time_filter(self) -> bool:
        """Перевірка часового фільтру"""
        if not self.strategy_config.get('use_time_filter', False):
            return True
        
        try:
            current_hour = datetime.now(timezone.utc).hour
            
            if self.strategy_config.get('avoid_early_hours', True):
                early_start = self.strategy_config.get('early_session_start', 0)
                early_end = self.strategy_config.get('early_session_end', 2)
                if early_start <= current_hour <= early_end:
                    self.logger.debug(f"Відфільтровано через ранній сеанс: {current_hour}:00 UTC")
                    return False
            
            if self.strategy_config.get('avoid_late_hours', True):
                late_start = self.strategy_config.get('late_session_start', 21)
                late_end = self.strategy_config.get('late_session_end', 23)
                if late_start <= current_hour <= late_end:
                    self.logger.debug(f"Відфільтровано через пізній сеанс: {current_hour}:00 UTC")
                    return False
            
            if self.strategy_config.get('avoid_lunch_time', True):
                lunch_start = self.strategy_config.get('lunch_time_start', 12)
                lunch_end = self.strategy_config.get('lunch_time_end', 14)
                if lunch_start <= current_hour <= lunch_end:
                    self.logger.debug(f"Відфільтровано через обідню перерву: {current_hour}:00 UTC")
                    return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Помилка перевірки часового фільтру: {e}")
            return True
    
    def _calculate_market_regime(self, df: pd.DataFrame) -> Dict[str, Any]:
     """Розрахунок ринкового режиму"""
     try:
         if df.empty or len(df) < self.strategy_config.get('regime_period', 20):
             return {'is_trending': False, 'is_ranging': False, 'regime_score': 0, 'status_text': "N/A - No Data"} # Додано status_text
         
         regime_period = self.strategy_config.get('regime_period', 20)
         trend_strength = self.strategy_config.get('trend_strength', 1.5)
         volatility_threshold = self.strategy_config.get('volatility_threshold', 0.02)
         momentum_period = self.strategy_config.get('momentum_period', 10)
         
         recent_high = df['high_price'].tail(regime_period).max()
         recent_low = df['low_price'].tail(regime_period).min()
         price_range = recent_high - recent_low
         
         atr_values = df['atr'].tail(regime_period)
         # Перевіряємо, чи atr_values не порожній і не містить тільки NaN перед викликом mean()
         atr_range = atr_values.mean() * regime_period if not atr_values.empty and not atr_values.isna().all() else price_range
         
         close_prices_regime = df['close_price'].tail(regime_period)
         volatility_regime = close_prices_regime.std() / close_prices_regime.mean() if not close_prices_regime.empty and len(close_prices_regime) > 1 and close_prices_regime.mean() != 0 else 0
         is_high_volatility = volatility_regime > volatility_threshold
         
         is_strong_momentum = False
         if len(df) >= momentum_period + regime_period: 
             momentum_series = df['close_price'].diff(momentum_period).tail(regime_period)
             if not momentum_series.empty and not momentum_series.isna().all():
                 momentum = momentum_series.iloc[-1] 
                 avg_momentum = abs(momentum_series).mean()
                 is_strong_momentum = abs(momentum) > avg_momentum if not pd.isna(momentum) and not pd.isna(avg_momentum) and avg_momentum != 0 else False # Додано перевірку avg_momentum != 0
         
         is_ema_expanding = False
         if ('ema_fast' in df.columns and 'ema_slow' in df.columns and 
             not df[['ema_fast', 'ema_slow', 'close_price']].tail(regime_period).isna().any().any() and
             not df['ema_fast'].tail(regime_period).empty and 
             not df['ema_slow'].tail(regime_period).empty and
             not df['close_price'].tail(regime_period).empty):

             ema_fast_series = df['ema_fast'].tail(regime_period)
             ema_slow_series = df['ema_slow'].tail(regime_period)
             close_price_series_regime = df['close_price'].tail(regime_period)

             if not ema_fast_series.empty and not ema_slow_series.empty and not close_price_series_regime.empty and close_price_series_regime.iloc[-1] != 0:
                 ema_separation = abs(ema_fast_series.iloc[-1] - ema_slow_series.iloc[-1]) / close_price_series_regime.iloc[-1]
                 avg_separation_numerator = abs(ema_fast_series - ema_slow_series).mean()
                 avg_separation_denominator = close_price_series_regime.mean()
                 avg_separation = avg_separation_numerator / avg_separation_denominator if avg_separation_denominator != 0 else 0
                 is_ema_expanding = ema_separation > avg_separation if not pd.isna(ema_separation) and not pd.isna(avg_separation) else False
         
         trending_score = 0
         trending_score += 1 if price_range > (atr_range * trend_strength) and atr_range > 0 else 0
         trending_score += 1 if is_high_volatility else 0
         trending_score += 1 if is_strong_momentum else 0
         trending_score += 1 if is_ema_expanding else 0
         
         is_trending_market = trending_score >= 3
         is_ranging_market = trending_score <= 1
         
         status_text = "N/A"
         if is_trending_market:
             status_text = "Trending"
         elif is_ranging_market:
             status_text = "Ranging"
         else:
             status_text = "Mixed"
         
         return {
             'is_trending': is_trending_market,
             'is_ranging': is_ranging_market,
             'regime_score': trending_score,
             'volatility_regime': volatility_regime,
             'price_range': price_range,
             'atr_range': atr_range,
             'status_text': status_text # Додано текстовий статус
         }
         
     except Exception as e:
         self.logger.error(f"Помилка розрахунку ринкового режиму: {e}", exc_info=True)
         return {'is_trending': False, 'is_ranging': False, 'regime_score': 0, 'status_text': "Error"} # Додано status_text
    
    def _get_adaptive_parameters(self, market_regime: Dict[str, Any]) -> Dict[str, Any]:
        """Отримання адаптивних параметрів залежно від ринкового режиму"""
        try:
            if not self.strategy_config.get('use_adaptive_params', True):
                # Повертаємо параметри для 'mixed' як дефолтні, якщо адаптивність вимкнена
                return {
                    'min_conf_long': self.strategy_config.get('mixed_min_conf_long', 2),
                    'min_conf_short': self.strategy_config.get('mixed_min_conf_short', 2),
                    'adx_thresh': self.strategy_config.get('mixed_adx_thresh', 20),
                    'tp_mult': self.strategy_config.get('mixed_tp_mult', 2.2),
                    'sl_mult': self.strategy_config.get('mixed_sl_mult', 1.0)
                }
            
            if market_regime.get('is_trending', False):
                return {
                    'min_conf_long': self.strategy_config.get('trending_min_conf_long', 2),
                    'min_conf_short': self.strategy_config.get('trending_min_conf_short', 2),
                    'adx_thresh': self.strategy_config.get('trending_adx_thresh', 20),  # Reduced from 25 to 20
                    'tp_mult': self.strategy_config.get('trending_tp_mult', 2.8),
                    'sl_mult': self.strategy_config.get('trending_sl_mult', 1.0)
                }
            elif market_regime.get('is_ranging', False):
                return {
                    'min_conf_long': self.strategy_config.get('ranging_min_conf_long', 2),  # Reduced from 3 to 2
                    'min_conf_short': self.strategy_config.get('ranging_min_conf_short', 2),  # Reduced from 3 to 2
                    'adx_thresh': self.strategy_config.get('ranging_adx_thresh', 15),
                    'tp_mult': self.strategy_config.get('ranging_tp_mult', 1.5),
                    'sl_mult': self.strategy_config.get('ranging_sl_mult', 0.8)
                }
            else:  # Mixed market
                return {
                    'min_conf_long': self.strategy_config.get('mixed_min_conf_long', 2),
                    'min_conf_short': self.strategy_config.get('mixed_min_conf_short', 2),
                    'adx_thresh': self.strategy_config.get('mixed_adx_thresh', 18),  # Reduced from 20 to 18
                    'tp_mult': self.strategy_config.get('mixed_tp_mult', 2.2),
                    'sl_mult': self.strategy_config.get('mixed_sl_mult', 1.0)
                }
                
        except Exception as e:
            self.logger.error(f"Помилка отримання адаптивних параметрів: {e}", exc_info=True)
            return { # Fallback to mixed/default
                'min_conf_long': self.strategy_config.get('mixed_min_conf_long', 2),
                'min_conf_short': self.strategy_config.get('mixed_min_conf_short', 2),
                'adx_thresh': self.strategy_config.get('mixed_adx_thresh', 18),  # Reduced from 20 to 18
                'tp_mult': self.strategy_config.get('mixed_tp_mult', 2.2),
                'sl_mult': self.strategy_config.get('mixed_sl_mult', 1.0)
            }
    
    def _calculate_lightning_volume_boost(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Розрахунок Lightning Volume boost"""
        try:
            if not self.strategy_config.get('use_lightning_volume', True) or df.empty:
                return {
                    'tp_multiplier_boost': 1.0, # Renamed from tp_multiplier to avoid clash
                    'volume_surge_active': False,
                    'super_volume_surge_active': False,
                    'final_tp_extension': 1.0
                }
            
            latest_row = df.iloc[-1]
            
            volume_surge = latest_row.get('volume_surge', 0) == 1
            super_volume_surge = latest_row.get('super_volume_surge', 0) == 1
            sustained_volume = latest_row.get('sustained_volume', 0) == 1
            
            volume_trend_val = latest_row.get('volume_trend', 1.0) # Renamed to avoid conflict
            strong_volume_trend = volume_trend_val > 1.2 if not pd.isna(volume_trend_val) else False
            
            tp_multiplier_boost = 1.0
            volume_surge_active = False
            super_volume_surge_active = False
            
            if super_volume_surge:
                super_volume_boost = self.strategy_config.get('super_volume_tp_boost', 35.0)
                tp_multiplier_boost = 1 + (super_volume_boost / DISPLAY_CONFIG['percentage_multiplier'])
                super_volume_surge_active = True
                volume_surge_active = True # Super surge implies normal surge criteria met
                # self.logger.info("Super Volume Surge активовано!") # Logged in main bot logic
                
            elif volume_surge or sustained_volume: # PineScript logic: (volumeSurge or sustainedVolume)
                vol_surge_boost = self.strategy_config.get('vol_surge_tp_boost', 15.0)
                tp_multiplier_boost = 1 + (vol_surge_boost / DISPLAY_CONFIG['percentage_multiplier'])
                volume_surge_active = True
                # self.logger.info("Volume Surge активовано!") # Logged in main bot logic
            
            final_tp_extension = 1.0
            if (self.strategy_config.get('use_volume_extension', True) and 
                sustained_volume and strong_volume_trend): # PineScript: sustainedVolume and strongVolumeTrend
                final_tp_extension = self.strategy_config.get('volume_extension_mult', 1.3)
                # self.logger.info("Volume TP Extension активовано!") # Logged in main bot logic
            
            return {
                'tp_multiplier_boost': tp_multiplier_boost,
                'volume_surge_active': volume_surge_active,
                'super_volume_surge_active': super_volume_surge_active,
                'final_tp_extension': final_tp_extension
            }
            
        except Exception as e:
            self.logger.error(f"Помилка розрахунку Lightning Volume boost: {e}", exc_info=True)
            return {
                'tp_multiplier_boost': 1.0,
                'volume_surge_active': False,
                'super_volume_surge_active': False,
                'final_tp_extension': 1.0
            }
    
    def _optimize_volume_divergence_for_scalping(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Оптимізований volume divergence для скальпінгу"""
        try:
            latest_row = df.iloc[-1]
            
            # Швидший volume divergence (менший період)
            period = 3  # замість 5
            
            if len(df) < period + 1:
                return {'exit_signal': False, 'strength': 0}
            
            price_change = df['close_price'].iloc[-1] - df['close_price'].iloc[-period]
            volume_change = df['volume'].iloc[-1] - df['volume'].iloc[-period]
            volume_avg = df['volume'].tail(period).mean()
            
            # Менші пороги для скальпінгу
            bullish_div = (price_change < 0 and volume_change > 0 and 
                        df['volume'].iloc[-1] > volume_avg * 1.2)  # було 1.5
            bearish_div = (price_change > 0 and volume_change < 0 and
                        df['volume'].iloc[-1] < volume_avg * 0.8)   # було 0.7
            
            return {
                'bullish_divergence': bullish_div,
                'bearish_divergence': bearish_div,
                'exit_signal': bullish_div or bearish_div,
                'strength': abs(volume_change) / volume_avg if volume_avg > 0 else 0
            }
        except Exception as e:
            self.logger.error(f"Помилка volume divergence: {e}")
            return {'exit_signal': False, 'strength': 0}    

    def _calculate_confirmations(self, df: pd.DataFrame, volume_boost: Dict[str, Any]) -> Tuple[int, int]:
        """Розрахунок підтверджень для Long та Short сигналів"""
        try:
            if df.empty:
                return 0, 0
            
            latest_row = df.iloc[-1]
            
            ema_bullish = latest_row.get('ema_bullish', 0) == 1
            ema_bearish = latest_row.get('ema_bearish', 0) == 1
            
            macd_bullish = latest_row.get('macd_bullish', 0) == 1
            macd_bearish = latest_row.get('macd_bearish', 0) == 1
            
            rsi = latest_row.get('rsi', 50)
            rsi_rising_1 = latest_row.get('rsi_rising_1', 0) == 1
            rsi_rising_2 = latest_row.get('rsi_rising_2', 0) == 1
            rsi_falling_1 = latest_row.get('rsi_falling_1', 0) == 1
            rsi_falling_2 = latest_row.get('rsi_falling_2', 0) == 1
            
            rsi_overbought = self.indicators_config.get('rsi_overbought', 70)
            rsi_oversold = self.indicators_config.get('rsi_oversold', 30)
            
            # Відповідає PineScript: (rsi < rsiOversold and isRSIRising1) or (rsi < 45 and isRSIRising2)
            rsi_bullish_signal = ((rsi < rsi_oversold and rsi_rising_1) or 
                                 (rsi < 45 and rsi_rising_2))
            # Відповідає PineScript: (rsi > rsiOverbought and isRSIFalling1) or (rsi > 65 and isRSIFalling2)
            rsi_bearish_signal = ((rsi > rsi_overbought and rsi_falling_1) or 
                                 (rsi > 65 and rsi_falling_2)) # PineScript має 65, не 55. Уточнено.
            
            volume_filter_active = latest_row.get('volume_filter', 0) == 1 # Renamed for clarity
            
            long_confirmations = 0
            long_confirmations += 1 if ema_bullish else 0
            long_confirmations += 1 if macd_bullish else 0
            long_confirmations += 1 if rsi_bullish_signal else 0
            long_confirmations += 1 if volume_filter_active else 0
            
            short_confirmations = 0
            short_confirmations += 1 if ema_bearish else 0
            short_confirmations += 1 if macd_bearish else 0
            short_confirmations += 1 if rsi_bearish_signal else 0
            short_confirmations += 1 if volume_filter_active else 0
            
            if self.strategy_config.get('use_lightning_volume', True):
                # PineScript: if volumeSurge or superVolumeSurge
                # volume_boost['volume_surge_active'] is true if either normal or super surge
                if volume_boost.get('volume_surge_active', False): 
                    bullish_trend_active = latest_row.get('bullish_trend', 0) == 1 # Renamed
                    bearish_trend_active = latest_row.get('bearish_trend', 0) == 1 # Renamed
                    
                    if bullish_trend_active:
                        long_confirmations += 1
                    if bearish_trend_active:
                        short_confirmations += 1
            
            return long_confirmations, short_confirmations
            
        except Exception as e:
            self.logger.error(f"Помилка розрахунку підтверджень: {e}", exc_info=True)
            return 0, 0
    
    def _check_tiered_entry_logic(self, long_confirmations: int, short_confirmations: int, 
                                 bullish_trend_check: bool, bearish_trend_check: bool, 
                                 adx_value: float, adaptive_params: Dict[str, Any]) -> Tuple[bool, bool, float]:
        """
        Оптимізована тірувана логіка входу на основі ADX та підтверджень
        
        Нові вимоги:
        - ADX > 45: 1 підтвердження (дуже висока впевненість)
        - ADX > 30: 1 підтвердження (висока впевненість) 
        - ADX > 20: 2 підтвердження (середня впевненість)
        - ADX > 15: 2 підтвердження (низька впевненість)
        
        Returns:
            Tuple[bool, bool, float]: (base_buy_signal, base_sell_signal, effective_adx_threshold)
        """
        try:
            # ✅ ДУЖЕ ВИСОКА ВПЕВНЕНІСТЬ: ADX > 45 + 1 підтвердження
            if adx_value > 45 and long_confirmations >= 1 and bullish_trend_check:
                self.logger.info(f"Very high confidence BUY: ADX {adx_value:.1f} > 45, confirmations {long_confirmations} >= 1")
                return True, False, 45
                
            if adx_value > 45 and short_confirmations >= 1 and bearish_trend_check:
                self.logger.info(f"Very high confidence SELL: ADX {adx_value:.1f} > 45, confirmations {short_confirmations} >= 1")
                return False, True, 45

            # ✅ ВИСОКА ВПЕВНЕНІСТЬ: ADX > 30 + 1 підтвердження
            if adx_value > 30 and long_confirmations >= 1 and bullish_trend_check:
                self.logger.info(f"High confidence BUY: ADX {adx_value:.1f} > 30, confirmations {long_confirmations} >= 1")
                return True, False, 30
                
            if adx_value > 30 and short_confirmations >= 1 and bearish_trend_check:
                self.logger.info(f"High confidence SELL: ADX {adx_value:.1f} > 30, confirmations {short_confirmations} >= 1")
                return False, True, 30
            
            # ✅ СЕРЕДНЯ ВПЕВНЕНІСТЬ: ADX > 20 + 2 підтвердження
            if adx_value > 20 and long_confirmations >= 2 and bullish_trend_check:
                self.logger.info(f"Medium confidence BUY: ADX {adx_value:.1f} > 20, confirmations {long_confirmations} >= 2")
                return True, False, 20
                
            if adx_value > 20 and short_confirmations >= 2 and bearish_trend_check:
                self.logger.info(f"Medium confidence SELL: ADX {adx_value:.1f} > 20, confirmations {short_confirmations} >= 2")
                return False, True, 20
            
            # ✅ НИЗЬКА ВПЕВНЕНІСТЬ: ADX > 15 + 2 підтвердження
            if adx_value > 15 and long_confirmations >= 2 and bullish_trend_check:
                self.logger.info(f"Low confidence BUY: ADX {adx_value:.1f} > 15, confirmations {long_confirmations} >= 2")
                return True, False, 15
                
            if adx_value > 15 and short_confirmations >= 2 and bearish_trend_check:
                self.logger.info(f"Low confidence SELL: ADX {adx_value:.1f} > 15, confirmations {short_confirmations} >= 2")
                return False, True, 15
            
            # Жодний сигнал не пройшов
            return False, False, adaptive_params.get('adx_thresh', 20)
            
        except Exception as e:
            self.logger.error(f"Помилка в тірованій логіці входу: {e}", exc_info=True)
            # Fallback до оригінальної логіки
            base_buy_signal = (long_confirmations >= adaptive_params['min_conf_long'] and 
                              bullish_trend_check and 
                              adx_value > adaptive_params['adx_thresh'])
            
            base_sell_signal = (short_confirmations >= adaptive_params['min_conf_short'] and 
                               bearish_trend_check and 
                               adx_value > adaptive_params['adx_thresh'])
            
            return base_buy_signal, base_sell_signal, adaptive_params.get('adx_thresh', 20)
    
    def _calculate_signal_strength(self, signal_data: Dict[str, Any], df: pd.DataFrame) -> float:
        """
        Розраховує силу торгового сигналу для пріоритизації.
        Повертає оцінку від 0.0 до 100.0, де більше значення = сильніший сигнал.
        """
        try:
            if signal_data.get('signal') == 'HOLD':
                return 0.0
            
            strength_score = 0.0
            weights = STRATEGY_CONFIG.get('signal_strength_weights', {
                'confirmations': 25.0,      # Вага технічних підтверджень
                'volume_surge': 20.0,       # Вага об'ємного сплеску
                'adx_strength': 15.0,       # Вага силі тренду (ADX)
                'volatility': 10.0,         # Вага волатильності (ATR)
                'divergence': 15.0,         # Вага дивергенції
                'market_regime': 10.0,      # Вага ринкового режиму
                'trend_alignment': 5.0      # Вага вирівнювання тренду
            })
            
            # 1. Технічні підтвердження (0-25 балів)
            confirmations = max(
                signal_data.get('long_confirmations_count', 0),
                signal_data.get('short_confirmations_count', 0)
            )
            max_confirmations = 8  # Максимальна кількість підтверджень
            confirmation_score = min(confirmations / max_confirmations, 1.0) * weights['confirmations']
            strength_score += confirmation_score
            
            # 2. Об'ємний сплеск (0-20 балів)
            volume_boost = signal_data.get('volume_boost_data', {})
            if volume_boost.get('super_volume_surge_active', False):
                volume_score = weights['volume_surge']  # Максимальний бал за супер-сплеск
            elif volume_boost.get('volume_surge_active', False):
                volume_score = weights['volume_surge'] * 0.7  # 70% за звичайний сплеск
            else:
                volume_mult = volume_boost.get('volume_multiplier', 1.0)
                volume_score = min((volume_mult - 1.0) * 10, weights['volume_surge'])  # Лінійний скейл
            strength_score += volume_score
            
            # 3. Сила тренду ADX (0-15 балів)
            adx_value = signal_data.get('adx_value', 0)
            adx_threshold = signal_data.get('adx_threshold', 20)
            if adx_value > adx_threshold:
                adx_score = min((adx_value - adx_threshold) / 30.0, 1.0) * weights['adx_strength']
            else:
                adx_score = 0.0
            strength_score += adx_score
            
            # 4. Волатильність ATR (0-10 балів)
            atr_value = signal_data.get('atr_value', 0)
            if atr_value > 0 and len(df) > 20:
                atr_percentile = df['atr'].rolling(20).quantile(0.8).iloc[-1] if 'atr' in df.columns else atr_value
                if atr_value > atr_percentile:
                    volatility_score = weights['volatility']  # Висока волатильність
                else:
                    volatility_score = (atr_value / atr_percentile) * weights['volatility']
            else:
                volatility_score = weights['volatility'] * 0.5  # Середня оцінка якщо немає даних
            strength_score += volatility_score
            
            # 5. Дивергенція (0-15 балів)
            has_bullish_div = signal_data.get('bullish_vol_divergence', False)
            has_bearish_div = signal_data.get('bearish_vol_divergence', False)
            signal_type = signal_data.get('signal', 'HOLD')
            
            if (signal_type == 'BUY' and has_bullish_div) or (signal_type == 'SELL' and has_bearish_div):
                divergence_score = weights['divergence']  # Дивергенція збігається з сигналом
            elif has_bullish_div or has_bearish_div:
                divergence_score = weights['divergence'] * 0.3  # Дивергенція є, але не збігається
            else:
                divergence_score = 0.0
            strength_score += divergence_score
            
            # 6. Ринковий режим (0-10 балів)
            market_status = signal_data.get('market_regime_status', '')
            if 'trending' in market_status.lower():
                regime_score = weights['market_regime']
            elif 'ranging' in market_status.lower():
                regime_score = weights['market_regime'] * 0.5
            else:
                regime_score = weights['market_regime'] * 0.3
            strength_score += regime_score
            
            # 7. Вирівнювання тренду (0-5 балів)
            adaptive_params = signal_data.get('adaptive_params_used', {})
            if adaptive_params:
                trend_score = weights['trend_alignment']  # Є адаптивні параметри
            else:
                trend_score = weights['trend_alignment'] * 0.5
            strength_score += trend_score
            
            # Нормалізуємо до діапазону 0-100
            max_possible_score = sum(weights.values())
            normalized_score = (strength_score / max_possible_score) * 100.0
            
            self.logger.debug(f"Сила сигналу {signal_data.get('symbol', 'N/A')}: {normalized_score:.1f} "
                            f"(C:{confirmation_score:.1f}, V:{volume_score:.1f}, A:{adx_score:.1f}, "
                            f"Vol:{volatility_score:.1f}, D:{divergence_score:.1f}, R:{regime_score:.1f}, T:{trend_score:.1f})")
            
            return round(normalized_score, 2)
            
        except Exception as e:
            self.logger.error(f"Помилка розрахунку сили сигналу: {e}", exc_info=True)
            return 0.0
    
    def analyze_signals(self, symbol: str, df: pd.DataFrame) -> Dict[str, Any]:
        """Аналіз торгових сигналів"""
        # Визначаємо значення за замовчуванням на початку
        default_market_status = "N/A"
        latest_close_fallback = 0
        latest_atr_fallback = 0.00001 # Маленьке ненульове значення
        latest_bullish_div_fallback = False
        latest_bearish_div_fallback = False

        if not df.empty:
            if 'close_price' in df.columns:
                latest_close_fallback = df.iloc[-1].get('close_price', 0)
            if 'atr' in df.columns:
                latest_atr_fallback = df.iloc[-1].get('atr', 0.00001)
                if latest_atr_fallback == 0: latest_atr_fallback = 0.00001 # Гарантуємо ненульове значення
            if 'bullish_vol_divergence' in df.columns:
                latest_bullish_div_fallback = df.iloc[-1].get('bullish_vol_divergence', 0) == 1
            if 'bearish_vol_divergence' in df.columns:
                latest_bearish_div_fallback = df.iloc[-1].get('bearish_vol_divergence', 0) == 1
        
        try:
            if df.empty or len(df) < self.indicators_config.get('slow_ma', 21) + 5:
                self.logger.warning(f"Недостатньо даних ({len(df)}) для аналізу {symbol}")
                return {
                    'signal': 'HOLD', 'confidence': 0, 'entry_price': latest_close_fallback, 
                    'stop_loss': 0, 'take_profits': [], 'reason': 'Недостатньо даних', 
                    'atr_value': latest_atr_fallback,
                    'bullish_vol_divergence': latest_bullish_div_fallback, 
                    'bearish_vol_divergence': latest_bearish_div_fallback,
                    'symbol': symbol, # Додано symbol
                    'market_regime_status': "N/A - Insufficient Data", # Додано market_regime_status
                    'adx_threshold': self.indicators_config.get('adx_threshold', 20)
                }
            
            latest_row = df.iloc[-1] # Get latest data after all calculations

            # Розраховуємо market_regime один раз
            market_regime = self._calculate_market_regime(df)
            current_market_status_text = market_regime.get('status_text', default_market_status)

            if not self._check_time_filter():
                return {
                    'signal': 'HOLD', 'confidence': 0, 'entry_price': latest_row.get('close_price', latest_close_fallback), 
                    'stop_loss': 0, 'take_profits': [], 'reason': 'Відфільтровано за часом', 
                    'atr_value': latest_row.get('atr', latest_atr_fallback),
                    'bullish_vol_divergence': latest_row.get('bullish_vol_divergence', 0) == 1,
                    'bearish_vol_divergence': latest_row.get('bearish_vol_divergence', 0) == 1,
                    'symbol': symbol, # Додано symbol
                    'market_regime_status': current_market_status_text, # Додано market_regime_status
                    'adx_threshold': self.indicators_config.get('adx_threshold', 20)
                }
            
            adaptive_params = self._get_adaptive_parameters(market_regime)
            volume_boost = self._calculate_lightning_volume_boost(df)
            long_confirmations, short_confirmations = self._calculate_confirmations(df, volume_boost)
            
            bullish_trend_check = latest_row.get('bullish_trend', 0) == 1
            bearish_trend_check = latest_row.get('bearish_trend', 0) == 1
            adx_value_calc = latest_row.get('adx', 0)
            atr_value_calc = latest_row.get('atr', latest_atr_fallback)
            if atr_value_calc == 0: atr_value_calc = latest_atr_fallback # Гарантуємо ненульове значення
            close_price = latest_row.get('close_price', latest_close_fallback)

            required_indicators = ['bullish_trend', 'bearish_trend', 'adx', 'atr', 'close_price', 
                                   'bullish_vol_divergence', 'bearish_vol_divergence']
            if any(pd.isna(latest_row.get(ind)) for ind in required_indicators):
                self.logger.warning(f"Відсутні деякі ключові індикатори для {symbol} на останній свічці.")
                return {
                    'signal': 'HOLD', 'confidence': 0, 'entry_price': close_price, 
                    'stop_loss': 0, 'take_profits': [], 'reason': 'Відсутні індикатори', 
                    'atr_value': atr_value_calc,
                    'bullish_vol_divergence': latest_row.get('bullish_vol_divergence', 0) == 1,
                    'bearish_vol_divergence': latest_row.get('bearish_vol_divergence', 0) == 1,
                    'symbol': symbol, # Додано symbol
                    'market_regime_status': current_market_status_text, # Додано market_regime_status
                    'adx_threshold': self.indicators_config.get('adx_threshold', 20)
                }

            # ✅ НОВА ТІРУВАНА ЛОГІКА ВХОДУ НА ОСНОВІ ADX
            base_buy_signal, base_sell_signal, effective_adx_thresh = self._check_tiered_entry_logic(
                long_confirmations, short_confirmations, bullish_trend_check, bearish_trend_check, 
                adx_value_calc, adaptive_params
            )
            
            final_dynamic_tp_mult = (adaptive_params['tp_mult'] * 
                                     volume_boost['tp_multiplier_boost'] * 
                                     volume_boost['final_tp_extension'])
            
            signal_data_out = {} 
            if base_buy_signal:
                signal_data_out = self._create_buy_signal(
                    symbol=symbol, close_price=close_price, atr=atr_value_calc,
                    adaptive_params=adaptive_params, dynamic_tp_mult=final_dynamic_tp_mult,
                    long_confirmations=long_confirmations, volume_boost_info=volume_boost,
                    effective_adx_thresh=effective_adx_thresh 
                )
            elif base_sell_signal:
                signal_data_out = self._create_sell_signal(
                    symbol=symbol, close_price=close_price, atr=atr_value_calc,
                    adaptive_params=adaptive_params, dynamic_tp_mult=final_dynamic_tp_mult,
                    short_confirmations=short_confirmations, volume_boost_info=volume_boost,
                    effective_adx_thresh=effective_adx_thresh 
                )
            else: # HOLD сигнал
                signal_data_out = {
                    'signal': 'HOLD',
                    'confidence': max(long_confirmations, short_confirmations),
                    'entry_price': close_price, 
                    'stop_loss': 0, 
                    'take_profits': [],
                    'reason': f'Недостатньо підтверджень (L:{long_confirmations}/S:{short_confirmations}, ADX:{adx_value_calc:.1f} vs {effective_adx_thresh})',
                    'adx_threshold': effective_adx_thresh
                }
            
            # Гарантовано додаємо/оновлюємо symbol та market_regime_status
            signal_data_out['symbol'] = symbol 
            signal_data_out['market_regime_status'] = current_market_status_text

            # Встановлюємо значення за замовчуванням для ключів, які можуть бути відсутні (особливо для HOLD)
            signal_data_out.setdefault('atr_value', atr_value_calc)
            signal_data_out.setdefault('volume_surge_active', volume_boost.get('volume_surge_active', False))
            signal_data_out.setdefault('super_volume_surge_active', volume_boost.get('super_volume_surge_active', False))
            signal_data_out.setdefault('entry_price', close_price) # Для HOLD, якщо не було встановлено
            signal_data_out.setdefault('stop_loss', 0) # Для HOLD
            signal_data_out.setdefault('take_profits', []) # Для HOLD
            signal_data_out.setdefault('confidence', max(long_confirmations, short_confirmations)) # Для HOLD, якщо не було встановлено

            # Додаємо/оновлюємо решту інформації, яка має бути в усіх випадках
            signal_data_out.update({
                'adaptive_params_used': adaptive_params, 
                'volume_boost_data': volume_boost, # Словник з деталями volume_boost
                'long_confirmations_count': long_confirmations, 
                'short_confirmations_count': short_confirmations, 
                'adx_value': adx_value_calc,
                'current_close_price': close_price, 
                'bullish_vol_divergence': latest_row.get('bullish_vol_divergence', 0) == 1,
                'bearish_vol_divergence': latest_row.get('bearish_vol_divergence', 0) == 1
            })
            
            # Розраховуємо силу сигналу для пріоритизації
            signal_strength = self._calculate_signal_strength(signal_data_out, df)
            signal_data_out['signal_strength'] = signal_strength
            
            return signal_data_out
            
        except Exception as e:
            self.logger.error(f"Помилка аналізу сигналів для {symbol}: {e}", exc_info=True)
            # Використовуємо значення за замовчуванням, визначені на початку
            return {
                'signal': 'HOLD', 'confidence': 0, 'entry_price': latest_close_fallback, 
                'stop_loss': 0, 'take_profits': [], 'reason': f'Помилка аналізу: {str(e)}', 
                'atr_value': latest_atr_fallback,
                'bullish_vol_divergence': latest_bullish_div_fallback, 
                'bearish_vol_divergence': latest_bearish_div_fallback,
                'symbol': symbol, # Додано symbol
                'market_regime_status': "N/A - Analysis Error", # Додано market_regime_status
                'adx_threshold': self.indicators_config.get('adx_threshold', 20),
                'signal_strength': 0.0  # Додано силу сигналу
            }
   
    def _create_buy_signal(self, symbol: str, close_price: float, atr: float, 
                      adaptive_params: Dict, dynamic_tp_mult: float, 
                      long_confirmations: int, volume_boost_info: Dict, 
                      effective_adx_thresh: float = None) -> Dict[str, Any]:
        """Створення BUY сигналу - НОВА СИСТЕМА БЕЗ FINAL TP"""
        try:
            if atr <= 0:
                self.logger.warning(f"ATR для {symbol} нульовий або від'ємний ({atr}). Неможливо розрахувати SL/TP.")
                return {
                    'signal': 'HOLD', 'confidence': 0, 'entry_price': close_price,
                    'stop_loss': 0, 'take_profits': [], 'reason': 'Некоректний ATR для BUY сигналу',
                    'atr_value': atr,
                    'adx_threshold': adaptive_params['adx_thresh']
                }

            sl_mult = adaptive_params['sl_mult'] * self.current_risk_multiplier
            stop_loss = close_price - (sl_mult * atr)
            
            take_profits_list = [] 
            
            if self.strategy_config.get('use_triple_partial_tp', True):
                # ✅ СПЕЦІАЛЬНА ЛОГІКА ДЛЯ СКАЛЬПІНГУ
                if volume_boost_info.get('volume_surge_active', False):
                    # При volume surge робимо TP ближчими для швидкого прибутку
                    first_partial_mult = 0.3  # дуже близький TP
                    second_partial_mult = 0.6
                    third_partial_mult = 1.0
                    self.logger.info(f"BUY {symbol}: Застосовано скальпінг TP через volume surge")
                else:
                    # Стандартні TP
                    first_partial_mult = self.strategy_config.get('first_partial_multiplier', 0.5)
                    second_partial_mult = self.strategy_config.get('second_partial_multiplier', 0.8)
                    third_partial_mult = self.strategy_config.get('third_partial_multiplier', 1.2)
                
                first_partial_percent = self.strategy_config.get('first_partial_percent', 30.0)
                second_partial_percent = self.strategy_config.get('second_partial_percent', 50.0)
                third_partial_percent = self.strategy_config.get('third_partial_percent', 20.0)
                
                # ✅ ПЕРЕВІРКА: сума відсотків має дорівнювати 100%
                total_percent = first_partial_percent + second_partial_percent + third_partial_percent
                if abs(total_percent - DISPLAY_CONFIG['percentage_multiplier']) > PRECISION_CONFIG['percentage_tolerance']:
                    self.logger.warning(f"BUY {symbol}: Сума TP відсотків ({total_percent}%) не дорівнює 100%. Нормалізація.")
                    # Нормалізуємо відсотки
                    first_partial_percent = (first_partial_percent / total_percent) * DISPLAY_CONFIG['percentage_multiplier']
                    second_partial_percent = (second_partial_percent / total_percent) * DISPLAY_CONFIG['percentage_multiplier']
                    third_partial_percent = (third_partial_percent / total_percent) * DISPLAY_CONFIG['percentage_multiplier']

                # Розрахунок цін для TP (для BUY: додаємо до ціни входу)
                tp1_price = close_price + (first_partial_mult * atr)
                tp2_price = close_price + (second_partial_mult * atr)
                tp3_price = close_price + (third_partial_mult * atr)

                # ✅ НОВА СИСТЕМА: тільки 3 TP без Final
                take_profits_list = [
                    {'price': tp1_price, 'percentage_to_close': first_partial_percent, 'type': 'partial_1'},
                    {'price': tp2_price, 'percentage_to_close': second_partial_percent, 'type': 'partial_2'},
                    {'price': tp3_price, 'percentage_to_close': third_partial_percent, 'type': 'partial_3'},
                ]
                
                # ✅ ОПЦІОНАЛЬНО: додаємо Final TP тільки якщо увімкнено
                if self.strategy_config.get('use_final_tp', False):
                    final_tp_price = close_price + (dynamic_tp_mult * atr)
                    # Переконуємося що Final TP вищий за TP3
                    if final_tp_price <= tp3_price:
                        final_tp_price = tp3_price + (0.5 * atr)
                    
                    # Якщо використовуємо Final TP, останній partial стає неповним
                    take_profits_list[-1]['percentage_to_close'] = third_partial_percent - 5.0  # Залишаємо 5% для Final
                    take_profits_list.append({
                        'price': final_tp_price, 
                        'percentage_to_close': 5.0, 
                        'type': 'final'
                    })
            else:
                # Якщо не використовуємо triple partial, тільки один TP
                final_tp_price = close_price + (dynamic_tp_mult * atr)
                take_profits_list = [
                    {'price': final_tp_price, 'percentage_to_close': DISPLAY_CONFIG['percentage_multiplier'], 'type': 'final'}
                ]

            # ✅ ПЕРЕВІРКА порядку TP для BUY позицій
            for i, tp in enumerate(take_profits_list):
                if tp['price'] <= close_price:
                    self.logger.warning(f"BUY {symbol}: TP price {tp['price']:.6f} ({tp['type']}) не вище ціни входу {close_price:.6f}. Коригування TP.")
                    tp['price'] = close_price + (atr * (0.1 + i * 0.1))
                
                # Перевірка порядку TP: кожен наступний має бути вищим
                if i > 0 and tp['price'] <= take_profits_list[i-1]['price']:
                    self.logger.warning(f"BUY {symbol}: TP {tp['type']} ({tp['price']:.6f}) не вищий за попередній TP. Коригування.")
                    tp['price'] = take_profits_list[i-1]['price'] + (atr * 0.1)

            # Перевірка SL
            if stop_loss >= close_price:
                self.logger.warning(f"BUY {symbol}: SL price {stop_loss:.6f} не нижче ціни входу {close_price:.6f}. Коригування SL.")
                stop_loss = close_price - (atr * 0.1)

            return {
                'signal': 'BUY', 'confidence': long_confirmations, 'entry_price': close_price,
                'stop_loss': stop_loss, 'take_profits': take_profits_list,
                'reason': f'Bullish: {long_confirmations} conf.',
                'atr_value': atr, 
                'volume_surge_active': volume_boost_info.get('volume_surge_active', False), 
                'super_volume_surge_active': volume_boost_info.get('super_volume_surge_active', False),
                'adx_threshold': effective_adx_thresh or adaptive_params['adx_thresh']
            }
            
        except Exception as e:
            self.logger.error(f"Помилка створення BUY сигналу для {symbol}: {e}", exc_info=True)
            return {
                'signal': 'HOLD', 'confidence': 0, 'entry_price': close_price,
                'stop_loss': 0, 'take_profits': [], 'reason': f'Помилка створення BUY: {str(e)}',
                'atr_value': atr,
                'adx_threshold': effective_adx_thresh or adaptive_params['adx_thresh']
            }

    def _create_sell_signal(self, symbol: str, close_price: float, atr: float, 
                    adaptive_params: Dict, dynamic_tp_mult: float, 
                    short_confirmations: int, volume_boost_info: Dict,
                    effective_adx_thresh: float = None) -> Dict[str, Any]:
        """Створення SELL сигналу - НОВА СИСТЕМА БЕЗ FINAL TP"""
        try:
            if atr <= 0:
                self.logger.warning(f"ATR для {symbol} нульовий або від'ємний ({atr}). Неможливо розрахувати SL/TP.")
                return {
                    'signal': 'HOLD', 'confidence': 0, 'entry_price': close_price,
                    'stop_loss': 0, 'take_profits': [], 'reason': 'Некоректний ATR для SELL сигналу',
                    'atr_value': atr,
                    'adx_threshold': adaptive_params['adx_thresh']
                }

            sl_mult = adaptive_params['sl_mult'] * self.current_risk_multiplier
            stop_loss = close_price + (sl_mult * atr)
            
            take_profits_list = [] 
            
            if self.strategy_config.get('use_triple_partial_tp', True):
                # ✅ СПЕЦІАЛЬНА ЛОГІКА ДЛЯ СКАЛЬПІНГУ
                if volume_boost_info.get('volume_surge_active', False):
                    # При volume surge робимо TP ближчими для швидкого прибутку
                    first_partial_mult = 0.3  # дуже близький TP
                    second_partial_mult = 0.6
                    third_partial_mult = 1.0
                    self.logger.info(f"SELL {symbol}: Застосовано скальпінг TP через volume surge")
                else:
                    # Стандартні TP
                    first_partial_mult = self.strategy_config.get('first_partial_multiplier', 0.5)
                    second_partial_mult = self.strategy_config.get('second_partial_multiplier', 0.8)
                    third_partial_mult = self.strategy_config.get('third_partial_multiplier', 1.2)
                
                first_partial_percent = self.strategy_config.get('first_partial_percent', 30.0)
                second_partial_percent = self.strategy_config.get('second_partial_percent', 50.0)
                third_partial_percent = self.strategy_config.get('third_partial_percent', 20.0)
                
                # ✅ ПЕРЕВІРКА: сума відсотків має дорівнювати 100%
                total_percent = first_partial_percent + second_partial_percent + third_partial_percent
                if abs(total_percent - DISPLAY_CONFIG['percentage_multiplier']) > PRECISION_CONFIG['percentage_tolerance']:
                    self.logger.warning(f"SELL {symbol}: Сума TP відсотків ({total_percent}%) не дорівнює 100%. Нормалізація.")
                    # Нормалізуємо відсотки
                    first_partial_percent = (first_partial_percent / total_percent) * DISPLAY_CONFIG['percentage_multiplier']
                    second_partial_percent = (second_partial_percent / total_percent) * DISPLAY_CONFIG['percentage_multiplier']
                    third_partial_percent = (third_partial_percent / total_percent) * DISPLAY_CONFIG['percentage_multiplier']

                # Розрахунок цін для TP (для SHORT: віднімаємо від ціни входу)
                tp1_price = close_price - (first_partial_mult * atr)
                tp2_price = close_price - (second_partial_mult * atr)
                tp3_price = close_price - (third_partial_mult * atr)

                # ✅ НОВА СИСТЕМА: тільки 3 TP без Final
                take_profits_list = [
                    {'price': tp1_price, 'percentage_to_close': first_partial_percent, 'type': 'partial_1'},
                    {'price': tp2_price, 'percentage_to_close': second_partial_percent, 'type': 'partial_2'},
                    {'price': tp3_price, 'percentage_to_close': third_partial_percent, 'type': 'partial_3'},
                ]
                
                # ✅ ОПЦІОНАЛЬНО: додаємо Final TP тільки якщо увімкнено
                if self.strategy_config.get('use_final_tp', False):
                    final_tp_price = close_price - (dynamic_tp_mult * atr)
                    # Переконуємося що Final TP нижчий за TP3
                    if final_tp_price >= tp3_price:
                        final_tp_price = tp3_price - (0.5 * atr)
                    
                    # Якщо використовуємо Final TP, останній partial стає неповним
                    take_profits_list[-1]['percentage_to_close'] = third_partial_percent - 5.0  # Залишаємо 5% для Final
                    take_profits_list.append({
                        'price': final_tp_price, 
                        'percentage_to_close': 5.0, 
                        'type': 'final'
                    })
            else:
                # Якщо не використовуємо triple partial, тільки один TP
                final_tp_price = close_price - (dynamic_tp_mult * atr)
                take_profits_list = [
                    {'price': final_tp_price, 'percentage_to_close': DISPLAY_CONFIG['percentage_multiplier'], 'type': 'final'}
                ]

            # ✅ ПЕРЕВІРКА порядку TP для SHORT позицій
            for i, tp in enumerate(take_profits_list):
                if tp['price'] >= close_price:
                    self.logger.warning(f"SELL {symbol}: TP price {tp['price']:.6f} ({tp['type']}) не нижче ціни входу {close_price:.6f}. Коригування TP.")
                    tp['price'] = close_price - (atr * (0.1 + i * 0.1))
                
                # Перевірка порядку TP: кожен наступний має бути нижчим
                if i > 0 and tp['price'] >= take_profits_list[i-1]['price']:
                    self.logger.warning(f"SELL {symbol}: TP {tp['type']} ({tp['price']:.6f}) не нижчий за попередній TP. Коригування.")
                    tp['price'] = take_profits_list[i-1]['price'] - (atr * 0.1)

            # Перевірка SL
            if stop_loss <= close_price:
                self.logger.warning(f"SELL {symbol}: SL price {stop_loss:.6f} не вище ціни входу {close_price:.6f}. Коригування SL.")
                stop_loss = close_price + (atr * 0.1)

            return {
                'signal': 'SELL', 'confidence': short_confirmations, 'entry_price': close_price,
                'stop_loss': stop_loss, 'take_profits': take_profits_list,
                'reason': f'Bearish: {short_confirmations} conf.',
                'atr_value': atr, 
                'volume_surge_active': volume_boost_info.get('volume_surge_active', False), 
                'super_volume_surge_active': volume_boost_info.get('super_volume_surge_active', False),
                'adx_threshold': effective_adx_thresh or adaptive_params['adx_thresh']
            }
            
        except Exception as e:
            self.logger.error(f"Помилка створення SELL сигналу для {symbol}: {e}", exc_info=True)
            return {
                'signal': 'HOLD', 'confidence': 0, 'entry_price': close_price,
                'stop_loss': 0, 'take_profits': [], 'reason': f'Помилка створення SELL: {str(e)}',
                'atr_value': atr,
                'adx_threshold': effective_adx_thresh or adaptive_params['adx_thresh']
            }
    
    def update_risk_management(self, trade_profit: float): 
        """Оновлення ризик-менеджменту на основі результатів торгівлі"""
        try:
            if not self.strategy_config.get('use_adaptive_risk', True):
                self.current_risk_multiplier = 1.0 
                return
            
            if trade_profit < 0:
                self.consecutive_losses += 1
            else:
                self.consecutive_losses = 0
            
            reduction_percent = self.strategy_config.get('risk_reduction_percent', 25.0)
            reduction_amount = self.consecutive_losses * (reduction_percent / DISPLAY_CONFIG['percentage_multiplier']) 
            self.current_risk_multiplier = max(1.0 - reduction_amount, 0.25) 
            
            if self.consecutive_losses > 0:
                self.logger.info(f"Адаптивний ризик: {self.consecutive_losses} збитків поспіль, "
                            f"множник ризику SL: {self.current_risk_multiplier:.2f}")
            elif self.current_risk_multiplier < 1.0: 
                self.logger.info(f"Адаптивний ризик: Збитки скинуто, множник ризику SL відновлюється до 1.0 (поточний: {self.current_risk_multiplier:.2f})")
                if self.consecutive_losses == 0:
                    self.current_risk_multiplier = 1.0
        except Exception as e:
            self.logger.error(f"Помилка оновлення ризик-менеджменту: {e}", exc_info=True)