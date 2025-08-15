# -*- coding: utf-8 -*-
"""
Розрахунок технічних індикаторів для торгового бота (ВИПРАВЛЕНА ВЕРСІЯ)
"""

import numpy as np
import pandas as pd
import logging
from typing import Dict, Tuple, Optional
from config.settings import INDICATORS_CONFIG

# Try to import TA-Lib, fall back to None if not available
try:
    import talib
    TALIB_AVAILABLE = True
except ImportError:
    talib = None
    TALIB_AVAILABLE = False

logger = logging.getLogger(__name__)

class TechnicalIndicators:
    """Клас для розрахунку технічних індикаторів"""
    
    def __init__(self):
        self.config = INDICATORS_CONFIG
        
        # Log TA-Lib availability
        if not TALIB_AVAILABLE:
            logger.warning("TA-Lib not available! Using fallback implementations.")
        else:
            logger.info("TA-Lib available, using optimized implementations.")
    
    def _fallback_rsi(self, close_prices: np.ndarray, period: int = 14) -> np.ndarray:
        """Fallback RSI implementation using pandas"""
        try:
            close_series = pd.Series(close_prices)
            delta = close_series.diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
            rs = gain / loss
            rsi = 100 - (100 / (1 + rs))
            return rsi.fillna(50.0).values  # Fill NaN with neutral RSI value
        except Exception as e:
            logger.error(f"Fallback RSI calculation failed: {e}")
            return np.full(len(close_prices), 50.0)
    
    def _fallback_ema(self, close_prices: np.ndarray, period: int) -> np.ndarray:
        """Fallback EMA implementation using pandas with improved NaN handling"""
        try:
            close_series = pd.Series(close_prices)
            
            # Check if we have enough data points for reliable EMA
            if len(close_prices) < period:
                logger.warning(f"Not enough data points ({len(close_prices)}) for EMA period {period}, using simple average")
                # Use expanding mean for the beginning, then EMA
                ema = close_series.expanding().mean()
            else:
                ema = close_series.ewm(span=period, adjust=False).mean()
            
            # Improved NaN handling
            ema = ema.bfill()  # Backward fill first
            ema = ema.ffill()  # Forward fill for any remaining
            
            # Final check: if still NaN, use original prices
            if ema.isna().any():
                ema = ema.fillna(close_series)
            
            return ema.values
        except Exception as e:
            logger.error(f"Fallback EMA calculation failed: {e}")
            return np.copy(close_prices)  # Return original prices as fallback
    
    def _fallback_macd(self, close_prices: np.ndarray, fast: int = 12, slow: int = 26, signal: int = 9) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
        """Fallback MACD implementation using pandas"""
        try:
            close_series = pd.Series(close_prices)
            ema_fast = close_series.ewm(span=fast, adjust=False).mean()
            ema_slow = close_series.ewm(span=slow, adjust=False).mean()
            macd_line = ema_fast - ema_slow
            macd_signal = macd_line.ewm(span=signal, adjust=False).mean()
            macd_histogram = macd_line - macd_signal
            
            # Fill NaN values
            macd_line = macd_line.fillna(0.0)
            macd_signal = macd_signal.fillna(0.0)
            macd_histogram = macd_histogram.fillna(0.0)
            
            return macd_line.values, macd_signal.values, macd_histogram.values
        except Exception as e:
            logger.error(f"Fallback MACD calculation failed: {e}")
            zeros = np.zeros(len(close_prices))
            return zeros, zeros, zeros
    
    def _fallback_atr(self, high_prices: np.ndarray, low_prices: np.ndarray, close_prices: np.ndarray, period: int = 14) -> np.ndarray:
        """Fallback ATR implementation using pandas"""
        try:
            df = pd.DataFrame({
                'high': high_prices,
                'low': low_prices,
                'close': close_prices
            })
            
            df['tr1'] = df['high'] - df['low']
            df['tr2'] = abs(df['high'] - df['close'].shift())
            df['tr3'] = abs(df['low'] - df['close'].shift())
            df['tr'] = df[['tr1', 'tr2', 'tr3']].max(axis=1)
            
            atr = df['tr'].rolling(window=period).mean()
            return atr.fillna(atr.mean()).values
        except Exception as e:
            logger.error(f"Fallback ATR calculation failed: {e}")
            # Return simple high-low range as fallback
            return np.maximum(high_prices - low_prices, 0.001)
        
    def _safe_convert_to_int_array(self, values: np.ndarray) -> np.ndarray:
        """Безпечна конвертація в цілочисельний масив 0/1"""
        try:
            # Конвертуємо в булевий масив, потім в int
            bool_array = np.array(values, dtype=bool)
            int_array = bool_array.astype(int)
            return int_array
        except Exception as e:
            logger.warning(f"Помилка конвертації в int array: {e}")
            return np.zeros_like(values, dtype=int)
    
    def _fix_ema_nan_values(self, ema_values: np.ndarray, close_prices: np.ndarray, ema_type: str) -> np.ndarray:
        """Fix NaN values in EMA calculations with proper forward/backward fill"""
        try:
            if not np.any(np.isnan(ema_values)):
                return ema_values  # No NaN values, return as is
            
            ema_series = pd.Series(ema_values)
            
            # Count NaN values before fixing
            nan_count_before = np.isnan(ema_values).sum()
            
            # First, try backward fill for leading NaN values
            ema_series = ema_series.bfill()
            
            # Then forward fill for any remaining NaN values
            ema_series = ema_series.ffill()
            
            # If still NaN values (edge case), fill with close prices
            if ema_series.isna().any():
                ema_series = ema_series.fillna(pd.Series(close_prices))
            
            # Final fallback: fill any remaining with interpolation
            if ema_series.isna().any():
                ema_series = ema_series.interpolate(method='linear').bfill().ffill()
            
            # Convert back to numpy array
            fixed_ema = ema_series.values
            
            # Count NaN values after fixing
            nan_count_after = np.isnan(fixed_ema).sum()
            
            if nan_count_before > 0:
                logger.debug(f"Fixed {nan_count_before - nan_count_after} NaN values in {ema_type} EMA")
            
            return fixed_ema
            
        except Exception as e:
            logger.error(f"Error fixing NaN values in {ema_type} EMA: {e}")
            # Emergency fallback: replace NaN with close prices
            ema_fixed = np.where(np.isnan(ema_values), close_prices, ema_values)
            return ema_fixed
        
    def calculate_rsi(self, close_prices: np.ndarray) -> Dict[str, np.ndarray]:
        """Розрахунок RSI та його похідних"""
        try:
            # Add detailed logging for debugging
            logger.debug(f"Calculating RSI with {len(close_prices)} data points")
            logger.debug(f"RSI period: {self.config['rsi_length']}")
            logger.debug(f"Price range: {close_prices.min():.6f} - {close_prices.max():.6f}")
            
            if TALIB_AVAILABLE:
                rsi = talib.RSI(close_prices, timeperiod=self.config['rsi_length'])
            else:
                rsi = self._fallback_rsi(close_prices, self.config['rsi_length'])
            
            # Check for NaN values and log
            nan_count = np.isnan(rsi).sum()
            logger.debug(f"RSI calculation: {nan_count} NaN values out of {len(rsi)}")
            
            # RSI Rising/Falling - виправлено
            rsi_rising_1 = np.zeros(len(rsi), dtype=int)
            rsi_rising_2 = np.zeros(len(rsi), dtype=int)
            rsi_falling_1 = np.zeros(len(rsi), dtype=int)
            rsi_falling_2 = np.zeros(len(rsi), dtype=int)
            
            for i in range(1, len(rsi)):
                if not np.isnan(rsi[i]) and not np.isnan(rsi[i-1]):
                    rsi_rising_1[i] = 1 if rsi[i] > rsi[i-1] else 0
                    rsi_falling_1[i] = 1 if rsi[i] < rsi[i-1] else 0
            
            for i in range(2, len(rsi)):
                if not np.isnan(rsi[i]) and not np.isnan(rsi[i-2]):
                    rsi_rising_2[i] = 1 if rsi[i] > rsi[i-2] else 0
                    rsi_falling_2[i] = 1 if rsi[i] < rsi[i-2] else 0
            
            logger.debug("RSI calculation completed successfully")
            
            return {
                'rsi': rsi,
                'rsi_rising_1': rsi_rising_1,
                'rsi_rising_2': rsi_rising_2,
                'rsi_falling_1': rsi_falling_1,
                'rsi_falling_2': rsi_falling_2
            }
        except Exception as e:
            logger.error(f"Помилка розрахунку RSI: {e}")
            # Return fallback values
            length = len(close_prices)
            return {
                'rsi': np.full(length, 50.0),  # Neutral RSI
                'rsi_rising_1': np.zeros(length, dtype=int),
                'rsi_rising_2': np.zeros(length, dtype=int),
                'rsi_falling_1': np.zeros(length, dtype=int),
                'rsi_falling_2': np.zeros(length, dtype=int)
            }
    
    def calculate_ema(self, close_prices: np.ndarray) -> Dict[str, np.ndarray]:
        """Розрахунок EMA та сигналів"""
        try:
            # Add detailed logging for debugging
            logger.debug(f"Calculating EMA with {len(close_prices)} data points")
            logger.debug(f"Fast MA period: {self.config['fast_ma']}")
            logger.debug(f"Slow MA period: {self.config['slow_ma']}")
            
            if TALIB_AVAILABLE:
                ema_fast = talib.EMA(close_prices, timeperiod=self.config['fast_ma'])
                ema_slow = talib.EMA(close_prices, timeperiod=self.config['slow_ma'])
                ema_trend = talib.EMA(close_prices, timeperiod=self.config['slow_ma'])
            else:
                ema_fast = self._fallback_ema(close_prices, self.config['fast_ma'])
                ema_slow = self._fallback_ema(close_prices, self.config['slow_ma'])
                ema_trend = self._fallback_ema(close_prices, self.config['slow_ma'])
            
            # Fix NaN values in EMA calculations with proper forward/backward fill
            ema_fast = self._fix_ema_nan_values(ema_fast, close_prices, 'fast')
            ema_slow = self._fix_ema_nan_values(ema_slow, close_prices, 'slow') 
            ema_trend = self._fix_ema_nan_values(ema_trend, close_prices, 'trend')
            
            # Check for NaN values and log
            nan_fast = np.isnan(ema_fast).sum()
            nan_slow = np.isnan(ema_slow).sum()
            nan_trend = np.isnan(ema_trend).sum()
            logger.debug(f"EMA calculation: Fast EMA {nan_fast} NaN, Slow EMA {nan_slow} NaN, Trend EMA {nan_trend} NaN")
            
            # EMA Crossover сигнали - виправлено
            ema_bullish = np.zeros(len(ema_fast), dtype=int)
            ema_bearish = np.zeros(len(ema_fast), dtype=int)
            
            for i in range(1, len(ema_fast)):
                if (not np.isnan(ema_fast[i]) and not np.isnan(ema_slow[i]) and 
                    not np.isnan(ema_fast[i-1]) and not np.isnan(ema_slow[i-1])):
                    
                    # Crossover up (bullish)
                    if ema_fast[i] > ema_slow[i] and ema_fast[i-1] <= ema_slow[i-1]:
                        ema_bullish[i] = 1
                    
                    # Crossunder down (bearish)
                    if ema_fast[i] < ema_slow[i] and ema_fast[i-1] >= ema_slow[i-1]:
                        ema_bearish[i] = 1
            
            logger.debug("EMA calculation completed successfully")
            
            return {
                'ema_fast': ema_fast,
                'ema_slow': ema_slow,
                'ema_trend': ema_trend,
                'ema_bullish': ema_bullish,
                'ema_bearish': ema_bearish
            }
        except Exception as e:
            logger.error(f"Помилка розрахунку EMA: {e}")
            # Return fallback values
            length = len(close_prices)
            return {
                'ema_fast': np.copy(close_prices),
                'ema_slow': np.copy(close_prices),
                'ema_trend': np.copy(close_prices),
                'ema_bullish': np.zeros(length, dtype=int),
                'ema_bearish': np.zeros(length, dtype=int)
            }
    
    def calculate_macd(self, close_prices: np.ndarray) -> Dict[str, np.ndarray]:
        """Розрахунок MACD та сигналів"""
        try:
            # Add detailed logging for debugging
            logger.debug(f"Calculating MACD with {len(close_prices)} data points")
            logger.debug(f"MACD fast: {self.config['macd_fast']}, slow: {self.config['macd_slow']}, signal: {self.config['macd_signal']}")
            
            if TALIB_AVAILABLE:
                macd_line, macd_signal, macd_histogram = talib.MACD(
                    close_prices,
                    fastperiod=self.config['macd_fast'],
                    slowperiod=self.config['macd_slow'],
                    signalperiod=self.config['macd_signal']
                )
            else:
                macd_line, macd_signal, macd_histogram = self._fallback_macd(
                    close_prices,
                    self.config['macd_fast'],
                    self.config['macd_slow'],
                    self.config['macd_signal']
                )
            
            # Check for NaN values and log
            nan_line = np.isnan(macd_line).sum()
            nan_signal = np.isnan(macd_signal).sum()
            nan_hist = np.isnan(macd_histogram).sum()
            logger.debug(f"MACD calculation: Line {nan_line} NaN, Signal {nan_signal} NaN, Histogram {nan_hist} NaN")
            
            # MACD сигнали - виправлено
            macd_bullish = np.zeros(len(macd_line), dtype=int)
            macd_bearish = np.zeros(len(macd_line), dtype=int)
            
            for i in range(1, len(macd_line)):
                if (not np.isnan(macd_line[i]) and not np.isnan(macd_signal[i]) and 
                    not np.isnan(macd_line[i-1]) and not np.isnan(macd_signal[i-1])):
                    
                    # Crossover signals
                    if macd_line[i] > macd_signal[i] and macd_line[i-1] <= macd_signal[i-1]:
                        macd_bullish[i] = 1
                    elif macd_line[i] < macd_signal[i] and macd_line[i-1] >= macd_signal[i-1]:
                        macd_bearish[i] = 1
                    
                    # Additional bullish condition
                    elif (macd_line[i] > macd_signal[i] and 
                          not np.isnan(macd_histogram[i]) and not np.isnan(macd_histogram[i-1]) and
                          macd_histogram[i] > macd_histogram[i-1]):
                        macd_bullish[i] = 1
                    
                    # Additional bearish condition  
                    elif (macd_line[i] < macd_signal[i] and
                          not np.isnan(macd_histogram[i]) and not np.isnan(macd_histogram[i-1]) and
                          macd_histogram[i] < macd_histogram[i-1]):
                        macd_bearish[i] = 1
            
            logger.debug("MACD calculation completed successfully")
            
            return {
                'macd_line': macd_line,
                'macd_signal': macd_signal,
                'macd_histogram': macd_histogram,
                'macd_bullish': macd_bullish,
                'macd_bearish': macd_bearish
            }
        except Exception as e:
            logger.error(f"Помилка розрахунку MACD: {e}")
            # Return fallback values
            length = len(close_prices)
            zeros = np.zeros(length)
            return {
                'macd_line': zeros,
                'macd_signal': zeros,
                'macd_histogram': zeros,
                'macd_bullish': np.zeros(length, dtype=int),
                'macd_bearish': np.zeros(length, dtype=int)
            }
    
    def calculate_adx(self, high_prices: np.ndarray, low_prices: np.ndarray, 
                     close_prices: np.ndarray) -> Dict[str, np.ndarray]:
        """Розрахунок ADX (Виправлена версія з обробкою ділення на нуль)"""
        try:
            length = self.config['adx_period']
            
            # Розрахунок True Range та Directional Movement
            up_move = np.diff(high_prices)
            down_move = -np.diff(low_prices)
            
            plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
            minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)
            
            # True Range
            tr1 = high_prices[1:] - low_prices[1:]
            tr2 = np.abs(high_prices[1:] - close_prices[:-1])
            tr3 = np.abs(low_prices[1:] - close_prices[:-1])
            tr = np.maximum(np.maximum(tr1, tr2), tr3)
            
            # Додавання початкових значень для збереження довжини масивів
            plus_dm = np.concatenate([[0], plus_dm])
            minus_dm = np.concatenate([[0], minus_dm])
            tr = np.concatenate([[0], tr])
            
            # Розрахунок RMA (аналог ta.rma в PineScript)
            def rma(values, period):
                result = np.full_like(values, np.nan)
                alpha = 1.0 / period
                for i in range(len(values)):
                    if i == 0:
                        result[i] = values[i] if not np.isnan(values[i]) else 0
                    else:
                        if not np.isnan(values[i]):
                            result[i] = alpha * values[i] + (1 - alpha) * result[i-1]
                        else:
                            result[i] = result[i-1]
                return result
            
            # ATR та DI розрахунки з захистом від ділення на нуль
            atr = rma(tr, length)
            
            # Захист від ділення на нуль
            atr_safe = np.where(atr == 0, 1e-10, atr)  # Заміна нулів на дуже маленьке число
            
            plus_di = 100 * rma(plus_dm, length) / atr_safe
            minus_di = 100 * rma(minus_dm, length) / atr_safe
            
            # DX та ADX з захистом від ділення на нуль
            di_sum = plus_di + minus_di
            di_sum_safe = np.where(di_sum == 0, 1e-10, di_sum)  # Захист від ділення на нуль
            
            dx = 100 * np.abs(plus_di - minus_di) / di_sum_safe
            dx = np.where(np.isnan(dx) | np.isinf(dx), 0, dx)
            adx = rma(dx, length)
            
            # Заміна NaN та Inf значень на 0
            adx = np.where(np.isnan(adx) | np.isinf(adx), 0, adx)
            plus_di = np.where(np.isnan(plus_di) | np.isinf(plus_di), 0, plus_di)
            minus_di = np.where(np.isnan(minus_di) | np.isinf(minus_di), 0, minus_di)
            
            return {
                'adx': adx,
                'plus_di': plus_di,
                'minus_di': minus_di
            }
        except Exception as e:
            logger.error(f"Помилка розрахунку ADX: {e}")
            return {}
    
    def calculate_atr(self, high_prices: np.ndarray, low_prices: np.ndarray, 
                     close_prices: np.ndarray) -> np.ndarray:
        """Розрахунок ATR"""
        try:
            logger.debug(f"Calculating ATR with {len(close_prices)} data points, period: {self.config['atr_length']}")
            
            if TALIB_AVAILABLE:
                atr = talib.ATR(high_prices, low_prices, close_prices, 
                               timeperiod=self.config['atr_length'])
            else:
                atr = self._fallback_atr(high_prices, low_prices, close_prices, self.config['atr_length'])
            
            # Check for NaN values and log
            nan_count = np.isnan(atr).sum()
            logger.debug(f"ATR calculation: {nan_count} NaN values out of {len(atr)}")
            
            # Заміна NaN значень
            atr = np.where(np.isnan(atr), np.nanmean(atr[atr > 0]) if np.any(atr > 0) else 0.001, atr)
            logger.debug("ATR calculation completed successfully")
            
            return atr
        except Exception as e:
            logger.error(f"Помилка розрахунку ATR: {e}")
            # Return simple high-low range as fallback
            return np.maximum(high_prices - low_prices, 0.001)
    
    def calculate_volume_indicators(self, volume: np.ndarray, close_prices: np.ndarray) -> Dict[str, np.ndarray]:
        """Розрахунок volume індикаторів"""
        try:
            logger.debug(f"Calculating volume indicators with {len(volume)} data points")
            
            # Volume SMA
            if TALIB_AVAILABLE:
                volume_sma = talib.SMA(volume, timeperiod=self.config['volume_lookback'])
            else:
                # Fallback using pandas
                volume_series = pd.Series(volume)
                volume_sma = volume_series.rolling(window=self.config['volume_lookback']).mean().bfill().values
            
            # Check for NaN values and log
            nan_sma = np.isnan(volume_sma).sum()
            logger.debug(f"Volume SMA calculation: {nan_sma} NaN values out of {len(volume_sma)}")
            
            # Volume Filter - виправлено
            volume_filter_bool = volume > (volume_sma * self.config['min_volume_mult'])
            volume_filter = self._safe_convert_to_int_array(volume_filter_bool)
            
            # Volume Surge Detection - виправлено  
            volume_surge_bool = volume > (volume_sma * self.config['volume_surge_mult'])
            volume_surge = self._safe_convert_to_int_array(volume_surge_bool)
            
            super_volume_surge_bool = volume > (volume_sma * self.config['super_volume_mult'])
            super_volume_surge = self._safe_convert_to_int_array(super_volume_surge_bool)
            
            # Volume Trend
            if TALIB_AVAILABLE:
                volume_ema_10 = talib.EMA(volume, timeperiod=10)
                volume_ema_20 = talib.EMA(volume, timeperiod=20)
            else:
                # Fallback using pandas
                volume_series = pd.Series(volume)
                volume_ema_10 = volume_series.ewm(span=10, adjust=False).mean().bfill().values
                volume_ema_20 = volume_series.ewm(span=20, adjust=False).mean().bfill().values
            
            # Захист від ділення на нуль
            volume_ema_20_safe = np.where(volume_ema_20 == 0, 1e-10, volume_ema_20)
            volume_trend = volume_ema_10 / volume_ema_20_safe
            volume_trend = np.where(np.isnan(volume_trend) | np.isinf(volume_trend), 1.0, volume_trend)
            
            # Sustained Volume - виправлено
            sustained_volume = np.zeros(len(volume), dtype=int)
            consecutive_count = 0
            
            for i in range(len(volume)):
                if not np.isnan(volume_sma[i]) and volume[i] > volume_sma[i] * 2.0:
                    consecutive_count += 1
                else:
                    consecutive_count = 0
                
                sustained_volume[i] = 1 if consecutive_count >= self.config['consecutive_vol_bars'] else 0
            
            # Volume Divergence - виправлено
            bullish_vol_divergence = np.zeros(len(volume), dtype=int)
            bearish_vol_divergence = np.zeros(len(volume), dtype=int)
            
            period = self.config['vol_divergence_period']
            if TALIB_AVAILABLE:
                volume_avg = talib.SMA(volume, timeperiod=period)
            else:
                volume_series = pd.Series(volume)
                volume_avg = volume_series.rolling(window=period).mean().bfill().values
            
            for i in range(period, len(close_prices)):
                if i >= period:
                    price_change = close_prices[i] - close_prices[i-period]
                    volume_change = volume[i] - volume[i-period]
                    
                    # Bullish divergence: price down, volume up
                    if (price_change < 0 and volume_change > 0 and 
                        not np.isnan(volume_avg[i]) and volume[i] > volume_avg[i] * 1.5):
                        bullish_vol_divergence[i] = 1
                    
                    # Bearish divergence: price up, volume down  
                    if (price_change > 0 and volume_change < 0 and
                        not np.isnan(volume_avg[i]) and volume[i] < volume_avg[i] * 0.7):
                        bearish_vol_divergence[i] = 1
            
            logger.debug("Volume indicators calculation completed successfully")
            
            return {
                'volume_sma': volume_sma,
                'volume_filter': volume_filter,
                'volume_surge': volume_surge,
                'super_volume_surge': super_volume_surge,
                'volume_trend': volume_trend,
                'sustained_volume': sustained_volume,
                'bullish_vol_divergence': bullish_vol_divergence,
                'bearish_vol_divergence': bearish_vol_divergence
            }
        except Exception as e:
            logger.error(f"Помилка розрахунку volume індикаторів: {e}")
            # Return fallback values
            length = len(volume)
            return {
                'volume_sma': np.copy(volume),
                'volume_filter': np.ones(length, dtype=int),
                'volume_surge': np.zeros(length, dtype=int),
                'super_volume_surge': np.zeros(length, dtype=int),
                'volume_trend': np.ones(length),
                'sustained_volume': np.zeros(length, dtype=int),
                'bullish_vol_divergence': np.zeros(length, dtype=int),
                'bearish_vol_divergence': np.zeros(length, dtype=int)
            }
    
    def calculate_trend_indicators(self, close_prices: np.ndarray, ema_trend: np.ndarray) -> Dict[str, np.ndarray]:
        """Розрахунок трендових індикаторів"""
        try:
            bullish_trend_bool = close_prices > ema_trend
            bearish_trend_bool = close_prices < ema_trend
            
            bullish_trend = self._safe_convert_to_int_array(bullish_trend_bool)
            bearish_trend = self._safe_convert_to_int_array(bearish_trend_bool)
            
            return {
                'bullish_trend': bullish_trend,
                'bearish_trend': bearish_trend
            }
        except Exception as e:
            logger.error(f"Помилка розрахунку трендових індикаторів: {e}")
            return {}
    
    def calculate_all_indicators(self, df: pd.DataFrame) -> Dict[str, np.ndarray]:
        """Розрахунок всіх індикаторів"""
        try:
            if df.empty or len(df) < 50:
                logger.warning(f"Недостатньо даних для розрахунку індикаторів: {len(df)} записів")
                return {}
            
            logger.info(f"Розпочинаю розрахунок індикаторів для {len(df)} записів")
            logger.debug(f"TA-Lib доступний: {TALIB_AVAILABLE}")
            logger.debug(f"Конфігурація індикаторів: {self.config}")
            
            # Конвертація в numpy arrays
            close_prices = df['close_price'].values.astype(float)
            high_prices = df['high_price'].values.astype(float)
            low_prices = df['low_price'].values.astype(float)
            volume = df['volume'].values.astype(float)
            
            # Детальна перевірка вхідних даних
            logger.debug(f"Діапазон цін: {close_prices.min():.6f} - {close_prices.max():.6f}")
            logger.debug(f"Діапазон обсягів: {volume.min():.0f} - {volume.max():.0f}")
            
            # Перевірка на некоректні дані
            invalid_close = np.isnan(close_prices) | (close_prices <= 0)
            invalid_high = np.isnan(high_prices) | (high_prices <= 0)
            invalid_low = np.isnan(low_prices) | (low_prices <= 0)
            invalid_volume = np.isnan(volume) | (volume < 0)
            
            if invalid_close.any() or invalid_high.any() or invalid_low.any():
                logger.warning(f"Виявлено некоректні ціни: close={invalid_close.sum()}, high={invalid_high.sum()}, low={invalid_low.sum()}")
            
            # Обробка NaN значень
            close_prices = np.where(invalid_close, np.nanmean(close_prices[close_prices > 0]), close_prices)
            high_prices = np.where(invalid_high, close_prices, high_prices)
            low_prices = np.where(invalid_low, close_prices, low_prices)
            volume = np.where(invalid_volume, 0, volume)
            
            all_indicators = {}
            
            # RSI
            logger.debug("Розраховую RSI...")
            rsi_indicators = self.calculate_rsi(close_prices)
            all_indicators.update(rsi_indicators)
            
            # EMA
            logger.debug("Розраховую EMA...")
            ema_indicators = self.calculate_ema(close_prices)
            all_indicators.update(ema_indicators)
            
            # MACD
            logger.debug("Розраховую MACD...")
            macd_indicators = self.calculate_macd(close_prices)
            all_indicators.update(macd_indicators)
            
            # ADX
            logger.debug("Розраховую ADX...")
            adx_indicators = self.calculate_adx(high_prices, low_prices, close_prices)
            all_indicators.update(adx_indicators)
            
            # ATR
            logger.debug("Розраховую ATR...")
            atr = self.calculate_atr(high_prices, low_prices, close_prices)
            all_indicators['atr'] = atr
            
            # Volume
            logger.debug("Розраховую Volume індикатори...")
            volume_indicators = self.calculate_volume_indicators(volume, close_prices)
            all_indicators.update(volume_indicators)
            
            # Trend
            if 'ema_trend' in all_indicators:
                logger.debug("Розраховую Trend індикатори...")
                trend_indicators = self.calculate_trend_indicators(close_prices, all_indicators['ema_trend'])
                all_indicators.update(trend_indicators)
            
            # Фінальна валідація
            total_indicators = len(all_indicators)
            nan_indicators = []
            for name, values in all_indicators.items():
                if isinstance(values, np.ndarray):
                    nan_count = np.isnan(values).sum()
                    if nan_count > len(values) * 0.5:  # Більше 50% NaN
                        nan_indicators.append(f"{name}({nan_count} NaN)")
            
            if nan_indicators:
                logger.warning(f"Індикатори з високим рівнем NaN: {nan_indicators}")
            else:
                logger.info(f"Всі {total_indicators} індикаторів розраховано успішно")
            
            logger.debug("Розрахунок індикаторів завершено")
            return all_indicators
            
        except Exception as e:
            logger.error(f"Критична помилка розрахунку індикаторів: {e}", exc_info=True)
            return {}
    
    def interpolate_nan_values(self, data: np.ndarray) -> np.ndarray:
        """Інтерполяція NaN значень"""
        try:
            if len(data) == 0:
                return data
                
            # Використання pandas для інтерполяції
            series = pd.Series(data)
            interpolated = series.interpolate(method='linear', limit_direction='both')
            
            # Заповнення залишкових NaN значень
            interpolated = interpolated.ffill().bfill()
            
            return interpolated.values
        except Exception as e:
            logger.error(f"Помилка інтерполяції: {e}")
            return data