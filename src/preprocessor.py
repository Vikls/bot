# -*- coding: utf-8 -*-
"""
Модуль обробки даних та розрахунку індикаторів (ВИПРАВЛЕНА ВЕРСІЯ)
"""

import asyncio
import logging
import pandas as pd
import numpy as np
import gc
import weakref
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Any, Generator, AsyncGenerator
import traceback 
from src.api_manager import BybitAPIManager
from src.db_manager import DatabaseManager
from src.indicators import TechnicalIndicators
from config.settings import (
    TRADING_CONFIG, INDICATORS_CONFIG, STRATEGY_CONFIG,
    SYSTEM_CONFIG, PRECISION_CONFIG, DISPLAY_CONFIG
)

# logger = logging.getLogger(__name__) # Цей рядок можна залишити, якщо потрібен логер на рівні модуля для якихось функцій поза класом
                                     # Але для методів класу краще використовувати self.logger

class DataPreprocessor:
    """Обробник даних та розрахунок індикаторів з оптимізаціями пам'яті"""
    
    def __init__(self, chunk_size: int = None, max_memory_usage_mb: int = None):
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self.api_manager = BybitAPIManager()
        self.db_manager = DatabaseManager()
        self.indicators = TechnicalIndicators()
        self.trading_config = TRADING_CONFIG
        self.is_running = False
        
        # Memory optimization settings з конфігурації
        self.chunk_size = chunk_size or SYSTEM_CONFIG['chunk_size_default']  # Розмір чанку для обробки
        self.max_memory_usage_mb = max_memory_usage_mb or SYSTEM_CONFIG['max_memory_usage_mb']
        self.critical_memory_usage_mb = SYSTEM_CONFIG['critical_memory_usage_mb']  # Critical threshold
        self._active_dataframes = weakref.WeakValueDictionary()  # Трекінг активних DataFrame
        self._memory_monitor_enabled = True
        
        # Memory cleanup cooldown mechanism
        self._last_cleanup_time = 0
        self._cleanup_cooldown_seconds = 30
        self._memory_usage_before_cleanup = 0
        self._memory_usage_after_cleanup = 0
        
        # Streaming data buffer
        self._stream_buffer = {}
        self._buffer_lock = asyncio.Lock()
        
        self.logger.info(f"Data Preprocessor ініціалізовано з оптимізаціями пам'яті (chunk_size={chunk_size}, max_memory={max_memory_usage_mb}MB, critical={self.critical_memory_usage_mb}MB)")
    
    def _monitor_memory_usage(self):
        """Моніторинг використання пам'яті з lazy loading psutil та cooldown механізмом"""
        if not self._memory_monitor_enabled:
            return
            
        try:
            # Lazy import для psutil
            psutil = self._get_psutil_module()
            if not psutil:
                return
                
            process = psutil.Process()
            memory_mb = process.memory_info().rss / 1024 / 1024
            current_time = datetime.now().timestamp()
            
            # Check if we're in cooldown period
            time_since_last_cleanup = current_time - self._last_cleanup_time
            
            if memory_mb > self.critical_memory_usage_mb:
                # Critical memory usage - bypass cooldown
                self.logger.warning(f"КРИТИЧНЕ використання пам'яті: {memory_mb:.1f}MB > {self.critical_memory_usage_mb}MB - негайне очищення")
                self._cleanup_memory(aggressive=True)
            elif memory_mb > self.max_memory_usage_mb:
                if time_since_last_cleanup >= self._cleanup_cooldown_seconds:
                    self.logger.warning(f"Високе використання пам'яті: {memory_mb:.1f}MB > {self.max_memory_usage_mb}MB")
                    self._cleanup_memory(aggressive=False)
                else:
                    remaining_cooldown = self._cleanup_cooldown_seconds - time_since_last_cleanup
                    self.logger.debug(f"Пам'ять {memory_mb:.1f}MB > {self.max_memory_usage_mb}MB, але в режимі очікування ({remaining_cooldown:.1f}с залишилось)")
            else:
                # Memory usage is normal, log occasionally for monitoring
                if hasattr(self, '_last_memory_log_time'):
                    if current_time - self._last_memory_log_time > 300:  # Log every 5 minutes when normal
                        self.logger.debug(f"Використання пам'яті: {memory_mb:.1f}MB (нормально)")
                        self._last_memory_log_time = current_time
                else:
                    self._last_memory_log_time = current_time
                
        except Exception as e:
            self.logger.debug(f"Помилка моніторингу пам'яті: {e}")
            self._memory_monitor_enabled = False
    
    def _get_psutil_module(self):
        """Lazy loading для psutil модуля"""
        if not hasattr(self, '_psutil_module'):
            try:
                import psutil
                self._psutil_module = psutil
                self.logger.debug("psutil модуль завантажено (lazy loading)")
            except ImportError:
                self._psutil_module = None
                self._memory_monitor_enabled = False
                self.logger.debug("psutil не доступний, моніторинг пам'яті вимкнено")
        
        return self._psutil_module
    
    def _cleanup_memory(self, aggressive: bool = False):
        """Принудове очищення пам'яті з покращеною ефективністю та відстеженням"""
        try:
            # Record memory before cleanup
            psutil = self._get_psutil_module()
            if psutil:
                try:
                    process = psutil.Process()
                    self._memory_usage_before_cleanup = process.memory_info().rss / 1024 / 1024
                except:
                    self._memory_usage_before_cleanup = 0
            else:
                self._memory_usage_before_cleanup = 0
            
            cleanup_actions = []
            
            # Clear active DataFrames more effectively
            active_df_count = len(self._active_dataframes)
            if active_df_count > 0:
                for df_id in list(self._active_dataframes.keys()):
                    try:
                        df = self._active_dataframes[df_id]
                        if hasattr(df, 'memory_usage'):
                            # Properly delete DataFrame by clearing all data
                            df.drop(df.index, inplace=True)
                            df.drop(df.columns, axis=1, inplace=True, errors='ignore')
                            del df
                        del self._active_dataframes[df_id]
                    except:
                        pass
                cleanup_actions.append(f"очищено {active_df_count} DataFrame")
            
            # Clear stream buffer
            buffer_size = len(self._stream_buffer)
            if buffer_size > 0:
                self._stream_buffer.clear()
                cleanup_actions.append(f"очищено буфер ({buffer_size} елементів)")
            
            # NEW: Clear pandas cache and internal objects
            import pandas as pd
            try:
                # Clear pandas' internal caches
                if hasattr(pd, '_config') and hasattr(pd._config, 'config'):
                    # Clear pandas configuration cache if it exists
                    pass
                cleanup_actions.append("очищено pandas кеш")
            except:
                pass
            
            # NEW: Cleanup numpy arrays and temporary objects 
            import numpy as np
            try:
                # Force cleanup of numpy error state
                np.seterr(all='ignore')  # Reset error handling
                cleanup_actions.append("скинуто numpy стан")
            except:
                pass
            
            # Aggressive cleanup for critical memory situations
            if aggressive:
                # Force more comprehensive cleanup
                import sys
                
                # Clear any large objects in local variables
                locals_cleared = 0
                try:
                    for frame in [sys._getframe(i) for i in range(1, min(4, sys.getrecursionlimit()))]:
                        try:
                            for var_name, var_value in list(frame.f_locals.items()):
                                if hasattr(var_value, '__len__') and len(var_value) > DISPLAY_CONFIG.get('max_variable_display_length', 1000):
                                    frame.f_locals[var_name] = None
                                    locals_cleared += 1
                        except:
                            pass
                except:
                    pass
                
                if locals_cleared > 0:
                    cleanup_actions.append(f"агресивне очищення локальних змінних ({locals_cleared})")
            
            # Multiple garbage collection passes for better effectiveness
            collected_objects = 0
            for _ in range(3 if aggressive else 2):
                collected = gc.collect()
                collected_objects += collected
            
            # NEW: Force garbage collection of specific object types
            try:
                # Force cleanup of cyclic references
                gc.set_threshold(0)  # Temporarily disable automatic GC
                gc.collect()  # Force collection
                gc.set_threshold(700, 10, 10)  # Reset to default thresholds
                cleanup_actions.append("форсований збір сміття")
            except:
                pass
            
            if collected_objects > 0:
                cleanup_actions.append(f"зібрано {collected_objects} об'єктів")
            
            # Ensure we always report some cleanup action
            if not cleanup_actions:
                cleanup_actions.append("виконано базове очищення")
            
            # Record memory after cleanup and update timing
            current_time = datetime.now().timestamp()
            self._last_cleanup_time = current_time
            
            if psutil:
                try:
                    self._memory_usage_after_cleanup = process.memory_info().rss / 1024 / 1024
                    memory_freed = self._memory_usage_before_cleanup - self._memory_usage_after_cleanup
                    
                    if memory_freed > 0.1:  # Only report if freed more than 0.1MB
                        cleanup_actions.append(f"звільнено {memory_freed:.1f}MB")
                        effectiveness = (memory_freed / self._memory_usage_before_cleanup) * DISPLAY_CONFIG.get('memory_effectiveness_multiplier', 100)
                        
                        if aggressive:
                            self.logger.info(f"Агресивне очищення пам'яті завершено: {', '.join(cleanup_actions)} (ефективність: {effectiveness:.1f}%)")
                        else:
                            self.logger.info(f"Очищення пам'яті завершено: {', '.join(cleanup_actions)} (ефективність: {effectiveness:.1f}%)")
                    else:
                        # Memory wasn't significantly freed, but report what was done
                        if len(cleanup_actions) > 1:  # If we have actual cleanup actions beyond base cleanup
                            self.logger.debug(f"Очищення пам'яті завершено: {', '.join(cleanup_actions)} (пам'ять стабільна)")
                        else:
                            self.logger.debug(f"Очищення пам'яті завершено: пам'ять стабільна ({self._memory_usage_after_cleanup:.1f}MB)")
                except:
                    if cleanup_actions:
                        action_type = "Агресивне очищення" if aggressive else "Очищення"
                        self.logger.info(f"{action_type} пам'яті завершено: {', '.join(cleanup_actions)}")
                    else:
                        self.logger.info("Виконано очищення пам'яті")
            else:
                if cleanup_actions:
                    action_type = "Агресивне очищення" if aggressive else "Очищення"
                    self.logger.info(f"{action_type} пам'яті завершено: {', '.join(cleanup_actions)}")
                else:
                    self.logger.info("Виконано очищення пам'яті")
            
        except Exception as e:
            self.logger.error(f"Помилка очищення пам'яті: {e}")
            # Still update timing even if cleanup failed
            self._last_cleanup_time = datetime.now().timestamp()
    
    def _create_memory_efficient_dataframe(self, data: List[Dict]) -> pd.DataFrame:
        """Створення DataFrame з оптимізованими типами даних"""
        if not data:
            return pd.DataFrame()
        
        df = pd.DataFrame(data)
        
        # Оптимізація типів даних для зменшення використання пам'яті
        for col in df.columns:
            if col in ['timestamp', 'timeframe', 'symbol']:
                continue  # Залишаємо рядки як є
            elif df[col].dtype == 'object':
                continue
            elif df[col].dtype == 'float64':
                # Зменшуємо точність float для економії пам'яті
                df[col] = pd.to_numeric(df[col], downcast='float')
            elif df[col].dtype in ['int64', 'int32']:
                # Оптимізуємо integer типи
                df[col] = pd.to_numeric(df[col], downcast='integer')
        
        # Додаємо до трекінгу
        self._active_dataframes[id(df)] = df
        
        self.logger.debug(f"Створено оптимізований DataFrame: {df.memory_usage(deep=True).sum() / 1024:.1f}KB")
        return df
    
    async def _stream_data_generator(self, symbol: str, chunk_size: int) -> AsyncGenerator[pd.DataFrame, None]:
        """Генератор для стрімінгу даних чанками"""
        try:
            total_candles = self.trading_config['load_candles_amount']
            processed = 0
            
            while processed < total_candles:
                current_chunk_size = min(chunk_size, total_candles - processed)
                
                # Отримуємо дані чанками з API
                klines = await self.api_manager.get_klines(
                    symbol=symbol,
                    interval=self.trading_config['timeframe'],
                    limit=current_chunk_size
                )
                
                if not klines:
                    break
                
                # Створюємо оптимізований DataFrame
                df_chunk = self._create_memory_efficient_dataframe(klines)
                
                if not df_chunk.empty:
                    yield df_chunk
                
                processed += len(klines)
                
                # Моніторинг пам'яті після кожного чанку
                self._monitor_memory_usage()
                
                # Невелика пауза для запобігання перевантаженню API
                await asyncio.sleep(0.1)
                
        except Exception as e:
            self.logger.error(f"Помилка в генераторі стрімінгу для {symbol}: {e}")
    
    def _chunk_dataframe_processing(self, df: pd.DataFrame, chunk_size: int) -> Generator[pd.DataFrame, None, None]:
        """Генератор для обробки DataFrame чанками"""
        total_rows = len(df)
        
        for start_idx in range(0, total_rows, chunk_size):
            end_idx = min(start_idx + chunk_size, total_rows)
            chunk = df.iloc[start_idx:end_idx].copy()
            
            # Додаємо до трекінгу
            self._active_dataframes[id(chunk)] = chunk
            
            yield chunk
            
            # Очищуємо чанк після обробки
            del chunk
            
        # Принудове збирання сміття після обробки всіх чанків
        gc.collect()
        
    async def initialize(self):
        """Ініціалізація компонентів"""
        try:
            await self.db_manager.initialize()
            
            time_sync = await self.api_manager.check_time_sync()
            if not time_sync:
                self.logger.warning("Час не синхронізовано з сервером!")
            
            valid_symbols = await self.api_manager.validate_symbols()
            invalid_symbols = [symbol for symbol, is_valid in valid_symbols.items() if not is_valid]
            
            if invalid_symbols:
                self.logger.warning(f"Недоступні символи: {invalid_symbols}")
            
            self.logger.info("Data Preprocessor повністю ініціалізовано (метод initialize)")
            
        except Exception as e:
            self.logger.error(f"Помилка ініціалізації Data Preprocessor: {e}", exc_info=True)
            raise
    
    async def load_initial_data(self):
        """Завантаження початкових даних з оптимізацією пам'яті"""
        try:
            for symbol in self.trading_config['trade_pairs']:
                self.logger.info(f"Завантаження початкових даних для {symbol} (streaming mode)")
                
                # Використовуємо стрімінг замість завантаження всіх даних одразу
                total_processed = 0
                async for chunk_df in self._stream_data_generator(symbol, self.chunk_size):
                    if chunk_df.empty:
                        continue
                    
                    await self._process_and_store_data_chunked(symbol, chunk_df)
                    total_processed += len(chunk_df)
                    
                    # Очищуємо чанк після обробки
                    del chunk_df
                    
                    self.logger.debug(f"{symbol}: оброблено {total_processed} записів")
                
                # Пауза між символами для запобігання перевантаженню
                await asyncio.sleep(0.2)
                
                # Моніторинг пам'яті після кожного символу
                self._monitor_memory_usage()
            
            self.logger.info("Початкові дані завантажено з оптимізацією пам'яті")
            
        except Exception as e:
            self.logger.error(f"Помилка завантаження початкових даних: {e}", exc_info=True)
    
    async def _process_and_store_data_chunked(self, symbol: str, df: pd.DataFrame):
        """Обробка та збереження даних чанками"""
        try:
            if df.empty:
                return
            
            # Валідація та очищення даних
            df_cleaned = self._validate_and_clean_data_optimized(df)
            
            # ✅ ЗБІЛЬШЕНО: Підвищено мінімальні вимоги до даних для надійних індикаторів
            min_required_candles = max(
                DISPLAY_CONFIG['base_minimum_records'],  # Базовий мінімум
                INDICATORS_CONFIG.get('slow_ma', 21) + 10,  # EMA + буфер
                INDICATORS_CONFIG.get('atr_length', 14) + 10,  # ATR + буфер
                INDICATORS_CONFIG.get('rsi_length', 14) + 10,  # RSI + буфер
                INDICATORS_CONFIG.get('volume_lookback', 5) * 3  # Volume lookback * 3
            )
            
            if df_cleaned.empty or len(df_cleaned) < min_required_candles:
                self.logger.warning(
                    f"Недостатньо даних для {symbol} після очищення: {len(df_cleaned)} записів. "
                    f"Мінімум потрібно: {min_required_candles} для надійних індикаторів"
                )
                return
            
            # Обробляємо дані чанками для економії пам'яті
            for chunk in self._chunk_dataframe_processing(df_cleaned, self.chunk_size):
                # Розрахунок індикаторів для чанку
                indicators_data = self.indicators.calculate_all_indicators(chunk)
                
                if not indicators_data:
                    self.logger.warning(f"Не вдалося розрахувати індикатори для чанку {symbol}")
                    continue
                
                # ✅ ДОДАНО: Валідація цілісності індикаторів перед збереженням
                missing_indicators = self._validate_indicators_completeness(indicators_data, len(chunk), symbol)
                if missing_indicators:
                    self.logger.warning(
                        f"Виявлено пропуски в індикаторах для {symbol}: {missing_indicators}. "
                        f"Застосовую заповнення пропусків..."
                    )
                    indicators_data = self._fill_indicator_gaps(indicators_data, len(chunk))
                
                # Підготовка даних для batch insert
                batch_data = []
                for i in range(len(chunk)):
                    candle_data = {
                        'timestamp': chunk.iloc[i]['timestamp'],
                        'timeframe': self.trading_config['timeframe'],
                        'open_price': float(chunk.iloc[i]['open_price']),
                        'high_price': float(chunk.iloc[i]['high_price']),
                        'low_price': float(chunk.iloc[i]['low_price']),
                        'close_price': float(chunk.iloc[i]['close_price']),
                        'volume': float(chunk.iloc[i]['volume'])
                    }
                    
                    # Додаємо індикатори з покращеною обробкою пропусків
                    for indicator_name, indicator_values in indicators_data.items():
                        if i < len(indicator_values):
                            value = indicator_values[i]
                            # ✅ ПОКРАЩЕНО: Краща обробка пропущених значень
                            if pd.isna(value) or np.isinf(value):
                                value = self._get_default_indicator_value(indicator_name)
                            if isinstance(value, (bool, np.bool_)):
                                value = 1 if value else 0
                            candle_data[indicator_name] = value
                        else:
                            # ✅ ДОДАНО: Застосовуємо дефолтне значення замість None
                            candle_data[indicator_name] = self._get_default_indicator_value(indicator_name)
                    
                    batch_data.append(candle_data)
                
                # Використовуємо batch insert замість окремих вставок
                for data in batch_data:
                    await self.db_manager.add_to_batch(symbol, data)
                
                # Очищуємо batch_data після використання
                del batch_data
                gc.collect()
            
            # Примусово очищуємо batch після обробки символу
            await self.db_manager.flush_all_batches()
            
            self.logger.debug(f"Дані для {symbol} оброблено та збережено (chunked)")
            
        except Exception as e:
            self.logger.error(f"Помилка обробки чанкових даних для {symbol}: {e}")
            self.logger.error(f"Трасування помилки: {traceback.format_exc()}")
    
    def _validate_and_clean_data_optimized(self, df: pd.DataFrame) -> pd.DataFrame:
        """Оптимізована валідація та очищення даних з меншим використанням пам'яті"""
        try:
            original_count = len(df)
            
            # Використовуємо in-place операції для економії пам'яті
            df = df.copy()  # Робимо копію для безпечної модифікації
            
            # Видаляємо рядки з пропущеними обов'язковими полями
            required_columns = ['open_price', 'high_price', 'low_price', 'close_price', 'volume', 'timestamp']
            df.dropna(subset=required_columns, inplace=True)
            
            # Конвертуємо типи даних з обробкою помилок
            numeric_columns = ['open_price', 'high_price', 'low_price', 'close_price', 'volume']
            for col in numeric_columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Видаляємо рядки, де конвертація не вдалася
            df.dropna(subset=numeric_columns, inplace=True)
            
            # Фільтруємо неприпустимі значення
            price_columns = ['open_price', 'high_price', 'low_price', 'close_price']
            for col in price_columns:
                df = df[df[col] > 0]
            
            df = df[df['volume'] >= 0]
            
            # Перевіряємо логічні співвідношення цін
            df = df[df['high_price'] >= df['low_price']]
            df = df[df['high_price'] >= df['open_price']]
            df = df[df['high_price'] >= df['close_price']]
            df = df[df['low_price'] <= df['open_price']]
            df = df[df['low_price'] <= df['close_price']]
            
            # Видаляємо дублікати
            df.drop_duplicates(subset=['timestamp'], keep='last', inplace=True)
            df.sort_values('timestamp', inplace=True)
            df.reset_index(drop=True, inplace=True)
            
            if len(df) < original_count:
                self.logger.debug(f"Дані очищено: було {original_count}, залишилося {len(df)} записів")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Помилка валідації даних: {e}", exc_info=True)
            return pd.DataFrame()  # Повертаємо порожній DataFrame у разі серйозної помилки
    
    async def update_latest_data(self):
        """Оновлення останніх даних з оптимізацією пам'яті"""
        try:
            for symbol in self.trading_config['trade_pairs']:
                # Отримуємо тільки необхідну кількість нових свічок
                latest_kline_list = await self.api_manager.get_klines(
                    symbol=symbol,
                    interval=self.trading_config['timeframe'],
                    limit=5  # Зменшено з 2 до 5 для більшої надійності
                )
                
                if not latest_kline_list or len(latest_kline_list) < 1:
                    self.logger.warning(f"Не отримано останню свічку для {symbol} під час оновлення.")
                    continue
                
                # Використовуємо передостанню свічку, якщо вона є
                kline_to_check = latest_kline_list[-2] if len(latest_kline_list) > 1 else latest_kline_list[-1]
                
                # Перевіряємо чи потрібно оновлення (memory-efficient query)
                should_update = await self._check_update_needed(symbol, kline_to_check['timestamp'])
                
                if should_update:
                    self.logger.info(f"Оновлення даних для {symbol}")
                    
                    # Завантажуємо мінімальну кількість даних для перерахунку індикаторів
                    num_candles_for_recalc = min(DISPLAY_CONFIG['recalc_candles_limit'], INDICATORS_CONFIG.get('slow_ma', 21) + DISPLAY_CONFIG['num_candles_for_recalc_base'])
                    recent_klines = await self.api_manager.get_klines(
                        symbol=symbol,
                        interval=self.trading_config['timeframe'],
                        limit=num_candles_for_recalc 
                    )
                    
                    if recent_klines:
                        # Використовуємо streaming для обробки навіть невеликих обновлень
                        df_chunk = self._create_memory_efficient_dataframe(recent_klines)
                        await self._process_and_store_data_chunked(symbol, df_chunk)
                        
                        # Очищуємо після обробки
                        del df_chunk, recent_klines
                        gc.collect()
                
                await asyncio.sleep(0.1)
                
                # Моніторинг пам'яті після кожного символу
                self._monitor_memory_usage()
            
        except Exception as e:
            self.logger.error(f"Помилка оновлення даних: {e}", exc_info=True)
    
    async def _check_update_needed(self, symbol: str, new_timestamp: str) -> bool:
        """Перевірка чи потрібно оновлення (memory-efficient)"""
        try:
            # Використовуємо LIMIT 1 для мінімального навантаження на пам'ять
            last_db_data_df = await self.db_manager.get_latest_candles(
                symbol=symbol,
                timeframe=self.trading_config['timeframe'],
                limit=1
            )
            
            if last_db_data_df.empty or 'timestamp' not in last_db_data_df.columns:
                return True
            
            last_db_timestamp_str = last_db_data_df.iloc[-1]['timestamp']
            should_update = new_timestamp > last_db_timestamp_str
            
            # Очищуємо DataFrame після використання
            del last_db_data_df
            gc.collect()
            
            return should_update
            
        except Exception as e:
            self.logger.error(f"Помилка перевірки необхідності оновлення для {symbol}: {e}")
            return True  # При помилці краще оновити
    
    async def validate_indicators_integrity(self, symbol: str) -> bool:
        """Валідація цілісності індикаторів з оптимізацією пам'яті"""
        try:
            self.logger.info(f"Запуск перевірки цілісності індикаторів для {symbol}")
            
            integrity_result = await self.db_manager.check_data_integrity(
                symbol=symbol,
                timeframe=self.trading_config['timeframe']
            )
            
            integrity_ok = integrity_result.get('integrity_ok', False)
            message = integrity_result.get('message', 'Немає деталей від db_manager.')
            
            if integrity_ok:
                self.logger.info(f"Цілісність індикаторів для {symbol} підтверджена: {message}")
                
                # ✅ ПОКРАЩЕНО: Збільшено мінімальні вимоги для надійності стратегії
                min_candles_for_strategy = max(
                    DISPLAY_CONFIG['min_candles_for_strategy'],  # Мінімум свічок для стратегії
                    INDICATORS_CONFIG.get('slow_ma', 21) * 2,  # EMA * 2
                    INDICATORS_CONFIG.get('atr_length', 14) * 3,  # ATR * 3 
                    STRATEGY_CONFIG.get('regime_period', 20) + 30  # Regime period + буфер
                )
                
                latest_data_check = await self.db_manager.get_candles_for_analysis(
                    symbol=symbol,
                    timeframe=self.trading_config['timeframe'],
                    limit=min_candles_for_strategy 
                )
                
                has_enough_data = not latest_data_check.empty and len(latest_data_check) >= min_candles_for_strategy
                
                # ✅ ДОДАНО: Додаткова перевірка якості даних
                if has_enough_data:
                    # Перевіряємо чи є пропуски в ключових індикаторах останніх записів
                    recent_data = latest_data_check.tail(min(30, len(latest_data_check)))  # Останні 30 записів
                    key_indicators = ['rsi', 'atr', 'ema_fast', 'ema_slow', 'adx']
                    
                    missing_count = 0
                    for indicator in key_indicators:
                        if indicator in recent_data.columns:
                            null_count = recent_data[indicator].isnull().sum()
                            missing_count += null_count
                    
                    data_quality_ok = missing_count <= len(recent_data) * 0.1  # Не більше 10% пропусків
                    
                    if not data_quality_ok:
                        self.logger.warning(
                            f"Для {symbol} достатньо даних ({len(latest_data_check)}), але якість незадовільна: "
                            f"{missing_count} пропусків в ключових індикаторах останніх {len(recent_data)} записів"
                        )
                        has_enough_data = False
                    else:
                        self.logger.info(
                            f"Якість даних для {symbol} підтверджена: {missing_count} пропусків в {len(recent_data)} записах"
                        )
                
                # Очищуємо DataFrame після перевірки
                del latest_data_check
                gc.collect()
                
                if not has_enough_data:
                    self.logger.warning(f"Для {symbol} цілісність ОК, але недостатньо якісних останніх свічок.")
                    return False
                    
                self.logger.info(f"Достатня кількість та якість останніх свічок для {symbol} підтверджена")
                return True
            else:
                self.logger.warning(f"Цілісність індикаторів для {symbol} НЕ підтверджена: {message}")
                return False

        except Exception as e:
            self.logger.error(f"Помилка перевірки цілісності індикаторів для {symbol}: {e}", exc_info=True)
            return False
            
    def calculate_next_update_time(self) -> datetime:
        now_utc = datetime.now(timezone.utc)
        timeframe_minutes = int(self.trading_config['timeframe'])
        
        # Розрахунок наступного повного інтервалу
        # Скільки хвилин пройшло в поточній годині відносно початку інтервалів
        minutes_past_hour_interval_aligned = (now_utc.minute // timeframe_minutes) * timeframe_minutes
        
        # Початок поточного інтервалу
        current_interval_start = now_utc.replace(minute=minutes_past_hour_interval_aligned, second=0, microsecond=0)
        
        # Наступний інтервал
        next_interval_start = current_interval_start + timedelta(minutes=timeframe_minutes)
        
        # Якщо поточний час вже після розрахованого початку наступного інтервалу (або дуже близько),
        # це означає, що ми маємо планувати на ще один інтервал вперед.
        # Це може статися, якщо, наприклад, зараз 10:04:58, а інтервал 5 хв.
        # current_interval_start = 10:00:00, next_interval_start = 10:05:00. Це коректно.
        # Якщо зараз 10:05:02, current_interval_start = 10:05:00, next_interval_start = 10:10:00.
        
        # Якщо `now_utc` вже більше або дорівнює `next_interval_start` (що малоймовірно, якщо логіка правильна),
        # або якщо `next_interval_start` занадто близький до `now_utc` (наприклад, менше 5-10 секунд),
        # то краще взяти наступний інтервал, щоб уникнути пропуску.
        if (next_interval_start - now_utc).total_seconds() < 10: # Буфер в 10 секунд
            # self.logger.debug(f"Розрахований час {next_interval_start.strftime('%H:%M:%S')} близький до поточного {now_utc.strftime('%H:%M:%S')}. Зсув на наступний інтервал.")
            next_interval_start += timedelta(minutes=timeframe_minutes)

        # self.logger.debug(f"Розраховано наступний час оновлення (calculate_next_update_time): {next_interval_start.strftime('%Y-%m-%d %H:%M:%S %Z')}")
        return next_interval_start
    
    async def start_continuous_update(self):
        """Запуск безперервного оновлення даних з оптимізацією пам'яті"""
        self.is_running = True
        self.logger.info("Запущено безперервне оновлення даних preprocessor з оптимізаціями пам'яті")
        
        while self.is_running:
            try:
                next_update_time_target = self.calculate_next_update_time()
                now_utc = datetime.now(timezone.utc)
                
                sleep_seconds = (next_update_time_target - now_utc).total_seconds()
                
                if sleep_seconds > 0: 
                    self.logger.info(f"Preprocessor: Наступне оновлення даних о {next_update_time_target.strftime('%Y-%m-%d %H:%M:%S %Z')}. Очікування: {sleep_seconds:.0f}с")
                    await asyncio.sleep(sleep_seconds)
                else:
                    self.logger.warning(f"Preprocessor: Розрахований час оновлення вже минув. Затримка 1с.")
                    await asyncio.sleep(1)
                    continue

                if self.is_running: 
                    self.logger.info(f"Preprocessor: Час оновлювати дані ({datetime.now(timezone.utc).strftime('%H:%M:%S')})")
                    await self.update_latest_data()
                    
                    # Принудове очищення пам'яті після кожного циклу оновлення
                    self._cleanup_memory()
                
            except asyncio.CancelledError:
                self.logger.info("Цикл оновлення даних preprocessor скасовано.")
                self.is_running = False
                break 
            except Exception as e:
                self.logger.error(f"Помилка в циклі оновлення даних preprocessor: {e}", exc_info=True)
                await asyncio.sleep(60)
    
    async def stop_continuous_update(self):
        """Зупинка безперервного оновлення"""
        self.is_running = False
        self.logger.info("Зупинено безперервне оновлення даних preprocessor")
    
    async def close(self):
        """Закриття preprocessor з повним очищенням ресурсів"""
        try:
            await self.stop_continuous_update()
            
            # Очищуємо всі активні DataFrame
            for df in list(self._active_dataframes.values()):
                try:
                    if hasattr(df, 'memory_usage'):
                        del df
                except:
                    pass
            
            # Очищуємо буфери
            self._stream_buffer.clear()
            
            # Очищуємо все в db_manager якщо він має batch data
            if hasattr(self.db_manager, 'flush_all_batches'):
                await self.db_manager.flush_all_batches()
            
            # Примусове збирання сміття
            gc.collect()
            
            self.logger.info("Data Preprocessor закрито з повним очищенням пам'яті")
            
        except Exception as e:
            self.logger.error(f"Помилка при закритті Data Preprocessor: {e}")
    
    def get_memory_stats(self) -> Dict[str, Any]:
        """Отримання статистики використання пам'яті"""
        try:
            current_time = datetime.now().timestamp()
            stats = {
                'active_dataframes': len(self._active_dataframes),
                'stream_buffer_size': len(self._stream_buffer),
                'chunk_size': self.chunk_size,
                'max_memory_usage_mb': self.max_memory_usage_mb,
                'critical_memory_usage_mb': self.critical_memory_usage_mb,
                'memory_monitor_enabled': self._memory_monitor_enabled,
                'cleanup_cooldown_seconds': self._cleanup_cooldown_seconds,
                'time_since_last_cleanup': current_time - self._last_cleanup_time if self._last_cleanup_time > 0 else None,
                'last_cleanup_memory_before': self._memory_usage_before_cleanup,
                'last_cleanup_memory_after': self._memory_usage_after_cleanup,
                'last_cleanup_memory_freed': self._memory_usage_before_cleanup - self._memory_usage_after_cleanup if self._memory_usage_before_cleanup > 0 and self._memory_usage_after_cleanup > 0 else 0
            }
            
            # Якщо psutil доступний, додаємо реальну статистику пам'яті
            psutil = self._get_psutil_module()
            if psutil:
                try:
                    process = psutil.Process()
                    memory_info = process.memory_info()
                    stats.update({
                        'current_memory_mb': memory_info.rss / 1024 / 1024,
                        'memory_percent': process.memory_percent(),
                        'memory_status': 'critical' if memory_info.rss / 1024 / 1024 > self.critical_memory_usage_mb 
                                       else 'high' if memory_info.rss / 1024 / 1024 > self.max_memory_usage_mb 
                                       else 'normal'
                    })
                except Exception as e:
                    self.logger.debug(f"Помилка отримання статистики psutil: {e}")
            
            return stats
            
        except Exception as e:
            self.logger.error(f"Помилка отримання статистики пам'яті: {e}")
            return {}

    def _validate_indicators_completeness(self, indicators_data: Dict, expected_length: int, symbol: str) -> List[str]:
        """
        ✅ ПОКРАЩЕНО: Валідація повноти індикаторів з урахуванням періодів розігріву
        
        Args:
            indicators_data: Словник з індикаторами
            expected_length: Очікувана довжина даних
            symbol: Символ для логування
            
        Returns:
            Список назв індикаторів з пропусками
        """
        missing_indicators = []
        
        # Важливі індикатори, які потрібно перевірити в першу чергу
        critical_indicators = ['rsi', 'ema_fast', 'ema_slow', 'macd_line', 'macd_signal', 'macd_histogram', 'atr']
        
        # Отримуємо періоди розігріву з конфігурації
        warmup_periods = INDICATORS_CONFIG.get('warmup_periods', {})
        
        for indicator_name, values in indicators_data.items():
            if values is None:
                missing_indicators.append(f"{indicator_name}(None)")
                continue
                
            if len(values) == 0:
                missing_indicators.append(f"{indicator_name}(empty)")
                continue
                
            if len(values) != expected_length:
                missing_indicators.append(f"{indicator_name}(len:{len(values)} != {expected_length})")
                continue
                
            # Перевіряємо кількість пропущених значень з урахуванням періоду розігріву
            if isinstance(values, np.ndarray):
                nan_count = sum(1 for v in values if pd.isna(v) or np.isinf(v))
                nan_percentage = (nan_count / expected_length) * 100
                
                # Отримуємо очікуваний період розігріву для індикатора
                expected_warmup = warmup_periods.get(indicator_name, 0)
                
                # Розраховуємо допустиму кількість NaN значень
                if expected_warmup > 0 and expected_length > expected_warmup:
                    # Для індикаторів з періодом розігріву: дозволяємо NaN в період розігріву + невеликий буфер
                    max_allowed_nan = expected_warmup + min(5, expected_length * 0.02)  # +5 або 2% буфер
                else:
                    # Для інших індикаторів: використовуємо старий підхід з відсотковими порогами
                    threshold = 0.05 if indicator_name in critical_indicators else 0.1  # 5% vs 10%
                    max_allowed_nan = expected_length * threshold
                
                if nan_count > max_allowed_nan:
                    if expected_warmup > 0:
                        missing_indicators.append(f"{indicator_name}({nan_count}/{expected_length} NaN, {nan_percentage:.1f}%, очікувано ≤{int(max_allowed_nan)})")
                    else:
                        missing_indicators.append(f"{indicator_name}({nan_count}/{expected_length} NaN, {nan_percentage:.1f}%)")
        
        if missing_indicators:
            # Більш детальне логування з урахуванням періодів розігріву
            critical_missing = [ind for ind in missing_indicators if any(crit in ind for crit in critical_indicators)]
            if critical_missing:
                # Перевіряємо чи це справді критична помилка або просто період розігріву
                actual_critical = []
                for missing in critical_missing:
                    indicator_name = missing.split('(')[0]
                    if warmup_periods.get(indicator_name, 0) == 0:
                        # Індикатор без періоду розігріву - справді критично
                        actual_critical.append(missing)
                
                if actual_critical:
                    self.logger.error(f"Критичні індикатори з пропусками для {symbol}: {actual_critical}")
                else:
                    self.logger.warning(f"Індикатори з надмірними пропусками для {symbol}: {critical_missing}")
            else:
                self.logger.debug(f"Некритичні індикатори з пропусками для {symbol}: {missing_indicators}")
        
        return missing_indicators

    def _fill_indicator_gaps(self, indicators_data: Dict, expected_length: int) -> Dict:
        """
        ✅ ДОДАНО: Заповнення пропусків в індикаторах
        
        Args:
            indicators_data: Словник з індикаторами
            expected_length: Очікувана довжина
            
        Returns:
            Оновлений словник з заповненими пропусками
        """
        filled_data = {}
        
        for indicator_name, values in indicators_data.items():
            if values is None or len(values) == 0:
                # Створюємо масив з дефолтними значеннями
                default_value = self._get_default_indicator_value(indicator_name)
                filled_data[indicator_name] = [default_value] * expected_length
                continue
            
            filled_values = list(values)
            
            # Розширюємо до потрібної довжини
            while len(filled_values) < expected_length:
                filled_values.append(self._get_default_indicator_value(indicator_name))
            
            # Заповнюємо NaN значення
            for i in range(len(filled_values)):
                if pd.isna(filled_values[i]) or np.isinf(filled_values[i]):
                    # Використовуємо попереднє валідне значення або дефолтне
                    if i > 0 and not (pd.isna(filled_values[i-1]) or np.isinf(filled_values[i-1])):
                        filled_values[i] = filled_values[i-1]
                    else:
                        filled_values[i] = self._get_default_indicator_value(indicator_name)
            
            filled_data[indicator_name] = filled_values
        
        return filled_data

    def _get_default_indicator_value(self, indicator_name: str):
        """
        ✅ ДОДАНО: Отримання дефолтного значення для індикатора
        
        Args:
            indicator_name: Назва індикатора
            
        Returns:
            Дефолтне значення для даного типу індикатора
        """
        # Булеві індикатори
        boolean_indicators = [
            'rsi_rising_1', 'rsi_rising_2', 'rsi_falling_1', 'rsi_falling_2',
            'ema_bullish', 'ema_bearish', 'macd_bullish', 'macd_bearish',
            'volume_filter', 'volume_surge', 'super_volume_surge', 'sustained_volume',
            'bullish_trend', 'bearish_trend', 'bullish_vol_divergence', 'bearish_vol_divergence',
            'buy_signal', 'sell_signal'
        ]
        
        # Лічильники (цілі числа)
        counter_indicators = [
            'long_confirmations', 'short_confirmations'
        ]
        
        # RSI індикатори
        rsi_indicators = ['rsi']
        
        # ATR індикатори  
        atr_indicators = ['atr']
        
        if indicator_name in boolean_indicators:
            return 0
        elif indicator_name in counter_indicators:
            return 0
        elif indicator_name in rsi_indicators:
            return DISPLAY_CONFIG['rsi_neutral_value']  # Нейтральне значення RSI
        elif indicator_name in atr_indicators:
            return 0.00001  # Мінімальне значення ATR
        elif 'ema' in indicator_name.lower() or 'ma' in indicator_name.lower():
            return 0.0  # Для moving averages
        elif 'adx' in indicator_name.lower():
            return 20.0  # Нейтральне значення ADX
        elif 'macd' in indicator_name.lower():
            return 0.0  # Нейтральне значення MACD
        else:
            return 0.0  # Дефолтне значення для інших індикаторів