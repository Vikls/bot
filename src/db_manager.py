# -*- coding: utf-8 -*-
"""
Менеджер бази даних SQLite для торгового бота (ВИПРАВЛЕНА ВЕРСІЯ)
"""

import aiosqlite
import asyncio
import pandas as pd
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
from contextlib import asynccontextmanager
from collections import defaultdict
import weakref
from config.settings import (
    DATABASE_CONFIG, TRADING_CONFIG, SYSTEM_CONFIG, 
    PRECISION_CONFIG, DISPLAY_CONFIG
)

logger = logging.getLogger(__name__)

class DatabaseManager:
    """Асинхронний менеджер бази даних SQLite з connection pooling та оптимізаціями"""
    
    def __init__(self, db_path: str = None, pool_size: int = 10):
        self.db_path = db_path or DATABASE_CONFIG['db_path']
        self.connection_timeout = DATABASE_CONFIG['connection_timeout']
        self.pool_size = pool_size
        self.symbols = TRADING_CONFIG['trade_pairs']
        
        # Connection pool management
        self._pool = asyncio.Queue(maxsize=pool_size)
        self._pool_initialized = False
        self._pool_lock = asyncio.Lock()
        
        # Prepared statements cache
        self._prepared_statements = {}
        self._batch_buffer = defaultdict(list)
        self._batch_size = SYSTEM_CONFIG['chunk_size_large']  # Configurable batch size
        self._batch_lock = asyncio.Lock()
        
        # Connection tracking for cleanup
        self._active_connections = weakref.WeakSet()
        
    async def _create_connection(self) -> aiosqlite.Connection:
        """Створення нового з'єднання з оптимізованими параметрами"""
        conn = await aiosqlite.connect(
            self.db_path,
            timeout=self.connection_timeout,
            # Performance optimizations
            isolation_level=None,  # Autocommit mode for better performance
        )
        
        # Apply SQLite performance optimizations
        await conn.execute("PRAGMA journal_mode=WAL")  # Write-Ahead Logging
        await conn.execute("PRAGMA synchronous=NORMAL")  # Balanced durability/performance
        await conn.execute(f"PRAGMA cache_size={SYSTEM_CONFIG['max_request_limit']}")  # Larger cache
        await conn.execute("PRAGMA temp_store=MEMORY")  # Store temp tables in memory
        await conn.execute("PRAGMA mmap_size=268435456")  # Memory-mapped I/O (256MB)
        
        self._active_connections.add(conn)
        return conn
    
    async def _init_connection_pool(self):
        """Ініціалізація пулу з'єднань"""
        if self._pool_initialized:
            return
            
        async with self._pool_lock:
            if self._pool_initialized:
                return
                
            for _ in range(self.pool_size):
                conn = await self._create_connection()
                await self._pool.put(conn)
            
            self._pool_initialized = True
            logger.info(f"Ініціалізовано пул з'єднань БД з {self.pool_size} з'єднань")
    
    @asynccontextmanager
    async def get_connection(self):
        """Context manager для отримання з'єднання з пулу"""
        if not self._pool_initialized:
            await self._init_connection_pool()
        
        conn = None
        try:
            conn = await asyncio.wait_for(self._pool.get(), timeout=5.0)
            yield conn
        except asyncio.TimeoutError:
            # Create temporary connection if pool is exhausted
            logger.warning("Пул з'єднань вичерпано, створюємо тимчасове з'єднання")
            conn = await self._create_connection()
            yield conn
        finally:
            if conn:
                try:
                    if not self._pool.full():
                        await self._pool.put(conn)
                    else:
                        await conn.close()
                except:
                    # Connection might be closed already
                    pass
        
    async def initialize(self):
        """Ініціалізація бази даних з оптимізаціями"""
        try:
            await self._init_connection_pool()
            await self._create_tables()
            await self._create_optimized_indexes()
            logger.info("База даних успішно ініціалізована з оптимізаціями")
        except Exception as e:
            logger.error(f"Помилка ініціалізації бази даних: {e}")
            raise
    
    def _get_table_name(self, symbol: str) -> str:
        """Отримання назви таблиці для символу"""
        # Замінюємо спеціальні символи на підкреслення
        clean_symbol = symbol.replace('/', '_').replace('-', '_')
        return f"candles_{clean_symbol.lower()}"
    
    async def _create_tables(self):
        """Створення окремих таблиць для кожної пари та таблиці для статистики"""
        async with self.get_connection() as db:
            for symbol in self.symbols:
                table_name = self._get_table_name(symbol)
                
                # Створення таблиці для символу (ваш існуючий код)
                await db.execute(f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp TEXT NOT NULL UNIQUE,
                        symbol TEXT NOT NULL,
                        timeframe TEXT NOT NULL,
                        open_price REAL NOT NULL,
                        high_price REAL NOT NULL,
                        low_price REAL NOT NULL,
                        close_price REAL NOT NULL,
                        volume REAL NOT NULL,
                        
                        -- RSI індикатор
                        rsi REAL,
                        rsi_rising_1 INTEGER DEFAULT 0,
                        rsi_rising_2 INTEGER DEFAULT 0,
                        rsi_falling_1 INTEGER DEFAULT 0,
                        rsi_falling_2 INTEGER DEFAULT 0,
                        
                        -- EMA індикатори
                        ema_fast REAL,
                        ema_slow REAL,
                        ema_trend REAL,
                        ema_bullish INTEGER DEFAULT 0,
                        ema_bearish INTEGER DEFAULT 0,
                        
                        -- MACD індикатори
                        macd_line REAL,
                        macd_signal REAL,
                        macd_histogram REAL,
                        macd_bullish INTEGER DEFAULT 0,
                        macd_bearish INTEGER DEFAULT 0,
                        
                        -- ADX індикатор
                        adx REAL,
                        plus_di REAL,
                        minus_di REAL,
                        
                        -- ATR індикатор
                        atr REAL,
                        
                        -- Volume індикатори
                        volume_sma REAL,
                        volume_filter INTEGER DEFAULT 0,
                        volume_surge INTEGER DEFAULT 0,
                        super_volume_surge INTEGER DEFAULT 0,
                        volume_trend REAL,
                        sustained_volume INTEGER DEFAULT 0,
                        
                        -- Тренд індикатори
                        bullish_trend INTEGER DEFAULT 0,
                        bearish_trend INTEGER DEFAULT 0,
                        
                        -- Volume дивергенція
                        bullish_vol_divergence INTEGER DEFAULT 0,
                        bearish_vol_divergence INTEGER DEFAULT 0,
                        
                        -- Стратегічні сигнали
                        long_confirmations INTEGER DEFAULT 0,
                        short_confirmations INTEGER DEFAULT 0,
                        buy_signal INTEGER DEFAULT 0,
                        sell_signal INTEGER DEFAULT 0,
                        
                        created_at TEXT DEFAULT CURRENT_TIMESTAMP
                    )
                """)

            # Додано: Створення таблиці для загальної статистики торгівлі
            await db.execute("""
                CREATE TABLE IF NOT EXISTS bot_trade_statistics (
                    id INTEGER PRIMARY KEY DEFAULT 1, -- Завжди оновлюємо один запис
                    last_updated_timestamp TEXT NOT NULL,
                    total_trades INTEGER DEFAULT 0,
                    winning_trades INTEGER DEFAULT 0,
                    losing_trades INTEGER DEFAULT 0,
                    total_pnl REAL DEFAULT 0.0,
                    initial_balance REAL 
                )
            """)
            
            await db.commit()
            logger.info("Всі таблиці (включаючи bot_trade_statistics) створено/перевірено успішно")
    
    async def _create_optimized_indexes(self):
        """Створення оптимізованих індексів для кращої продуктивності"""
        async with self.get_connection() as db:
            for symbol in self.symbols:
                table_name = self._get_table_name(symbol)
                
                # Створення композитних індексів для кращої продуктивності
                await db.execute(f"""
                    CREATE INDEX IF NOT EXISTS idx_{table_name}_timestamp_timeframe 
                    ON {table_name}(timestamp, timeframe)
                """)
                
                await db.execute(f"""
                    CREATE INDEX IF NOT EXISTS idx_{table_name}_symbol_timestamp 
                    ON {table_name}(symbol, timestamp DESC)
                """)
                
                # Індекс для queries з сигналами
                await db.execute(f"""
                    CREATE INDEX IF NOT EXISTS idx_{table_name}_signals 
                    ON {table_name}(timeframe, buy_signal, sell_signal, timestamp DESC)
                """)
                
                # Індекс для індикаторів (часто використовуються разом)
                await db.execute(f"""
                    CREATE INDEX IF NOT EXISTS idx_{table_name}_indicators 
                    ON {table_name}(timeframe, timestamp DESC) 
                    WHERE rsi IS NOT NULL AND ema_fast IS NOT NULL
                """)

            await db.commit()
            logger.info("Оптимізовані індекси створено успішно")

    async def save_trade_stats(self, trade_stats: dict):
        """Зберігає або оновлює загальну статистику торгівлі в БД."""
        try:
            async with self.get_connection() as db:
                # Використовуємо INSERT OR REPLACE для простоти, оскільки у нас один запис
                # або ON CONFLICT DO UPDATE, якщо ви віддаєте перевагу UPSERT
                await db.execute(
                    """
                    INSERT OR REPLACE INTO bot_trade_statistics 
                    (id, last_updated_timestamp, total_trades, winning_trades, losing_trades, total_pnl, initial_balance)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        1,  # Фіксований ID для єдиного запису статистики
                        datetime.now(timezone.utc).isoformat(),
                        trade_stats.get('total_trades', 0),
                        trade_stats.get('winning_trades', 0),
                        trade_stats.get('losing_trades', 0),
                        trade_stats.get('total_pnl', 0.0),
                        trade_stats.get('initial_balance') # Може бути None
                    )
                )
                await db.commit()
                logger.info(f"Статистика торгівлі успішно збережена в БД: {trade_stats}")
        except Exception as e:
            logger.error(f"Помилка збереження статистики торгівлі в БД: {e}", exc_info=True)
    
    async def add_to_batch(self, symbol: str, data: Dict[str, Any]):
        """Додати дані до батчу для пакетної вставки"""
        async with self._batch_lock:
            self._batch_buffer[symbol].append(data)
            
            # Якщо батч досяг максимального розміру, обробити його
            if len(self._batch_buffer[symbol]) >= self._batch_size:
                await self._flush_batch(symbol)
    
    async def _flush_batch(self, symbol: str):
        """Вставка всіх даних з батчу для символу"""
        if not self._batch_buffer[symbol]:
            return
            
        batch_data = self._batch_buffer[symbol].copy()
        self._batch_buffer[symbol].clear()
        
        try:
            await self._batch_insert_candle_data(symbol, batch_data)
            logger.debug(f"Пакетно вставлено {len(batch_data)} записів для {symbol}")
        except Exception as e:
            logger.error(f"Помилка пакетної вставки для {symbol}: {e}")
            # Повертаємо дані назад до батчу для повторної спроби
            self._batch_buffer[symbol].extend(batch_data)
    
    async def flush_all_batches(self):
        """Примусове очищення всіх батчів"""
        async with self._batch_lock:
            for symbol in list(self._batch_buffer.keys()):
                if self._batch_buffer[symbol]:
                    await self._flush_batch(symbol)
    
    async def _batch_insert_candle_data(self, symbol: str, batch_data: List[Dict[str, Any]]):
        """Пакетна вставка даних свічок з використанням prepared statements"""
        if not batch_data:
            return
            
        table_name = self._get_table_name(symbol)
        
        # Список всіх колонок в правильному порядку (без id та created_at)
        columns = [
            'timestamp', 'symbol', 'timeframe', 'open_price', 'high_price', 
            'low_price', 'close_price', 'volume',
            'rsi', 'rsi_rising_1', 'rsi_rising_2', 'rsi_falling_1', 'rsi_falling_2',
            'ema_fast', 'ema_slow', 'ema_trend', 'ema_bullish', 'ema_bearish',
            'macd_line', 'macd_signal', 'macd_histogram', 'macd_bullish', 'macd_bearish',
            'adx', 'plus_di', 'minus_di', 'atr',
            'volume_sma', 'volume_filter', 'volume_surge', 'super_volume_surge',
            'volume_trend', 'sustained_volume',
            'bullish_trend', 'bearish_trend',
            'bullish_vol_divergence', 'bearish_vol_divergence',
            'long_confirmations', 'short_confirmations', 'buy_signal', 'sell_signal'
        ]
        
        # Підготовка значень для всіх записів
        values_batch = []
        for data in batch_data:
            values = []
            
            # Додаємо символ явно
            data['symbol'] = symbol
            
            for column in columns:
                value = data.get(column)
                
                # Обробка різних типів значень
                if column in ['timestamp', 'timeframe', 'symbol']:
                    values.append(value)
                elif column in ['rsi_rising_1', 'rsi_rising_2', 'rsi_falling_1', 'rsi_falling_2',
                              'ema_bullish', 'ema_bearish', 'macd_bullish', 'macd_bearish',
                              'volume_filter', 'volume_surge', 'super_volume_surge', 'sustained_volume',
                              'bullish_trend', 'bearish_trend', 'bullish_vol_divergence', 'bearish_vol_divergence',
                              'long_confirmations', 'short_confirmations', 'buy_signal', 'sell_signal']:
                    # Для boolean/integer полів
                    if value is None:
                        values.append(0)
                    elif isinstance(value, bool):
                        values.append(1 if value else 0)
                    else:
                        values.append(int(value) if value is not None else 0)
                else:
                    # Для REAL полів
                    if value is None or (isinstance(value, float) and (value != value)):  # NaN check
                        values.append(None)
                    else:
                        values.append(float(value) if value is not None else None)
            
            values_batch.append(values)
        
        # Створення плейсхолдерів
        placeholders = ', '.join(['?' for _ in columns])
        columns_str = ', '.join(columns)
        
        async with self.get_connection() as db:
            await db.executemany(f"""
                INSERT OR REPLACE INTO {table_name} ({columns_str})
                VALUES ({placeholders})
            """, values_batch)
            await db.commit()
    
    async def insert_candle_data(self, symbol: str, data: Dict[str, Any]) -> bool:
        """Вставка даних свічки (тепер через батчинг для оптимізації)"""
        try:
            await self.add_to_batch(symbol, data)
            return True
        except Exception as e:
            logger.error(f"Помилка додавання до батчу для {symbol}: {e}")
            return False
    
    async def force_insert_candle_data(self, symbol: str, data: Dict[str, Any]) -> bool:
        """Примусова вставка одного запису (минаючи батчинг)"""
        try:
            if not symbol:
                logger.error("Символ не вказано як параметр")
                return False
                
            table_name = self._get_table_name(symbol)
            
            # Список всіх колонок в правильному порядку (без id та created_at)
            columns = [
                'timestamp', 'symbol', 'timeframe', 'open_price', 'high_price', 
                'low_price', 'close_price', 'volume',
                'rsi', 'rsi_rising_1', 'rsi_rising_2', 'rsi_falling_1', 'rsi_falling_2',
                'ema_fast', 'ema_slow', 'ema_trend', 'ema_bullish', 'ema_bearish',
                'macd_line', 'macd_signal', 'macd_histogram', 'macd_bullish', 'macd_bearish',
                'adx', 'plus_di', 'minus_di', 'atr',
                'volume_sma', 'volume_filter', 'volume_surge', 'super_volume_surge',
                'volume_trend', 'sustained_volume',
                'bullish_trend', 'bearish_trend',
                'bullish_vol_divergence', 'bearish_vol_divergence',
                'long_confirmations', 'short_confirmations', 'buy_signal', 'sell_signal'
            ]
            
            # Підготовка значень
            values = []
            
            # Додаємо символ явно
            data['symbol'] = symbol
            
            for column in columns:
                value = data.get(column)
                
                # Обробка різних типів значень
                if column in ['timestamp', 'timeframe', 'symbol']:
                    values.append(value)
                elif column in ['rsi_rising_1', 'rsi_rising_2', 'rsi_falling_1', 'rsi_falling_2',
                              'ema_bullish', 'ema_bearish', 'macd_bullish', 'macd_bearish',
                              'volume_filter', 'volume_surge', 'super_volume_surge', 'sustained_volume',
                              'bullish_trend', 'bearish_trend', 'bullish_vol_divergence', 'bearish_vol_divergence',
                              'long_confirmations', 'short_confirmations', 'buy_signal', 'sell_signal']:
                    # Для boolean/integer полів
                    if value is None:
                        values.append(0)
                    elif isinstance(value, bool):
                        values.append(1 if value else 0)
                    else:
                        values.append(int(value) if value is not None else 0)
                else:
                    # Для REAL полів
                    if value is None or (isinstance(value, float) and (value != value)):  # NaN check
                        values.append(None)
                    else:
                        values.append(float(value) if value is not None else None)
            
            # Створення плейсхолдерів
            placeholders = ', '.join(['?' for _ in columns])
            columns_str = ', '.join(columns)
            
            async with self.get_connection() as db:
                await db.execute(f"""
                    INSERT OR REPLACE INTO {table_name} ({columns_str})
                    VALUES ({placeholders})
                """, values)
                await db.commit()
                return True
                
        except Exception as e:
            logger.error(f"Помилка вставки даних свічки для {symbol}: {e}")
            return False
    
    async def get_latest_candles(self, symbol: str, timeframe: str, limit: int = None) -> pd.DataFrame:
        """Отримання останніх свічок з оптимізованим запитом"""
        try:
            if limit is None:
                limit = SYSTEM_CONFIG['default_request_limit']
            table_name = self._get_table_name(symbol)
            
            # Обмежуємо limit для запобігання завантаженню занадто великих обсягів даних
            safe_limit = min(limit, SYSTEM_CONFIG['max_request_limit'])
            
            async with self.get_connection() as db:
                cursor = await db.execute(f"""
                    SELECT * FROM {table_name} 
                    WHERE timeframe = ?
                    ORDER BY timestamp DESC 
                    LIMIT ?
                """, (timeframe, safe_limit))
                
                rows = await cursor.fetchall()
                
                if not rows:
                    return pd.DataFrame()
                
                # Отримання назв колонок
                column_names = [description[0] for description in cursor.description]
                
                # Створення DataFrame
                df = pd.DataFrame(rows, columns=column_names)
                df = df.sort_values('timestamp').reset_index(drop=True)
                
                return df
                
        except Exception as e:
            logger.error(f"Помилка отримання свічок для {symbol}: {e}")
            return pd.DataFrame()
    
    async def get_candles_for_analysis(self, symbol: str, timeframe: str, limit: int = None) -> pd.DataFrame:
        """Отримання свічок для аналізу стратегії з оптимізованим запитом"""
        try:
            table_name = self._get_table_name(symbol)
            
            # Використовуємо обмежений набір колонок для кращої продуктивності
            async with self.get_connection() as db:
                cursor = await db.execute(f"""
                    SELECT timestamp, symbol, timeframe, open_price, high_price, low_price, 
                           close_price, volume, rsi, ema_fast, ema_slow, ema_trend,
                           macd_line, macd_signal, macd_histogram, adx, atr,
                           volume_sma, volume_filter, volume_surge, super_volume_surge,
                           bullish_trend, bearish_trend, long_confirmations, short_confirmations,
                           buy_signal, sell_signal, rsi_rising_1, rsi_rising_2, rsi_falling_1, rsi_falling_2,
                           ema_bullish, ema_bearish, macd_bullish, macd_bearish,
                           volume_trend, sustained_volume, bullish_vol_divergence, bearish_vol_divergence
                    FROM {table_name} 
                    WHERE timeframe = ?
                    ORDER BY timestamp DESC 
                    LIMIT ?
                """, (timeframe, min(limit, SYSTEM_CONFIG['default_request_limit'])))
                
                rows = await cursor.fetchall()
                
                if not rows:
                    return pd.DataFrame()
                
                column_names = [description[0] for description in cursor.description]
                df = pd.DataFrame(rows, columns=column_names)
                df = df.sort_values('timestamp').reset_index(drop=True)
                
                return df
                
        except Exception as e:
            logger.error(f"Помилка отримання даних для аналізу {symbol}: {e}")
            return pd.DataFrame()
    
    async def check_data_integrity(self, symbol: str, timeframe: str) -> Dict[str, Any]:
        """
        Перевірка цілісності даних з оптимізованим запитом
        """
        try:
            table_name = self._get_table_name(symbol)
            MIN_RECORDS_IN_SUBSET_FOR_RELIABLE_CHECK = DISPLAY_CONFIG['min_records_for_reliable_check'] // 3
            MIN_TOTAL_RECORDS_FOR_50_PERCENT_CHECK = MIN_RECORDS_IN_SUBSET_FOR_RELIABLE_CHECK * DISPLAY_CONFIG['min_total_records_multiplier']

            async with self.get_connection() as db:
                # Загальна кількість записів
                cursor = await db.execute(f"SELECT COUNT(*) FROM {table_name} WHERE timeframe = ?", (timeframe,))
                total_records_tuple = await cursor.fetchone()
                
                if not total_records_tuple:
                    logger.warning(f"Не знайдено записів для {symbol} {timeframe} для перевірки цілісності")
                    return {'total_records': 0, 'checked_records_count': 0, 'missing_indicators': 0, 
                           'last_timestamp': None, 'integrity_ok': False, 'message': 'No records found'}
                
                total_records = total_records_tuple[0]

                # Останній запис
                cursor_last_ts = await db.execute(
                    f"SELECT timestamp FROM {table_name} WHERE timeframe = ? ORDER BY timestamp DESC LIMIT 1", 
                    (timeframe,)
                )
                last_timestamp_tuple = await cursor_last_ts.fetchone()
                last_timestamp = last_timestamp_tuple[0] if last_timestamp_tuple else None

                if total_records < MIN_TOTAL_RECORDS_FOR_50_PERCENT_CHECK:
                    message = (f"Загальна кількість записів ({total_records}) замала для надійної перевірки")
                    logger.warning(f"{message} ({symbol} {timeframe})")
                    return {
                        'total_records': total_records,
                        'checked_records_count': 0,
                        'missing_indicators': -1,
                        'last_timestamp': last_timestamp,
                        'integrity_ok': False,
                        'message': message
                    }

                limit_for_recent_half = total_records // 2
                
                # Оптимізований запит для перевірки останніх записів
                cursor_missing = await db.execute(f"""
                    SELECT COUNT(*)
                    FROM (
                        SELECT rsi, ema_fast, macd_line, adx
                        FROM {table_name}
                        WHERE timeframe = ?
                        ORDER BY timestamp DESC
                        LIMIT ? 
                    ) AS recent_candles
                    WHERE (rsi IS NULL OR ema_fast IS NULL OR macd_line IS NULL OR adx IS NULL)
                """, (timeframe, limit_for_recent_half))
                
                missing_indicators_tuple = await cursor_missing.fetchone()
                missing_indicators = missing_indicators_tuple[0] if missing_indicators_tuple else 0
                
                integrity_ok = (missing_indicators == 0)
                
                message = (f"Цілісність ключових індикаторів в останніх {limit_for_recent_half} записах " +
                          ("підтверджена" if integrity_ok else f"НЕ підтверджена ({missing_indicators} пропусків)"))

                return {
                    'total_records': total_records,
                    'checked_records_count': limit_for_recent_half,
                    'missing_indicators': missing_indicators,
                    'last_timestamp': last_timestamp,
                    'integrity_ok': integrity_ok,
                    'message': message
                }
                
        except Exception as e:
            logger.error(f"Помилка перевірки цілісності даних для {symbol}: {e}")
            return {'total_records': -1, 'checked_records_count': -1, 'missing_indicators': -1, 
                   'last_timestamp': None, 'integrity_ok': False, 'error': str(e)}
    
    async def cleanup_old_data(self, symbol: str, timeframe: str, keep_days: int = 30):
        """Очищення старих даних з оптимізованим запитом"""
        try:
            table_name = self._get_table_name(symbol)
            cutoff_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            async with self.get_connection() as db:
                # Спочатку порахуємо кількість записів для видалення
                cursor = await db.execute(f"""
                    SELECT COUNT(*) FROM {table_name} 
                    WHERE timeframe = ? 
                    AND datetime(timestamp) < datetime(?, '-{keep_days} days')
                """, (timeframe, cutoff_date))
                
                count_to_delete = (await cursor.fetchone())[0]
                
                if count_to_delete > 0:
                    await db.execute(f"""
                        DELETE FROM {table_name} 
                        WHERE timeframe = ? 
                        AND datetime(timestamp) < datetime(?, '-{keep_days} days')
                    """, (timeframe, cutoff_date))
                    
                    await db.commit()
                    logger.info(f"Видалено {count_to_delete} старих записів для {symbol}")
                
        except Exception as e:
            logger.error(f"Помилка очищення старих даних для {symbol}: {e}")
    
    async def close(self):
        """Закриття з'єднань та очищення ресурсів"""
        try:
            # Очищуємо всі батчі перед закриттям
            await self.flush_all_batches()
            
            # Закриваємо всі з'єднання в пулі
            while not self._pool.empty():
                try:
                    conn = await asyncio.wait_for(self._pool.get(), timeout=1.0)
                    await conn.close()
                except asyncio.TimeoutError:
                    break
                except Exception as e:
                    logger.warning(f"Помилка закриття з'єднання з пулу: {e}")
            
            # Закриваємо активні з'єднання
            for conn in self._active_connections.copy():
                try:
                    await conn.close()
                except Exception as e:
                    logger.warning(f"Помилка закриття активного з'єднання: {e}")
            
            self._active_connections.clear()
            self._pool_initialized = False
            
            logger.info("DatabaseManager закрито з очищенням всіх ресурсів")
            
        except Exception as e:
            logger.error(f"Помилка при закритті DatabaseManager: {e}")

    async def debug_table_structure(self, symbol: str):
        """Дебаг структури таблиці з використанням пулу з'єднань"""
        try:
            table_name = self._get_table_name(symbol)
            
            async with self.get_connection() as db:
                cursor = await db.execute(f"PRAGMA table_info({table_name})")
                columns = await cursor.fetchall()
                
                logger.info(f"Структура таблиці {table_name} підтверджена ({len(columns)} колонок)")
                
                return len(columns)
                
        except Exception as e:
            logger.error(f"Помилка дебагу структури таблиці: {e}")
            return 0