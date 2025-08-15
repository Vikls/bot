# -*- coding: utf-8 -*-
"""
Менеджер API для взаємодії з Bybit API v5 Unified Account
"""

import asyncio
import aiohttp
import time
import logging
import random
import weakref
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any, Tuple
from pybit.unified_trading import HTTP
from collections import defaultdict
from functools import wraps
import hashlib
import json
from config.settings import (
    API_CONFIG, TRADING_CONFIG, SYSTEM_CONFIG, 
    PRECISION_CONFIG, DISPLAY_CONFIG
)

logger = logging.getLogger(__name__)


def safe_float_convert(value, default=0.0):
    """Safely convert value to float with fallback."""
    if not value or value == '':
        return default
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


class CacheEntry:
    """Запис кешу з TTL"""
    def __init__(self, data: Any, ttl_seconds: int):
        self.data = data
        self.expires_at = time.time() + ttl_seconds
        self.created_at = time.time()
    
    def is_expired(self) -> bool:
        return time.time() > self.expires_at
    
    def get_age(self) -> float:
        return time.time() - self.created_at


class CircuitBreakerState:
    """Стан circuit breaker для кожного endpoint"""
    def __init__(self, failure_threshold: int = None, recovery_timeout: int = None):
        self.failure_count = 0
        self.last_failure_time = 0
        self.failure_threshold = failure_threshold or SYSTEM_CONFIG['circuit_breaker_failure_threshold']
        self.recovery_timeout = recovery_timeout or SYSTEM_CONFIG['circuit_breaker_recovery_timeout']
        self.state = 'CLOSED'  # CLOSED, OPEN, HALF_OPEN
    
    def record_success(self):
        self.failure_count = 0
        self.state = 'CLOSED'
    
    def record_failure(self):
        self.failure_count += 1
        self.last_failure_time = time.time()
        if self.failure_count >= self.failure_threshold:
            self.state = 'OPEN'
    
    def can_attempt(self) -> bool:
        if self.state == 'CLOSED':
            return True
        elif self.state == 'OPEN':
            if time.time() - self.last_failure_time > self.recovery_timeout:
                self.state = 'HALF_OPEN'
                return True
            return False
        else:  # HALF_OPEN
            return True


def cache_result(ttl_seconds: int = None):
    """Декоратор для кешування результатів API запитів"""
    if ttl_seconds is None:
        ttl_seconds = SYSTEM_CONFIG['cache_ttl_default']
    def decorator(func):
        @wraps(func)
        async def wrapper(self, *args, **kwargs):
            # Створюємо ключ кешу на основі імені функції та аргументів
            cache_key = self._create_cache_key(func.__name__, args, kwargs)
            
            # Перевіряємо кеш
            if cache_key in self._cache:
                entry = self._cache[cache_key]
                if not entry.is_expired():
                    logger.debug(f"Cache hit for {func.__name__} (age: {entry.get_age():.1f}s)")
                    return entry.data
                else:
                    del self._cache[cache_key]
            
            # Виконуємо запит та зберігаємо в кеш
            result = await func(self, *args, **kwargs)
            if result:  # Кешуємо тільки успішні результати
                self._cache[cache_key] = CacheEntry(result, ttl_seconds)
                logger.debug(f"Cached result for {func.__name__} (TTL: {ttl_seconds}s)")
            
            return result
        return wrapper
    return decorator

class BybitAPIManager:
    """Менеджер для роботи з Bybit API v5 Unified Account з оптимізаціями"""
    
    def __init__(self):
        self.config = API_CONFIG
        self.trading_config = TRADING_CONFIG
        self.last_request_time = 0
        self.request_count = 0
        self.rate_limit = self.config['rate_limit']
        
        # Кеш для зменшення кількості API запитів
        self._cache = {}
        self._cache_lock = asyncio.Lock()
        
        # Circuit breakers для різних endpoints
        self._circuit_breakers = defaultdict(lambda: CircuitBreakerState())
        
        # Батчинг запитів
        self._batch_queue = defaultdict(list)
        self._batch_lock = asyncio.Lock()
        self._batch_size = SYSTEM_CONFIG['chunk_size_default']  # Максимум символів в одному батчі
        self._batch_timeout = 1.0  # Таймаут для автоматичної обробки батчу
        
        # HTTP сесія з keep-alive та connection pooling
        self._session = None
        self._session_created_at = 0
        self._session_lifetime = SYSTEM_CONFIG['session_lifetime']  # 1 година
        
        # Tracking активних запитів для graceful shutdown
        self._active_requests = weakref.WeakSet()
        
        # Ініціалізація клієнта
        mode = self.trading_config['mode'].lower()
        if mode == 'demo':
            api_credentials = self.config['demo']
        else:
            api_credentials = self.config['live']
            
        self.client = HTTP(
            api_key=api_credentials['api_key'],
            api_secret=api_credentials['api_secret'],
            testnet=self.config['testnet'],  # Завжди False!
            demo=True if mode == 'demo' else False
        )
        
        logger.info(f"API Manager ініціалізовано в режимі: {mode.upper()} з оптимізаціями")
    
    def _create_cache_key(self, func_name: str, args: tuple, kwargs: dict) -> str:
        """Створення ключа кешу на основі параметрів запиту"""
        # Створюємо хеш з назви функції та параметрів
        key_data = {
            'func': func_name,
            'args': str(args),
            'kwargs': {k: v for k, v in kwargs.items() if k not in ['password', 'secret']}
        }
        key_str = json.dumps(key_data, sort_keys=True)
        return hashlib.md5(key_str.encode()).hexdigest()
    
    async def _get_http_session(self) -> aiohttp.ClientSession:
        """Отримання HTTP сесії з connection pooling та keep-alive"""
        current_time = time.time()
        
        # Перевіряємо чи потрібно створити нову сесію
        if (self._session is None or 
            current_time - self._session_created_at > self._session_lifetime):
            
            if self._session:
                await self._session.close()
            
            # Налаштування для оптимальної продуктивності
            connector = aiohttp.TCPConnector(
                limit=SYSTEM_CONFIG['max_concurrent_connections'],  # Максимум одночасних з'єднань
                limit_per_host=SYSTEM_CONFIG['max_connections_per_host'],  # Максимум з'єднань на хост
                keepalive_timeout=SYSTEM_CONFIG['keepalive_timeout'],  # Keep-alive timeout
                enable_cleanup_closed=True,
                use_dns_cache=True,
                ttl_dns_cache=SYSTEM_CONFIG['dns_cache_ttl']
            )
            
            timeout = aiohttp.ClientTimeout(
                total=SYSTEM_CONFIG['default_timeout'], 
                connect=SYSTEM_CONFIG['connect_timeout']
            )
            
            self._session = aiohttp.ClientSession(
                connector=connector,
                timeout=timeout,
                headers={
                    'User-Agent': 'bbbot/1.0',
                    'Connection': 'keep-alive'
                }
            )
            self._session_created_at = current_time
            
        return self._session
    
    async def _exponential_backoff_with_jitter(self, attempt: int, base_delay: float = 1.0) -> float:
        """Експоненційний backoff з jitter для запобігання thundering herd"""
        # Базовий експоненційний backoff
        delay = base_delay * (2 ** attempt)
        
        # Додаємо jitter (випадкову затримку) для розподілення навантаження
        jitter = random.uniform(0.1, 0.3) * delay
        
        # Обмежуємо максимальну затримку
        max_delay = SYSTEM_CONFIG['max_retry_delay']
        final_delay = min(delay + jitter, max_delay)
        
        logger.debug(f"Backoff delay: {final_delay:.2f}s (attempt {attempt})")
        return final_delay
    
    async def _rate_limit_check(self):
        """Покращена перевірка обмежень API з adaptive rate limiting"""
        current_time = time.time()
        
        # Скидання лічильника кожну секунду
        if current_time - self.last_request_time >= 1.0:
            self.request_count = 0
            self.last_request_time = current_time
        
        # Adaptive rate limiting на основі історії відповідей
        effective_rate_limit = self.rate_limit
        
        # Якщо недавно мали rate limit помилки, зменшуємо швидкість
        recent_failures = sum(1 for cb in self._circuit_breakers.values() 
                             if cb.failure_count > 0 and time.time() - cb.last_failure_time < SYSTEM_CONFIG['circuit_breaker_recovery_timeout'])
        
        if recent_failures > 0:
            effective_rate_limit = max(1, self.rate_limit // 2)
            logger.debug(f"Adaptive rate limiting: reduced to {effective_rate_limit}/s due to recent failures")
        
        # Перевірка ліміту
        if self.request_count >= effective_rate_limit:
            sleep_time = 1.0 - (current_time - self.last_request_time)
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)
                self.request_count = 0
                self.last_request_time = time.time()
        
        self.request_count += 1
    
    async def _make_request_with_retry(self, request_func, *args, endpoint: str = None, **kwargs):
        """Виконання запиту з retry логікою"""
        last_exception = None  # Зберігаємо останній виняток для логування, якщо всі спроби невдалі
        
        for attempt in range(self.config['retry_attempts']):
            try:
                await self._rate_limit_check()
                result = request_func(*args, **kwargs)
                
                # Перевіряємо, чи result взагалі існує і має атрибут 'get'
                if hasattr(result, 'get'):
                    ret_code = result.get('retCode')
                    # Успішний запит АБО помилка "not modified" (34040), яку ми обробляємо як успіх на цьому рівні
                    if ret_code == 0 or ret_code == 34040: 
                        return result 
                    elif ret_code == 10002:  # Rate limit error
                        logger.warning(f"Rate limit досягнуто (retCode: {ret_code}), спроба {attempt + 1}/{self.config['retry_attempts']}")
                        await asyncio.sleep(self.config['retry_delay'] * (attempt + 1))
                        last_exception = Exception(f"Rate limit error: {result.get('retMsg', 'N/A')}")  # Зберігаємо як виняток
                        continue
                    else:  # Інші помилки API
                        logger.error(f"API помилка (retCode: {ret_code}): {result.get('retMsg', 'N/A')}. Спроба {attempt + 1}/{self.config['retry_attempts']}")
                        # Для інших помилок API, ми можемо або повернути результат для обробки вище,
                        # або продовжити спроби, якщо це доцільно.
                        # Поточна логіка pybit генерує виняток, якщо retCode != 0, тому ми теж можемо це зробити
                        # або повернути результат. Для консистентності з pybit, давайте повернемо його,
                        # але залогуємо як помилку.
                        last_exception = Exception(f"API error: {result.get('retMsg', 'N/A')} (Code: {ret_code})")
                        # Якщо це остання спроба, виняток буде піднято нижче.
                        # Якщо не остання, можемо спробувати ще раз або повернути помилку.
                        # Поки що, для інших помилок, крім rate limit, повертаємо результат, 
                        # оскільки pybit може сам генерувати винятки для них.
                        # Якщо pybit не генерує виняток, а повертає результат з помилкою,
                        # то вищий рівень коду має це обробити.
                        # Однак, згідно з трейсбеком, pybit генерує InvalidRequestError.
                        # Тому ми повинні дозволити цьому винятку прокинутися, якщо це не 34040.
                        # У цьому випадку `request_func` сам підніме виняток, і ми його зловимо.
                        return result  # Повертаємо результат з помилкою, якщо це не rate limit і не 34040
                                    # і якщо pybit не підняв виняток сам.
                else:
                    # Якщо result не має 'get', це може бути несподівана відповідь або помилка
                    logger.error(f"Несподівана відповідь від API (спроба {attempt + 1}): {result}")
                    last_exception = Exception(f"Unexpected API response type: {type(result)}")
                    if attempt < self.config['retry_attempts'] - 1:
                        await asyncio.sleep(self.config['retry_delay'] * (attempt + 1))
                        continue
                    else:  # Остання спроба
                        if last_exception: 
                            raise last_exception
                        return None  # Або повернути None, якщо не хочемо піднімати виняток

            except Exception as e:
                # Перевіряємо, чи це помилка "not modified" (34040) з pybit.exceptions.InvalidRequestError
                if isinstance(e, Exception) and hasattr(e, 'status_code') and e.status_code == 34040:  # pybit може використовувати status_code
                    logger.warning(f"Перехоплено помилку 'not modified' (34040) з pybit: {e}. Повертаємо як успіх.")
                    # Нам потрібна структура відповіді, яку очікує вищий рівень.
                    # pybit.exceptions.InvalidRequestError містить message, status_code, response.
                    # Ми можемо спробувати відтворити структуру відповіді або повернути спеціальний об'єкт.
                    # Краще, якщо `request_func` для 34040 повертав би результат, а не виняток.
                    # Якщо pybit завжди кидає виняток для 34040, тоді ця логіка в _update_active_tpsl_on_exchange не спрацює.
                    # Давайте припустимо, що ми хочемо, щоб InvalidRequestError для 34040 НЕ піднімався далі.
                    # Нам потрібно отримати дані з `e.response` або створити фейкову відповідь.
                    # Згідно з трейсбеком, `e` має атрибути `message`, `status_code`, `response`.
                    # `e.response` - це об'єкт `requests.Response`. Нам потрібен його JSON.
                    try:
                        response_json = e.response.json() if hasattr(e, 'response') and e.response else {}
                        # Переконуємось, що retCode встановлено правильно
                        if 'retCode' not in response_json or response_json['retCode'] != 34040:
                            response_json['retCode'] = 34040
                            response_json['retMsg'] = response_json.get('retMsg', str(e))  # Беремо retMsg з відповіді, або з винятку
                        logger.info(f"Повертаємо оброблену відповідь для помилки 34040: {response_json}")
                        return response_json
                    except Exception as parsing_exc:
                        logger.error(f"Помилка розбору відповіді з винятку 34040: {parsing_exc}")
                        # Повертаємо словник, який імітує відповідь Bybit
                        return {
                            "retCode": 34040, 
                            "retMsg": str(e), 
                            "result": {}, 
                            "retExtInfo": {}, 
                            "time": int(time.time() * 1000)
                        }

                logger.error(f"Помилка запиту (спроба {attempt + 1}/{self.config['retry_attempts']}): {e}", exc_info=False)  # Не логуємо повний трейсбек тут
                last_exception = e
                if attempt < self.config['retry_attempts'] - 1:
                    await asyncio.sleep(self.config['retry_delay'] * (attempt + 1))
                else:  # Остання спроба
                    if last_exception: 
                        raise last_exception  # Піднімаємо останній виняток
        
        # Якщо цикл завершився без повернення (малоймовірно при поточній логіці)
        logger.error(f"Всі {self.config['retry_attempts']} спроб запиту не вдалися.")
        if last_exception: 
            raise last_exception  # Піднімаємо останній виняток, якщо він був
        return None  # Або повернути None/спеціальне значення помилки
    
    async def add_to_request_batch(self, batch_type: str, request_data: Dict[str, Any]):
        """Додавання запиту до батчу для групової обробки"""
        async with self._batch_lock:
            self._batch_queue[batch_type].append(request_data)
            
            # Якщо батч готовий, обробляємо його
            if len(self._batch_queue[batch_type]) >= self._batch_size:
                await self._process_batch(batch_type)
    
    async def _process_batch(self, batch_type: str):
        """Обробка батчу запитів"""
        if not self._batch_queue[batch_type]:
            return
        
        batch = self._batch_queue[batch_type].copy()
        self._batch_queue[batch_type].clear()
        
        try:
            if batch_type == 'klines':
                await self._process_klines_batch(batch)
            elif batch_type == 'tickers':
                await self._process_tickers_batch(batch)
            else:
                logger.warning(f"Невідомий тип батчу: {batch_type}")
                
        except Exception as e:
            logger.error(f"Помилка обробки батчу {batch_type}: {e}")
            # Повертаємо запити назад до черги для повторної спроби
            self._batch_queue[batch_type].extend(batch)
    
    async def _process_klines_batch(self, batch: List[Dict]):
        """Паралельна обробка запитів klines"""
        tasks = []
        for request in batch:
            task = self.get_klines(
                symbol=request['symbol'],
                interval=request['interval'],
                limit=request.get('limit', SYSTEM_CONFIG['default_request_limit'])
            )
            tasks.append(task)
        
        # Виконуємо запити паралельно з обмеженням
        semaphore = asyncio.Semaphore(SYSTEM_CONFIG['max_concurrent_requests'])  # Максимум одночасних запитів
        
        async def limited_request(task):
            async with semaphore:
                return await task
        
        results = await asyncio.gather(*[limited_request(task) for task in tasks], return_exceptions=True)
        
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Помилка в батчевому запиті klines для {batch[i]['symbol']}: {result}")
    
    async def _process_tickers_batch(self, batch: List[Dict]):
        """Паралельна обробка запитів tickers"""
        # Для tickers можемо зробити один запит для всіх символів
        symbols = [request['symbol'] for request in batch]
        
        try:
            # Отримуємо тікери для всіх символів одним запитом
            result = await self.get_tickers("linear")  # Без symbol отримує всі
            
            if result and result.get('retCode') == 0:
                ticker_list = result.get('result', {}).get('list', [])
                # Фільтруємо тільки потрібні символи
                filtered_tickers = [t for t in ticker_list if t.get('symbol') in symbols]
                return filtered_tickers
            
        except Exception as e:
            logger.error(f"Помилка батчевого запиту tickers: {e}")
    
    async def get_multiple_klines_concurrent(self, symbols: List[str], interval: str, limit: int = None) -> Dict[str, List[Dict]]:
        """Паралельне отримання klines для декількох символів"""
        if limit is None:
            limit = SYSTEM_CONFIG['default_request_limit']
        semaphore = asyncio.Semaphore(SYSTEM_CONFIG['max_concurrent_requests'])  # Обмежуємо кількість одночасних запитів
        
        async def get_klines_with_limit(symbol):
            async with semaphore:
                try:
                    return symbol, await self.get_klines(symbol, interval, limit)
                except Exception as e:
                    logger.error(f"Помилка отримання klines для {symbol}: {e}")
                    return symbol, []
        
        tasks = [get_klines_with_limit(symbol) for symbol in symbols]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Перетворюємо результати в словник
        klines_data = {}
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Виняток в concurrent klines: {result}")
                continue
            
            symbol, data = result
            klines_data[symbol] = data
        
        return klines_data
    
    async def flush_all_batches(self):
        """Примусова обробка всіх батчів"""
        async with self._batch_lock:
            for batch_type in list(self._batch_queue.keys()):
                if self._batch_queue[batch_type]:
                    await self._process_batch(batch_type)
    
    async def close(self):
        """Закриття API менеджера з proper cleanup"""
        try:
            # Обробляємо всі залишкові батчі
            await self.flush_all_batches()
            
            # Закриваємо HTTP сесію
            if self._session:
                await self._session.close()
                self._session = None
            
            # Очищуємо кеш
            async with self._cache_lock:
                self._cache.clear()
            
            logger.info("API Manager закрито з повним очищенням ресурсів")
            
        except Exception as e:
            logger.error(f"Помилка при закритті API Manager: {e}")

    async def get_execution_history(self, symbol: str = None, limit: int = None,
                              start_time: Optional[int] = None, 
                              end_time: Optional[int] = None,
                              order_filter: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Отримання історії виконання угод (executions) з Bybit API v5.
        
        Args:
            symbol: Торгова пара
            limit: Кількість записів (макс. 100)
            start_time: Початковий час у мілісекундах UTC
            end_time: Кінцевий час у мілісекундах UTC
            order_filter: Фільтр по orderId (опціонально)
            
        Returns:
            List[Dict]: Список виконаних угод з деталями
        """
        try:
            if limit is None:
                limit = SYSTEM_CONFIG['execution_history_limit']
            
            params = {
                "category": "linear",
                "limit": min(limit, SYSTEM_CONFIG['execution_history_limit'])
            }
            
            if symbol:
                params["symbol"] = symbol
                
            if order_filter:
                params["orderId"] = order_filter
                
            if start_time:
                params["startTime"] = str(start_time)
                
            if end_time:
                params["endTime"] = str(end_time)
            
            # 🔧 Розширили вікно пошуку до 7 днів замість 24 годин
            if not start_time and not end_time:
                current_time = int(datetime.now(timezone.utc).timestamp() * 1000)
                start_time_7d = current_time - (7 * 24 * 60 * 60 * 1000)  # 7 днів назад
                params["startTime"] = str(start_time_7d)
                params["endTime"] = str(current_time)
            
            # 🆕 Завжди логуємо детальну інформацію про запит
            logger.info(f"🔍 EXECUTION HISTORY DEBUG: Запит історії виконання")
            logger.info(f"   📊 Символ: {symbol or 'ВСІ'}")
            logger.info(f"   📅 Період: {datetime.fromtimestamp(int(params['startTime'])/1000).strftime('%Y-%m-%d %H:%M:%S')} - {datetime.fromtimestamp(int(params['endTime'])/1000).strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info(f"   🔧 Параметри API: {params}")
            
            result = await self._make_request_with_retry(
                self.client.get_executions,
                **params
            )
            
            # 🆕 Детальне логування відповіді API
            logger.info(f"📨 EXECUTION HISTORY RESPONSE:")
            logger.info(f"   ✅ retCode: {result.get('retCode') if result else 'None'}")
            logger.info(f"   📝 retMsg: {result.get('retMsg') if result else 'None'}")
            
            if not result or result.get('retCode') != 0:
                logger.error(f"❌ Помилка отримання історії виконання для {symbol}: {result}")
                return []
            
            executions_data = result.get('result', {}).get('list', [])
            logger.info(f"   📈 Кількість raw executions: {len(executions_data)}")
            
            # 🆕 Логування перших кількох executions для діагностики
            if executions_data:
                logger.info(f"   🔍 Перші 3 executions (raw):")
                for i, exec_raw in enumerate(executions_data[:3]):
                    logger.info(f"     {i+1}. Symbol: {exec_raw.get('symbol')}, Side: {exec_raw.get('side')}, Qty: {exec_raw.get('execQty')}, Price: {exec_raw.get('execPrice')}, Time: {exec_raw.get('execTime')}")
            
            # Форматування даних з додатковими полями для аналізу
            formatted_executions = []
            for execution in executions_data:
                try:
                    exec_time_ms = int(execution.get('execTime', 0))
                    formatted_execution = {
                        'symbol': execution.get('symbol'),
                        'order_id': execution.get('orderId'),
                        'execution_id': execution.get('execId'),
                        'side': execution.get('side'),  # Buy/Sell
                        'order_type': execution.get('orderType'),  # Market/Limit
                        'price': safe_float_convert(execution.get('execPrice', 0)),
                        'quantity': safe_float_convert(execution.get('execQty', 0)),
                        'exec_value': safe_float_convert(execution.get('execValue', 0)),
                        'exec_fee': safe_float_convert(execution.get('execFee', 0)),
                        'fee_rate': safe_float_convert(execution.get('feeRate', 0)),
                        'exec_time': exec_time_ms,
                        'exec_time_formatted': datetime.fromtimestamp(
                            exec_time_ms / 1000, tz=timezone.utc
                        ).strftime('%Y-%m-%d %H:%M:%S UTC'),
                        'is_maker': execution.get('isMaker', False),
                        'closed_size': safe_float_convert(execution.get('closedSize', 0)),  # ⭐ Важливо для аналізу
                        'last_liquidity_ind': execution.get('lastLiquidityInd', ''),
                        'mark_price': safe_float_convert(execution.get('markPrice', 0)),
                        'index_price': safe_float_convert(execution.get('indexPrice', 0)),
                        'block_trade_id': execution.get('blockTradeId', ''),
                        'leaf_qty': safe_float_convert(execution.get('leavesQty', 0)),
                        'cum_exec_qty': safe_float_convert(execution.get('cumExecQty', 0)),
                        'cum_exec_value': safe_float_convert(execution.get('cumExecValue', 0)),
                        'cum_exec_fee': safe_float_convert(execution.get('cumExecFee', 0)),
                        
                        # 🆕 Додаткові поля для аналізу
                        'exec_datetime': datetime.fromtimestamp(exec_time_ms / 1000, tz=timezone.utc),
                        'net_position_change': 0.0,  # Буде розраховано пізніше
                        'is_opening': False,  # Відкриття позиції
                        'is_closing': False,  # Закриття позиції
                        'raw_data': execution
                    }
                    formatted_executions.append(formatted_execution)
                except Exception as e:
                    logger.warning(f"⚠️ Помилка форматування execution {execution.get('execId', 'Unknown')}: {e}")
                    continue
            
            # Сортуємо за часом виконання (найстаріші спочатку для правильного аналізу)
            formatted_executions.sort(key=lambda x: x['exec_time'])
            
            # 🆕 Детальне логування результату
            logger.info(f"✅ EXECUTION HISTORY RESULT:")
            logger.info(f"   📊 Форматованих executions: {len(formatted_executions)}")
            if formatted_executions:
                logger.info(f"   🕐 Період executions: {formatted_executions[0]['exec_time_formatted']} - {formatted_executions[-1]['exec_time_formatted']}")
                
                # Логування останніх executions
                logger.info(f"   📈 Останні 3 formatted executions:")
                for i, exec_fmt in enumerate(formatted_executions[-3:]):
                    logger.info(f"     {i+1}. {exec_fmt['symbol']} {exec_fmt['side']} {exec_fmt['quantity']:.6f} @ {exec_fmt['price']:.6f} ({exec_fmt['exec_time_formatted']})")
            
            return formatted_executions
            
        except Exception as e:
            logger.error(f"💥 Критична помилка отримання історії виконання для {symbol}: {e}", exc_info=True)
            return []

    async def get_position_via_api(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        🆕 НОВИЙ МЕТОД: Отримання позиції безпосередньо через API позицій.
        Використовується як fallback для Demo аккаунтів.
        """
        try:
            logger.info(f"🔍 POSITION API: Запит позиції через API для {symbol}")
            
            positions = await self.get_positions(symbol=symbol)
            
            if not positions:
                logger.info(f"📊 POSITION API: Позицій для {symbol} не знайдено")
                return None
            
            for position in positions:
                # Перевіряємо чи позиція активна
                pos_size = safe_float_convert(position.get('size', 0))
                pos_side = position.get('side', '')
                pos_symbol = position.get('symbol', '')
                
                if pos_symbol == symbol and pos_size > 0:
                    # Знайшли активну позицію
                    avg_price = safe_float_convert(position.get('avgPrice', 0))
                    unrealized_pnl = safe_float_convert(position.get('unrealisedPnl', 0))
                    
                    logger.info(f"✅ POSITION API: Знайдено активну позицію {symbol}")
                    logger.info(f"   📊 Сторона: {pos_side}, Розмір: {pos_size}")
                    logger.info(f"   💰 Середня ціна: {avg_price}")
                    logger.info(f"   📈 Unrealized P&L: {unrealized_pnl}")
                    
                    return {
                        'symbol': pos_symbol,
                        'side': pos_side,
                        'size': pos_size,
                        'avg_price': avg_price,
                        'unrealized_pnl': unrealized_pnl,
                        'mark_price': safe_float_convert(position.get('markPrice', 0)),
                        'position_value': safe_float_convert(position.get('positionValue', 0)),
                        'leverage': safe_float_convert(position.get('leverage', 0)),
                        'created_time': position.get('createdTime', ''),
                        'updated_time': position.get('updatedTime', ''),
                        'source': 'positions_api',
                        'raw_position': position
                    }
            
            logger.info(f"📊 POSITION API: Активних позицій для {symbol} не знайдено")
            return None
            
        except Exception as e:
            logger.error(f"💥 Помилка отримання позиції через API для {symbol}: {e}", exc_info=True)
            return None

    async def analyze_position_from_execution_history(self, symbol: str,
                                                bot_order_id: str = None,
                                                lookback_hours: int = 48) -> Tuple[Optional[Dict], List[Dict], Dict]:
        """
        Аналізує історію виконання угод для визначення поточного стану позиції.
        З fallback на API позицій для Demo аккаунтів.

        Args:
            symbol: Торгова пара для аналізу
            bot_order_id: ID ордера бота (опціонально, для точного відстеження)
            lookback_hours: Кількість годин назад для пошуку (за замовчуванням 48)

        Returns:
            Tuple[Optional[Dict], List[Dict], Dict]:
                - Агрегований стан позиції або None якщо закрита
                - Список релевантних executions (може бути порожнім для DEMO fallback)
                - Додаткова діагностична інформація
        """
        try:
            is_demo_mode = self.trading_config['mode'].lower() == 'demo'

            logger.info(f"🔍 POSITION ANALYSIS DEBUG: Початок аналізу позиції {symbol}")
            logger.info(f"   📊 Order ID для пошуку: {bot_order_id or 'НЕ ВКАЗАНО'}")
            logger.info(f"   🕐 Lookback hours: {lookback_hours}")
            logger.info(f"   🎭 Demo режим: {is_demo_mode}")

            current_time_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
            start_time_ms = current_time_ms - (lookback_hours * 60 * 60 * 1000)

            all_executions = await self.get_execution_history(
                symbol=symbol,
                limit=100,
                start_time=start_time_ms,
                end_time=current_time_ms,
                order_filter=bot_order_id if bot_order_id else None
            )

            if not all_executions and bot_order_id:
                logger.info(f"⚠️ Не знайдено executions для {symbol} з orderId {bot_order_id}. Повторний запит без фільтра orderId...")
                all_executions = await self.get_execution_history(
                    symbol=symbol,
                    limit=100,
                    start_time=start_time_ms,
                    end_time=current_time_ms
                )

            logger.info(f"📊 POSITION ANALYSIS: Отримано {len(all_executions)} executions для {symbol}")

            diagnostics = {
                'total_executions_api': len(all_executions),
                'lookback_hours': lookback_hours,
                'final_position_calc': 0.0,
                'position_timeline_calc': [],
                'analysis_time': datetime.now(timezone.utc).isoformat(),
                'demo_mode': is_demo_mode,
                'source_of_truth': 'executions'
            }

            if not all_executions and is_demo_mode:
                logger.warning(f"⚠️ DEMO FALLBACK для {symbol}: Executions порожні, перевіряємо API позицій та історію ордерів.")
                diagnostics['source_of_truth'] = 'demo_fallback_api_pos_and_orders'

                api_position_data = await self.get_positions(symbol=symbol)
                active_api_pos = None
                if api_position_data:
                    for pos_item in api_position_data:
                        if pos_item.get('symbol') == symbol and safe_float_convert(pos_item.get('size', "0")) > PRECISION_CONFIG['qty_tolerance']:
                            active_api_pos = pos_item
                            break

                if active_api_pos:
                    logger.info(f"✅ DEMO FALLBACK: Знайдено активну позицію {symbol} через API get_positions.")
                    pos_side_api = active_api_pos.get('side', 'Buy')
                    position_side_enum = 'BUY' if pos_side_api == 'Buy' else 'SELL'

                    aggregated_position = {
                        'symbol': symbol,
                        'side': position_side_enum,
                        'size': safe_float_convert(active_api_pos.get('size', 0)),
                        'avg_price': safe_float_convert(active_api_pos.get('avgPrice', 0)),
                        'latest_price': safe_float_convert(active_api_pos.get('markPrice', active_api_pos.get('avgPrice', 0))),
                        'total_fees': 0.0,
                        'executions_count': 0,
                        'latest_execution_time': int(active_api_pos.get('updatedTime', current_time_ms)),
                        'latest_execution_time_formatted': datetime.fromtimestamp(int(active_api_pos.get('updatedTime', current_time_ms))/1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC'),
                        'opening_executions': 1,
                        'closing_executions': 0,
                        'source': 'demo_api_positions',
                        'raw_data_source': active_api_pos,
                        'unrealized_pnl': safe_float_convert(active_api_pos.get('unrealisedPnl', 0))
                    }
                    diagnostics['api_position_details'] = active_api_pos
                    return aggregated_position, [], diagnostics

                logger.info(f"ℹ️ DEMO FALLBACK: Активних позицій {symbol} не знайдено. Перевірка історії ордерів...")
                order_history = await self.get_order_history(
                    symbol=symbol,
                    limit=10,
                    start_time=start_time_ms,
                    end_time=current_time_ms
                )

                if order_history:
                    for order in order_history:
                        order_status = order.get('orderStatus', '').lower()
                        created_type = order.get('createType', '').lower()
                        stop_order_type_hist = order.get('stopOrderType', '').lower()

                        is_closing_order = (
                            order_status == 'filled' and
                            order.get('reduceOnly', False) and
                            order.get('orderType', '').lower() == 'market'
                        )
                        is_system_tpsl_filled = (
                            order_status == 'filled' and
                            ('take_profit' in stop_order_type_hist or 'stop_loss' in stop_order_type_hist) and
                            ('createbytakeprofit' in created_type or 'createbystoploss' in created_type or 'tpsl' in created_type) # Додано 'tpsl'
                        )

                        if is_closing_order or is_system_tpsl_filled:
                            logger.info(f"✅ DEMO FALLBACK: Знайдено ордер закриття для {symbol} в історії ордерів.")
                            diagnostics['last_closing_order_details'] = order
                            return None, [], diagnostics

                logger.info(f"ℹ️ DEMO FALLBACK: Не знайдено ні активних позицій, ні нещодавніх ордерів закриття для {symbol}. Вважаємо закритою.")
                return None, [], diagnostics

            if not all_executions: # Якщо і після fallback (або не DEMO) немає executions
                logger.warning(f"⚠️ POSITION ANALYSIS: Історія виконання для {symbol} порожня (після всіх перевірок).")
                return None, [], {'error': 'no_executions_final', 'debug': 'no executions found for symbol'}


            position_timeline = []
            running_position_qty = 0.0

            for execution in all_executions: # all_executions вже відсортовані в get_execution_history
                qty = execution['quantity']
                side = execution['side']

                position_change = qty if side == 'Buy' else -qty
                old_position_qty = running_position_qty
                running_position_qty += position_change

                is_opening_calc = (abs(running_position_qty) > abs(old_position_qty)) or \
                                (old_position_qty == 0 and running_position_qty != 0) or \
                                (old_position_qty * running_position_qty < 0)

                is_closing_calc = (abs(running_position_qty) < abs(old_position_qty)) or \
                                (running_position_qty == 0 and old_position_qty != 0) or \
                                (old_position_qty * running_position_qty < 0)

                execution['net_position_change'] = position_change
                execution['running_position_after'] = running_position_qty
                execution['is_opening_calc'] = is_opening_calc
                execution['is_closing_calc'] = is_closing_calc

                position_timeline.append({
                    'exec_time': execution['exec_time'], 'exec_id': execution['execution_id'],
                    'side': side, 'qty': qty, 'price': execution['price'],
                    'position_before': old_position_qty, 'position_after': running_position_qty,
                    'change': position_change, 'is_opening': is_opening_calc, 'is_closing': is_closing_calc,
                    'raw_exec_data': execution.get('raw_data', {})
                })

            diagnostics['final_position_calc'] = running_position_qty
            diagnostics['position_timeline_calc'] = position_timeline

            logger.info(f"📊 POSITION ANALYSIS RESULT (після розрахунку таймлайну):")
            logger.info(f"   📈 Всього executions проаналізовано: {len(position_timeline)}")
            logger.info(f"   📊 Розрахункова фінальна позиція: {running_position_qty:.8f}")

            tolerance = PRECISION_CONFIG['qty_tolerance']
            if abs(running_position_qty) <= tolerance:
                logger.info(f"✅ POSITION ANALYSIS: Позиція {symbol} закрита (розрахункова: {running_position_qty:.8f})")
                return None, all_executions, diagnostics

            position_side_enum = 'BUY' if running_position_qty > 0 else 'SELL'
            position_size_abs = abs(running_position_qty)

            # Розрахунок середньої ціни входу для поточної відкритої позиції
            # Йдемо з кінця таймлайну, поки не наберемо поточний розмір позиції
            current_open_execs_for_avg_price = []
            temp_qty_sum_for_avg = 0
            for exec_event in reversed(position_timeline):
                event_qty_effect = exec_event['qty'] if exec_event['side'] == position_side_enum else -exec_event['qty']
                
                # Враховуємо тільки ті виконання, що формують поточну відкриту частину
                # Якщо це виконання відкривало або додавало до поточної позиції
                if (position_side_enum == 'BUY' and exec_event['side'] == 'Buy') or \
                (position_side_enum == 'SELL' and exec_event['side'] == 'Sell'):
                    
                    qty_to_consider = min(exec_event['qty'], position_size_abs - temp_qty_sum_for_avg)
                    if qty_to_consider > 0:
                        current_open_execs_for_avg_price.append({'price': exec_event['price'], 'qty': qty_to_consider})
                        temp_qty_sum_for_avg += qty_to_consider
                    
                    if temp_qty_sum_for_avg >= position_size_abs - tolerance:
                        break
            
            avg_entry_price_calc = 0
            if current_open_execs_for_avg_price:
                total_value_open_calc = sum(ex['price'] * ex['qty'] for ex in current_open_execs_for_avg_price)
                total_qty_open_calc = sum(ex['qty'] for ex in current_open_execs_for_avg_price)
                if total_qty_open_calc > 0:
                    avg_entry_price_calc = total_value_open_calc / total_qty_open_calc
            elif all_executions: # Fallback, якщо логіка вище не спрацювала
                avg_entry_price_calc = all_executions[-1]['price']


            total_fees_calc = sum(ex['exec_fee'] for ex in all_executions)
            latest_execution_details = all_executions[-1] if all_executions else {}

            aggregated_position = {
                'symbol': symbol,
                'side': position_side_enum,
                'size': position_size_abs,
                'avg_price': avg_entry_price_calc,
                'latest_price': latest_execution_details.get('price', 0),
                'total_fees': total_fees_calc,
                'executions_count': len(all_executions),
                'latest_execution_time': latest_execution_details.get('exec_time', 0),
                'latest_execution_time_formatted': latest_execution_details.get('exec_time_formatted', 'N/A'),
                'opening_executions_calc': len([ex for ex in position_timeline if ex['is_opening']]),
                'closing_executions_calc': len([ex for ex in position_timeline if ex['is_closing']]),
                'source': 'executions_analysis',
                'raw_data_source': all_executions,
                'unrealized_pnl': (latest_execution_details.get('price', 0) - avg_entry_price_calc) * position_size_abs if position_side_enum == 'BUY' else (avg_entry_price_calc - latest_execution_details.get('price', 0)) * position_size_abs if avg_entry_price_calc > 0 else 0
            }

            logger.info(f"📋 POSITION ANALYSIS: Агрегована позиція {symbol} (з executions):")
            logger.info(f"   📊 Сторона: {position_side_enum}, Розмір: {position_size_abs:.8f}")
            logger.info(f"   💰 Розрахункова середня ціна: {avg_entry_price_calc:.6f}")

            return aggregated_position, all_executions, diagnostics

        except Exception as e:
            logger.error(f"💥 Критична помилка аналізу позиції з історії для {symbol}: {e}", exc_info=True)
            return None, [], {'error_critical': str(e), 'source_of_truth': 'error_in_analysis'}

    async def analyze_close_reason(self, executions: List[Dict], local_position: Dict) -> str:
        """
        Аналізує executions для визначення точної причини закриття позиції.
        Покращена версія з аналізом деталей виконання.
        """
        if not executions:
            logger.warning("analyze_close_reason: Список executions порожній.")
            return "external_no_executions"

        # Сортуємо executions за часом, найновіші в кінці
        # (get_execution_history вже має сортувати, але для надійності)
        sorted_executions = sorted(executions, key=lambda x: x.get('exec_time', 0))
        
        # Беремо останні кілька виконань, що стосуються закриття
        # Ми шукаємо виконання, яке зменшило або закрило позицію
        closing_executions = []
        current_pos_qty_simulated = local_position.get('initial_quantity', 0) # Починаємо з початкової кількості
        
        # Симулюємо зміну позиції на основі всіх переданих executions
        # Це важливо, якщо executions містять і відкриття, і закриття
        temp_pos_qty = 0
        if local_position.get('side') == 'BUY':
            for exec_item in sorted_executions:
                if exec_item['side'] == 'Buy':
                    temp_pos_qty += exec_item['quantity']
                elif exec_item['side'] == 'Sell':
                    temp_pos_qty -= exec_item['quantity']
        elif local_position.get('side') == 'SELL':
            for exec_item in sorted_executions:
                if exec_item['side'] == 'Sell':
                    temp_pos_qty -= exec_item['quantity'] # Для шорта кількість від'ємна
                elif exec_item['side'] == 'Buy':
                    temp_pos_qty += exec_item['quantity']
        
        # Якщо після всіх executions позиція не нульова, щось не так з логікою або даними
        # Однак, для визначення причини закриття, ми дивимось на останні ордери, що зменшували позицію
        
        # Розглянемо останнє виконання, яке зменшило розмір позиції
        # Ми шукаємо orderLinkId, який міг би бути встановлений для SL/TP
        # Формат orderLinkId для SL/TP зазвичай: 'tpslOrder' або може містити 'adv_sl'/'adv_tp'
        
        last_closing_execution = None
        for exec_item in reversed(sorted_executions): # Дивимось з кінця
            is_closing_trade = False
            if local_position.get('side') == 'BUY' and exec_item.get('side') == 'Sell':
                is_closing_trade = True
            elif local_position.get('side') == 'SELL' and exec_item.get('side') == 'Buy':
                is_closing_trade = True

            if is_closing_trade:
                last_closing_execution = exec_item
                break # Знайшли останнє закриваюче виконання

        if not last_closing_execution:
            logger.warning(
                f"analyze_close_reason: Не знайдено закриваючих виконань для {local_position.get('symbol')}. "
                f"Локальна сторона: {local_position.get('side')}. Всього виконань: {len(sorted_executions)}"
            )
            return "external_no_closing_exec"

        close_price = last_closing_execution.get('price', 0)
        order_link_id = last_closing_execution.get('raw_data', {}).get('orderLinkId', '')
        order_id_exec = last_closing_execution.get('order_id', '') # orderId самого виконання
        # Bybit може використовувати поле blockTradeId для TP/SL ордерів, що спрацювали як Market
        # Або orderType може бути Market, але orderLinkId вказуватиме на tpsl.
        # Також є поле `stopOrderType` у відповіді `get_open_orders` та `get_order_history`

        logger.info(
            f"analyze_close_reason для {local_position.get('symbol')}: "
            f"Останнє закриваюче виконання: Ціна={close_price}, "
            f"OrderLinkID='{order_link_id}', OrderID='{order_id_exec}', "
            f"Тип ордера виконання: {last_closing_execution.get('order_type')}"
        )
        logger.debug(f"Повне останнє закриваюче виконання: {last_closing_execution.get('raw_data')}")


        # 1. Перевірка за orderLinkId (найбільш надійний спосіб, якщо Bybit його встановлює для SL/TP)
        # Bybit часто використовує `tpslOrder` або `DEFAULT_ORDER_LINK_ID_PREFIX_TPSL`
        # (або щось подібне, потрібно перевірити реальні дані)
        if order_link_id:
            if "tpsl" in order_link_id.lower() or "stop_loss" in order_link_id.lower() or "take_profit" in order_link_id.lower():
                # Потрібно розрізнити SL від TP
                # Спробуємо за ціною, якщо orderLinkId не дає точної відповіді
                local_sl = local_position.get('current_stop_loss', 0)
                sl_tolerance_abs = local_sl * 0.002 # 0.2% толерантність
                
                if local_sl > 0 and abs(close_price - local_sl) <= sl_tolerance_abs:
                    logger.info(f"Причина закриття: STOP_LOSS (по orderLinkId='{order_link_id}' та близькості ціни)")
                    return "stop_loss"

                for i, tp_level in enumerate(local_position.get('take_profit_levels', []), 1):
                    tp_price = tp_level.get('price', 0)
                    tp_tolerance_abs = tp_price * 0.002 # 0.2% толерантність
                    if tp_price > 0 and abs(close_price - tp_price) <= tp_tolerance_abs:
                        logger.info(f"Причина закриття: TAKE_PROFIT_{i} (по orderLinkId='{order_link_id}' та близькості ціни)")
                        return f"take_profit_{i}"
                
                # Якщо orderLinkId вказує на tpsl, але ціна не збігається точно, це може бути трейлінг або щось інше
                logger.warning(f"OrderLinkId '{order_link_id}' вказує на TP/SL, але ціна не збігається точно. Потрібен детальніший аналіз.")
                # Тут можна додати перевірку, чи був активний трейлінг, і чи SL був нещодавно оновлений.
                # Для простоти поки що повернемо більш загальну причину
                if local_position.get('trailing_stop_active'):
                     logger.info(f"Причина закриття: TRAILING_STOP (по orderLinkId='{order_link_id}' та активному трейлінгу)")
                     return "trailing_stop" # Припускаємо трейлінг, якщо він був активний

                # Якщо не вдалося визначити точніше, але orderLinkId вказує на TP/SL
                logger.info(f"Причина закриття: TP_SL_ORDER_LINK_ID (по orderLinkId='{order_link_id}', точний тип не визначено за ціною)")
                return "tpsl_by_order_link_id"


        # 2. Якщо orderLinkId не дав відповіді, аналізуємо ціни (менш надійно через прослизання)
        local_sl = local_position.get('current_stop_loss', 0)
        local_entry = local_position.get('entry_price', 0)
        
        # Толерантність для порівняння цін (можна налаштувати)
        # Використовуємо абсолютну толерантність на основі ATR, якщо є, або %
        atr_at_entry = local_position.get('initial_atr_at_entry', 0)
        price_tolerance_abs = 0
        if atr_at_entry > 0:
            price_tolerance_abs = atr_at_entry * 0.1 # наприклад, 10% від ATR
        else:
            price_tolerance_abs = close_price * 0.0015 # 0.15% від ціни закриття

        logger.info(f"Аналіз цін для визначення причини закриття: ClosePrice={close_price}, LocalSL={local_sl}, LocalEntry={local_entry}, PriceToleranceAbs={price_tolerance_abs:.6f}")

        # Перевірка Stop Loss
        if local_sl > 0 and abs(close_price - local_sl) <= price_tolerance_abs:
            logger.info(f"Причина закриття: STOP_LOSS (по близькості ціни)")
            return "stop_loss"

        # Перевірка Take Profit рівнів
        for i, tp_level in enumerate(local_position.get('take_profit_levels', []), 1):
            tp_price = tp_level.get('price', 0)
            if tp_price > 0 and abs(close_price - tp_price) <= price_tolerance_abs:
                logger.info(f"Причина закриття: TAKE_PROFIT_{i} (по близькості ціни)")
                return f"take_profit_{i}"
        
        # Перевірка Breakeven (якщо SL був переміщений близько до ціни входу)
        # Це може бути складно відрізнити від звичайного SL, якщо SL близько до входу
        # Breakeven зазвичай спрацьовує, коли SL = entry_price +/- buffer
        # Якщо local_sl дуже близький до local_entry
        if local_sl > 0 and local_entry > 0:
            breakeven_range_factor = 0.001 # 0.1% від ціни входу як діапазон для беззбитка
            breakeven_min_range = local_entry * breakeven_range_factor
            
            is_sl_near_entry = abs(local_sl - local_entry) <= breakeven_min_range
            
            if is_sl_near_entry and abs(close_price - local_sl) <= price_tolerance_abs:
                 # Якщо SL був близько до входу і спрацював, це міг бути беззбиток
                 logger.info(f"Причина закриття: BREAKEVEN (SL був близько до входу і спрацював по ціні)")
                 return "breakeven"


        # Якщо жодна з умов не спрацювала, вважаємо зовнішнім закриттям
        logger.info(f"Причина закриття: EXTERNAL (жодна з умов SL/TP/BE не спрацювала за ціною або orderLinkId)")
        return "external_price_mismatch"

    async def reconcile_position_with_history(self, symbol: str,
                                        local_position: Dict[str, Any]) -> Tuple[bool, Optional[Dict], str]:
        """
        Звіряє локальну позицію бота з історією виконання на біржі.
        З покращеним обробленням Demo режиму та автоматичним очищенням позицій.

        Returns:
            Tuple[bool, Optional[Dict], str]:
                - bool: True якщо синхронізовані, False якщо є розбіжності
                - Optional[Dict]: Оновлена позиція або None якщо закрита/не знайдена.
                                Якщо позиція закрита, словник може містити 'closed_externally_details'
                                з інформацією для розрахунку P&L та причиною.
                - str: Детальний опис статусу синхронізації
        """
        try:
            is_demo_mode = self.trading_config['mode'].lower() == 'demo'

            logger.info(f"🔄 RECONCILE DEBUG: Початок звірки позиції {symbol} з історією")
            logger.info(f"   🎭 Demo режим: {is_demo_mode}")

            lookback_hours = TRADING_CONFIG.get('sync_lookback_hours', 72)
            qty_tolerance = PRECISION_CONFIG['qty_tolerance']
            price_tolerance_percentage = TRADING_CONFIG.get('sync_tolerance_price_percentage', 0.002)  # 0.2% толерантність для ціни

            local_qty = local_position.get('quantity', 0.0)
            local_side = local_position.get('side', '')
            local_entry_price = local_position.get('entry_price', 0.0)
            local_order_id = local_position.get('exchange_order_id')

            logger.info(f"📊 RECONCILE: Локальна позиція {symbol}:")
            logger.info(f"   📈 Сторона: {local_side}, Кількість: {local_qty:.8f}")
            logger.info(f"   💰 Ціна входу: {local_entry_price:.6f}")
            logger.info(f"   🆔 Order ID відкриття: {local_order_id}")

            history_position, executions_history, diagnostics = await self.analyze_position_from_execution_history(
                symbol=symbol,
                bot_order_id=local_order_id,
                lookback_hours=lookback_hours
            )

            sync_report = {
                'timestamp': datetime.now(timezone.utc).isoformat(), 'symbol': symbol,
                'local_data': {'side': local_side, 'quantity': local_qty, 'entry_price': local_entry_price, 'order_id': local_order_id},
                'history_analysis': {'position_found': history_position is not None,
                                    'details': history_position if history_position else "No active position found in history"},
                'diagnostics_from_analysis': diagnostics, 'discrepancies': [], 'action_taken': 'none', 'demo_mode': is_demo_mode
            }

            if not history_position:
                sync_report['history_data'] = {'status': 'closed_or_not_found', 'source': diagnostics.get('source_of_truth', 'unknown')}

                if abs(local_qty) > qty_tolerance:
                    # Позиція закрита зовнішньо. Збираємо дані для зовнішнього розрахунку P&L.
                    close_price_for_pnl_calc = local_entry_price  # Fallback
                    analyzed_reason_str = "Unknown - history shows no active position"

                    if diagnostics.get('last_closing_order_details'):
                        last_order = diagnostics['last_closing_order_details']
                        close_price_for_pnl_calc = safe_float_convert(last_order.get('avgPrice', local_entry_price))
                        analyzed_reason_str = (f"Last order: ID {last_order.get('orderId')}, "
                                            f"Status {last_order.get('orderStatus')}, "
                                            f"SL/TP Type: {last_order.get('stopOrderType','N/A')}, "
                                            f"Create Type: {last_order.get('createType','N/A')}")
                        logger.info(f"ℹ️ RECONCILE: Використовуємо avgPrice '{close_price_for_pnl_calc}' з останнього ордера закриття.")
                    elif executions_history:
                        last_exec = executions_history[-1]
                        close_price_for_pnl_calc = last_exec.get('price', local_entry_price)
                        analyzed_reason_str = await self.analyze_close_reason(executions_history, local_position)
                        logger.info(f"ℹ️ RECONCILE: Використовуємо ціну останнього execution '{close_price_for_pnl_calc}'. Причина: {analyzed_reason_str}")
                    elif is_demo_mode:
                        try:
                            current_mark_price = await self._get_current_market_price(symbol)
                            if current_mark_price > 0:
                                close_price_for_pnl_calc = current_mark_price
                            analyzed_reason_str = "Assumed closed at current mark price (DEMO, no exec/order history)"
                            logger.info(f"ℹ️ RECONCILE (DEMO): Використовуємо поточну ринкову ціну '{close_price_for_pnl_calc}'.")
                        except Exception: 
                            pass

                    sync_report['discrepancies'].append(f'position_not_found_in_history_local_open_{diagnostics.get("source_of_truth")}')
                    sync_report['action_taken'] = 'mark_as_closed_due_to_history'
                    
                    # Формуємо словник з деталями для зовнішнього закриття
                    closed_externally_details = {
                        'entry_price': local_entry_price,
                        'close_price': close_price_for_pnl_calc,
                        'quantity': local_qty,
                        'side': local_side,
                        'reason': analyzed_reason_str,
                        'source': diagnostics.get("source_of_truth", "unknown")
                    }

                    status_msg = (
                        f"🚨 ІСТОРІЯ/API: Позиція {symbol} ({local_side} {local_qty:.8f}) "
                        f"не знайдена активною. Ймовірно, закрита ~{close_price_for_pnl_calc:.6f}. "
                        f"Джерело: {diagnostics.get('source_of_truth', 'unknown')}. Деталі: {analyzed_reason_str}"
                    )
                    logger.warning(f"❌ RECONCILE: {status_msg}")
                    # Повертаємо None для позиції, але передаємо деталі для обробки в TradingBot
                    return False, {'closed_externally_details': closed_externally_details}, status_msg
                else:
                    sync_report['action_taken'] = 'already_closed_confirmed_by_history'
                    status_msg = f"✅ Позиція {symbol} закрита/відсутня локально та підтверджена аналізом історії/API ({diagnostics.get('source_of_truth', 'unknown')})."
                    logger.info(f"ℹ️ RECONCILE: {status_msg}")
                    return True, None, status_msg

            history_qty = history_position['size']
            history_side = history_position['side']
            history_avg_price = history_position['avg_price']
            history_source = history_position.get('source', diagnostics.get('source_of_truth', 'unknown'))

            sync_report['history_data'] = {
                'status': 'open', 'side': history_side, 'quantity': history_qty,
                'avg_price': history_avg_price, 'source': history_source,
                'raw_details': history_position.get('raw_data_source', 'N/A')
            }

            logger.info(f"📊 RECONCILE: Дані з історії/API для {symbol}:")
            logger.info(f"   📈 Сторона: {history_side}, Кількість: {history_qty:.8f}")
            logger.info(f"   💰 Середня ціна: {history_avg_price:.6f}, Джерело: {history_source}")

            qty_match = abs(local_qty - history_qty) <= qty_tolerance
            side_match = local_side.upper() == history_side.upper()
            price_match = True  # За замовчуванням
            if local_entry_price > 0 and history_avg_price > 0:  # Тільки якщо обидві ціни валідні
                price_diff_percentage = abs(local_entry_price - history_avg_price) / local_entry_price
                price_match = price_diff_percentage <= price_tolerance_percentage
            elif local_entry_price == 0 and history_avg_price == 0:  # Якщо обидві нульові, вважаємо збігом
                price_match = True
            elif local_entry_price == 0 or history_avg_price == 0:  # Якщо одна з них нульова, а інша ні - не збіг
                price_match = False

            if qty_match and side_match and price_match:
                sync_report['action_taken'] = 'fully_synchronized'
                status_msg = f"✅ Позиція {symbol} повністю синхронізована (Джерело: {history_source})."
                logger.info(f"ℹ️ RECONCILE: {status_msg}")
                updated_pos_synced = local_position.copy()
                
                # ✅ CRITICAL FIX: Preserve valid entry price during sync confirmation
                final_sync_entry_price = local_entry_price
                if local_entry_price <= 0 and history_avg_price > 0:
                    final_sync_entry_price = history_avg_price
                    logger.info(f"🔧 SYNC ENTRY PRICE RECOVERY: Using history price {history_avg_price:.6f} for {symbol}")
                elif local_entry_price > 0:
                    final_sync_entry_price = local_entry_price
                    logger.debug(f"🔧 SYNC ENTRY PRICE PRESERVATION: Keeping local price {local_entry_price:.6f} for {symbol}")
                
                updated_pos_synced.update({
                    'quantity': history_qty,
                    'entry_price': final_sync_entry_price,  # Use preserved entry price
                    'latest_execution_time': history_position.get('latest_execution_time'),
                    'latest_execution_time_formatted': history_position.get('latest_execution_time_formatted'),
                    'last_sync_time': datetime.now(timezone.utc),
                    'sync_source': f'reconcile_confirmed_{history_source}',
                    'unrealized_pnl_from_api': history_position.get('unrealized_pnl')  # Додаємо PnL з API
                })
                return True, updated_pos_synced, status_msg

            discrepancy_details = []
            if not qty_match: 
                discrepancy_details.append(f"qty_local={local_qty:.8f}, history={history_qty:.8f}")
            if not side_match: 
                discrepancy_details.append(f"side_local={local_side}, history={history_side}")
            if not price_match and local_entry_price > 0 and history_avg_price > 0:
                price_diff_percentage = abs(local_entry_price - history_avg_price) / local_entry_price
                discrepancy_details.append(f"price_local={local_entry_price:.6f}, history={history_avg_price:.6f} (diff: {price_diff_percentage*100:.3f}%)")
            elif not price_match:  # Якщо одна з цін нульова
                discrepancy_details.append(f"price_local={local_entry_price:.6f}, history={history_avg_price:.6f} (one is zero)")

            sync_report['discrepancies'] = discrepancy_details

            updated_position_from_history = local_position.copy()
            
            # ✅ CRITICAL FIX: Preserve valid entry price to prevent corruption
            final_entry_price = local_entry_price  # Default to local entry price
            if local_entry_price <= 0 and history_avg_price > 0:
                # Only use history price if local is invalid and history is valid
                final_entry_price = history_avg_price
                logger.info(f"🔧 ENTRY PRICE RECOVERY: Using history entry price {history_avg_price:.6f} for {symbol} (local was {local_entry_price:.6f})")
            elif local_entry_price > 0 and history_avg_price <= 0:
                # Keep local price if it's valid and history is not
                final_entry_price = local_entry_price
                logger.info(f"🔧 ENTRY PRICE PRESERVATION: Keeping local entry price {local_entry_price:.6f} for {symbol} (history was {history_avg_price:.6f})")
            elif local_entry_price > 0 and history_avg_price > 0:
                # Both are valid - use local price to preserve original entry
                final_entry_price = local_entry_price
                logger.debug(f"🔧 ENTRY PRICE PRESERVATION: Both prices valid, keeping local {local_entry_price:.6f} for {symbol}")
            
            updated_position_from_history.update({
                'quantity': history_qty,
                'side': history_side.upper(),
                'entry_price': final_entry_price,  # Use preserved/recovered entry price
                'latest_execution_time': history_position.get('latest_execution_time'),
                'latest_execution_time_formatted': history_position.get('latest_execution_time_formatted'),
                'last_sync_time': datetime.now(timezone.utc),
                'sync_source': f'reconcile_update_{history_source}',
                'sync_report': sync_report,
                'unrealized_pnl_from_api': history_position.get('unrealized_pnl')  # Додаємо PnL з API
            })

            sync_report['action_taken'] = 'local_position_updated_from_history'
            status_msg = f"🔄 ОНОВЛЕНО З ІСТОРІЇ/API ({history_source}): {symbol} - Розбіжності: {', '.join(discrepancy_details)}"
            logger.warning(f"⚠️ RECONCILE: {status_msg}")

            return False, updated_position_from_history, status_msg

        except Exception as e:
            error_msg = f"💥 Критична помилка звірки позиції {symbol}: {str(e)}"
            logger.error(error_msg, exc_info=True)
            # Повертаємо False та оригінальну локальну позицію, щоб уникнути її видалення або неправильного оновлення
            return False, local_position, error_msg

    async def validate_position_exists_on_exchange(self, symbol: str) -> Tuple[bool, Optional[Dict], str]:
        """
        Перевіряє, чи існує позиція на біржі за допомогою комбінації API позицій та історії.
        Використовується для швидкої валідації та автоматичного очищення локальних позицій.
        
        Returns:
            Tuple[bool, Optional[Dict], str]:
                - bool: True якщо позиція існує на біржі
                - Optional[Dict]: Дані позиції з біржі або None
                - str: Статус перевірки
        """
        try:
            logger.debug(f"🔍 Перевірка існування позиції {symbol} на біржі...")
            
            # Спочатку перевіряємо через API позицій (швидше)
            exchange_position = await self.get_position_via_api(symbol)
            if exchange_position:
                logger.debug(f"✅ Позиція {symbol} знайдена через API позицій")
                return True, exchange_position, "position_found_via_api"
            
            # Якщо не знайдено через API, перевіряємо історію (може бути затримка синхронізації)
            lookback_hours = TRADING_CONFIG.get('sync_lookback_hours_short', 24)
            history_position, _, diagnostics = await self.analyze_position_from_execution_history(
                symbol=symbol,
                lookback_hours=lookback_hours
            )
            
            if history_position:
                logger.debug(f"✅ Позиція {symbol} знайдена через історію виконання")
                return True, history_position, f"position_found_via_history_{diagnostics.get('source_of_truth', 'unknown')}"
            
            logger.debug(f"❌ Позиція {symbol} не знайдена ні через API, ні через історію")
            return False, None, "position_not_found_on_exchange"
            
        except Exception as e:
            error_msg = f"Помилка перевірки існування позиції {symbol}: {str(e)}"
            logger.error(error_msg, exc_info=True)
            # У випадку помилки повертаємо True для безпеки (щоб не видаляти позицію випадково)
            return True, None, error_msg

    async def analyze_and_get_position_closure_details(self, symbol: str, local_position: Dict) -> Optional[Dict]:
        """
        Аналізує закриття позиції та повертає детальну інформацію про причини та обставини закриття.
        
        Args:
            symbol: Торгова пара
            local_position: Локальні дані позиції
            
        Returns:
            Dict з деталями закриття або None якщо аналіз неможливий
        """
        try:
            logger.info(f"🔍 Аналіз закриття позиції {symbol}...")
            
            # Отримуємо історію виконання для аналізу
            lookback_hours = TRADING_CONFIG.get('sync_lookback_hours_short', 24)
            history_position, executions, diagnostics = await self.analyze_position_from_execution_history(
                symbol=symbol,
                lookback_hours=lookback_hours
            )
            
            if not executions:
                logger.warning(f"Немає історії виконання для аналізу закриття {symbol}")
                return None
            
            # Аналізуємо причину закриття
            closure_reason = await self.analyze_close_reason(executions, local_position)
            
            # Знаходимо останнє виконання, що закрило позицію
            sorted_executions = sorted(executions, key=lambda x: x.get('exec_time', 0))
            
            last_closing_execution = None
            for exec_item in reversed(sorted_executions):
                is_closing = False
                if local_position.get('side') == 'BUY' and exec_item.get('side') == 'Sell':
                    is_closing = True
                elif local_position.get('side') == 'SELL' and exec_item.get('side') == 'Buy':
                    is_closing = True
                
                if is_closing:
                    last_closing_execution = exec_item
                    break
            
            if not last_closing_execution:
                logger.warning(f"Не знайдено закриваючого виконання для {symbol}")
                return None
            
            # Обчислюємо деталі закриття
            entry_price = float(local_position.get('entry_price', 0))
            exit_price = float(last_closing_execution.get('price', 0))
            quantity = float(local_position.get('quantity', 0))
            side = local_position.get('side', 'BUY')
            
            # ✅ CRITICAL FIX: Validate entry price and attempt recovery
            if entry_price <= 0:
                # Try to recover entry price from other sources
                initial_entry_price = float(local_position.get('initial_entry_price', 0))
                original_signal_entry = 0
                if 'original_signal_data' in local_position:
                    original_signal_entry = float(local_position['original_signal_data'].get('entry_price', 0))
                
                if initial_entry_price > 0:
                    entry_price = initial_entry_price
                    logger.warning(f"🔧 ENTRY PRICE RECOVERY: Recovered entry price {entry_price:.6f} from initial_entry_price for {symbol}")
                elif original_signal_entry > 0:
                    entry_price = original_signal_entry
                    logger.warning(f"🔧 ENTRY PRICE RECOVERY: Recovered entry price {entry_price:.6f} from original signal for {symbol}")
                else:
                    logger.error(f"❌ ENTRY PRICE ERROR: Cannot recover entry price for {symbol}, using exit price as fallback")
                    entry_price = exit_price  # Use exit price as last resort to prevent division by zero
            
            # Розрахунок P&L з додатковою валідацією
            pnl_usdt = 0.0
            if entry_price > 0 and exit_price > 0 and quantity > 0:
                if side == 'BUY':
                    pnl_usdt = (exit_price - entry_price) * quantity
                else:  # SELL
                    pnl_usdt = (entry_price - exit_price) * quantity
            else:
                logger.error(f"❌ P&L CALCULATION ERROR: Invalid values for {symbol} - entry: {entry_price}, exit: {exit_price}, qty: {quantity}")
            
            # ✅ IMPROVED: Safe percentage calculation with better validation
            pnl_percentage = 0.0
            if entry_price > 0 and quantity > 0:
                position_value = entry_price * quantity
                if position_value > 0:
                    pnl_percentage = (pnl_usdt / position_value) * 100
                else:
                    logger.warning(f"⚠️ P&L WARNING: Invalid position value for {symbol}")
            else:
                logger.warning(f"⚠️ P&L WARNING: Cannot calculate percentage for {symbol} - entry: {entry_price}, qty: {quantity}")
            
            # Визначаємо тип закриття для відображення
            closure_display_type = self._get_closure_display_type(closure_reason)
            
            # Час закриття
            exec_time = last_closing_execution.get('exec_time', 0)
            closure_time = datetime.fromtimestamp(exec_time / 1000, tz=timezone.utc) if exec_time > 0 else datetime.now(timezone.utc)
            
            # Перевіряємо чи це повне або часткове закриття
            total_closed_quantity = sum(
                exec_item['quantity'] for exec_item in sorted_executions 
                if ((side == 'BUY' and exec_item.get('side') == 'Sell') or 
                    (side == 'SELL' and exec_item.get('side') == 'Buy'))
            )
            
            is_full_closure = abs(total_closed_quantity - quantity) < PRECISION_CONFIG.get('qty_tolerance', 0.000001)
            
            closure_details = {
                'symbol': symbol,
                'closure_reason': closure_reason,
                'closure_display_type': closure_display_type,
                'entry_price': entry_price,
                'exit_price': exit_price,
                'quantity': quantity,
                'closed_quantity': total_closed_quantity,
                'side': side,
                'pnl_usdt': pnl_usdt,
                'pnl_percentage': pnl_percentage,
                'closure_time': closure_time,
                'is_full_closure': is_full_closure,
                'execution_details': last_closing_execution,
                'total_executions': len(executions),
                'analysis_source': diagnostics.get('source_of_truth', 'executions')
            }
            
            logger.info(f"✅ Аналіз закриття {symbol} завершено: {closure_display_type}, P&L: {pnl_usdt:.3f} USDT ({pnl_percentage:.2f}%)")
            
            return closure_details
            
        except Exception as e:
            logger.error(f"Помилка аналізу закриття позиції {symbol}: {e}", exc_info=True)
            return None
    
    def _get_closure_display_type(self, closure_reason: str) -> str:
        """Конвертує внутрішній код причини закриття в зрозумілий текст для користувача"""
        closure_mapping = {
            'stop_loss': 'SL Hit',
            'take_profit_1': 'TP 1 Hit',
            'take_profit_2': 'TP 2 Hit', 
            'take_profit_3': 'TP 3 Hit',
            'trailing_stop': 'Trailing SL Hit',
            'tpsl_by_order_link_id': 'TP/SL Hit',
            'breakeven_close': 'Breakeven Close',
            'external_close': 'Manual Close',
            'external_no_executions': 'External Close',
            'external_no_closing_exec': 'External Close'
        }
        
        return closure_mapping.get(closure_reason, 'Position Closed')

    async def _get_current_market_price(self, symbol: str) -> float:
        """Отримання поточної ринкової ціни для символу"""
        try:
            ticker_result = await self.get_tickers("linear", symbol)
            if ticker_result and ticker_result.get('retCode') == 0:
                ticker_list = ticker_result.get('result', {}).get('list', [])
                if ticker_list:
                    return safe_float_convert(ticker_list[0].get('lastPrice', 0))
            return 0.0
        except Exception as e:
            logger.error(f"Помилка отримання ринкової ціни для {symbol}: {e}")
            return 0.0
    
    # Інші методи залишаються без змін...
    async def get_server_time(self) -> Dict[str, Any]:
        """Отримання серверного часу"""
        try:
            result = await self._make_request_with_retry(
                self.client.get_server_time
            )
            return result
        except Exception as e:
            logger.error(f"Помилка отримання серверного часу: {e}")
            return {}
    
    async def check_time_sync(self) -> bool:
        """Перевірка синхронізації часу з сервером"""
        try:
            server_time_result = await self.get_server_time()
            if not server_time_result or server_time_result.get('retCode') != 0:
                logger.error("Не вдалося отримати серверний час")
                return False
            
            server_time = int(server_time_result['result']['timeSecond'])
            local_time = int(datetime.now(timezone.utc).timestamp())
            
            time_diff = abs(server_time - local_time)
            
            if time_diff > 5:  # Різниця більше 5 секунд
                logger.warning(f"Різниця часу з сервером: {time_diff} секунд")
                return False
            
            logger.info(f"Час синхронізовано. Різниця: {time_diff} секунд")
            return True
            
        except Exception as e:
            logger.error(f"Помилка перевірки синхронізації часу: {e}")
            return False
    
    @cache_result(ttl_seconds=30)  # Кешуємо тікери на 30 секунд
    async def get_tickers(self, category: str, symbol: Optional[str] = None) -> Dict[str, Any]:
        """Отримання інформації про тикери."""
        try:
            params = {"category": category}
            if symbol:
                params["symbol"] = symbol
            
            result = await self._make_request_with_retry(
                self.client.get_tickers,
                **params
            )
            
            if not result or result.get('retCode') != 0:
                logger.error(f"Помилка отримання тикерів для {category} (символ: {symbol}): {result}")
                return {} # Повертаємо порожній словник у разі помилки
            
            logger.debug(f"Тикери отримано успішно для {category} (символ: {symbol}): {result}")
            return result # Повертаємо повний результат
            
        except Exception as e:
            logger.error(f"Критична помилка отримання тикерів для {category} (символ: {symbol}): {e}", exc_info=True)
            return {}

    @cache_result(ttl_seconds=60)  # Кешуємо klines на 1 хвилину
    async def get_klines(self, symbol: str, interval: str, limit: int = 1000) -> List[Dict]:
        """Отримання історичних свічок"""
        try:
            result = await self._make_request_with_retry(
                self.client.get_kline,
                category="linear",
                symbol=symbol,
                interval=interval,
                limit=limit
            )
            
            if not result or result.get('retCode') != 0:
                logger.error(f"Помилка отримання свічок для {symbol}: {result}")
                return []
            
            klines_data = result['result']['list']
            
            # Конвертація в зрозумілий формат
            formatted_klines = []
            for kline in klines_data:
                formatted_kline = {
                    'timestamp': datetime.fromtimestamp(int(kline[0])/1000).strftime('%Y-%m-%d %H:%M:%S'),
                    'open_price': safe_float_convert(kline[1]),
                    'high_price': safe_float_convert(kline[2]),
                    'low_price': safe_float_convert(kline[3]),
                    'close_price': safe_float_convert(kline[4]),
                    'volume': safe_float_convert(kline[5])
                }
                formatted_klines.append(formatted_kline)
            
            # Сортування за часом (від старого до нового)
            formatted_klines.sort(key=lambda x: x['timestamp'])
            
            logger.info(f"Отримано {len(formatted_klines)} свічок для {symbol}")
            return formatted_klines
            
        except Exception as e:
            logger.error(f"Помилка отримання свічок для {symbol}: {e}")
            return []
    
    async def get_latest_kline(self, symbol: str, interval: str) -> Optional[Dict]:
        """Отримання останньої свічки"""
        try:
            klines = await self.get_klines(symbol, interval, limit=1)
            return klines[0] if klines else None
        except Exception as e:
            logger.error(f"Помилка отримання останньої свічки для {symbol}: {e}")
            return None

    async def get_order_history(self,
                            symbol: Optional[str] = None,
                            order_id: Optional[str] = None,
                            limit: int = 50,
                            start_time: Optional[int] = None,
                            end_time: Optional[int] = None,
                            order_status: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Отримання історії ордерів з Bybit API v5.

        Args:
            symbol: Торгова пара
            order_id: ID ордера
            limit: Кількість записів (макс. 100)
            start_time: Початковий час у мілісекундах UTC
            end_time: Кінцевий час у мілісекундах UTC
            order_status: Фільтр за статусом ордера (наприклад, "Filled", "Cancelled")

        Returns:
            List[Dict]: Список ордерів з деталями
        """
        try:
            params = {
                "category": "linear",
                "limit": min(limit, 100)
            }
            if symbol:
                params["symbol"] = symbol
            if order_id:
                params["orderId"] = order_id
            if start_time:
                params["startTime"] = str(start_time)
            if end_time:
                params["endTime"] = str(end_time)
            if order_status:
                params["orderStatus"] = order_status

            # Якщо немає часових рамок, Bybit за замовчуванням повертає за останні 7 днів
            # Для більшої ясності, можна встановити їх явно, якщо потрібно
            if not start_time and not end_time:
                current_time_ms_hist = int(datetime.now(timezone.utc).timestamp() * 1000)
                start_time_7d_hist = current_time_ms_hist - (7 * 24 * 60 * 60 * 1000)
                params["startTime"] = str(start_time_7d_hist)
                params["endTime"] = str(current_time_ms_hist)


            logger.info(f"🔍 ORDER HISTORY DEBUG: Запит історії ордерів")
            logger.info(f"   📊 Символ: {symbol or 'ВСІ'}, OrderID: {order_id or 'N/A'}")
            logger.info(f"   📅 Період: {params.get('startTime')} - {params.get('endTime')}")
            logger.info(f"   🔧 Параметри API: {params}")

            result = await self._make_request_with_retry(
                self.client.get_order_history,
                **params
            )

            logger.info(f"📨 ORDER HISTORY RESPONSE:")
            logger.info(f"   ✅ retCode: {result.get('retCode') if result else 'None'}")
            logger.info(f"   📝 retMsg: {result.get('retMsg') if result else 'None'}")

            if not result or result.get('retCode') != 0:
                logger.error(f"❌ Помилка отримання історії ордерів для {symbol}: {result}")
                return []

            orders_data = result.get('result', {}).get('list', [])
            logger.info(f"   📈 Кількість raw orders: {len(orders_data)}")
            
            # Bybit вже повертає в форматі, зручному для використання, тому додаткове форматування може бути мінімальним
            # Можна додати datetime об'єкти для зручності, якщо потрібно
            for order in orders_data:
                order['createdTimeDT'] = datetime.fromtimestamp(int(order.get('createdTime', 0))/1000, tz=timezone.utc) if order.get('createdTime') else None
                order['updatedTimeDT'] = datetime.fromtimestamp(int(order.get('updatedTime', 0))/1000, tz=timezone.utc) if order.get('updatedTime') else None

            orders_data.sort(key=lambda x: x.get('updatedTime', 0), reverse=True) # Найновіші спочатку

            return orders_data

        except Exception as e:
            logger.error(f"💥 Критична помилка отримання історії ордерів для {symbol}: {e}", exc_info=True)
            return []

    async def get_account_balance(self) -> Dict[str, Any]:
        """Отримання балансу акаунта"""
        try:
            result = await self._make_request_with_retry(
                self.client.get_wallet_balance,
                accountType="UNIFIED"
            )
            
            if not result or result.get('retCode') != 0:
                logger.error(f"Помилка отримання балансу: {result}")
                return {}
            
            return result['result']
            
        except Exception as e:
            logger.error(f"Помилка отримання балансу: {e}")
            return {}
    
    async def get_usdt_balance(self) -> float:
        """Отримання USDT балансу"""
        try:
            balance_result = await self.get_account_balance()
            
            if not balance_result or 'list' not in balance_result:
                return 0.0
            
            for account in balance_result['list']:
                if account.get('accountType') == 'UNIFIED':
                    for coin in account.get('coin', []):
                        if coin.get('coin') == 'USDT':
                            return safe_float_convert(coin.get('walletBalance', 0))
            
            return 0.0
            
        except Exception as e:
            logger.error(f"Помилка отримання USDT балансу: {e}")
            return 0.0
    
    async def place_order(self, 
                          symbol: str, 
                          side: str, 
                          qty: str, 
                          order_type: str = "Market",
                          price: Optional[str] = None,
                          take_profit_price: Optional[str] = None,
                          stop_loss_price: Optional[str] = None,
                          position_idx: int = 0,
                          time_in_force: str = "GTC") -> Dict[str, Any]:
        """
        Розміщення ордеру з можливістю вказати TP/SL та ціну для лімітних ордерів.
        Для Unified Trading Account, category="linear" для USDT-M контрактів.
        positionIdx: 0 для one-way mode, 1 для Buy (hedge mode), 2 для Sell (hedge mode).
        """
        try:
            params = {
                "category": "linear",
                "symbol": symbol,
                "side": side,
                "orderType": order_type,
                "qty": qty,
                "positionIdx": position_idx,
            }

            if order_type == "Limit":
                if not price:
                    logger.error(f"Ціна (price) є обов'язковою для лімітного ордера ({symbol}). Ордер не розміщено.")
                    return {"retCode": -1, "retMsg": "Price is required for Limit order", "result": {}, "retExtInfo": {}, "time": 0}
                params["price"] = price
                params["timeInForce"] = time_in_force # GTC, IOC, FOK

            # Ці параметри можуть бути застосовані і до лімітних ордерів
            if take_profit_price:
                params["takeProfit"] = take_profit_price
            if stop_loss_price:
                params["stopLoss"] = stop_loss_price
            
            if take_profit_price or stop_loss_price:
                 params["tpslMode"] = "Full" # Або "Partial", якщо потрібно для TP/SL на лімітних

            logger.debug(f"Attempting to place order with params: {params}")

            result = await self._make_request_with_retry(
                self.client.place_order,
                **params
            )
            
            if result and result.get('retCode') == 0:
                order_id = result.get('result', {}).get('orderId', 'N/A')
                logger.info(f"Ордер ({order_type}) успішно розміщено: {symbol} {side} {qty} @ {price if price else 'Market'}. Order ID: {order_id}.")
            else:
                logger.error(f"Помилка розміщення ордеру ({order_type}) для {symbol} {side} {qty}: {result}")
            
            return result
            
        except Exception as e:
            logger.error(f"Критична помилка під час розміщення ордеру ({order_type}) для {symbol}: {e}", exc_info=True)
            return {"retCode": -1, "retMsg": str(e), "result": {}, "retExtInfo": {}, "time": 0}
    
    async def cancel_order(self, symbol: str, order_id: str) -> Dict[str, Any]:
        """Скасування ордеру"""
        try:
            result = await self._make_request_with_retry(
                self.client.cancel_order,
                category="linear",
                symbol=symbol,
                orderId=order_id
            )
            
            if result and result.get('retCode') == 0:
                logger.info(f"Ордер скасовано: {order_id}")
            else:
                logger.error(f"Помилка скасування ордеру: {result}")
            
            return result
            
        except Exception as e:
            logger.error(f"Помилка скасування ордеру: {e}")
            return {}
    
    async def get_open_orders(self, symbol: str = None) -> List[Dict]:
        """Отримання відкритих ордерів"""
        try:
            params = {"category": "linear"}
            if symbol:
                params["symbol"] = symbol
                
            result = await self._make_request_with_retry(
                self.client.get_open_orders,
                **params
            )
            
            if not result or result.get('retCode') != 0:
                logger.error(f"Помилка отримання відкритих ордерів: {result}")
                return []
            
            return result['result']['list']
            
        except Exception as e:
            logger.error(f"Помилка отримання відкритих ордерів: {e}")
            return []
    
    async def get_positions(self, symbol: str = None) -> List[Dict]:
        """Отримання позицій"""
        try:
            params = {
                "category": "linear",
                "settleCoin": "USDT"  # ✅ ДОДАЄМО ОБОВ'ЯЗКОВИЙ ПАРАМЕТР!
            }
            if symbol:
                params["symbol"] = symbol
                
            result = await self._make_request_with_retry(
                self.client.get_positions,
                **params
            )
            
            if not result or result.get('retCode') != 0:
                logger.error(f"Помилка отримання позицій: {result}")
                return []
            
            return result['result']['list']
            
        except Exception as e:
            logger.error(f"Помилка отримання позицій: {e}")
            return []

    async def get_instrument_info(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Отримання інформації про інструмент (фільтри лотів, кроки цін тощо)"""
        try:
            result = await self._make_request_with_retry(
                self.client.get_instruments_info,
                category="linear",
                symbol=symbol
            )
            if result and result.get('retCode') == 0 and result['result']['list']:
                # Повертаємо інформацію про перший (і єдиний очікуваний) інструмент у списку
                return result['result']['list'][0]
            else:
                logger.error(f"Помилка отримання інфо для інструменту {symbol}: {result}")
                return None
        except Exception as e:
            logger.error(f"Критична помилка отримання інфо для інструменту {symbol}: {e}", exc_info=True)
            return None

    async def set_trading_stop_for_position(
        self, 
        symbol: str, 
        tpsl_mode: str = "Full", 
        position_idx: int = 0, 
        take_profit: Optional[str] = None, 
        tp_trigger_by: str = "LastPrice", 
        stop_loss: Optional[str] = None, 
        sl_trigger_by: str = "LastPrice",
        category: str = "linear"
    ) -> Optional[Dict[str, Any]]:
        """
        Встановлює або змінює TP/SL для позиції з перевіркою існування позиції.
        """
        try:
            # Перевірка, чи self.client ініціалізовано (додаткова безпека)
            if self.client is None:
                logger.critical(f"Клієнт Bybit (self.client) не ініціалізований в set_trading_stop_for_position для {symbol}.")
                return {
                    "retCode": -1, 
                    "retMsg": "Bybit client not initialized in API manager",
                    "result": {}, "retExtInfo": {}, "time": int(time.time() * 1000)
                }

            # ✅ ДОДАНО: Перевірка існування позиції перед встановленням TP/SL
            logger.debug(f"Перевірка існування позиції для {symbol} перед встановленням TP/SL...")
            try:
                positions = await self.get_positions(symbol=symbol)
                position_exists = False
                position_size = 0.0
                
                if positions:
                    for pos in positions:
                        if (pos.get('symbol') == symbol and 
                            safe_float_convert(pos.get('size', 0)) > PRECISION_CONFIG['price_tolerance']):
                            position_exists = True
                            position_size = safe_float_convert(pos.get('size', 0))
                            logger.debug(f"Знайдено позицію {symbol}: size={position_size}")
                            break
                
                if not position_exists:
                    logger.warning(f"Спроба встановити TP/SL для {symbol}, але позиція не існує або має нульовий розмір")
                    return {
                        "retCode": 10001, 
                        "retMsg": "can not set tp/sl/ts for zero position",
                        "result": {}, "retExtInfo": {}, "time": int(time.time() * 1000)
                    }
                
                logger.info(f"Позиція {symbol} підтверджена (size={position_size}), продовжуємо встановлення TP/SL...")
                
            except Exception as e_pos_check:
                logger.error(f"Помилка перевірки позиції для {symbol}: {e_pos_check}")
                # Продовжуємо виконання навіть якщо перевірка не вдалася, але логуємо попередження
                logger.warning(f"Не вдалося перевірити позицію {symbol}, продовжуємо встановлення TP/SL (може призвести до помилки API)")

            params = {
                "category": category,
                "symbol": symbol,
                "tpslMode": tpsl_mode,
                "positionIdx": position_idx,
                "tpTriggerBy": tp_trigger_by,
                "slTriggerBy": sl_trigger_by,
            }
            if take_profit is not None:
                params["takeProfit"] = take_profit
            if stop_loss is not None:
                params["stopLoss"] = stop_loss
            
            logger.debug(f"Запит на встановлення TP/SL для {symbol}: {params}")

            result = await self._make_request_with_retry(
                self.client.set_trading_stop, # <--- ЗМІНЕНО з self.session на self.client
                **params 
            )

            if result and result.get('retCode') == 0:
                logger.info(
                    f"Успішно встановлено/змінено TP/SL для {symbol}. "
                    f"SL: {stop_loss or 'N/A'}, TP: {take_profit or 'N/A'}. "
                    f"Відповідь: {result.get('retMsg')}"
                )
                return result
            elif result and result.get('retCode') == 34040:
                logger.warning(
                    f"TP/SL для {symbol} не змінено на біржі (код 34040 - not modified). "
                    f"SL: {stop_loss or 'N/A'}, TP: {take_profit or 'N/A'}. "
                    f"Відповідь API: {result.get('retMsg')}"
                )
                return result 
            else:
                error_message = result.get('retMsg', 'Невідома помилка API') if result else 'Відповідь API відсутня або клієнт не ініціалізований'
                error_code_val = result.get('retCode', 'N/A') if result else 'N/A'
                
                # ✅ ДОДАНО: Покращена обробка помилки нульової позиції
                if str(error_code_val) == '10001' and 'zero position' in error_message.lower():
                    logger.warning(
                        f"Спроба встановити TP/SL для {symbol}, але позиція нульова (API підтвердження). "
                        f"Відповідь API: {error_message}"
                    )
                else:
                    logger.error(
                        f"Помилка встановлення/зміни TP/SL для позиції {symbol} (retCode: {error_code_val}): {error_message}. "
                        f"Запит: {params}. Повна відповідь: {result}"
                    )
                return result

        except AttributeError as ae: 
            if 'NoneType' in str(ae) and ('set_trading_stop' in str(ae) or 'client' in str(ae)): # Перевіряємо і client
                logger.critical(f"Критична помилка: self.client є None при виклику set_trading_stop для {symbol}. Помилка: {ae}", exc_info=True)
                return {
                    "retCode": -2, 
                    "retMsg": f"AttributeError: Bybit client was None for {symbol} - {str(ae)}",
                    "result": {}, "retExtInfo": {}, "time": int(time.time() * 1000)
                }
            else: 
                logger.critical(f"Критична помилка AttributeError у set_trading_stop_for_position для {symbol}: {ae}", exc_info=True)
                # Повертаємо None або словник з помилкою, залежно від вашої логіки обробки
                return {"retCode": -3, "retMsg": f"Unexpected AttributeError: {str(ae)}", "result": {}, "retExtInfo": {}, "time": int(time.time() * 1000)}
        except Exception as e:
            logger.critical(f"Критична помилка у set_trading_stop_for_position для {symbol}: {e}", exc_info=True)
            # Повертаємо None або словник з помилкою
            return {"retCode": -4, "retMsg": f"General Exception: {str(e)}", "result": {}, "retExtInfo": {}, "time": int(time.time() * 1000)}
    
    async def place_reduce_order(self, 
                                 symbol: str, 
                                 side: str,
                                 qty: str, 
                                 order_type: str = "Market",
                                 position_idx: int = 0) -> Dict[str, Any]:
        """
        Розміщення ордера для часткового або повного закриття позиції (reduce-only).
        side: 'Buy' для закриття Short позиції, 'Sell' для закриття Long позиції.
        """
        try:
            params = {
                "category": "linear",
                "symbol": symbol,
                "side": side, 
                "orderType": order_type,
                "qty": qty,
                "reduceOnly": True,
                "positionIdx": position_idx,
            }
            
            logger.debug(f"Attempting to place reduce-only order for {symbol} with params: {params}")

            result = await self._make_request_with_retry(
                self.client.place_order,
                **params
            )
            
            if result and result.get('retCode') == 0:
                order_id = result.get('result', {}).get('orderId', 'N/A')
                logger.info(f"Reduce-only ордер для {symbol} ({side} {qty}) успішно розміщено. Order ID: {order_id}. Відповідь API: {result}")
            else:
                logger.error(f"Помилка розміщення reduce-only ордера для {symbol}: {result}")
            
            return result

        except Exception as e:
            logger.error(f"Критична помилка розміщення reduce-only ордера для {symbol}: {e}", exc_info=True)
            return {"retCode": -1, "retMsg": str(e), "result": {}, "retExtInfo": {}, "time": 0}

    async def set_leverage(self, symbol: str, leverage: int) -> Dict[str, Any]:
        """Встановлення левериджу"""
        try:
            result = await self._make_request_with_retry(
                self.client.set_leverage,
                category="linear",
                symbol=symbol,
                buyLeverage=str(leverage),
                sellLeverage=str(leverage)
            )
            
            if result and result.get('retCode') == 0:
                logger.info(f"Леверидж встановлено: {symbol} {leverage}x")
            else:
                logger.warning(f"Не вдалося встановити леверидж: {result}")
            
            return result
            
        except Exception as e:
            logger.error(f"Помилка встановлення левериджу: {e}")
            return {}
    
    async def validate_symbols(self) -> Dict[str, bool]:
        """Валідація торгових пар"""
        valid_symbols = {}
        
        for symbol in self.trading_config['trade_pairs']:
            try:
                # Спробуємо отримати одну свічку для перевірки
                result = await self.get_klines(symbol, self.trading_config['timeframe'], limit=1)
                valid_symbols[symbol] = len(result) > 0
                
                if valid_symbols[symbol]:
                    logger.info(f"Символ {symbol} валідний")
                else:
                    logger.warning(f"Символ {symbol} недоступний")
                    
            except Exception as e:
                logger.error(f"Помилка валідації символу {symbol}: {e}")
                valid_symbols[symbol] = False
        
        return valid_symbols
    
        logger.info("API Manager закрито")
    
    async def health_check(self) -> Dict[str, Any]:
        """Перевірка стану API Manager"""
        try:
            health_stats = {
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'cache_size': len(self._cache),
                'active_batches': {k: len(v) for k, v in self._batch_queue.items()},
                'circuit_breakers': {},
                'session_active': self._session is not None,
                'session_age_seconds': time.time() - self._session_created_at if self._session else 0
            }
            
            # Стан circuit breakers
            for endpoint, cb in self._circuit_breakers.items():
                health_stats['circuit_breakers'][endpoint] = {
                    'state': cb.state,
                    'failure_count': cb.failure_count,
                    'last_failure_time': cb.last_failure_time
                }
            
            # Перевірка з'єднання простим запитом
            try:
                server_time_result = await asyncio.wait_for(self.get_server_time(), timeout=5.0)
                health_stats['api_connectivity'] = server_time_result.get('retCode') == 0
            except asyncio.TimeoutError:
                health_stats['api_connectivity'] = False
                health_stats['connectivity_error'] = 'Timeout'
            except Exception as e:
                health_stats['api_connectivity'] = False
                health_stats['connectivity_error'] = str(e)
            
            return health_stats
            
        except Exception as e:
            logger.error(f"Помилка health check API Manager: {e}")
            return {'error': str(e), 'timestamp': datetime.now(timezone.utc).isoformat()}