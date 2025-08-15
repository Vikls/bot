# -*- coding: utf-8 -*-
"""
Основний скрипт торгового бота
"""

import asyncio
import logging
import ntplib
import sys
import signal
import os
import pandas as pd
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional, List

# Додавання поточної директорії до Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.api_manager import BybitAPIManager
from src.db_manager import DatabaseManager
from src.preprocessor import DataPreprocessor
from src.strategy import LightningVolumeStrategy
from src.telegram import TelegramNotifier
from src.utils.pnl_calculator import PnLCalculator
from config.settings import (
    TRADING_CONFIG, STRATEGY_CONFIG, LOGGING_CONFIG, 
    API_CONFIG, TELEGRAM_CONFIG, INDICATORS_CONFIG 
)


# Налаштування логування
def setup_logging():
    """Налаштування системи логування"""
    log_level = getattr(logging, LOGGING_CONFIG.get('level', 'INFO'))
    os.makedirs(LOGGING_CONFIG.get('log_dir', 'logs'), exist_ok=True)
    log_format = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    if LOGGING_CONFIG.get('enable_console', True):
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(log_level)
        console_handler.setFormatter(log_format)
        root_logger.addHandler(console_handler)
    from logging.handlers import RotatingFileHandler
    main_log_handler = RotatingFileHandler(
        os.path.join(LOGGING_CONFIG.get('log_dir', 'logs'), 'logs.log'), 
        maxBytes=LOGGING_CONFIG.get('max_file_size', 10*1024*1024),
        backupCount=LOGGING_CONFIG.get('backup_count', 5),
        encoding='utf-8'
    )
    main_log_handler.setLevel(log_level)
    main_log_handler.setFormatter(log_format)
    root_logger.addHandler(main_log_handler)
    error_log_handler = RotatingFileHandler(
        os.path.join(LOGGING_CONFIG.get('log_dir', 'logs'), 'errors.log'), 
        maxBytes=LOGGING_CONFIG.get('max_file_size', 10*1024*1024),
        backupCount=LOGGING_CONFIG.get('backup_count', 5),
        encoding='utf-8'
    )
    error_log_handler.setLevel(logging.ERROR)
    error_log_handler.setFormatter(log_format)
    root_logger.addHandler(error_log_handler)
    trade_log_handler = RotatingFileHandler(
        os.path.join(LOGGING_CONFIG.get('log_dir', 'logs'), 'trades.log'), 
        maxBytes=LOGGING_CONFIG.get('max_file_size', 10*1024*1024),
        backupCount=LOGGING_CONFIG.get('backup_count', 5),
        encoding='utf-8'
    )
    # --- ЗМІНА: Рівень логування для trading ---
    trade_log_level_str = LOGGING_CONFIG.get('trade_log_level', 'INFO').upper()
    trade_log_level = getattr(logging, trade_log_level_str, logging.INFO)
    # --- КІНЕЦЬ ЗМІНИ ---
    
    trade_log_handler.setLevel(trade_log_level)
    trade_log_handler.setFormatter(log_format)
    
    trade_logger = logging.getLogger('trading') 
    trade_logger.addHandler(trade_log_handler)
    trade_logger.propagate = False 
    trade_logger.setLevel(trade_log_level)


class TradingBot:
    """Основний клас торгового бота"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.trade_logger = logging.getLogger('trading')
        
        self.api_manager = BybitAPIManager()
        self.db_manager = DatabaseManager()
        self.preprocessor = DataPreprocessor()
        self.strategy = LightningVolumeStrategy()
        self.telegram = TelegramNotifier()
        
        # Initialize unified P&L calculator
        self.pnl_calculator = PnLCalculator(api_config=API_CONFIG, logger=self.logger)
        
        self.is_running = False
        self.positions: Dict[str, Dict[str, Any]] = {} 
        self.last_analysis_time: Dict[str, datetime] = {}
        
        # ✅ ДОДАНО: Система відстеження закритих позицій для запобігання дублюванню
        self.processed_closures: Dict[str, Dict[str, Any]] = {}  # Ключ: symbol, Значення: дані про закриття
        self.closure_cleanup_interval_hours = 48  # Очищення записів старше 48 годин
        
        # Async task management
        self._tasks: List[asyncio.Task] = []
        self._task_groups = {
            'data_processing': [],
            'trading': [],
            'monitoring': [],
            'maintenance': []
        }
        self._shutdown_event = asyncio.Event()
        
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        # Trade statistics
        self.trade_stats = {
            'total_trades': 0,
            'winning_trades': 0,
            'losing_trades': 0,
            'total_pnl': 0.0,
            'initial_balance': None
        }
        
        self.logger.info("Trading Bot ініціалізовано з покращеним async task management")

    async def _validate_and_correct_trade_statistics(self) -> bool:
        """
        ✅ НОВА ФУНКЦІЯ: Періодична валідація та корекція торгової статистики
        
        Returns:
            bool: True якщо статистика була валідною або успішно виправлена
        """
        try:
            original_stats = self.trade_stats.copy()
            corrections_made = []
            
            # Перевірка основної консистентності
            calculated_total = self.trade_stats['winning_trades'] + self.trade_stats['losing_trades']
            
            if calculated_total != self.trade_stats['total_trades']:
                self.trade_stats['total_trades'] = calculated_total
                corrections_made.append(f"total_trades: {original_stats['total_trades']} → {calculated_total}")
            
            # Перевірка на негативні значення
            if self.trade_stats['winning_trades'] < 0:
                self.trade_stats['winning_trades'] = 0
                corrections_made.append(f"winning_trades: {original_stats['winning_trades']} → 0")
            
            if self.trade_stats['losing_trades'] < 0:
                self.trade_stats['losing_trades'] = 0
                corrections_made.append(f"losing_trades: {original_stats['losing_trades']} → 0")
            
            if self.trade_stats['total_trades'] < 0:
                self.trade_stats['total_trades'] = 0
                corrections_made.append(f"total_trades: {original_stats['total_trades']} → 0")
            
            # Перевірка логічності P&L
            if not isinstance(self.trade_stats['total_pnl'], (int, float)):
                self.trade_stats['total_pnl'] = 0.0
                corrections_made.append(f"total_pnl: {original_stats['total_pnl']} → 0.0 (invalid type)")
            
            # Якщо були зроблені корекції
            if corrections_made:
                self.logger.warning(
                    f"📊 СТАТИСТИКА ВИПРАВЛЕНА: Знайдено {len(corrections_made)} помилок:\n" +
                    "\n".join(f"   • {correction}" for correction in corrections_made)
                )
                
                # Зберігаємо виправлену статистику
                await self.db_manager.save_trade_stats(self.trade_stats)
                
                # Сповіщення про корекцію
                if hasattr(self, 'telegram') and len(corrections_made) > 2:  # Тільки для значних корекцій
                    correction_msg = f"📊 Статистику торгівлі виправлено ({len(corrections_made)} помилок)"
                    await self.telegram.send_notification(correction_msg, message_type='statistics_correction')
                
                return True
            else:
                self.logger.debug("✅ Валідація статистики пройшла успішно")
                return True
                
        except Exception as e:
            self.logger.error(f"❌ Помилка валідації/корекції статистики: {e}", exc_info=True)
            return False

    async def _log_detailed_statistics(self, context: str = "periodic_check"):
        """
        ✅ НОВА ФУНКЦІЯ: Детальне логування статистики для діагностики
        
        Args:
            context: Контекст виклику для ідентифікації
        """
        try:
            # Розрахункові значення
            total_calculated = self.trade_stats['winning_trades'] + self.trade_stats['losing_trades']
            win_rate = (self.trade_stats['winning_trades'] / self.trade_stats['total_trades'] * 100) if self.trade_stats['total_trades'] > 0 else 0
            avg_trade = self.trade_stats['total_pnl'] / self.trade_stats['total_trades'] if self.trade_stats['total_trades'] > 0 else 0
            
            # Стан позицій
            active_positions = len(self.positions)
            positions_list = list(self.positions.keys()) if self.positions else []
            
            # Стан дедуплікації
            processed_trades = len(getattr(self, '_processed_trade_stats', {}))
            processed_closures = len(getattr(self, 'processed_closures', {}))
            
            self.logger.info(
                f"📊 ДЕТАЛЬНА СТАТИСТИКА ({context}):\n"
                f"   🎯 Угоди: {self.trade_stats['total_trades']} "
                f"(✅{self.trade_stats['winning_trades']} / ❌{self.trade_stats['losing_trades']} = {total_calculated})\n"
                f"   💰 P&L: {self.trade_stats['total_pnl']:+.4f} USDT (середня: {avg_trade:+.4f} USDT)\n"
                f"   📈 Вінрейт: {win_rate:.1f}%\n"
                f"   📍 Активні позиції: {active_positions} {positions_list}\n"
                f"   🔄 Дедуплікація: {processed_trades} торг. записів, {processed_closures} закриттів\n"
                f"   ⚖️ Консистентність: {'✅ OK' if total_calculated == self.trade_stats['total_trades'] else '❌ ПОМИЛКА'}"
            )
            
        except Exception as e:
            self.logger.error(f"Помилка детального логування статистики: {e}")

    def _is_closure_already_processed(self, symbol: str, closure_data: Dict[str, Any]) -> bool:
        """Перевіряє чи було вже оброблено це закриття позиції"""
        try:
            if symbol not in self.processed_closures:
                return False
            
            processed_closure = self.processed_closures[symbol]
            
            # Перевіряємо час обробки - якщо минуло більше ніж cleanup_interval, вважаємо необробленим
            processed_time = processed_closure.get('processed_at')
            if processed_time:
                hours_passed = (datetime.now(timezone.utc) - processed_time).total_seconds() / 3600
                if hours_passed > self.closure_cleanup_interval_hours:
                    # Видаляємо застарілий запис
                    del self.processed_closures[symbol]
                    return False
            
            # Порівнюємо ключові параметри закриття
            current_exit_price = closure_data.get('exit_price', 0)
            current_quantity = closure_data.get('quantity', 0)
            current_closure_type = closure_data.get('closure_type', '')
            
            processed_exit_price = processed_closure.get('exit_price', 0)
            processed_quantity = processed_closure.get('quantity', 0) 
            processed_closure_type = processed_closure.get('closure_type', '')
            
            # Якщо всі ключові параметри збігаються, вважаємо вже обробленим
            price_tolerance = 0.000001
            qty_tolerance = 0.0000001
            
            if (abs(current_exit_price - processed_exit_price) < price_tolerance and
                abs(current_quantity - processed_quantity) < qty_tolerance and
                current_closure_type == processed_closure_type):
                return True
            
            return False
            
        except Exception as e:
            self.logger.error(f"Помилка перевірки обробленого закриття для {symbol}: {e}")
            return False
        """Перевіряє чи було вже оброблено це закриття позиції"""
        try:
            if symbol not in self.processed_closures:
                return False
            
            processed_closure = self.processed_closures[symbol]
            
            # Перевіряємо час обробки - якщо минуло більше ніж cleanup_interval, вважаємо необробленим
            processed_time = processed_closure.get('processed_at')
            if processed_time:
                hours_passed = (datetime.now(timezone.utc) - processed_time).total_seconds() / 3600
                if hours_passed > self.closure_cleanup_interval_hours:
                    # Видаляємо застарілий запис
                    del self.processed_closures[symbol]
                    return False
            
            # Порівнюємо ключові параметри закриття
            current_exit_price = closure_data.get('exit_price', 0)
            current_quantity = closure_data.get('quantity', 0)
            current_closure_type = closure_data.get('closure_type', '')
            
            processed_exit_price = processed_closure.get('exit_price', 0)
            processed_quantity = processed_closure.get('quantity', 0) 
            processed_closure_type = processed_closure.get('closure_type', '')
            
            # Якщо всі ключові параметри збігаються, вважаємо вже обробленим
            price_tolerance = 0.000001
            qty_tolerance = 0.0000001
            
            if (abs(current_exit_price - processed_exit_price) < price_tolerance and
                abs(current_quantity - processed_quantity) < qty_tolerance and
                current_closure_type == processed_closure_type):
                return True
            
            return False
            
        except Exception as e:
            self.logger.error(f"Помилка перевірки обробленого закриття для {symbol}: {e}")
            return False
    
    def _mark_closure_as_processed(self, symbol: str, closure_data: Dict[str, Any]):
        """Позначає закриття позиції як оброблене"""
        try:
            self.processed_closures[symbol] = {
                'exit_price': closure_data.get('exit_price', 0),
                'quantity': closure_data.get('quantity', 0),
                'closure_type': closure_data.get('closure_type', ''),
                'processed_at': datetime.now(timezone.utc),
                'pnl_usdt': closure_data.get('pnl_usdt', 0),
                'closure_reason': closure_data.get('closure_reason', '')
            }
            
            # Очищуємо застарілі записи періодично
            if len(self.processed_closures) > 50:  # Якщо багато записів
                self._cleanup_old_processed_closures()
                
        except Exception as e:
            self.logger.error(f"Помилка збереження обробленого закриття для {symbol}: {e}")
    
    def _cleanup_old_processed_closures(self):
        """Очищає застарілі записи про оброблені закриття"""
        try:
            current_time = datetime.now(timezone.utc)
            expired_symbols = []
            
            for symbol, closure_data in self.processed_closures.items():
                processed_time = closure_data.get('processed_at')
                if processed_time:
                    hours_passed = (current_time - processed_time).total_seconds() / 3600
                    if hours_passed > self.closure_cleanup_interval_hours:
                        expired_symbols.append(symbol)
            
            for symbol in expired_symbols:
                del self.processed_closures[symbol]
            
            if expired_symbols:
                self.logger.info(f"Очищено {len(expired_symbols)} застарілих записів про закриття позицій")
                
        except Exception as e:
            self.logger.error(f"Помилка очищення застарілих записів закриття: {e}")

    def validate_trade_stats(self) -> bool:
        """Валідація консистентності торгової статистики"""
        try:
            total_calculated = self.trade_stats['winning_trades'] + self.trade_stats['losing_trades']
            
            if total_calculated != self.trade_stats['total_trades']:
                self.logger.warning(
                    f"⚠️ СТАТИСТИКА НЕКОНСИСТЕНТНА: "
                    f"Розраховано угод: {total_calculated} "
                    f"(W:{self.trade_stats['winning_trades']} + L:{self.trade_stats['losing_trades']}), "
                    f"Збережено: {self.trade_stats['total_trades']}, "
                    f"Різниця: {self.trade_stats['total_trades'] - total_calculated}"
                )
                return False
            
            if self.trade_stats['total_trades'] < 0 or self.trade_stats['winning_trades'] < 0 or self.trade_stats['losing_trades'] < 0:
                self.logger.error(
                    f"❌ НЕГАТИВНІ ЗНАЧЕННЯ В СТАТИСТИЦІ: "
                    f"Total:{self.trade_stats['total_trades']}, "
                    f"Win:{self.trade_stats['winning_trades']}, "
                    f"Loss:{self.trade_stats['losing_trades']}"
                )
                return False
                
            return True
            
        except Exception as e:
            self.logger.error(f"Помилка валідації статистики: {e}")
            return False

    async def _update_trade_statistics(self, symbol: str, pnl: float, trade_data: Dict[str, Any], reason: str = "trade_completed") -> bool:
        """
        ✅ НОВА ФУНКЦІЯ: Централізоване оновлення торгової статистики з захистом від подвійного підрахунку
        
        Args:
            symbol: Торгова пара
            pnl: P&L угоди
            trade_data: Дані про угоду
            reason: Причина оновлення статистики
            
        Returns:
            bool: True якщо статистика оновлена, False якщо пропущено (дублікат)
        """
        try:
            # ✅ ЗАХИСТ ВІД ПОДВІЙНОГО ПІДРАХУНКУ
            trade_id = f"{symbol}_{trade_data.get('action', 'unknown')}_{trade_data.get('price', 0):.6f}_{trade_data.get('quantity', 0):.8f}_{reason}"
            
            # Перевіряємо чи не оновлювали вже статистику для цієї угоди
            if not hasattr(self, '_processed_trade_stats'):
                self._processed_trade_stats = {}
            
            if trade_id in self._processed_trade_stats:
                time_since_processed = (datetime.now(timezone.utc) - self._processed_trade_stats[trade_id]).total_seconds()
                if time_since_processed < 300:  # 5 хвилин захист
                    self.logger.debug(f"🔄 Статистика для {symbol} вже оновлена {time_since_processed:.0f}с тому, пропускаємо")
                    return False
            
            # Оновлюємо статистику
            previous_stats = self.trade_stats.copy()
            
            self.trade_stats['total_trades'] += 1
            self.trade_stats['total_pnl'] += pnl
            
            if pnl > 0:
                self.trade_stats['winning_trades'] += 1
                trade_result = "WIN"
            elif pnl < 0:
                self.trade_stats['losing_trades'] += 1
                trade_result = "LOSS"
            else:
                self.trade_stats['losing_trades'] += 1  # Breakeven вважаємо як збиток
                trade_result = "BREAKEVEN"
            
            # Позначаємо як оброблене
            self._processed_trade_stats[trade_id] = datetime.now(timezone.utc)
            
            # Очищуємо старі записи (старше 1 години)
            if len(self._processed_trade_stats) > 100:
                current_time = datetime.now(timezone.utc)
                expired_keys = [
                    key for key, timestamp in self._processed_trade_stats.items()
                    if (current_time - timestamp).total_seconds() > 3600
                ]
                for key in expired_keys:
                    del self._processed_trade_stats[key]
            
            # Валідуємо статистику
            if self.validate_trade_stats():
                # Зберігаємо в базу даних
                await self.db_manager.save_trade_stats(self.trade_stats)
                
                # Оновлюємо ризик-менеджмент
                if hasattr(self, 'strategy') and self.strategy:
                    self.strategy.update_risk_management(pnl)
                
                self.logger.info(
                    f"✅ СТАТИСТИКА ОНОВЛЕНА: {symbol} ({trade_result}) - "
                    f"P&L: {pnl:+.4f} USDT, Причина: {reason}\n"
                    f"   📊 Поточна статистика: "
                    f"Угод: {self.trade_stats['total_trades']} "
                    f"(W:{self.trade_stats['winning_trades']}/L:{self.trade_stats['losing_trades']}), "
                    f"Загальний P&L: {self.trade_stats['total_pnl']:+.4f} USDT"
                )
                return True
            else:
                # Відновлюємо попередню статистику при помилці валідації
                self.trade_stats = previous_stats
                self.logger.error(f"❌ Валідація статистики не пройшла, відновлено попередні значення для {symbol}")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ Помилка оновлення торгової статистики для {symbol}: {e}", exc_info=True)
            return False
    
    def _create_task(self, coro, name: str, group: str = 'general') -> asyncio.Task:
        """Створення та реєстрація задачі з proper tracking"""
        task = asyncio.create_task(coro, name=name)
        self._tasks.append(task)
        
        if group in self._task_groups:
            self._task_groups[group].append(task)
        
        # Callback для очищення завершених задач
        def task_done_callback(completed_task):
            try:
                self._tasks.remove(completed_task)
                for group_tasks in self._task_groups.values():
                    if completed_task in group_tasks:
                        group_tasks.remove(completed_task)
                        break
            except ValueError:
                pass  # Task already removed
        
        task.add_done_callback(task_done_callback)
        self.logger.debug(f"Створено задачу: {name} в групі {group}")
        return task
    
    async def _cancel_task_group(self, group: str, timeout: float = 10.0):
        """Скасування групи задач з таймаутом"""
        if group not in self._task_groups:
            return
        
        tasks = self._task_groups[group].copy()
        if not tasks:
            return
        
        self.logger.info(f"Скасування {len(tasks)} задач в групі '{group}'")
        
        # Скасовуємо всі задачі
        for task in tasks:
            if not task.done():
                task.cancel()
        
        # Чекаємо завершення з таймаутом
        try:
            await asyncio.wait_for(
                asyncio.gather(*tasks, return_exceptions=True),
                timeout=timeout
            )
        except asyncio.TimeoutError:
            self.logger.warning(f"Таймаут при скасуванні задач групи '{group}'")
    
    async def _shutdown_all_tasks(self):
        """Graceful shutdown всіх активних задач"""
        try:
            # Сигналізуємо про shutdown
            self._shutdown_event.set()
            
            # Скасовуємо задачі по групах
            for group in ['maintenance', 'monitoring', 'trading', 'data_processing']:
                await self._cancel_task_group(group, timeout=5.0)
            
            # Скасовуємо решту задач
            remaining_tasks = [task for task in self._tasks if not task.done()]
            if remaining_tasks:
                self.logger.info(f"Скасування {len(remaining_tasks)} задач що залишилися")
                for task in remaining_tasks:
                    task.cancel()
                
                try:
                    await asyncio.wait_for(
                        asyncio.gather(*remaining_tasks, return_exceptions=True),
                        timeout=10.0
                    )
                except asyncio.TimeoutError:
                    self.logger.warning("Таймаут при завершенні задач")
            
            self.logger.info("Всі задачі завершено")
            
        except Exception as e:
            self.logger.error(f"Помилка при shutdown задач: {e}")

    def _signal_handler(self, signum, frame):
        self.logger.info(f"Отримано сигнал {signum}, зупинка бота...")
        self.is_running = False
    
    async def check_ntp_sync(self) -> bool:
        try:
            ntp_client = ntplib.NTPClient()
            ntp_servers = ['pool.ntp.org', 'time.google.com', 'time.cloudflare.com', 'time.windows.com']
            for server in ntp_servers:
                try:
                    response = ntp_client.request(server, version=3, timeout=5)
                    ntp_time = datetime.fromtimestamp(response.tx_time, tz=timezone.utc)
                    local_time = datetime.now(timezone.utc)
                    time_diff = abs((ntp_time - local_time).total_seconds())
                    self.logger.info(
                        f"NTP сервер: {server}, NTP час: {ntp_time.strftime('%Y-%m-%d %H:%M:%S')} UTC, "
                        f"Локальний час: {local_time.strftime('%Y-%m-%d %H:%M:%S')} UTC, "
                        f"Різниця: {time_diff:.2f} секунд"
                    )
                    if time_diff > 10:
                        self.logger.warning(f"Великий зсув часу: {time_diff:.2f} секунд!")
                        await self.telegram.send_error_notification({
                            'type': 'TIME_SYNC_WARNING', 
                            'message': f'Зсув часу з NTP: {time_diff:.2f} секунд'
                        })
                    self.logger.info("Час синхронізовано успішно")
                    return True
                except Exception as e:
                    self.logger.warning(f"Помилка підключення до NTP сервера {server}: {e}")
            self.logger.error("Не вдалося підключитися до жодного NTP сервера")
            return False
        except Exception as e:
            self.logger.error(f"Критична помилка перевірки NTP: {e}")
            return False

    async def validate_candle_data(self, symbol: str, candle_data: Dict) -> bool:
        try:
            if not candle_data:
                return False
            required_fields = ['open_price', 'high_price', 'low_price', 'close_price', 'volume']
            if any(field not in candle_data or candle_data[field] is None for field in required_fields):
                self.logger.warning(f"Відсутнє поле в даних свічки для {symbol}")
                return False
            
            open_price, high_price, low_price, close_price, volume = (
                float(candle_data['open_price']), float(candle_data['high_price']),
                float(candle_data['low_price']), float(candle_data['close_price']),
                float(candle_data['volume'])
            )

            if any(price <= 0 for price in [open_price, high_price, low_price, close_price]):
                self.logger.warning(
                    f"Аномальні ціни для {symbol}: OHLC={open_price},{high_price},{low_price},{close_price}"
                )
                return False
            if high_price < low_price:
                self.logger.warning(f"High < Low для {symbol}: {high_price} < {low_price}")
                return False
            if not (low_price <= open_price <= high_price and low_price <= close_price <= high_price):
                self.logger.warning(f"Open/Close поза межами High/Low для {symbol}")
                return False
            if volume < 0:
                self.logger.warning(f"Від'ємний об'єм для {symbol}: {volume}")
                return False
            if open_price > 0: 
                max_change = max(
                    abs(high_price - low_price) / open_price, 
                    abs(close_price - open_price) / open_price
                )
                if max_change > 0.5:  # 50% change in one candle
                    self.logger.warning(f"Екстремальна зміна ціни для {symbol}: {max_change*100:.1f}%")
            return True
        except Exception as e:
            self.logger.error(f"Помилка валідації свічки для {symbol}: {e}")
            return False

    async def initialize(self) -> bool:
        try:
            self.logger.info("Початок ініціалізації торгового бота...")
            ntp_sync = await self.check_ntp_sync()
            if not ntp_sync:
                self.logger.warning("NTP синхронізація не пройшла, але продовжуємо...")
            api_sync = await self.api_manager.check_time_sync()
            if not api_sync:
                self.logger.error("API синхронізація не пройшла!")
                await self.telegram.send_error_notification({
                    'type': 'API_SYNC_ERROR', 
                    'message': 'Помилка синхронізації з Bybit API'
                })
                return False
            await self.preprocessor.initialize()
            self.logger.info("Завантаження початкових даних...")
            await self.preprocessor.load_initial_data()
            for symbol in TRADING_CONFIG['trade_pairs']:
                integrity_ok = await self.preprocessor.validate_indicators_integrity(symbol)
                if not integrity_ok:
                    self.logger.warning(f"Проблеми з цілісністю індикаторів для {symbol}")
            if self.telegram.bot:
                telegram_test = await self.telegram.test_connection()
                if telegram_test:
                    await self.telegram.send_bot_status("INITIALIZED", {
                        "Режим": TRADING_CONFIG['mode'],
                        "Пари": ', '.join(TRADING_CONFIG['trade_pairs']),
                        "Таймфрейм": f"{TRADING_CONFIG['timeframe']}m"
                    })
                else:
                    self.logger.warning("Telegram сповіщення не працюють")
            self.logger.info("Торговий бот успішно ініціалізовано!")
            return True
        except Exception as e:
            self.logger.error(f"Критична помилка ініціалізації: {e}", exc_info=True)
            await self.telegram.send_error_notification({
                'type': 'INITIALIZATION_ERROR', 
                'message': str(e)
            })
            return False

    async def execute_trade(self, symbol: str, signal_data: Dict[str, Any]) -> Dict[str, Any]:
        try:
            signal_type = signal_data['signal'] # BUY or SELL
            intended_entry_price = float(signal_data['entry_price']) 
            
            self.trade_logger.info(
                f"🚀 EXECUTE_TRADE START: {signal_type} trade for {symbol} "
                f"at intended entry price ~{intended_entry_price:.6f}"
            )

            # Крок 1: Отримання балансу
            try:
                usdt_balance = await self.api_manager.get_usdt_balance()
                self.trade_logger.info(f"✅ Step 1 - Current USDT balance: {usdt_balance:.2f}")
            except Exception as e:
                self.trade_logger.error(f"❌ Step 1 FAILED - Error getting USDT balance: {e}", exc_info=True)
                return {'symbol': symbol, 'action': 'ERROR_GETTING_BALANCE', 'signal': signal_type, 'success': False, 'error': str(e)}

            # Крок 2: Розрахунок розміру позиції
            position_size_percent = TRADING_CONFIG.get('min_order_amount', 10)
            position_size_usdt = usdt_balance * (position_size_percent / 100.0)
            leverage = TRADING_CONFIG.get('leverage', 1)
            position_value_with_leverage = position_size_usdt * leverage
            min_order_value_usdt_config = TRADING_CONFIG.get('min_order_value_usdt', 5.0)
            
            self.trade_logger.info(
                f"✅ Step 2 - Position calculation: "
                f"Balance: {usdt_balance:.2f} USDT, "
                f"Allocation: {position_size_percent}% = {position_size_usdt:.2f} USDT, "
                f"Leverage: {leverage}x, "
                f"Position value with leverage: {position_value_with_leverage:.2f} USDT, "
                f"Min required (base): {min_order_value_usdt_config} USDT"
            )
            if position_size_usdt < min_order_value_usdt_config:
                self.trade_logger.warning(f"❌ Step 2 FAILED - Base position value {position_size_usdt:.2f} USDT is below minimum {min_order_value_usdt_config} USDT")
                return {'symbol': symbol, 'action': 'ORDER_VALUE_TOO_SMALL', 'signal': signal_type, 'success': False, 'error': f'Base order value {position_size_usdt:.2f} USDT too small (min: {min_order_value_usdt_config})'}

            # Крок 3: Розрахунок кількості
            initial_raw_quantity = position_value_with_leverage / intended_entry_price if intended_entry_price > 0 else 0
            self.trade_logger.info(f"✅ Step 3 - Quantity calculation: Pos value w/ lev: {position_value_with_leverage:.2f} USDT, Entry price: {intended_entry_price:.6f}, Initial qty: {initial_raw_quantity:.8f}")
            if initial_raw_quantity == 0:
                self.trade_logger.error(f"❌ Step 3 FAILED - Initial quantity is zero for {symbol}")
                return {'symbol': symbol, 'action': 'ZERO_INITIAL_QUANTITY', 'signal': signal_type, 'success': False, 'error': 'Initial quantity calculated as zero'}

            # Крок 4: Перевірка максимальної кількості позицій
            if len(self.positions) >= TRADING_CONFIG.get('max_orders_qty', 5):
                self.trade_logger.warning(f"❌ Step 4 FAILED - Maximum number of positions reached: {len(self.positions)}/{TRADING_CONFIG.get('max_orders_qty', 5)}")
                return {'symbol': symbol, 'action': 'MAX_POSITIONS_REACHED', 'signal': signal_type, 'success': False, 'error': f'Maximum positions limit reached: {len(self.positions)}'}

            # Крок 5: Перевірка типу символа
            if "USDT" not in symbol: 
                self.trade_logger.error(f"❌ Step 5 FAILED - {symbol} is not a USDT pair")
                return {'symbol': symbol, 'action': 'INVALID_SYMBOL_TYPE', 'signal': signal_type, 'success': False, 'error': 'Not a USDT pair'}
            self.trade_logger.info(f"✅ Step 5 - Symbol validation passed: {symbol}")

            # Крок 6: Отримання інформації про інструмент та форматування кількості
            quantity_for_api_float = initial_raw_quantity 
            quantity_str_for_api = ""
            instrument_details = None
            price_decimals_from_instrument = 6 # За замовчуванням для USDT пар

            try:
                self.trade_logger.info(f"🔍 Step 6 - Getting instrument info for {symbol}...")
                instrument_details = await self.api_manager.get_instrument_info(symbol=symbol)
                
                if instrument_details:
                    self.trade_logger.info(f"✅ Step 6 - Instrument details received for {symbol}")
                    lot_size_filter = instrument_details.get('lotSizeFilter', {})
                    qty_step_str = lot_size_filter.get('qtyStep')
                    min_order_qty_str = lot_size_filter.get('minOrderQty')
                    min_notional_value_str = lot_size_filter.get('minNotionalValue')
                    
                    price_filter = instrument_details.get('priceFilter', {})
                    tick_size_str_for_price = price_filter.get('tickSize')
                    if tick_size_str_for_price:
                        if '.' in tick_size_str_for_price: price_decimals_from_instrument = len(tick_size_str_for_price.split('.')[1].rstrip('0'))
                        else: price_decimals_from_instrument = 0

                    self.trade_logger.info(f"📊 Step 6 - Filters: qtyStep={qty_step_str}, minOrderQty={min_order_qty_str}, minNotional={min_notional_value_str}, priceDecimals={price_decimals_from_instrument}")

                    qty_decimals_for_api = 0 
                    if qty_step_str and '.' in qty_step_str:
                        qty_decimals_for_api = len(qty_step_str.split('.')[1].rstrip('0'))
                    
                    if qty_step_str:
                        qty_step = float(qty_step_str)
                        if qty_step > 0:
                            quantity_for_api_float = round(initial_raw_quantity / qty_step) * qty_step
                            if quantity_for_api_float == 0 and initial_raw_quantity > 0: 
                                quantity_for_api_float = qty_step 
                            self.trade_logger.info(f"✅ Step 6.1 - Qty adjusted to qtyStep: {quantity_for_api_float:.{qty_decimals_for_api}f}")
                    
                    if min_order_qty_str:
                        min_order_qty = float(min_order_qty_str)
                        if quantity_for_api_float < min_order_qty and min_order_qty > 0:
                            quantity_for_api_float = min_order_qty
                            self.trade_logger.info(f"✅ Step 6.2 - Qty adjusted to minOrderQty: {quantity_for_api_float:.{qty_decimals_for_api}f}")
                    
                    quantity_str_for_api = f"{quantity_for_api_float:.{qty_decimals_for_api}f}"
                    if '.' in quantity_str_for_api and qty_decimals_for_api == 0 :
                        quantity_str_for_api = quantity_str_for_api.split('.')[0]
                    elif '.' in quantity_str_for_api: # Для випадків типу 123.000 -> 123
                        if float(quantity_str_for_api) == int(float(quantity_str_for_api)):
                            quantity_str_for_api = str(int(float(quantity_str_for_api)))
                        else: # 123.4500 -> 123.45
                            quantity_str_for_api = quantity_str_for_api.rstrip('0').rstrip('.')


                    self.trade_logger.info(f"✅ Step 6.3 - Qty formatted for API: '{quantity_str_for_api}' (float: {quantity_for_api_float})")

                    if min_notional_value_str:
                        min_notional_value = float(min_notional_value_str)
                        current_notional_value = quantity_for_api_float * intended_entry_price
                        self.trade_logger.info(f"🔍 Step 6.4 - Notional value check: current={current_notional_value:.2f}, required_min={min_notional_value:.2f}")
                        if current_notional_value < min_notional_value:
                            self.trade_logger.error(f"❌ Step 6.4 FAILED - Order notional value {current_notional_value:.2f} is below minNotionalValue {min_notional_value:.2f}")
                            return {'symbol': symbol, 'action': 'ORDER_BELOW_MIN_NOTIONAL', 'signal': signal_type, 'success': False, 'error': f'Order value {current_notional_value:.2f} below minNotional {min_notional_value:.2f}'}
                        self.trade_logger.info(f"✅ Step 6.4 - Notional value check passed")
                else: 
                    self.trade_logger.warning(f"⚠️ Step 6 WARNING - Failed to get instrument details for {symbol}. Using default rounding.")
                    quantity_for_api_float = round(initial_raw_quantity, price_decimals_from_instrument) 
                    quantity_str_for_api = f"{quantity_for_api_float:.{price_decimals_from_instrument}f}".rstrip('0').rstrip('.')
                    if '.' in quantity_str_for_api and float(quantity_str_for_api) == int(float(quantity_str_for_api)):
                        quantity_str_for_api = str(int(float(quantity_str_for_api)))

            except Exception as e_instr:
                self.trade_logger.error(f"❌ Step 6 ERROR - Error getting/processing instrument info for {symbol}: {e_instr}", exc_info=True)
                quantity_for_api_float = round(initial_raw_quantity, price_decimals_from_instrument)
                quantity_str_for_api = f"{quantity_for_api_float:.{price_decimals_from_instrument}f}".rstrip('0').rstrip('.')
                if '.' in quantity_str_for_api and float(quantity_str_for_api) == int(float(quantity_str_for_api)):
                    quantity_str_for_api = str(int(float(quantity_str_for_api)))


            # Крок 7: Фінальна валідація quantity
            if not quantity_str_for_api or float(quantity_str_for_api) <= 0:
                self.trade_logger.error(f"❌ Step 7 FAILED - Final quantity for {symbol} is zero or invalid: '{quantity_str_for_api}'")
                return {'symbol': symbol, 'action': 'ZERO_FINAL_QUANTITY', 'signal': signal_type, 'success': False, 'error': f'Final API quantity zero or invalid: {quantity_str_for_api}'}
            self.trade_logger.info(f"✅ Step 7 - Final order params: {signal_type} {symbol}, qty={quantity_str_for_api}, approx_value={quantity_for_api_float * intended_entry_price:.2f} USDT")
            
            # Крок 8: Розміщення ордера
            api_side = "Buy" if signal_type == "BUY" else "Sell"
            self.trade_logger.info(f"🚀 Step 8 - About to place MARKET order: {symbol} {api_side} {quantity_str_for_api}")
            try:
                api_order_result = await self.api_manager.place_order(
                    symbol=symbol, side=api_side, qty=quantity_str_for_api, order_type="Market", position_idx=0 
                )
                self.trade_logger.info(f"📨 Step 8 - API response received: retCode={api_order_result.get('retCode') if api_order_result else 'None'}")
                if api_order_result: self.trade_logger.debug(f"Full API response: {api_order_result}")
            except Exception as e_api:
                self.trade_logger.error(f"❌ Step 8 CRITICAL ERROR - API call failed: {e_api}", exc_info=True)
                return {'symbol': symbol, 'action': 'API_CALL_FAILED', 'signal': signal_type, 'success': False, 'error': str(e_api)}

            # Крок 9: Обробка відповіді API
            order_id = None
            if api_order_result and api_order_result.get('retCode') == 0:
                order_id = api_order_result.get('result', {}).get('orderId')
                self.trade_logger.info(f"🎉 Step 9 - ORDER PLACED SUCCESSFULLY! {symbol} ({signal_type}), Order ID: {order_id}")
                
                delay_ms = TRADING_CONFIG.get('delay_after_market_order_ms', 2000)
                self.trade_logger.info(f"⏳ Waiting {delay_ms}ms before getting fill price...")
                await asyncio.sleep(delay_ms / 1000.0)

                # Крок 10: Отримання фактичної ціни виконання
                actual_avg_fill_price = None
                self.trade_logger.info(f"🔍 Step 10 - Getting actual fill price for {symbol} (Order ID: {order_id})...")
                for attempt in range(5): 
                    try:
                        positions_list = await self.api_manager.get_positions(symbol=symbol)
                        if positions_list:
                            for pos_item in positions_list:
                                if (pos_item.get('symbol') == symbol and 
                                    pos_item.get('side', '').lower() == api_side.lower() and 
                                    float(pos_item.get('size', "0")) > 0): 
                                    actual_avg_fill_price = float(pos_item.get('avgPrice', "0"))
                                    if actual_avg_fill_price > 0:
                                        self.trade_logger.info(f"✅ Step 10 - Got actual avg entry price from get_positions for {symbol}: {actual_avg_fill_price:.{price_decimals_from_instrument}f}")
                                        break
                        if actual_avg_fill_price and actual_avg_fill_price > 0: break

                        if order_id: 
                            executions = await self.api_manager.get_execution_history(symbol=symbol, order_filter=order_id, limit=5)
                            if executions:
                                total_val, total_qty_exec = 0, 0
                                for exec_item in executions:
                                    total_val += exec_item['exec_value']
                                    total_qty_exec += exec_item['quantity']
                                if total_qty_exec > 0 :
                                    actual_avg_fill_price = total_val / total_qty_exec
                                    self.trade_logger.info(f"✅ Step 10 - Got actual avg entry price from get_executions for {symbol}: {actual_avg_fill_price:.{price_decimals_from_instrument}f}")
                                    break
                        
                        if actual_avg_fill_price and actual_avg_fill_price > 0: break
                        self.trade_logger.warning(f"⚠️ Step 10 - Attempt {attempt+1}/5: Could not get avgPrice for {symbol}. Retrying...")
                        await asyncio.sleep(1.0 + attempt * 0.5) 
                    except Exception as e_pos:
                        self.trade_logger.error(f"❌ Step 10 - Error getting position/execution data: {e_pos}", exc_info=True)
                
                if not actual_avg_fill_price or actual_avg_fill_price <= 0:
                    self.trade_logger.warning(f"⚠️ Step 10 WARNING - Could not determine actual fill price for {symbol}. Using intended price {intended_entry_price:.{price_decimals_from_instrument}f} as fallback")
                    actual_avg_fill_price = intended_entry_price 

                # Крок 11: Розрахунок SL/TP на основі ФАКТИЧНОЇ ціни входу
                self.trade_logger.info(f"🎯 Step 11 - Calculating SL/TP for {symbol} using strategy's SL logic...")
                
                original_sl_price_from_signal = float(signal_data.get('stop_loss', 0)) # SL з даних сигналу
                original_tp_levels_from_signal = signal_data.get('take_profits', []) # TP з даних сигналу
                atr_at_signal_time = float(signal_data.get('atr_value', 0.00001))
                if atr_at_signal_time <= 0: atr_at_signal_time = 0.00001

                sl_atr_multiplier_from_strategy = STRATEGY_CONFIG.get('sl_atr_multiplier', 1.5)
                
                final_stop_loss_price = 0.0
                if signal_type == 'BUY':
                    final_stop_loss_price = actual_avg_fill_price - (atr_at_signal_time * sl_atr_multiplier_from_strategy)
                else:  # SELL
                    final_stop_loss_price = actual_avg_fill_price + (atr_at_signal_time * sl_atr_multiplier_from_strategy)

                if signal_type == 'BUY' and final_stop_loss_price >= actual_avg_fill_price:
                    self.trade_logger.error(f"❌ Invalid SL for BUY {symbol}: SL {final_stop_loss_price:.{price_decimals_from_instrument}f} >= Entry {actual_avg_fill_price:.{price_decimals_from_instrument}f}")
                    final_stop_loss_price = actual_avg_fill_price * (1 - 0.01 / leverage) # Зменшуємо на 1% від маржі
                elif signal_type == 'SELL' and final_stop_loss_price <= actual_avg_fill_price:
                    self.trade_logger.error(f"❌ Invalid SL for SELL {symbol}: SL {final_stop_loss_price:.{price_decimals_from_instrument}f} <= Entry {actual_avg_fill_price:.{price_decimals_from_instrument}f}")
                    final_stop_loss_price = actual_avg_fill_price * (1 + 0.01 / leverage) # Збільшуємо на 1% від маржі
                
                self.trade_logger.info(
                    f"📊 SL Calculation Details: Signal Entry: {intended_entry_price:.{price_decimals_from_instrument}f}, "
                    f"Signal SL (from strategy): {original_sl_price_from_signal:.{price_decimals_from_instrument}f}, "
                    f"SL Distance (ATR*mult): {(atr_at_signal_time * sl_atr_multiplier_from_strategy):.{price_decimals_from_instrument}f}, "
                    f"Actual Entry: {actual_avg_fill_price:.{price_decimals_from_instrument}f}, "
                    f"Calculated New SL: {final_stop_loss_price:.{price_decimals_from_instrument}f}"
                )
                final_stop_loss_price_str = f"{final_stop_loss_price:.{price_decimals_from_instrument}f}"

                final_take_profit_levels_for_bot = []
                first_tp_price_for_exchange_str = None

                if original_tp_levels_from_signal:
                    for tp_level_index, tp_level in enumerate(original_tp_levels_from_signal):
                        tp_mult_for_level = 0
                        # Визначаємо множник ATR для кожного рівня TP
                        if tp_level['type'] == 'partial_1': tp_mult_for_level = STRATEGY_CONFIG.get('first_partial_multiplier', 0.8)
                        elif tp_level['type'] == 'partial_2': tp_mult_for_level = STRATEGY_CONFIG.get('second_partial_multiplier', 1.3)
                        elif tp_level['type'] == 'partial_3': tp_mult_for_level = STRATEGY_CONFIG.get('third_partial_multiplier', 1.8)
                        elif tp_level['type'] == 'final':
                            adaptive_params_sig = signal_data.get('adaptive_params_used', {})
                            volume_boost_sig = signal_data.get('volume_boost_data', {})
                            tp_mult_base_final = adaptive_params_sig.get('tp_mult', STRATEGY_CONFIG.get('mixed_tp_mult', 2.2))
                            tp_mult_boost_final = volume_boost_sig.get('tp_multiplier_boost', 1.0)
                            tp_extension_final = volume_boost_sig.get('final_tp_extension', 1.0)
                            tp_mult_for_level = tp_mult_base_final * tp_mult_boost_final * tp_extension_final
                        
                        new_tp_price_val = 0.0
                        if signal_type == 'BUY':
                            new_tp_price_val = actual_avg_fill_price + (atr_at_signal_time * tp_mult_for_level)
                        else: # SELL
                            new_tp_price_val = actual_avg_fill_price - (atr_at_signal_time * tp_mult_for_level)

                        new_tp_price_val_str = f"{new_tp_price_val:.{price_decimals_from_instrument}f}"
                        final_take_profit_levels_for_bot.append({
                            **tp_level, 
                            'price': float(new_tp_price_val_str), 
                            'hit': False 
                        })
                        if tp_level_index == 0: # Перший TP для встановлення на біржі
                            first_tp_price_for_exchange_str = new_tp_price_val_str
                
                self.trade_logger.info(
                    f"✅ Step 11 (Post SL/TP Calc) - Final SL: {final_stop_loss_price_str}, "
                    f"First TP for exchange: {first_tp_price_for_exchange_str or 'N/A'}, "
                    f"Full TP Levels: {final_take_profit_levels_for_bot}"
                )

                # Крок 12: Встановлення SL/TP на біржі
                if float(final_stop_loss_price_str) > 0:
                    self.trade_logger.info(f"🎯 Step 12 - Setting SL ({final_stop_loss_price_str}) & First TP ({first_tp_price_for_exchange_str or 'None'}) on exchange for {symbol}...")
                    try:
                        sl_tp_set_response = await self.api_manager.set_trading_stop_for_position(
                            symbol=symbol,
                            stop_loss=final_stop_loss_price_str,
                            take_profit=first_tp_price_for_exchange_str if first_tp_price_for_exchange_str else None,
                            position_idx=0,
                            tpsl_mode="Full" 
                        )
                        if sl_tp_set_response and sl_tp_set_response.get('retCode') == 0:
                            self.trade_logger.info(f"✅ Step 12 - SL/TP set successfully for {symbol}")
                        else:
                            self.trade_logger.error(f"❌ Step 12 FAILED - Could not set SL/TP for {symbol}. Response: {sl_tp_set_response}")
                    except Exception as e_sltp:
                        self.trade_logger.error(f"❌ Step 12 ERROR - Exception setting SL/TP: {e_sltp}", exc_info=True)
                
                # Крок 13: Збереження позиції в бот
                self.trade_logger.info(f"💾 Step 13 - Saving position to bot memory for {symbol}...")
                self.positions[symbol] = {
                    'entry_price': actual_avg_fill_price, 
                    'initial_entry_price': actual_avg_fill_price,  # ✅ NEW: Backup entry price for recovery
                    'quantity': quantity_for_api_float, 
                    'initial_quantity': quantity_for_api_float, 
                    'side': signal_type,
                    'initial_stop_loss': float(final_stop_loss_price_str), 
                    'current_stop_loss': float(final_stop_loss_price_str),
                    'take_profit_levels': final_take_profit_levels_for_bot, 
                    'initial_atr_at_entry': atr_at_signal_time, 
                    'first_partial_tp_hit': False,
                    'breakeven_applied': False, 
                    'volume_divergence_exit_done': False,
                    'trailing_stop_active': False,
                    'highest_high_since_entry': actual_avg_fill_price if signal_type == 'BUY' else -1,
                    'lowest_low_since_entry': actual_avg_fill_price if signal_type == 'SELL' else float('inf'),
                    'highest_high_since_trail_active': -1, 
                    'lowest_low_since_trail_active': float('inf'), 
                    'entry_timestamp': datetime.now(timezone.utc),
                    'original_signal_data': signal_data, 
                    'exchange_order_id': order_id 
                }
                
                # Крок 14: Створення trade log і сповіщення
                trade_log_data_for_telegram = {
                    'action': f'OPEN_{signal_type}', 
                    'symbol': symbol, 'side': signal_type, 
                    'price': actual_avg_fill_price, 
                    'entry_price': actual_avg_fill_price, 
                    'quantity': quantity_for_api_float, 
                    'total_value_approx': quantity_for_api_float * actual_avg_fill_price, 
                    'stop_loss': float(final_stop_loss_price_str),
                    'take_profits': final_take_profit_levels_for_bot, 
                    'confidence': signal_data.get('confidence'), 
                    'atr_at_entry': atr_at_signal_time,
                    'reason': signal_data.get('reason', 'Signal triggered'),
                    'volume_surge_active': signal_data.get('volume_surge_active', False),
                    'super_volume_surge_active': signal_data.get('super_volume_surge_active', False),
                    'exchange_order_id': order_id
                }
                self.trade_logger.info(f"📱 Step 14 - Sending Telegram notification for {symbol}...")
                await self.telegram.send_trade_notification(trade_log_data_for_telegram) 
                self.trade_logger.info(f"🎉 EXECUTE_TRADE SUCCESS: {symbol} trade completed successfully!")
                return {'symbol': symbol, 'action': 'REAL_TRADE_OPENED_WITH_SLTP_SET', 'signal': signal_type, 'success': True, 'trade_data': trade_log_data_for_telegram, 'order_id': order_id}
            else: 
                failed_request_params = api_order_result.get('retExtInfo', {}).get('req', api_order_result.get('request_params', {})) if api_order_result else {}
                error_code_api = api_order_result.get('retCode') if api_order_result else 'None'
                error_msg_api_text = api_order_result.get('retMsg', 'Unknown error') if api_order_result else 'No API response'
                
                error_message_log = (f"❌ Step 9 FAILED - Could not place order for {symbol} ({signal_type}). API Error: {error_msg_api_text} (Code: {error_code_api})")
                if failed_request_params: error_message_log += f" Request params: {failed_request_params}"
                self.trade_logger.error(error_message_log)
                
                await self.telegram.send_error_notification({
                    'type': 'EXCHANGE_MARKET_ORDER_FAILED', 'message': error_message_log, 
                    'symbol': symbol, 'action': signal_type, 'api_response': str(api_order_result) 
                })
                return {'symbol': symbol, 'action': 'ERROR_PLACING_MARKET_EXCHANGE_ORDER', 'signal': signal_type, 'success': False, 'error': str(api_order_result)}
                
        except Exception as e:
            self.trade_logger.critical(f"💥 EXECUTE_TRADE CRITICAL ERROR for {symbol}: {e}", exc_info=True)
            await self.telegram.send_error_notification({
                'type': 'TRADE_ENTRY_CRITICAL_ERROR', 'message': str(e),
                'symbol': symbol, 'action': signal_data.get('signal', 'Unknown')
            })
            return {'symbol': symbol, 'action': 'ERROR_ENTRY_CRITICAL', 'signal': signal_data.get('signal'), 'success': False, 'error': str(e)}

    async def manage_active_position(self, symbol: str, position_data_arg: Dict[str, Any], latest_candle: Dict[str, Any]):
        # Швидка перевірка існування позиції на біржі на початку
        try:
            # Швидка перевірка існування позиції на біржі
            exchange_positions = await self.api_manager.get_positions(symbol=symbol)
            position_exists_on_exchange = False
            
            if exchange_positions:
                for pos in exchange_positions:
                    if (pos.get('symbol') == symbol and 
                        float(pos.get('size', 0)) > 0.000001):
                        position_exists_on_exchange = True
                        break
            
            if not position_exists_on_exchange:
                # Спробуємо проаналізувати закриття позиції замість загального попередження
                if symbol in self.positions:
                    closure_analyzed = await self.analyze_and_notify_position_closure(symbol, self.positions[symbol])
                    if not closure_analyzed:
                        # Якщо аналіз не вдався, виконуємо звичайну синхронізацію
                        await self.sync_single_position_with_history(symbol)
                else:
                    # Позиція відсутня локально - звичайна синхронізація
                    self.trade_logger.warning(f"⚠️ ШВИДКА ПЕРЕВІРКА: {symbol} відсутня на біржі")
                    await self.sync_single_position_with_history(symbol)
                return
                
        except Exception as e:
            self.logger.error(f"Помилка швидкої перевірки {symbol}: {e}")
        
        # Перевірка існування позиції локально
        if symbol not in self.positions:
            self.logger.debug(f"Пропуск manage_active_position для {symbol}: відсутній в self.positions.")
            return

        current_candle_close_price = float(latest_candle['close_price'])
        # Переконуємось, що ми працюємо з актуальними даними з self.positions
        if symbol not in self.positions: # Ще одна перевірка, якщо символ видалили асинхронно
            self.logger.info(f"Позиція {symbol} була видалена з self.positions перед отриманням даних. Пропуск manage_active_position.")
            return
        position_data = self.positions[symbol] 

        if position_data.get('quantity', 0) <= TRADING_CONFIG.get('min_trade_quantity_threshold', 0.000001):
            self.logger.debug(f"Позиція {symbol} має нульову/мінімальну кількість ({position_data.get('quantity', 0):.8f}) в self.positions перед синхронізацією. Видалення, якщо ще існує.")
            if symbol in self.positions:
                del self.positions[symbol]
            return
        
        try:
            # Синхронізація з біржею
            exchange_position_list = await self.api_manager.get_positions(symbol=symbol)
            actual_exchange_pos_details = None
            if exchange_position_list:
                for pos_item in exchange_position_list:
                    if (pos_item.get('symbol') == symbol and 
                        float(pos_item.get('size', "0")) > TRADING_CONFIG.get('min_trade_quantity_threshold', 0.000001)):
                        actual_exchange_pos_details = pos_item
                        break
            
            if symbol not in self.positions: # Перевірка після асинхронного виклику
                self.logger.info(f"Позиція {symbol} була видалена з self.positions під час отримання даних з біржі (manage_active_position). Пропуск.")
                return

            if not actual_exchange_pos_details:
                self.trade_logger.warning(f"⚠️ Позиція {symbol} відсутня на біржі (manage_active_position). Запуск детальної синхронізації з історією...")
                # Делегуємо обробку sync_single_position_with_history
                await self.sync_single_position_with_history(symbol)
                
                # Після sync_single_position_with_history позиція може бути видалена з self.positions
                if symbol not in self.positions:
                    self.logger.info(f"Позиція {symbol} оброблена (ймовірно, закрита) через sync_single_position_with_history в manage_active_position. Завершення управління.")
                    return # Важливо вийти, оскільки стан позиції вже оброблено
                # Якщо позиція все ще є (наприклад, була оновлена, а не закрита), продовжуємо manage_active_position
                # оновлюємо position_data, оскільки sync_single_position_with_history міг її змінити
                position_data = self.positions[symbol]
            else:
                # Позиція є на біржі, проводимо швидку синхронізацію кількості та SL
                exchange_qty = float(actual_exchange_pos_details.get('size', "0"))
                exchange_side_api = actual_exchange_pos_details.get('side', "")
                local_bot_signal_side_sync = position_data['side']

                if not ((local_bot_signal_side_sync == 'BUY' and exchange_side_api == 'Buy') or \
                        (local_bot_signal_side_sync == 'SELL' and exchange_side_api == 'Sell')):
                    self.trade_logger.warning(f"Розбіжність напрямку позиції для {symbol} (Локально: {local_bot_signal_side_sync}, Біржа: {exchange_side_api}). Запуск детальної синхронізації.")
                    await self.sync_single_position_with_history(symbol)
                    if symbol not in self.positions:
                        self.logger.info(f"Позиція {symbol} оброблена після розбіжності напрямку (manage_active_position). Завершення.")
                        return
                    position_data = self.positions[symbol] # Оновлюємо дані
                
                # Перевіряємо знову, чи позиція ще існує після можливої синхронізації
                if symbol not in self.positions: return

                if abs(position_data['quantity'] - exchange_qty) > TRADING_CONFIG.get('sync_tolerance_qty', 0.0000001):
                    self.trade_logger.warning(
                        f"Розбіжність кількості для {symbol} (manage_active_position). "
                        f"Локально: {position_data['quantity']:.8f}, "
                        f"Біржа: {exchange_qty:.8f}. Оновлюю локальну кількість."
                    )
                    position_data['quantity'] = exchange_qty
                
                exchange_sl_str = actual_exchange_pos_details.get('stopLoss', "0")
                if exchange_sl_str and float(exchange_sl_str) > 0:
                    exchange_sl_float = float(exchange_sl_str)
                    # Отримуємо точність для порівняння
                    price_decimals_local = 8
                    instrument_details_local = await self.api_manager.get_instrument_info(symbol)
                    if instrument_details_local:
                        price_filter_local = instrument_details_local.get('priceFilter', {})
                        tick_size_str_local = price_filter_local.get('tickSize')
                        if tick_size_str_local and '.' in tick_size_str_local:
                            price_decimals_local = len(tick_size_str_local.split('.')[1].rstrip('0'))
                        elif not tick_size_str_local or '.' not in tick_size_str_local : # 0 or 1
                            price_decimals_local = 0

                    if abs(round(position_data['current_stop_loss'], price_decimals_local) - round(exchange_sl_float, price_decimals_local)) > (10**(-price_decimals_local))/2 : # Порівняння з половиною тіку
                        self.trade_logger.info(
                            f"Оновлення SL для {symbol} з біржі (manage_active_position): {exchange_sl_float:.{price_decimals_local}f} "
                            f"(був {position_data['current_stop_loss']:.{price_decimals_local}f})"
                        )
                        position_data['current_stop_loss'] = exchange_sl_float
        
        except Exception as e_sync:
            self.logger.error(f"Помилка синхронізації стану позиції {symbol} з біржею (manage_active_position): {e_sync}", exc_info=True)
            await self.telegram.send_error_notification({
                'type': 'POSITION_SYNC_ERROR_MANAGE',
                'message': f"Помилка синхронізації {symbol} (manage_active_position): {e_sync}",
                'symbol': symbol
            })

        # Перевірка існування позиції після блоку синхронізації
        if symbol not in self.positions or self.positions[symbol].get('quantity', 0) <= TRADING_CONFIG.get('min_trade_quantity_threshold', 0.000001):
            self.logger.info(f"Позиція {symbol} відсутня або нульова після синхронізації (manage_active_position). Завершую управління.")
            if symbol in self.positions and self.positions[symbol].get('quantity', 0) <= TRADING_CONFIG.get('min_trade_quantity_threshold', 0.000001) :
                del self.positions[symbol] # Видаляємо, якщо кількість стала нульовою/мінімальною
            return
        
        # Оновлюємо position_data тут, щоб працювати з найсвіжішими даними після синхронізації
        position_data = self.positions[symbol]
        bot_signal_side = position_data['side']
        current_high = float(latest_candle['high_price'])
        current_low = float(latest_candle['low_price'])
        current_atr = float(latest_candle.get('atr', position_data.get('initial_atr_at_entry', 0.00001)))
        if current_atr <= 0: current_atr = max(position_data.get('initial_atr_at_entry', 0.00001), 0.00001)
        
        if bot_signal_side == 'BUY':
            position_data['highest_high_since_entry'] = max(
                position_data.get('highest_high_since_entry', current_high), current_high
            )
        else: # SELL
            position_data['lowest_low_since_entry'] = min(
                position_data.get('lowest_low_since_entry', current_low), current_low
            )
        
        sl_hit_price = 0.0
        sl_triggered_by_bot_logic = False
        if bot_signal_side == 'BUY' and current_low <= position_data['current_stop_loss']:
            sl_hit_price = position_data['current_stop_loss']
            sl_triggered_by_bot_logic = True
        elif bot_signal_side == 'SELL' and current_high >= position_data['current_stop_loss']:
            sl_hit_price = position_data['current_stop_loss']
            sl_triggered_by_bot_logic = True
        
        if sl_triggered_by_bot_logic:
            self.trade_logger.info(
                f"ЛОГІКА БОТА: Stop Loss для {symbol} ({bot_signal_side}) "
                f"мав би спрацювати на {sl_hit_price:.6f} (Low/High: {current_low:.6f}/{current_high:.6f})"
            )
            await self._close_position(symbol, current_candle_close_price, "Stop Loss Hit (Bot Logic Triggered)")
            return 
        
        if symbol not in self.positions: return # Перевірка після SL
        
        position_updated_after_tp_or_logic = False # Флаг, що SL/TP могли змінитися
        for tp_level_index, tp_level in enumerate(list(position_data.get('take_profit_levels', []))):
            if symbol not in self.positions or self.positions[symbol]['quantity'] <= TRADING_CONFIG.get('min_trade_quantity_threshold', 0.000001):
                break 
            if tp_level.get('hit', False):
                continue
            
            current_pos_qty_for_tp_calc = self.positions[symbol]['quantity'] 
            tp_price = tp_level['price']
            close_percentage = tp_level['percentage_to_close']
            qty_to_close_for_this_tp_based_on_initial = self.positions[symbol]['initial_quantity'] * (close_percentage / 100.0)
            actual_qty_to_close_now = min(abs(qty_to_close_for_this_tp_based_on_initial), abs(current_pos_qty_for_tp_calc))
            
            if actual_qty_to_close_now <= TRADING_CONFIG.get('min_trade_quantity_threshold', 0.000001):
                continue
            
            tp_triggered_by_bot_logic = False
            if bot_signal_side == 'BUY' and current_high >= tp_price:
                tp_triggered_by_bot_logic = True
            elif bot_signal_side == 'SELL' and current_low <= tp_price:
                tp_triggered_by_bot_logic = True
            
            if tp_triggered_by_bot_logic:
                self.trade_logger.info(
                    f"ЛОГІКА БОТА: {tp_level['type']} TP для {symbol} ({bot_signal_side}) на {tp_price:.6f} "
                    f"(Low/High: {current_low:.6f}/{current_high:.6f})"
                )
                await self._close_position(
                    symbol, tp_price, f"{tp_level['type']} Hit", 
                    quantity_to_close=actual_qty_to_close_now
                )
                position_updated_after_tp_or_logic = True
                
                if symbol not in self.positions: return 
                
                # Оновлюємо стан TP після успішного закриття
                # self.positions доступний, оскільки ми вийшли б, якби символ був видалений
                self.positions[symbol]['take_profit_levels'][tp_level_index]['hit'] = True
                self.trade_logger.info(f"TP рівень {tp_level['type']} для {symbol} позначено як 'hit'.")
                
                if (tp_level['type'] == 'partial_1' and 
                    not self.positions[symbol].get('first_partial_tp_hit', False)):
                    self.positions[symbol]['first_partial_tp_hit'] = True
                
                break # Виходимо з циклу TP, щоб оновити SL/наступний TP
            
        if symbol not in self.positions: return # Перевірка після TP

        new_sl_price_to_set = position_data['current_stop_loss']
        sl_update_reason = "current" 

        if (STRATEGY_CONFIG.get('use_breakeven', True) and
            position_data.get('first_partial_tp_hit', False) and
            not position_data.get('breakeven_applied', False)):
            potential_be_sl = await self._calculate_breakeven_sl_price(symbol, position_data)
            if potential_be_sl is not None:
                if (bot_signal_side == 'BUY' and potential_be_sl > new_sl_price_to_set) or \
                (bot_signal_side == 'SELL' and potential_be_sl < new_sl_price_to_set):
                    new_sl_price_to_set = potential_be_sl
                    self.positions[symbol]['breakeven_applied'] = True
                    sl_update_reason = "breakeven"
                    position_updated_after_tp_or_logic = True

        if symbol not in self.positions: return # Перевірка після BE

        should_activate_trailing = (
            position_data.get('first_partial_tp_hit', False) or
            (not STRATEGY_CONFIG.get('use_triple_partial_tp', True)) # Якщо не використовуємо 3 TP, трейлінг може активуватися раніше
        )
        
        if (STRATEGY_CONFIG.get('use_trailing_stop', True) and
            should_activate_trailing and
            position_data.get('quantity', 0) > TRADING_CONFIG.get('min_trade_quantity_threshold', 0.000001)):
            
            if not position_data.get('trailing_stop_active', False):
                self.positions[symbol]['trailing_stop_active'] = True
                self.positions[symbol]['highest_high_since_trail_active'] = current_high if bot_signal_side == 'BUY' else position_data.get('entry_price', current_high)
                self.positions[symbol]['lowest_low_since_trail_active'] = current_low if bot_signal_side == 'SELL' else position_data.get('entry_price', current_low)
                self.trade_logger.info(
                    f"РЕАЛЬНИЙ: Trailing Stop активовано для {symbol} ({bot_signal_side}). "
                    f"Початковий SL: {position_data['current_stop_loss']:.6f}"
                )
                position_updated_after_tp_or_logic = True

            if self.positions[symbol].get('trailing_stop_active', False):
                if bot_signal_side == 'BUY':
                    self.positions[symbol]['highest_high_since_trail_active'] = max(
                        self.positions[symbol].get('highest_high_since_trail_active', current_high), current_high
                    )
                else: # SELL
                    self.positions[symbol]['lowest_low_since_trail_active'] = min(
                        self.positions[symbol].get('lowest_low_since_trail_active', current_low), current_low
                    )
                
                potential_trail_sl = await self._calculate_trailing_sl_price(symbol, self.positions[symbol], current_atr)
                if potential_trail_sl is not None:
                    if (bot_signal_side == 'BUY' and potential_trail_sl > new_sl_price_to_set) or \
                    (bot_signal_side == 'SELL' and potential_trail_sl < new_sl_price_to_set):
                        new_sl_price_to_set = potential_trail_sl
                        sl_update_reason = "trailing"
                        position_updated_after_tp_or_logic = True
        
        if symbol not in self.positions: return # Перевірка після трейлінгу

        if position_updated_after_tp_or_logic: 
            if symbol in self.positions and self.positions[symbol]['quantity'] > TRADING_CONFIG.get('min_trade_quantity_threshold', 0.000001):
                await self._update_active_tpsl_on_exchange(symbol, new_sl_price_to_set, sl_update_reason)
            else: return

        if symbol not in self.positions: return 

        current_pos_data_for_div = self.positions[symbol] 
        if (STRATEGY_CONFIG.get('use_volume_divergence', True) and
            not current_pos_data_for_div.get('volume_divergence_exit_done', False) and
            current_pos_data_for_div.get('quantity', 0) > TRADING_CONFIG.get('min_trade_quantity_threshold', 0.000001)):
            
            bullish_div = latest_candle.get('bullish_vol_divergence', False)
            bearish_div = latest_candle.get('bearish_vol_divergence', False)
            close_percent_div = STRATEGY_CONFIG.get('volume_divergence_close_percent', 50.0)
            qty_to_close_div = current_pos_data_for_div['quantity'] * (close_percent_div / 100.0)
            
            if qty_to_close_div > TRADING_CONFIG.get('min_trade_quantity_threshold', 0.000001):
                div_triggered = False
                if bot_signal_side == 'BUY' and bearish_div: div_triggered = True
                elif bot_signal_side == 'SELL' and bullish_div: div_triggered = True
                
                if div_triggered:
                    self.trade_logger.info(f"ЛОГІКА БОТА: Volume Divergence для {symbol} ({bot_signal_side}).")
                    await self._close_position(
                        symbol, current_candle_close_price, "Volume Divergence Exit", 
                        quantity_to_close=qty_to_close_div
                    )
                    if symbol not in self.positions: return
                    self.positions[symbol]['volume_divergence_exit_done'] = True
                    if self.positions[symbol]['quantity'] > TRADING_CONFIG.get('min_trade_quantity_threshold', 0.000001):
                        await self._update_active_tpsl_on_exchange(symbol, self.positions[symbol]['current_stop_loss'], "divergence_partial_close")
    
    async def _calculate_breakeven_sl_price(self, symbol: str, position_data: Dict[str, Any]) -> Optional[float]:
        """Розраховує ціну SL для беззбитку, враховуючи напрямок та спред."""
        entry_price = position_data['entry_price']
        bot_signal_side = position_data['side']
        initial_atr = position_data.get('initial_atr_at_entry', 0.00001)
        if initial_atr <= 0: initial_atr = 0.00001

        atr_buffer_mult = STRATEGY_CONFIG.get('breakeven_buffer', 0.05) # 5% від ATR як буфер
        atr_based_buffer = initial_atr * atr_buffer_mult

        tick_size = 0.00000001 # Default, буде оновлено
        price_decimals = 8
        instrument_details = await self.api_manager.get_instrument_info(symbol)
        if instrument_details:
            price_filter = instrument_details.get('priceFilter', {})
            tick_size_str = price_filter.get('tickSize')
            if tick_size_str:
                try:
                    tick_size = float(tick_size_str)
                    if '.' in tick_size_str: price_decimals = len(tick_size_str.split('.')[1].rstrip('0'))
                    else: price_decimals = 0
                except ValueError:
                    self.logger.warning(f"Не вдалося перетворити tickSize '{tick_size_str}' на float для {symbol} (BE)")
        
        min_buffer_ticks = STRATEGY_CONFIG.get('breakeven_min_buffer_ticks', 3)
        tick_based_min_buffer = tick_size * min_buffer_ticks
        
        # Використовуємо більший з двох буферів: ATR-based або tick-based
        final_breakeven_buffer_value = max(atr_based_buffer, tick_based_min_buffer)

        proposed_new_sl_price = 0.0
        if bot_signal_side == 'BUY':
            # SL для Long має бути трохи ВИЩЕ ціни входу
            proposed_new_sl_price = entry_price + final_breakeven_buffer_value
            # Перевірка, чи новий SL дійсно кращий (вищий) за поточний SL
            if proposed_new_sl_price <= position_data['current_stop_loss']:
                self.trade_logger.debug(f"BE SL SKIP ({symbol} Long): Новий SL {proposed_new_sl_price:.{price_decimals}f} не кращий за поточний {position_data['current_stop_loss']:.{price_decimals}f}.")
                return None
            if proposed_new_sl_price <= entry_price + tick_size: # Має бути хоча б на 1 тік вище входу
                self.trade_logger.warning(f"BE SL SKIP ({symbol} Long): Новий SL {proposed_new_sl_price:.{price_decimals}f} недостатньо вище ціни входу {entry_price:.{price_decimals}f}.")
                return None
        elif bot_signal_side == 'SELL':
            # SL для Short має бути трохи НИЖЧЕ ціни входу
            proposed_new_sl_price = entry_price - final_breakeven_buffer_value
            # Перевірка, чи новий SL дійсно кращий (нижчий) за поточний SL
            if proposed_new_sl_price >= position_data['current_stop_loss']:
                self.trade_logger.debug(f"BE SL SKIP ({symbol} Short): Новий SL {proposed_new_sl_price:.{price_decimals}f} не кращий за поточний {position_data['current_stop_loss']:.{price_decimals}f}.")
                return None
            if proposed_new_sl_price >= entry_price - tick_size: # Має бути хоча б на 1 тік нижче входу
                self.trade_logger.warning(f"BE SL SKIP ({symbol} Short): Новий SL {proposed_new_sl_price:.{price_decimals}f} недостатньо нижче ціни входу {entry_price:.{price_decimals}f}.")
                return None
                
        if proposed_new_sl_price == 0.0: return None

        # Перевірка відстані від ринку з покращеною логікою буферу
        market_price = await self._get_current_market_price(symbol)
        if market_price == 0: market_price = entry_price # Fallback, якщо не вдалося отримати ринкову ціну

        # Use symbol-specific buffer calculation
        safety_buffer_market = self._calculate_symbol_specific_buffer(symbol, tick_size, market_price)

        if bot_signal_side == 'BUY':
            if proposed_new_sl_price >= market_price - safety_buffer_market:
                self.trade_logger.warning(f"BE SL SKIP ({symbol} Long): Новий SL {proposed_new_sl_price:.{price_decimals}f} занадто близько/вище ринку {market_price:.{price_decimals}f} (буфер {safety_buffer_market:.{price_decimals}f}).")
                return None
        elif bot_signal_side == 'SELL':
            if proposed_new_sl_price <= market_price + safety_buffer_market:
                self.trade_logger.warning(f"BE SL SKIP ({symbol} Short): Новий SL {proposed_new_sl_price:.{price_decimals}f} занадто близько/нижче ринку {market_price:.{price_decimals}f} (буфер {safety_buffer_market:.{price_decimals}f}).")
                return None
                
        return round(proposed_new_sl_price, price_decimals)

    async def _calculate_trailing_sl_price(self, symbol: str, position_data: Dict[str, Any], current_atr: float) -> Optional[float]:
        """Розраховує ціну для трейлінг стопу."""
        bot_signal_side = position_data['side']
        entry_price = position_data['entry_price'] 
        current_sl_price = position_data['current_stop_loss']
        
        if current_atr <= 0: current_atr = max(position_data.get('initial_atr_at_entry', 0.00001), 0.00001)

        trail_atr_mult = STRATEGY_CONFIG.get('trail_atr_mult', 0.7)
        trail_value = current_atr * trail_atr_mult

        tick_size = 0.00000001 
        price_decimals = 8
        instrument_details = await self.api_manager.get_instrument_info(symbol)
        if instrument_details:
            price_filter = instrument_details.get('priceFilter', {})
            tick_size_str = price_filter.get('tickSize')
            if tick_size_str:
                try:
                    tick_size = float(tick_size_str)
                    if '.' in tick_size_str: price_decimals = len(tick_size_str.split('.')[1].rstrip('0'))
                    else: price_decimals = 0
                except ValueError:
                    self.logger.warning(f"Не вдалося перетворити tickSize '{tick_size_str}' (трейлінг) для {symbol}")

        proposed_new_sl_price = current_sl_price # Починаємо з поточного SL
        reference_price_for_trail = 0.0

        if bot_signal_side == 'BUY':
            reference_price_for_trail = position_data.get('highest_high_since_trail_active', position_data.get('highest_high_since_entry', entry_price))
            calculated_sl = reference_price_for_trail - trail_value
            if calculated_sl > current_sl_price: # Новий SL має бути вищим (кращим)
                proposed_new_sl_price = calculated_sl
            else: 
                self.trade_logger.debug(f"TRAIL SL SKIP ({symbol} Long): Розрахований SL {calculated_sl:.{price_decimals}f} не покращує поточний {current_sl_price:.{price_decimals}f}.")
                return None 
        elif bot_signal_side == 'SELL':
            reference_price_for_trail = position_data.get('lowest_low_since_trail_active', position_data.get('lowest_low_since_entry', entry_price))
            calculated_sl = reference_price_for_trail + trail_value
            if calculated_sl < current_sl_price: # Новий SL має бути нижчим (кращим)
                proposed_new_sl_price = calculated_sl
            else: 
                self.trade_logger.debug(f"TRAIL SL SKIP ({symbol} Short): Розрахований SL {calculated_sl:.{price_decimals}f} не покращує поточний {current_sl_price:.{price_decimals}f}.")
                return None 
        
        # Перевірка відстані від ринку
        market_price = await self._get_current_market_price(symbol)
        if market_price == 0: market_price = reference_price_for_trail 

        min_sl_dist_market_ticks = TRADING_CONFIG.get('min_sl_market_distance_tick_multiplier', 5)
        safety_buffer_market = tick_size * min_sl_dist_market_ticks

        if bot_signal_side == 'BUY':
            if proposed_new_sl_price >= market_price - safety_buffer_market:
                self.trade_logger.warning(f"TRAIL SL SKIP ({symbol} Long): Новий SL {proposed_new_sl_price:.{price_decimals}f} занадто близько/вище ринку {market_price:.{price_decimals}f} (буфер {safety_buffer_market:.{price_decimals}f}).")
                return None
        elif bot_signal_side == 'SELL':
            if proposed_new_sl_price <= market_price + safety_buffer_market:
                self.trade_logger.warning(f"TRAIL SL SKIP ({symbol} Short): Новий SL {proposed_new_sl_price:.{price_decimals}f} занадто близько/нижче ринку {market_price:.{price_decimals}f} (буфер {safety_buffer_market:.{price_decimals}f}).")
                return None
        
        if abs(proposed_new_sl_price - current_sl_price) < tick_size / 2: # Якщо зміна менша за півтіка
            self.trade_logger.debug(f"TRAIL SL SKIP ({symbol}): Зміна SL ({proposed_new_sl_price:.{price_decimals}f} vs {current_sl_price:.{price_decimals}f}) менша за півтіка.")
            return None

        return round(proposed_new_sl_price, price_decimals)

    async def _update_active_tpsl_on_exchange(self, symbol: str, new_sl_price: float, reason_for_update: str):
        """Оновлює активний SL та встановлює наступний TP на біржі."""
        if symbol not in self.positions or self.positions[symbol]['quantity'] <= TRADING_CONFIG.get('min_trade_quantity_threshold', 0.000001): # Use config for threshold
            self.logger.debug(f"Оновлення TP/SL для {symbol} пропущено: позиція не існує або нульова.")
            return

        position_data = self.positions[symbol]
        
        next_active_tp_level = None
        for tp_level_data in position_data.get('take_profit_levels', []):
            if not tp_level_data.get('hit', False):
                next_active_tp_level = tp_level_data
                break
        
        next_tp_price_str = None
        price_decimals_from_instrument = 6 
        instrument_details = await self.api_manager.get_instrument_info(symbol)
        if instrument_details:
            price_filter = instrument_details.get('priceFilter', {})
            tick_size_str = price_filter.get('tickSize')
            if tick_size_str:
                if '.' in tick_size_str: 
                    price_decimals_from_instrument = len(tick_size_str.split('.')[1].rstrip('0'))
                else: 
                    price_decimals_from_instrument = 0
            
        if next_active_tp_level and 'price' in next_active_tp_level:
            next_tp_price_to_set = float(next_active_tp_level['price'])
            if next_tp_price_to_set > 0: # Only set TP if price is valid
                next_tp_price_str = f"{next_tp_price_to_set:.{price_decimals_from_instrument}f}"
            else:
                self.logger.warning(f"Розрахована ціна для наступного TP для {symbol} недійсна ({next_tp_price_to_set}). TP не буде встановлено.")
                next_tp_price_str = None # Ensure it's None if price is invalid

        new_sl_price_str = f"{new_sl_price:.{price_decimals_from_instrument}f}"

        current_sl_on_record = position_data.get('current_stop_loss')
        current_tp_on_record = position_data.get('current_active_tp_price') # Assumes this field exists

        current_sl_on_record_str = None
        if current_sl_on_record is not None:
            current_sl_on_record_str = f"{float(current_sl_on_record):.{price_decimals_from_instrument}f}"

        current_tp_on_record_str = None
        if current_tp_on_record is not None:
            current_tp_on_record_str = f"{float(current_tp_on_record):.{price_decimals_from_instrument}f}"
        
        # Enhanced redundancy check with better logging
        sl_unchanged = (current_sl_on_record_str == new_sl_price_str)
        
        tp_unchanged = False
        if next_tp_price_str is None and current_tp_on_record_str is None: # Both are None
            tp_unchanged = True
        elif next_tp_price_str is not None and current_tp_on_record_str is not None: # Both have values
            tp_unchanged = (current_tp_on_record_str == next_tp_price_str)
        # Else (one is None and other is not), they are different, so tp_unchanged remains False

        # Add debug logging for redundancy check
        self.trade_logger.debug(
            f"Redundancy check for {symbol}: SL unchanged={sl_unchanged} "
            f"(current={current_sl_on_record_str} vs new={new_sl_price_str}), "
            f"TP unchanged={tp_unchanged} "
            f"(current={current_tp_on_record_str} vs new={next_tp_price_str})"
        )

        if sl_unchanged and tp_unchanged:
            self.trade_logger.info(
                f"TP/SL для {symbol} не змінилися (Поточний SL: {current_sl_on_record_str}, Новий SL: {new_sl_price_str}; "
                f"Поточний TP: {current_tp_on_record_str or 'None'}, Новий TP: {next_tp_price_str or 'None'}). "
                f"Оновлення на біржі пропущено. Причина запиту на оновлення: {reason_for_update}"
            )
            # Оновлюємо локальний стан та додаємо timestamp для redundancy tracking
            position_data['current_stop_loss'] = float(new_sl_price_str)
            if next_tp_price_str:
                position_data['current_active_tp_price'] = float(next_tp_price_str)
            else:
                position_data['current_active_tp_price'] = None
            position_data['last_tpsl_update_reason'] = reason_for_update
            position_data['last_tpsl_update_time'] = datetime.now(timezone.utc)
            position_data['redundant_calls_prevented'] = position_data.get('redundant_calls_prevented', 0) + 1
            return

        self.trade_logger.info(
            f"РЕАЛЬНИЙ (Оновлення TP/SL для {symbol} через '{reason_for_update}'): "
            f"Новий SL: {new_sl_price_str}, Наступний TP: {next_tp_price_str or 'Немає (всі TP досягнуті або не встановлені)'}"
        )

        # ✅ ДОДАНО: Додаткова перевірка позиції перед встановленням TP/SL
        if symbol not in self.positions:
            self.trade_logger.warning(f"Позиція {symbol} відсутня в локальному стані перед оновленням TP/SL")
            return
        
        current_qty = self.positions[symbol].get('quantity', 0)
        if current_qty <= TRADING_CONFIG.get('min_trade_quantity_threshold', 0.000001):
            self.trade_logger.warning(f"Позиція {symbol} має нульову кількість ({current_qty:.8f}) перед оновленням TP/SL")
            return

        api_response = await self.api_manager.set_trading_stop_for_position(
            symbol=symbol,
            stop_loss=new_sl_price_str,
            take_profit=next_tp_price_str, # Pass None if no TP to set
            position_idx=0, 
            tpsl_mode="Full" 
        )

        if api_response and api_response.get('retCode') == 0:
            self.trade_logger.info(f"TP/SL для {symbol} успішно оновлено на біржі (причина: {reason_for_update}). SL: {new_sl_price_str}, TP: {next_tp_price_str or 'N/A'}")
            position_data['current_stop_loss'] = float(new_sl_price_str)
            position_data['current_active_tp_price'] = float(next_tp_price_str) if next_tp_price_str else None
            position_data['last_tpsl_update_reason'] = reason_for_update
            position_data['last_tpsl_update_time'] = datetime.now(timezone.utc)
        else:
            error_code = api_response.get('retCode') if api_response else 'N/A'
            error_msg_api = api_response.get('retMsg', 'No API response').lower() if api_response else 'no api response'
            
            if str(error_code) == '34040' or "not modified" in error_msg_api: # ErrCode: 34040, ErrMsg: not modified
                self.trade_logger.warning(
                    f"TP/SL для {symbol} не змінено на біржі (not modified - {error_code}). "
                    f"Ймовірно, параметри SL: {new_sl_price_str}, TP: {next_tp_price_str or 'N/A'} вже встановлені або запит ідентичний. "
                    f"Причина оновлення: {reason_for_update}. Відповідь API: {api_response.get('retMsg')}"
                )
                # Оновлюємо локальні дані, оскільки біржа підтвердила, що такі параметри вже є (або їх не було і не треба)
                position_data['current_stop_loss'] = float(new_sl_price_str)
                position_data['current_active_tp_price'] = float(next_tp_price_str) if next_tp_price_str else None
                position_data['last_tpsl_update_reason'] = reason_for_update
                position_data['last_tpsl_update_time'] = datetime.now(timezone.utc)
                # Не вважаємо це критичною помилкою, а підтвердженням поточного стану або того, що зміни не потрібні
                return 

            self.trade_logger.error(
                f"Помилка оновлення TP/SL для {symbol} на біржі (причина: {reason_for_update}). "
                f"SL: {new_sl_price_str}, TP: {next_tp_price_str or 'N/A'}. "
                f"API Code: {error_code}, Msg: {error_msg_api}"
            )
            
            is_zero_pos_error = (
                str(error_code) == '10001' and (
                    "zero position" in error_msg_api or 
                    "can not set tp/sl/ts for zero position" in error_msg_api or
                    "position not exist" in error_msg_api or
                    "no position found" in error_msg_api
                )
            ) or (
                str(error_code) in ['110017', '110025', '30036', '34036'] and (
                    "position not exist" in error_msg_api or
                    "no position found" in error_msg_api or
                    "position is not an order" in error_msg_api or
                    "order not exists or too late to cancel" in error_msg_api or 
                    "cannot set read only" in error_msg_api or
                    "position is closing" in error_msg_api
                )
            )

            if is_zero_pos_error:
                self.trade_logger.warning(
                    f"Спроба оновити TP/SL для {symbol}, але позиція, ймовірно, вже нульова/відсутня/закривається на біржі. "
                    f"Причина оновлення була: {reason_for_update}. Перевірка стану..."
                )
                # Подальша логіка синхронізації має обробити це, якщо позиція дійсно закрита.
                # Тут ми просто логуємо і не надсилаємо сповіщення про помилку,
                # оскільки це, ймовірно, стан "позиція вже закрита".
                # Головний цикл синхронізації має це виявити.
                # Можна запланувати негайну синхронізацію для цієї пари, якщо є така можливість.
                if symbol in self.positions: # Якщо позиція ще існує локально
                    self.logger.info(f"Позиція {symbol} ще існує локально, але API вказує на її відсутність/закриття під час оновлення TP/SL. Запускаю синхронізацію.")
                    asyncio.create_task(self.sync_single_position_with_history(symbol))

            else: # Інші помилки, не пов'язані з відсутністю позиції
                 await self.telegram.send_error_notification({
                    'type': 'SET_TPSL_ON_EXCHANGE_FAILED',
                    'message': f"Failed to update TP/SL for {symbol} (Reason: {reason_for_update}). SL: {new_sl_price_str}, TP: {next_tp_price_str or 'N/A'}. API Code: {error_code}, Msg: {error_msg_api}",
                    'symbol': symbol,
                    'action': f'UPDATE_TPSL_{reason_for_update}'
                })
    
    async def _apply_breakeven_sl(self, symbol: str, position_data: Dict[str, Any]):
        if symbol not in self.positions or position_data.get('quantity', 0) <= 0.0000001:
            self.trade_logger.debug(f"BE SL SKIP ({symbol}): Позиція не існує або нульова кількість.")
            return

        entry_price = position_data['entry_price']
        initial_atr = position_data.get('initial_atr_at_entry', 0.00001) 
        if initial_atr <= 0:
            initial_atr = 0.00001 
        
        atr_buffer_mult = STRATEGY_CONFIG.get('breakeven_buffer', 0.05) 
        atr_based_buffer = initial_atr * atr_buffer_mult
        
        current_sl_price = position_data['current_stop_loss']
        bot_signal_side = position_data['side']

        market_price = None
        tick_size = 0.00000001 
        instrument_details = await self.api_manager.get_instrument_info(symbol)
        price_decimals = 8 
        if instrument_details:
            price_filter = instrument_details.get('priceFilter', {})
            tick_size_str = price_filter.get('tickSize')
            if tick_size_str:
                try: # Додано try-except для безпечного перетворення
                    tick_size = float(tick_size_str)
                    if '.' in tick_size_str:
                        price_decimals = len(tick_size_str.split('.')[1].rstrip('0'))
                    else:
                        price_decimals = 0
                except ValueError:
                    self.logger.warning(f"Не вдалося перетворити tickSize '{tick_size_str}' на float для {symbol}")
                    # Залишаємо price_decimals = 8 за замовчуванням
        
        min_buffer_ticks = STRATEGY_CONFIG.get('breakeven_min_buffer_ticks', 3)
        tick_based_min_buffer = tick_size * min_buffer_ticks
        
        final_breakeven_buffer_value = max(atr_based_buffer, tick_based_min_buffer)

        self.trade_logger.debug(
            f"BE SL CALC ({symbol}): Entry: {entry_price:.{price_decimals}f}, ATR: {initial_atr:.{price_decimals}f}, "
            f"ATRBufferMult: {atr_buffer_mult}, ATRBasedBuffer: {atr_based_buffer:.{price_decimals}f}, "
            f"MinBufferTicks: {min_buffer_ticks}, TickBasedMinBuffer: {tick_based_min_buffer:.{price_decimals}f}, "
            f"FinalBEBuffer: {final_breakeven_buffer_value:.{price_decimals}f}"
        )

        proposed_new_sl_price = 0.0
        if bot_signal_side == 'BUY':
            proposed_new_sl_price = entry_price + final_breakeven_buffer_value
            if proposed_new_sl_price <= current_sl_price: 
                self.trade_logger.debug(
                    f"BE SL SKIP ({symbol} Long): Новий SL {proposed_new_sl_price:.{price_decimals}f} "
                    f"не кращий за поточний {current_sl_price:.{price_decimals}f}."
                )
                return
            # --- ЗМІНА: Перевірка, чи новий SL дійсно вище за ціну входу ---
            if proposed_new_sl_price <= entry_price + tick_size: # Додаємо tick_size як мінімальний буфер
                self.trade_logger.warning(
                    f"BE SL SKIP ({symbol} Long): Новий SL {proposed_new_sl_price:.{price_decimals}f} "
                    f"недостатньо вище ціни входу {entry_price:.{price_decimals}f}."
                )
                return
        elif bot_signal_side == 'SELL':
            proposed_new_sl_price = entry_price - final_breakeven_buffer_value
            if proposed_new_sl_price >= current_sl_price: 
                self.trade_logger.debug(
                    f"BE SL SKIP ({symbol} Short): Новий SL {proposed_new_sl_price:.{price_decimals}f} "
                    f"не кращий за поточний {current_sl_price:.{price_decimals}f}."
                )
                return
            # --- ЗМІНА: Перевірка, чи новий SL дійсно нижче за ціну входу ---
            if proposed_new_sl_price >= entry_price - tick_size: # Додаємо tick_size як мінімальний буфер
                self.trade_logger.warning(
                    f"BE SL SKIP ({symbol} Short): Новий SL {proposed_new_sl_price:.{price_decimals}f} "
                    f"недостатньо нижче ціни входу {entry_price:.{price_decimals}f}."
                )
                return
        
        if proposed_new_sl_price == 0.0: 
            self.logger.warning(f"BE SL SKIP ({symbol}): Новий SL розрахований як 0.")
            return

        ticker_data = await self.api_manager.get_tickers(category="linear", symbol=symbol)
        if ticker_data and ticker_data.get('retCode') == 0 and ticker_data['result']['list']:
            market_price_str = ticker_data['result']['list'][0].get('lastPrice')
            if market_price_str: market_price = float(market_price_str) # Безпечне перетворення
            else: market_price = entry_price # Fallback
        else:
            self.trade_logger.warning(f"BE SL ({symbol}): Не вдалося отримати ринкову ціну. Використовую ціну входу.")
            market_price = entry_price

        # Use symbol-specific buffer calculation for breakeven SL
        safety_buffer_market = self._calculate_symbol_specific_buffer(symbol, tick_size, market_price)

        if bot_signal_side == 'BUY':
            if proposed_new_sl_price >= market_price - safety_buffer_market:
                self.trade_logger.warning(
                    f"BE SL SKIP ({symbol} Long): Новий SL {proposed_new_sl_price:.{price_decimals}f} "
                    f"занадто близько/вище ринку {market_price:.{price_decimals}f} "
                    f"(буфер {safety_buffer_market:.{price_decimals}f})."
                )
                return
        elif bot_signal_side == 'SELL':
            if proposed_new_sl_price <= market_price + safety_buffer_market:
                self.trade_logger.warning(
                    f"BE SL SKIP ({symbol} Short): Новий SL {proposed_new_sl_price:.{price_decimals}f} "
                    f"занадто близько/нижче ринку {market_price:.{price_decimals}f} "
                    f"(буфер {safety_buffer_market:.{price_decimals}f})."
                )
                return
        
        new_sl_price_str = f"{proposed_new_sl_price:.{price_decimals}f}"
        self.trade_logger.info(
            f"РЕАЛЬНИЙ (Breakeven SL): {symbol} ({bot_signal_side}). "
            f"Entry: {entry_price:.{price_decimals}f}, Market: {market_price:.{price_decimals}f}, "
            f"Current SL: {current_sl_price:.{price_decimals}f}, "
            f"Proposed New SL: {proposed_new_sl_price:.{price_decimals}f}. "
            f"Formatted New SL: {new_sl_price_str}."
        )
        
        current_tp_price_str = None
        if symbol in self.positions: # Перевірка існування позиції
            active_tp_level = next(
                (tp for tp in self.positions[symbol].get('take_profit_levels', []) 
                 if not tp.get('hit')), 
                None
            )
            if active_tp_level and 'price' in active_tp_level: # Перевірка наявності ключа 'price'
                current_tp_price_str = f"{active_tp_level['price']:.{price_decimals}f}"
        else:
            self.logger.warning(f"BE SL ({symbol}): Позиція не знайдена в self.positions перед встановленням TP.")
            return # Якщо позиції немає, не продовжуємо

        api_response = await self.api_manager.set_trading_stop_for_position(
            symbol=symbol, stop_loss=new_sl_price_str, take_profit=current_tp_price_str
        )

        if api_response and api_response.get('retCode') == 0:
            self.trade_logger.info(
                f"Breakeven SL для {symbol} ({bot_signal_side}) успішно встановлено: {new_sl_price_str}"
            )
            if symbol in self.positions: # Перевірка існування позиції
                self.positions[symbol]['current_stop_loss'] = float(new_sl_price_str)
        else:
            error_code = api_response.get('retCode') if api_response else 'N/A'
            error_msg_api = api_response.get('retMsg', 'No API response').lower() if api_response else 'no api response'
            self.trade_logger.error(
                f"Помилка встановлення Breakeven SL для {symbol} на біржі. "
                f"Відповідь API: {api_response}. Запит SL: {new_sl_price_str}, TP: {current_tp_price_str}."
            )
            is_zero_pos_error = (
                str(error_code) == '10001' and # Порівняння рядків для кодів помилок
                ("zero position" in error_msg_api or 
                 "cannot set tp/sl/ts for zero position" in error_msg_api or
                 "position not exist" in error_msg_api or
                 "position is not an order" in error_msg_api)
            ) or str(error_code) == '30036' # Порівняння рядків
            
            if is_zero_pos_error:
                self.trade_logger.warning(
                    f"Спроба встановити Breakeven SL для {symbol}, але позиція вже нульова/відсутня на біржі."
                )
                await self.telegram.send_error_notification({
                    'type': f'SET_BE_SL_ON_ZERO_POS_{bot_signal_side}',
                    'message': (
                        f"Set Breakeven SL for {symbol} failed: position zero/closed. "
                        f"API: {api_response.get('retMsg', '') if api_response else 'N/A'} ({error_code})"
                    ),
                    'symbol': symbol, 
                    'action': 'SET_BREAKEVEN_SL'
                })
                if symbol in self.positions: # Перевірка існування позиції
                    del self.positions[symbol]

    async def _apply_trailing_sl(self, symbol: str, position_data: Dict[str, Any], current_atr: float):
        if symbol not in self.positions or position_data.get('quantity', 0) <= 0.0000001:
            self.trade_logger.debug(f"TRAIL SL SKIP ({symbol}): Позиція не існує або нульова кількість.")
            return
        
        bot_signal_side = position_data['side']
        entry_price = position_data['entry_price']
        current_sl_price = position_data['current_stop_loss']
        
        if current_atr <= 0:
            self.trade_logger.debug(
                f"TRAIL SL ({symbol}): Поточний ATR ({current_atr}) не позитивний. Використовую initial_atr."
            )
            current_atr = position_data.get('initial_atr_at_entry', 0.00001)
            if current_atr <= 0:
                current_atr = 0.00001 

        trail_atr_mult = STRATEGY_CONFIG.get('trail_atr_mult', 0.7)
        trail_value = current_atr * trail_atr_mult
        
        market_price = None
        tick_size = 0.00000001 
        instrument_details = await self.api_manager.get_instrument_info(symbol)
        price_decimals = 8
        if instrument_details:
            price_filter = instrument_details.get('priceFilter', {})
            tick_size_str = price_filter.get('tickSize')
            if tick_size_str:
                try: # Додано try-except
                    tick_size = float(tick_size_str)
                    if '.' in tick_size_str:
                        price_decimals = len(tick_size_str.split('.')[1].rstrip('0'))
                    else:
                        price_decimals = 0
                except ValueError:
                     self.logger.warning(f"Не вдалося перетворити tickSize '{tick_size_str}' на float для {symbol} (трейлінг)")

        ticker_data = await self.api_manager.get_tickers(category="linear", symbol=symbol)
        if ticker_data and ticker_data.get('retCode') == 0 and ticker_data['result']['list']:
            market_price_str = ticker_data['result']['list'][0].get('lastPrice')
            if market_price_str: market_price = float(market_price_str) # Безпечне перетворення
            else: market_price = entry_price # Fallback
        else:
            self.trade_logger.warning(f"TRAIL SL ({symbol}): Не вдалося отримати ринкову ціну. Використовую ціну входу.")
            market_price = entry_price

        proposed_new_sl_price = current_sl_price 
        reference_price_for_trail = 0.0

        if bot_signal_side == 'BUY':
            reference_price_for_trail = position_data.get(
                'highest_high_since_trail_active', 
                position_data.get('highest_high_since_entry', entry_price) # fallback
            )
            calculated_sl = reference_price_for_trail - trail_value
            if calculated_sl > current_sl_price: 
                proposed_new_sl_price = calculated_sl
            else:
                self.trade_logger.debug(
                    f"TRAIL SL SKIP ({symbol} Long): Розрахований SL {calculated_sl:.{price_decimals}f} "
                    f"не покращує поточний {current_sl_price:.{price_decimals}f}."
                )
                return
        elif bot_signal_side == 'SELL':
            reference_price_for_trail = position_data.get(
                'lowest_low_since_trail_active', 
                position_data.get('lowest_low_since_entry', entry_price) # fallback
            )
            calculated_sl = reference_price_for_trail + trail_value
            if calculated_sl < current_sl_price:
                proposed_new_sl_price = calculated_sl
            else:
                self.trade_logger.debug(
                    f"TRAIL SL SKIP ({symbol} Short): Розрахований SL {calculated_sl:.{price_decimals}f} "
                    f"не покращує поточний {current_sl_price:.{price_decimals}f}."
                )
                return
        
        self.trade_logger.debug(
            f"TRAIL SL CALC ({symbol} {bot_signal_side}): Market: {market_price:.{price_decimals}f}, "
            f"RefPrice: {reference_price_for_trail:.{price_decimals}f}, ATR: {current_atr:.{price_decimals}f}, "
            f"TrailVal: {trail_value:.{price_decimals}f}, "
            f"CalcSL: {(reference_price_for_trail - trail_value) if bot_signal_side == 'BUY' else (reference_price_for_trail + trail_value):.{price_decimals}f}, "
            f"CurrentSL: {current_sl_price:.{price_decimals}f}, ProposedSL: {proposed_new_sl_price:.{price_decimals}f}"
        )

        # Use symbol-specific buffer calculation for trailing SL
        safety_buffer_market = self._calculate_symbol_specific_buffer(symbol, tick_size, market_price)

        if bot_signal_side == 'BUY':
            if proposed_new_sl_price >= market_price - safety_buffer_market:
                self.trade_logger.warning(
                    f"TRAIL SL SKIP ({symbol} Long): Новий SL {proposed_new_sl_price:.{price_decimals}f} "
                    f"занадто близько/вище ринку {market_price:.{price_decimals}f} "
                    f"(буфер {safety_buffer_market:.{price_decimals}f})."
                )
                return
        elif bot_signal_side == 'SELL':
            if proposed_new_sl_price <= market_price + safety_buffer_market:
                self.trade_logger.warning(
                    f"TRAIL SL SKIP ({symbol} Short): Новий SL {proposed_new_sl_price:.{price_decimals}f} "
                    f"занадто близько/нижче ринку {market_price:.{price_decimals}f} "
                    f"(буфер {safety_buffer_market:.{price_decimals}f})."
                )
                return

        if abs(proposed_new_sl_price - current_sl_price) < tick_size / 2: # Порівняння з половиною тіку
            self.trade_logger.debug(
                f"TRAIL SL SKIP ({symbol}): Зміна SL "
                f"({proposed_new_sl_price:.{price_decimals}f} vs {current_sl_price:.{price_decimals}f}) "
                f"менша за півтіка ({tick_size / 2:.{price_decimals+1}f})."
            )
            return
            
        new_sl_price_str = f"{proposed_new_sl_price:.{price_decimals}f}"
        self.trade_logger.info(
            f"РЕАЛЬНИЙ (Trailing SL): {symbol} ({bot_signal_side}). "
            f"Market: {market_price:.{price_decimals}f}, "
            f"RefPrice: {reference_price_for_trail:.{price_decimals}f}, "
            f"Current SL: {current_sl_price:.{price_decimals}f}, "
            f"Proposed New SL: {proposed_new_sl_price:.{price_decimals}f}. "
            f"Formatted New SL: {new_sl_price_str}."
        )
        
        current_tp_price_str = None
        if symbol in self.positions: # Перевірка існування позиції
            active_tp_level = next(
                (tp for tp in self.positions[symbol].get('take_profit_levels', []) 
                 if not tp.get('hit')), 
                None
            )
            if active_tp_level and 'price' in active_tp_level: # Перевірка наявності ключа 'price'
                current_tp_price_str = f"{active_tp_level['price']:.{price_decimals}f}"
        else:
            self.logger.warning(f"TRAIL SL ({symbol}): Позиція не знайдена в self.positions перед встановленням TP.")
            return # Якщо позиції немає, не продовжуємо

        api_response = await self.api_manager.set_trading_stop_for_position(
            symbol=symbol, stop_loss=new_sl_price_str, take_profit=current_tp_price_str 
        )

        if api_response and api_response.get('retCode') == 0:
            self.trade_logger.info(
                f"Trailing SL для {symbol} ({bot_signal_side}) успішно оновлено: {new_sl_price_str}"
            )
            if symbol in self.positions: # Перевірка існування позиції
                self.positions[symbol]['current_stop_loss'] = float(new_sl_price_str)
        else:
            error_code = api_response.get('retCode') if api_response else 'N/A'
            error_msg_api = api_response.get('retMsg', 'No API response').lower() if api_response else 'no api response'
            self.trade_logger.error(
                f"Помилка оновлення Trailing SL для {symbol} на біржі. "
                f"Відповідь API: {api_response}. Запит SL: {new_sl_price_str}, TP: {current_tp_price_str}."
            )
            is_zero_pos_error = (
                str(error_code) == '10001' and # Порівняння рядків
                ("zero position" in error_msg_api or 
                 "cannot set tp/sl/ts for zero position" in error_msg_api or
                 "position not exist" in error_msg_api or
                 "position is not an order" in error_msg_api)
            ) or str(error_code) == '30036' # Порівняння рядків

            if is_zero_pos_error:
                self.trade_logger.warning(
                    f"Спроба оновити Trailing SL для {symbol}, але позиція вже нульова/відсутня на біржі."
                )
                await self.telegram.send_error_notification({
                    'type': f'SET_TRAIL_SL_ON_ZERO_POS_{bot_signal_side}',
                    'message': (
                        f"Update Trailing SL for {symbol} failed: position zero/closed. "
                        f"API: {api_response.get('retMsg', '') if api_response else 'N/A'} ({error_code})"
                    ),
                    'symbol': symbol, 
                    'action': 'SET_TRAILING_SL'
                })
                if symbol in self.positions: # Перевірка існування позиції
                    del self.positions[symbol]



    def _format_position_type(self, side: str, action: str) -> str:
        """
        ✅ Форматування типу позиції з відповідними емодзі
        """
        if side == 'BUY':
            if 'CLOSE' in action:
                return "📈 LONG CLOSE"
            elif 'PARTIAL' in action:
                return "📈 LONG PARTIAL"
            else:
                return "📈 LONG"
        else:  # SELL
            if 'CLOSE' in action:
                return "📉 SHORT CLOSE"
            elif 'PARTIAL' in action:
                return "📉 SHORT PARTIAL"  
            else:
                return "📉 SHORT"

    async def _close_position(self, symbol: str, close_price: float, reason: str, quantity_to_close: Optional[float] = None):
        if symbol not in self.positions:
            self.logger.warning(f"Спроба закрити неіснуючу позицію для {symbol} (перевірка на вході в _close_position)")
            return

        position_data = self.positions[symbol]
        bot_signal_side = position_data['side'] 
        entry_price = position_data['entry_price']
        initial_pos_quantity = position_data['initial_quantity'] # Важливо для розрахунку PnL частини
        remaining_quantity_before_close = position_data['quantity']
        
        if remaining_quantity_before_close <= 0.0000001:
            self.logger.info(f"Позиція {symbol} вже має нульову кількість ({remaining_quantity_before_close:.8f}). Пропуск закриття.")
            if symbol in self.positions:
                del self.positions[symbol]
            return

        qty_to_close_float = abs(quantity_to_close) if quantity_to_close is not None else abs(remaining_quantity_before_close)
        qty_to_close_float = min(qty_to_close_float, abs(remaining_quantity_before_close)) 

        if qty_to_close_float <= 0.0000001:
            self.logger.warning(f"Кількість для закриття позиції {symbol} ({qty_to_close_float:.8f}) нульова або від'ємна. Пропуск.")
            return

        close_order_side_api = "Sell" if bot_signal_side == "BUY" else "Buy" 
        self.trade_logger.info(
            f"РЕАЛЬНИЙ: Спроба закриття ~{qty_to_close_float:.8f} {symbol} "
            f"({bot_signal_side} позиція) через {close_order_side_api} ордер. Причина: {reason}"
        )

        qty_to_close_str_for_api = ""
        instrument_info = await self.api_manager.get_instrument_info(symbol)
        final_qty_to_close_for_api_float = qty_to_close_float
        qty_decimals = 8 

        if instrument_info:
            lot_size_filter = instrument_info.get('lotSizeFilter', {})
            qty_step_str = lot_size_filter.get('qtyStep')
            if qty_step_str:
                try:
                    qty_step = float(qty_step_str)
                    if '.' in qty_step_str: 
                        qty_decimals = len(qty_step_str.split('.')[1].rstrip('0'))
                    else: 
                        qty_decimals = 0

                    if qty_step > 0:
                        final_qty_to_close_for_api_float = round(qty_to_close_float / qty_step) * qty_step
                        if final_qty_to_close_for_api_float == 0 and qty_to_close_float > 0:
                            if quantity_to_close is None: 
                                min_order_qty_str = lot_size_filter.get('minOrderQty')
                                if min_order_qty_str:
                                    min_order_qty = float(min_order_qty_str)
                                    if remaining_quantity_before_close >= min_order_qty:
                                        final_qty_to_close_for_api_float = min_order_qty
                                    elif remaining_quantity_before_close >= qty_step:
                                        final_qty_to_close_for_api_float = qty_step
                                elif remaining_quantity_before_close >= qty_step:
                                    final_qty_to_close_for_api_float = qty_step
                            elif qty_to_close_float >= qty_step: 
                                final_qty_to_close_for_api_float = qty_step
                        qty_to_close_str_for_api = f"{final_qty_to_close_for_api_float:.{qty_decimals}f}"
                except ValueError:
                    self.logger.error(f"Не вдалося перетворити qtyStep '{qty_step_str}' на float для {symbol}")
                    qty_to_close_str_for_api = f"{final_qty_to_close_for_api_float:.{qty_decimals}f}".rstrip('0').rstrip('.')
            else: 
                qty_to_close_str_for_api = f"{final_qty_to_close_for_api_float:.{qty_decimals}f}".rstrip('0').rstrip('.')
        else: 
            qty_to_close_str_for_api = f"{final_qty_to_close_for_api_float:.{qty_decimals}f}".rstrip('0').rstrip('.')

        if not qty_to_close_str_for_api or float(qty_to_close_str_for_api) <= 0.0000001:
            self.logger.error(
                f"Фінальна кількість для закриття {symbol} нульова ('{qty_to_close_str_for_api}'). "
                f"Ордер не буде розміщено. Початкова кількість для закриття: {qty_to_close_float:.8f}"
            )
            if "Stop Loss" in reason or "Hit" in reason: 
                if symbol in self.positions:
                    self.trade_logger.warning(f"Нульова кількість для закриття {symbol} по '{reason}'. Видалення локальної позиції.")
                    del self.positions[symbol]
            return

        api_response = await self.api_manager.place_reduce_order(
            symbol=symbol, side=close_order_side_api, qty=qty_to_close_str_for_api, order_type="Market"
        )
        
        actually_closed_qty_for_pnl = 0.0
        try:
            actually_closed_qty_for_pnl = float(qty_to_close_str_for_api)
        except ValueError: 
            actually_closed_qty_for_pnl = qty_to_close_float
            self.logger.error(
                f"Не вдалося перетворити qty_to_close_str_for_api '{qty_to_close_str_for_api}' на float "
                f"для розрахунку PnL {symbol}. Використано qty_to_close_float: {qty_to_close_float}"
            )

        if api_response and api_response.get('retCode') == 0:
            closed_order_id = api_response.get('result', {}).get('orderId')
            self.trade_logger.info(
                f"РЕАЛЬНИЙ: Позиція {symbol} ({bot_signal_side}) успішно закрито {actually_closed_qty_for_pnl:.8f} од. "
                f"на біржі. Order ID: {closed_order_id}. Причина: {reason}"
            )
            
            current_remaining_qty_local = 0.0
            if symbol in self.positions:
                self.positions[symbol]['quantity'] -= actually_closed_qty_for_pnl
                current_remaining_qty_local = self.positions[symbol]['quantity']
            else: 
                self.logger.warning(f"Позиція {symbol} була видалена з self.positions під час виконання _close_position до оновлення кількості.")

            pnl_for_closed_part = self.pnl_calculator.calculate_simple_pnl(
                entry_price=entry_price,
                close_price=close_price, # Ціна, за якою спрацював тригер (SL ціна, TP ціна, ринкова ціна для дивергенції)
                quantity=actually_closed_qty_for_pnl,
                side=bot_signal_side
            )
            
            # PnL відсоток розраховується від вартості закритої частини позиції
            cost_of_closed_part = entry_price * actually_closed_qty_for_pnl
            pnl_percentage_for_part = (pnl_for_closed_part / cost_of_closed_part) * 100 if cost_of_closed_part != 0 else 0

            # ✅ ПОКРАЩЕНО: Більш точне визначення повного закриття з кращою логікою
            is_full_close = False
            
            # Визначаємо чи це повне закриття на основі декількох критеріїв
            if quantity_to_close is None:
                # Якщо не вказана кількість, то це повне закриття
                is_full_close = True
            elif abs(current_remaining_qty_local) <= TRADING_CONFIG.get('min_trade_quantity_threshold', 0.000001):
                # Якщо залишок мізерний, вважаємо повним закриттям
                is_full_close = True
            elif "Повне закриття" in reason or "Full closure" in reason or "Stop Loss" in reason:
                # Якщо в причині вказано повне закриття або SL (завжди повне)
                is_full_close = True
            elif abs(actually_closed_qty_for_pnl - abs(remaining_quantity_before_close)) <= TRADING_CONFIG.get('min_trade_quantity_threshold', 0.000001):
                # Якщо закрили всю наявну кількість
                is_full_close = True
            
            self.trade_logger.debug(
                f"📊 Аналіз закриття {symbol}: quantity_to_close={quantity_to_close}, "
                f"remaining_after={current_remaining_qty_local:.8f}, "
                f"closed_qty={actually_closed_qty_for_pnl:.8f}, "
                f"original_qty={remaining_quantity_before_close:.8f}, "
                f"is_full_close={is_full_close}"
            )
            
            # ✅ ПОКРАЩЕНО: Негайне видалення позиції при повному закритті
            if is_full_close and symbol in self.positions:
                self.trade_logger.info(f"🔄 Негайне видалення позиції {symbol} з пам'яті після повного закриття. Причина: {reason}")
                del self.positions[symbol]
            
            # ✅ ПОКРАЩЕНО: Використовуємо централізоване оновлення статистики тільки для повних закриттів
            if is_full_close:
                trade_data_for_stats = {
                    'action': f'CLOSE_{bot_signal_side}',
                    'symbol': symbol,
                    'price': close_price,
                    'quantity': actually_closed_qty_for_pnl,
                    'entry_price': entry_price,
                    'side': bot_signal_side
                }
                
                stats_updated = await self._update_trade_statistics(
                    symbol=symbol,
                    pnl=pnl_for_closed_part, 
                    trade_data=trade_data_for_stats,
                    reason=f"direct_close_{reason.replace(' ', '_').lower()}"
                )
                
                if not stats_updated:
                    # Fallback до старого методу, якщо централізований не спрацював
                    self.trade_stats['total_pnl'] += pnl_for_closed_part
                    self.trade_stats['total_trades'] += 1
                    if pnl_for_closed_part > 0:
                        self.trade_stats['winning_trades'] += 1
                    else:
                        self.trade_stats['losing_trades'] += 1
                    
                    await self.db_manager.save_trade_stats(self.trade_stats)
                    self.logger.warning(f"📊 Використано fallback статистику для {symbol}")
            else:
                # Для часткових закриттів тільки оновлюємо загальний P&L
                self.trade_stats['total_pnl'] += pnl_for_closed_part
                await self.db_manager.save_trade_stats(self.trade_stats)
                
                self.logger.info(
                    f"📊 P&L для часткового закриття {symbol} ({actually_closed_qty_for_pnl:.4f} од.): {pnl_for_closed_part:+.4f} USDT. "
                    f"Загальний P&L бота: {self.trade_stats['total_pnl']:.4f} USDT"
                )


            action_type_suffix = ""
            if "Stop Loss" in reason: action_type_suffix = "SL_HIT"
            elif "partial_1" in reason: action_type_suffix = "PARTIAL_TP1_HIT"
            elif "partial_2" in reason: action_type_suffix = "PARTIAL_TP2_HIT"
            elif "partial_3" in reason: action_type_suffix = "PARTIAL_TP3_HIT"
            elif "final" in reason: action_type_suffix = "FINAL_TP_HIT"
            elif "Volume Divergence" in reason: action_type_suffix = "VOL_DIV_EXIT"
            else: action_type_suffix = "CLOSE" # Загальне закриття, якщо причина не стандартна

            action_type = f'{action_type_suffix}_{bot_signal_side}' if is_full_close else f'PARTIAL_{action_type_suffix}_{bot_signal_side}'
            if "Hit" not in reason and "Exit" not in reason: # Якщо це не системний вихід, а, наприклад, ручне закриття
                action_type = f'MANUAL_CLOSE_{bot_signal_side}' if is_full_close else f'MANUAL_PARTIAL_CLOSE_{bot_signal_side}'


            trade_notification_data = {
                'action': action_type, 
                'symbol': symbol, 
                'side': bot_signal_side, 
                'price': close_price,
                'entry_price': entry_price,
                'quantity': actually_closed_qty_for_pnl, 
                'reason': reason, 
                'pnl': pnl_for_closed_part, 
                'pnl_percentage': pnl_percentage_for_part,
                'remaining_quantity': max(0, current_remaining_qty_local),
                'exchange_order_id': closed_order_id
            }
            await self.telegram.send_trade_notification(trade_notification_data)
            # ✅ ПОКРАЩЕНО: Ризик-менеджмент оновлюється в централізованому методі
            # self.strategy.update_risk_management(pnl_for_closed_part) - Закоментовано, щоб уникнути подвійного виклику 

            # ✅ ПОКРАЩЕНО: Очищення позицій після повного закриття вже зроблено вище
            if not is_full_close and symbol in self.positions: 
                self.trade_logger.info(f"Позиція {symbol} частково закрита. Залишок: {current_remaining_qty_local:.8f}")
                if abs(current_remaining_qty_local) <= 0.0000001:
                    self.trade_logger.info(f"Залишок позиції {symbol} ({current_remaining_qty_local:.8f}) дуже малий. Видалення.")
                    if symbol in self.positions: 
                        del self.positions[symbol]
        else:
            # ... (існуюча обробка помилок place_reduce_order) ...
            failed_request_params = api_response.get('retExtInfo', {}).get('req', api_response.get('request_params', {})) if api_response else {}
            error_code = api_response.get('retCode') if api_response else 'N/A'
            error_msg_api = api_response.get('retMsg', 'Unknown error or no API response') if api_response else 'No API response'
            error_msg_api_lower = error_msg_api.lower()

            log_message = (
                f"Помилка закриття позиції {symbol} на біржі. "
                f"API Response: {error_msg_api} (Code: {error_code}). "
                f"Причина запиту на закриття: {reason}. Запит: {failed_request_params}"
            )
            self.trade_logger.error(log_message)
            
            is_zero_pos_error = False
            if (str(error_code) in ['110017', '110025', '30036'] or 
                "position is zero" in error_msg_api_lower or
                "zero position" in error_msg_api_lower or
                "no position" in error_msg_api_lower or
                "position not exist" in error_msg_api_lower or
                (str(error_code) == '10001' and "position" in error_msg_api_lower and 
                ("zero" in error_msg_api_lower or "not exist" in error_msg_api_lower))):
                is_zero_pos_error = True

            if is_zero_pos_error:
                self.trade_logger.warning(
                    f"Спроба закрити позицію {symbol}, яка вже нульова/відсутня на біржі "
                    f"(причина запиту на закриття: {reason})"
                )
                pnl_for_already_closed_val = "N/A (already closed)"
                if entry_price and actually_closed_qty_for_pnl > 0:
                    pnl_for_already_closed_val = self.pnl_calculator.calculate_simple_pnl(
                        entry_price=entry_price, close_price=close_price,
                        quantity=actually_closed_qty_for_pnl, side=bot_signal_side
                    )

                await self.telegram.send_trade_notification({
                    'action': f'ALREADY_CLOSED_{bot_signal_side}', 
                    'symbol': symbol, 'side': bot_signal_side, 'price': close_price,
                    'entry_price': entry_price, 'quantity': qty_to_close_float,
                    'reason': f"{reason} (attempt on zero/closed pos)", 
                    'pnl': pnl_for_already_closed_val, 'remaining_quantity': 0, 
                    'details': f"API Err: {error_msg_api} ({error_code})"
                })
                if symbol in self.positions:
                    self.positions[symbol]['quantity'] = 0 
                    del self.positions[symbol] 
                return 
            
            await self.telegram.send_error_notification({
                'type': 'EXCHANGE_CLOSE_ORDER_FAILED', 'message': log_message, 
                'symbol': symbol, 'action': f'CLOSE_{bot_signal_side}', 
                'api_response': str(api_response)
            })

    async def pre_cycle_position_validation(self):
        """
        Система перевірки позицій перед початком нового торгового циклу.
        Виконується за 5-7 секунд до початку циклу для валідації всіх локальних позицій.
        """
        try:
            pre_cycle_config = STRATEGY_CONFIG.get('pre_cycle_validation', {})
            if not pre_cycle_config.get('enable', True):
                self.logger.debug("Pre-cycle validation вимкнено в конфігурації")
                return
                
            self.logger.info("🔄 Розпочинання pre-cycle position validation...")
            validation_start_time = datetime.now(timezone.utc)
            
            if not self.positions:
                self.logger.info("✅ Pre-cycle validation: немає локальних позицій для валідації")
                return
                
            positions_count = len(self.positions)
            self.logger.info(f"🔍 Валідація {positions_count} локальних позицій перед новим циклом")
            
            # Отримуємо актуальні позиції з біржі
            exchange_positions = await self.api_manager.get_positions()
            if exchange_positions is None:
                self.logger.warning("⚠️  Не вдалося отримати позиції з біржі для pre-cycle validation")
                return
                
            # Створюємо мапу активних символів на біржі
            active_symbols_on_exchange = {}
            for pos in exchange_positions:
                pos_size = float(pos.get('size', 0))
                pos_symbol = pos.get('symbol', '')
                threshold = TRADING_CONFIG.get('min_trade_quantity_threshold', 0.000001)
                
                if pos_size > threshold:
                    active_symbols_on_exchange[pos_symbol] = {
                        'side': pos.get('side', 'None'),
                        'size': pos_size,
                        'avg_price': float(pos.get('avgPrice', 0)),
                        'unrealized_pnl': float(pos.get('unrealisedPnl', 0))
                    }
                    
            # Валідуємо кожну локальну позицію
            validated_positions = 0
            cleaned_positions = 0
            positions_to_remove = []
            
            for symbol in list(self.positions.keys()):
                if symbol in active_symbols_on_exchange:
                    # Позиція підтверджена на біржі
                    exchange_pos = active_symbols_on_exchange[symbol]
                    local_pos = self.positions[symbol]
                    
                    # Синхронізуємо кількість якщо є розбіжності
                    local_qty = local_pos.get('remaining_quantity', 0)
                    exchange_qty = exchange_pos['size']
                    
                    if abs(local_qty - exchange_qty) > TRADING_CONFIG.get('sync_tolerance_qty', 0.0000001):
                        self.logger.info(f"🔄 Синхронізація кількості для {symbol}: {local_qty:.8f} → {exchange_qty:.8f}")
                        self.positions[symbol]['remaining_quantity'] = exchange_qty
                        
                    validated_positions += 1
                    self.logger.debug(f"✅ {symbol}: валідовано {exchange_pos['side']} {exchange_qty:.8f}")
                else:
                    # Позиція відсутня на біржі - потребує очищення
                    if pre_cycle_config.get('cleanup_invalid_positions', True):
                        self.logger.warning(f"🧹 {symbol}: локальна позиція відсутня на біржі, планується очищення")
                        positions_to_remove.append(symbol)
                    else:
                        self.logger.warning(f"⚠️  {symbol}: локальна позиція відсутня на біржі, але очищення вимкнено")
                        
            # Очищуємо неактивні позиції
            for symbol in positions_to_remove:
                self.logger.info(f"🗑️  Видалення неактивної позиції: {symbol}")
                del self.positions[symbol]
                cleaned_positions += 1
                
            # Логування результатів валідації
            validation_duration = (datetime.now(timezone.utc) - validation_start_time).total_seconds()
            
            self.logger.info(
                f"✅ Pre-cycle validation завершено за {validation_duration:.2f}с: "
                f"валідовано {validated_positions}, очищено {cleaned_positions} позицій"
            )
            
            if cleaned_positions > 0:
                await self.telegram.send_notification(
                    f"🧹 Pre-cycle очищення: видалено {cleaned_positions} неактивних позицій з {positions_count}",
                    message_type='pre_cycle_cleanup'
                )
                
        except Exception as e:
            self.logger.error(f"Помилка в pre-cycle position validation: {e}", exc_info=True)
            await self.telegram.send_error_notification({
                'type': 'PRE_CYCLE_VALIDATION_ERROR', 
                'message': str(e)
            })
    
    async def quick_check_active_positions(self):
        """
        Покращена швидка перевірка стану активних позицій з відновленням
        
        Покращення:
        - Кращий error handling
        - Механізм відновлення позицій
        - Детальне логування життєвого циклу позицій
        - Обробка demo mode синхронізації
        """
        try:
            if not self.positions:
                self.logger.debug("🔍 Швидка перевірка: немає активних локальних позицій")
                return
            
            positions_count = len(self.positions)
            self.logger.info(f"🔍 Швидка перевірка {positions_count} активних позицій")
            
            # Отримуємо поточні позиції з біржі з retry логікою
            exchange_positions = None
            retry_count = 0
            max_retries = 3
            
            while retry_count < max_retries:
                try:
                    exchange_positions = await self.api_manager.get_positions()
                    break
                except Exception as e_get_pos:
                    retry_count += 1
                    self.logger.warning(f"❌ Спроба {retry_count}/{max_retries} отримати позиції не вдалася: {e_get_pos}")
                    if retry_count < max_retries:
                        await asyncio.sleep(1.0 * retry_count)  # Exponential backoff
                    else:
                        self.logger.error(f"💥 Всі спроби отримати позиції з біржі не вдалися після {max_retries} спроб")
                        return
            
            if exchange_positions is None:
                self.logger.error("💥 Не вдалося отримати позиції з біржі - пропускаємо швидку перевірку")
                return
                
            # Аналіз позицій на біржі
            active_symbols_on_exchange = {}
            total_exchange_positions = len(exchange_positions) if exchange_positions else 0
            self.logger.debug(f"🔍 Отримано {total_exchange_positions} позицій з біржі")
            
            for pos in exchange_positions:
                pos_size = float(pos.get('size', 0))
                pos_symbol = pos.get('symbol', '')
                pos_side = pos.get('side', 'None')
                pos_avg_price = float(pos.get('avgPrice', 0))
                pos_pnl = float(pos.get('unrealisedPnl', 0))
                
                threshold = TRADING_CONFIG.get('min_trade_quantity_threshold', 0.000001)
                if pos_size > threshold:
                    active_symbols_on_exchange[pos_symbol] = {
                        'side': pos_side,
                        'size': pos_size,
                        'avg_price': pos_avg_price,
                        'unrealized_pnl': pos_pnl,
                        'raw_position': pos
                    }
                    self.logger.debug(f"✅ Біржова позиція: {pos_symbol} {pos_side} {pos_size:.8f} @ {pos_avg_price:.6f} (PnL: {pos_pnl:.4f})")
            
            self.logger.debug(f"🏦 Активні символи на біржі: {list(active_symbols_on_exchange.keys())}")
            self.logger.debug(f"🏠 Локальні позиції: {list(self.positions.keys())}")
            
            # Перевіряємо кожну локальну позицію
            positions_to_check = list(self.positions.keys())
            missing_positions = []
            reconciled_positions = []
            
            for symbol in positions_to_check:
                try:
                    local_pos = self.positions.get(symbol, {})
                    local_qty = local_pos.get('quantity', 0)
                    local_side = local_pos.get('side', 'Unknown')
                    
                    if symbol not in active_symbols_on_exchange:
                        # Позиція відсутня на біржі - аналізуємо закриття
                        missing_positions.append(symbol)
                        
                        # ✅ ДОДАНО: Перевірка чи було вже оброблено це закриття
                        closure_check_data = {
                            'exit_price': 0,  # Невідома ціна виходу
                            'quantity': local_qty,
                            'closure_type': 'missing_on_exchange'
                        }
                        
                        if self._is_closure_already_processed(symbol, closure_check_data):
                            self.logger.debug(f"Пропускаємо аналіз закриття для {symbol} - вже оброблено")
                            continue
                        
                        # Спробуємо проаналізувати закриття позиції замість загального попередження
                        closure_analyzed = await self.analyze_and_notify_position_closure(symbol, local_pos)
                        
                        if not closure_analyzed:
                            # Якщо аналіз не вдався, показуємо оригінальне попередження з деталями
                            self.logger.warning(
                                f"⚠️ ШВИДКА ПЕРЕВІРКА: {symbol} відсутня на біржі "
                                f"(локальна {local_side} позиція: {local_qty:.8f})"
                            )
                            
                            # Додаємо інформацію про можливі причини зникнення
                            last_sl = local_pos.get('stop_loss', 0)
                            last_tp = local_pos.get('take_profits', [])
                            entry_price = local_pos.get('entry_price', 0)
                            
                            self.logger.info(
                                f"📊 {symbol} позиція деталі: entry={entry_price:.6f}, "
                                f"SL={last_sl:.6f}, TP={len(last_tp)} рівнів"
                            )
                        
                        # Спробуємо відновити позицію через детальну синхронізацію
                        await self.sync_single_position_with_history(symbol)
                        
                    else:
                        # Позиція знайдена - перевіряємо узгодженість
                        exchange_pos = active_symbols_on_exchange[symbol]
                        exchange_qty = exchange_pos['size']
                        exchange_side = exchange_pos['side']
                        
                        # Перевірка розміру позиції
                        qty_diff = abs(local_qty - exchange_qty)
                        qty_tolerance = TRADING_CONFIG.get('sync_tolerance_qty', 0.0000001)
                        
                        if qty_diff > qty_tolerance:
                            self.logger.warning(
                                f"⚠️ Розбіжність розміру {symbol}: локально={local_qty:.8f}, "
                                f"біржа={exchange_qty:.8f}, різниця={qty_diff:.8f}"
                            )
                            reconciled_positions.append(symbol)
                            # Оновлюємо локальну позицію
                            self.positions[symbol].update({
                                'quantity': exchange_qty,
                                'last_reconcile_time': datetime.now(timezone.utc).isoformat(),
                                'reconcile_reason': 'quick_check_qty_mismatch'
                            })
                        
                        # Перевірка сторони позиції
                        if local_side.upper() != exchange_side.upper():
                            self.logger.warning(
                                f"⚠️ Розбіжність сторони {symbol}: локально={local_side}, "
                                f"біржа={exchange_side}"
                            )
                            reconciled_positions.append(symbol)
                            # Оновлюємо сторону
                            self.positions[symbol]['side'] = exchange_side
                        
                        self.logger.debug(f"✅ {symbol} узгоджено: {exchange_side} {exchange_qty:.8f}")
                        
                except Exception as e_symbol:
                    self.logger.error(f"❌ Помилка перевірки {symbol}: {e_symbol}")
                    continue
            
            # Підсумковий звіт
            if missing_positions:
                self.logger.warning(
                    f"📊 Швидка перевірка: {len(missing_positions)} позицій відсутні на біржі: {missing_positions}"
                )
                # Відправляємо сповіщення про зниклі позиції
                if hasattr(self, 'telegram_notifier') and self.telegram_notifier:
                    await self.telegram_notifier.send_notification(
                        f"⚠️ Позиції зникли з біржі: {', '.join(missing_positions)}\n"
                        f"Виконую синхронізацію для відновлення...",
                        message_type='position_tracking'
                    )
            
            if reconciled_positions:
                self.logger.info(
                    f"🔧 Швидка перевірка: {len(reconciled_positions)} позицій узгоджено: {reconciled_positions}"
                )
            
            if not missing_positions and not reconciled_positions:
                self.logger.debug(
                    f"✅ Швидка перевірка: всі {len(positions_to_check)} локальні позиції узгоджені з біржею"
                )
            
        except Exception as e:
            self.logger.error(f"💥 Критична помилка швидкої перевірки: {e}", exc_info=True)
            # Спробуємо повну синхронізацію як fallback
            if hasattr(self, 'positions') and self.positions:
                self.logger.info("🔄 Запускаємо повну синхронізацію як fallback після помилки швидкої перевірки")
                try:
                    await self.sync_positions_with_execution_history()
                except Exception as e_fallback:
                    self.logger.error(f"💥 Fallback синхронізація також не вдалася: {e_fallback}")

    async def sync_positions_with_execution_history(self):
        """
        Покращена синхронізація локальних позицій з історією виконання угод на біржі.
        Включає автоматичне очищення позицій, які відсутні на біржі.
        """
        if not TRADING_CONFIG.get('position_sync_enabled', True):
            self.logger.debug("🔕 Синхронізація позицій вимкнена в конфігурації")
            return
        
        try:
            debug_mode = TRADING_CONFIG.get('sync_debug_mode', False)
            
            if debug_mode:
                self.logger.info("🔄 DEBUG: Початок детальної синхронізації позицій з історією виконання (sync_positions_with_execution_history)")
            
            sync_results = {}
            symbols_to_sync = list(self.positions.keys()) # Копіюємо для безпечної ітерації
            
            if not symbols_to_sync:
                if debug_mode:
                    self.logger.info("📭 DEBUG: Немає локальних позицій для періодичної синхронізації")
                return
            
            self.logger.info(f"🔄 Періодична синхронізація {len(symbols_to_sync)} позицій: {symbols_to_sync}")
            
            # Додана швидка валідація позицій на біржі
            positions_to_cleanup = []
            active_exchange_positions = {}
            
            try:
                exchange_positions_raw = await self.api_manager.get_positions()
                active_exchange_positions = {
                    pos.get('symbol'): {
                        'side': pos.get('side'),
                        'size': float(pos.get('size', 0)),
                        'avg_price': float(pos.get('avgPrice', 0)),
                        'unrealized_pnl': float(pos.get('unrealisedPnl', 0))
                    }
                    for pos in exchange_positions_raw 
                    if float(pos.get('size', 0)) > TRADING_CONFIG.get('min_trade_quantity_threshold', 0.000001)
                }
                if debug_mode:
                    self.logger.info(f"🏦 DEBUG (sync_positions): Активні позиції на біржі: {list(active_exchange_positions.keys())}")
                
                # Виявляємо позиції, які є локально, але відсутні на біржі
                for symbol in symbols_to_sync:
                    if symbol not in active_exchange_positions:
                        # Додаткова перевірка через валідацію
                        exists_on_exchange, exchange_data, status = await self.api_manager.validate_position_exists_on_exchange(symbol)
                        if not exists_on_exchange and status == "position_not_found_on_exchange":
                            positions_to_cleanup.append(symbol)
                            self.logger.warning(f"🧹 Позиція {symbol} відсутня на біржі, планується очищення")
                
            except Exception as e_get_pos:
                self.logger.error(f"❌ Помилка отримання позицій з біржі під час періодичної синхронізації: {e_get_pos}")

            # Автоматичне очищення позицій, які відсутні на біржі
            if positions_to_cleanup:
                await self._cleanup_missing_positions(positions_to_cleanup)

            for symbol in symbols_to_sync:
                if symbol not in self.positions: # Якщо символ був видалений іншим процесом або очищенням
                    sync_results[symbol] = {'is_synced': True, 'status': 'Symbol removed locally during sync iteration', 'action_taken': 'skipped_removed_locally', 'timestamp': datetime.now(timezone.utc).isoformat()}
                    continue
                
                local_pos_before_sync = self.positions[symbol].copy() # Копія для порівняння
                
                if debug_mode:
                    self.logger.info(f"🔍 DEBUG (sync_positions): Синхронізація {symbol}...")

                # Викликаємо sync_single_position_with_history, який вже містить логіку reconcile
                await self.sync_single_position_with_history(symbol)

                # Аналізуємо результат після виклику sync_single_position_with_history
                if symbol not in self.positions:
                    # Позиція була закрита та видалена sync_single_position_with_history
                    sync_results[symbol] = {
                        'is_synced': True, # Вважаємо синхронізованим, оскільки стан оброблено (закрито)
                        'status': f"Position for {symbol} was closed and removed by detailed sync.",
                        'action_taken': 'position_removed_by_detailed_sync_periodic',
                        'exchange_status': active_exchange_positions.get(symbol, "not_found_on_exchange_after_sync"),
                        'timestamp': datetime.now(timezone.utc).isoformat()
                    }
                    self.logger.info(f"✅ {symbol} оброблено (закрито) під час періодичної синхронізації.")
                else:
                    # Позиція все ще існує, перевіряємо, чи були зміни
                    current_local_pos = self.positions[symbol]
                    action_desc = "no_significant_changes_by_detailed_sync_periodic"
                    status_desc = f"Position {symbol} remains active after detailed sync."
                    
                    if current_local_pos.get('sync_source', '').startswith('reconcile_update'):
                        action_desc = "position_updated_by_detailed_sync_periodic"
                        status_desc = f"Position {symbol} data updated by detailed sync. Report: {current_local_pos.get('sync_report', {}).get('status', 'N/A')}"
                    elif current_local_pos.get('sync_source', '').startswith('reconcile_confirmed'):
                         action_desc = "position_confirmed_by_detailed_sync_periodic"
                         status_desc = f"Position {symbol} confirmed as synchronized by detailed sync."
                    
                    sync_results[symbol] = {
                        'is_synced': True, # Якщо sync_single_position_with_history не видалив, значить вона або оновлена, або підтверджена
                        'status': status_desc,
                        'action_taken': action_desc,
                        'exchange_status': active_exchange_positions.get(symbol, "not_found_on_exchange_after_sync"), # Стан на біржі до sync_single
                        'final_local_qty': current_local_pos.get('quantity'),
                        'timestamp': datetime.now(timezone.utc).isoformat()
                    }
                    self.logger.info(f"✅ {symbol} залишається активною після періодичної синхронізації. Дія: {action_desc}")

                await asyncio.sleep(0.1) # Затримка між символами
            
            total_processed = len(sync_results)
            actions_summary = {k: v['action_taken'] for k, v in sync_results.items()}
            self.logger.info(f"✅ Періодична синхронізація завершена: оброблено {total_processed} символів.")
            if actions_summary:
                self.logger.info(f"🔧 Дії під час періодичної синхронізації: {actions_summary}")
            
            self.last_sync_results = { # Оновлюємо загальний результат
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'total_positions_at_start': len(symbols_to_sync),
                'processed_symbols_count': total_processed,
                'results_summary': actions_summary,
                'detailed_results': sync_results # Для детального аналізу, якщо потрібно
            }
            
        except Exception as e:
            error_msg = f"💥 Критична помилка періодичної синхронізації позицій: {str(e)}"
            self.logger.error(error_msg, exc_info=True)
            await self.telegram.send_error_notification({
                'type': 'PERIODIC_SYNC_CRITICAL_ERROR',
                'message': error_msg
            })
    
    def _calculate_symbol_specific_buffer(self, symbol: str, tick_size: float, market_price: float) -> float:
        """Calculate symbol-specific buffer for SL distance from market price"""
        try:
            # Base multiplier from config
            base_multiplier = TRADING_CONFIG.get('min_sl_market_distance_tick_multiplier', 5)
            
            # Symbol-specific adjustments based on price ranges and volatility
            if market_price > 1000:  # High-value symbols like BTC
                multiplier = max(2, base_multiplier * 0.5)  # Reduce buffer for high-value symbols
            elif market_price > 100:  # Medium-value symbols like ETH
                multiplier = max(3, base_multiplier * 0.7)  
            elif market_price > 1:   # Standard symbols
                multiplier = base_multiplier
            else:  # Low-value symbols like CHZUSDT, DOGEUSDT
                multiplier = max(base_multiplier, 8)  # Increase buffer for very low-value symbols
            
            # Additional adjustment for very small tick sizes (sub-penny)
            if tick_size < 0.0001:  # Very small tick sizes
                multiplier = max(multiplier, 10)
            elif tick_size < 0.001:  # Small tick sizes
                multiplier = max(multiplier, 8)
            
            # Calculate the buffer
            buffer = tick_size * multiplier
            
            # Ensure minimum buffer relative to market price (0.01% minimum)
            min_buffer_percent = market_price * 0.0001  # 0.01%
            buffer = max(buffer, min_buffer_percent)
            
            self.logger.debug(f"Buffer calculation for {symbol}: tick={tick_size}, price={market_price:.6f}, multiplier={multiplier}, buffer={buffer:.6f}")
            
            return buffer
            
        except Exception as e:
            self.logger.error(f"Error calculating buffer for {symbol}: {e}")
            # Fallback to original calculation
            return tick_size * TRADING_CONFIG.get('min_sl_market_distance_tick_multiplier', 5)

    async def _get_current_market_price(self, symbol: str) -> float:
        """Отримує поточну ринкову ціну для символа"""
        try:
            ticker_data = await self.api_manager.get_tickers(category="linear", symbol=symbol)
            if ticker_data and ticker_data.get('retCode') == 0 and ticker_data['result']['list']:
                market_price_str = ticker_data['result']['list'][0].get('lastPrice')
                if market_price_str:
                    return float(market_price_str)
            return 0.0
        except Exception as e:
            self.logger.error(f"Помилка отримання ринкової ціни для {symbol}: {e}")
            return 0.0

    async def analyze_and_trade(self, symbol: str, execute_trade: bool = True) -> Dict[str, Any]:
        try:
            if symbol in self.positions and self.positions[symbol].get('quantity', 0) > 0:
                self.logger.debug(f"Активна позиція вже існує для {symbol}, пропускаємо аналіз на вхід.")
                return {'symbol': symbol, 'action': 'POSITION_ALREADY_OPEN', 'position_data': self.positions[symbol]}

            df = await self.db_manager.get_candles_for_analysis(
                symbol=symbol,
                timeframe=TRADING_CONFIG['timeframe'],
                limit=max(
                    200, 
                    INDICATORS_CONFIG.get('slow_ma', 21) + 
                    STRATEGY_CONFIG.get('regime_period', 20) + 
                    STRATEGY_CONFIG.get('momentum_period', 10) + 50
                ) 
            )
            
            if df.empty or len(df) < 50: # Мінімальна кількість свічок для аналізу
                self.logger.warning(f"Відсутні або недостатньо даних для аналізу {symbol} ({len(df)} свічок)")
                return {'symbol': symbol, 'action': 'NO_DATA'}
            
            latest_candle_validation_dict = df.iloc[-1].to_dict() # Перетворення на словник
            if not await self.validate_candle_data(symbol, latest_candle_validation_dict):
                self.logger.warning(f"Аномальні дані останньої свічки для {symbol}")
                return {'symbol': symbol, 'action': 'INVALID_DATA'}
            
            signal_data = self.strategy.analyze_signals(symbol, df)
            
            log_reason = signal_data.get('reason', '')
            if "Підтвердження" in log_reason and len(log_reason) > 100:
                log_reason = (
                    log_reason.split('Фінальні підтвердження:')[0] + "..." 
                    if 'Фінальні підтвердження:' in log_reason 
                    else log_reason[:100] + "..."
                )

            confidence_value = signal_data.get(
                'confidence', 
                signal_data.get('long_confirmations_count', 0) if signal_data.get('signal') == 'BUY' 
                else signal_data.get('short_confirmations_count', 0)
            )

            self.logger.info(
                f"Аналіз {symbol}: {signal_data['signal']} "
                f"(Підтв: {confidence_value}, "
                f"ADX: {signal_data.get('adx_value', 0.0):.1f} vs {signal_data.get('adx_threshold', 0.0):.1f}) "
                f"Причина: {log_reason}"
            )
            
            time_filter_reason_config = TRADING_CONFIG.get('time_filter_settings', {}).get('time_filter_reason', 'Відфільтровано за часом')
            if signal_data.get('reason') == time_filter_reason_config and signal_data['signal'] in ['BUY', 'SELL']:
                self.logger.info(f"Сигнал для {symbol} ({signal_data['signal']}) відфільтровано за часом. Не відкриваємо позицію.")
                return {'symbol': symbol, 'action': 'HOLD_TIME_FILTERED', 'signal_data': signal_data}

            # 🆕 ПОКРАЩЕНА ЛОГІКА: Telegram notifications тільки для відфільтрованих сигналів
            telegram_config = STRATEGY_CONFIG.get('telegram_filtering', {})
            if not telegram_config.get('notify_only_selected_signals', True):
                # Стара система - відправляємо всі сигнали (для сумісності)
                if signal_data['signal'] not in ['HOLD', 'ERROR_ANALYSIS'] and signal_data.get('entry_price', 0) > 0:
                    await self.telegram.send_signal_notification(signal_data)
            
            if signal_data['signal'] in ['BUY', 'SELL']:
                # Якщо execute_trade=False, повертаємо сигнал для буферизації
                if not execute_trade:
                    return {'symbol': symbol, 'action': 'SIGNAL_BUFFERED', 'signal_data': signal_data}
                
                # Не виконуємо торгівлю відразу, якщо включена пріоритизація сигналів
                if (STRATEGY_CONFIG.get('enable_signal_prioritization', True) and 
                    len(self.positions) >= TRADING_CONFIG.get('max_orders_qty', 3)):
                    self.logger.info(
                        f"Сигнал {symbol} ({signal_data['signal']}) додано до черги пріоритизації. "
                        f"Активних позицій: {len(self.positions)}/{TRADING_CONFIG.get('max_orders_qty', 3)}"
                    )
                    # Повертаємо сигнал для подальшої обробки
                    return {'symbol': symbol, 'action': 'PENDING_PRIORITIZATION', 'signal_data': signal_data}
                elif len(self.positions) >= TRADING_CONFIG.get('max_orders_qty', 3):
                    # Старий підхід без пріоритизації
                    self.logger.warning(
                        f"Досягнуто максимальну кількість активних позицій ({len(self.positions)}). "
                        f"Новий ордер для {symbol} не буде розміщено."
                    )
                    return {'symbol': symbol, 'action': 'MAX_ORDERS_REACHED', 'signal_data': signal_data}
                
                # Виконуємо торгівлю відразу, якщо є вільні слоти
                trade_result = await self.execute_trade(symbol, signal_data)
                return trade_result
            
            return {'symbol': symbol, 'action': 'HOLD', 'signal_data': signal_data}
            
        except Exception as e:
            self.logger.error(f"Помилка аналізу та торгівлі для {symbol}: {e}", exc_info=True)
            await self.telegram.send_error_notification({'type': 'ANALYSIS_ERROR', 'message': str(e), 'symbol': symbol})
            return {'symbol': symbol, 'action': 'ERROR', 'error': str(e)}
            
    async def run_trading_cycle(self):
        try:
            self.logger.info("-" * 30 + " Початок торгового циклу " + "-" * 30)
            current_time_utc = datetime.now(timezone.utc)
            usdt_balance = await self.api_manager.get_usdt_balance() 
            self.logger.info(f"Поточний баланс USDT: {usdt_balance:.2f}")

            if self.positions:
                self.logger.info(f"Управління активними позиціями: {list(self.positions.keys())}")
                active_symbols = list(self.positions.keys()) # Копіюємо ключі для безпечної ітерації
                for symbol in active_symbols:
                    if symbol not in self.positions: # Перевіряємо, чи позиція все ще існує
                        continue
                    
                    # Отримуємо дані свічок для управління позицією
                    df_manage = await self.db_manager.get_candles_for_analysis(
                        symbol=symbol, 
                        timeframe=TRADING_CONFIG['timeframe'], 
                        # Збільшуємо ліміт для надійності розрахунку індикаторів
                        limit=max(100, INDICATORS_CONFIG.get('atr_length', 14) + INDICATORS_CONFIG.get('volume_divergence_period', 20) + 5) 
                    )
                    if not df_manage.empty:
                        latest_candle_for_manage = df_manage.iloc[-1].to_dict() # Перетворюємо на словник
                        
                        # Забезпечуємо наявність ATR та даних дивергенції
                        if 'atr' not in latest_candle_for_manage or pd.isna(latest_candle_for_manage['atr']):
                            # Якщо ATR відсутній, використовуємо початковий ATR або дефолтне значення
                            latest_candle_for_manage['atr'] = self.positions[symbol].get('initial_atr_at_entry', 0.00001)
                        
                        if 'bullish_vol_divergence' not in latest_candle_for_manage:
                            latest_candle_for_manage['bullish_vol_divergence'] = df_manage.iloc[-1].get('bullish_vol_divergence', False) # False за замовчуванням
                        if 'bearish_vol_divergence' not in latest_candle_for_manage:
                            latest_candle_for_manage['bearish_vol_divergence'] = df_manage.iloc[-1].get('bearish_vol_divergence', False) # False за замовчуванням
                        
                        # Передаємо актуальний стан позиції з self.positions
                        await self.manage_active_position(symbol, self.positions[symbol], latest_candle_for_manage)
                    else:
                        self.logger.warning(f"Не вдалося отримати дані для управління позицією {symbol}")
                    await asyncio.sleep(TRADING_CONFIG.get('delay_between_symbols_ms', 200) / 1000.0)

            # 🆕 ПОКРАЩЕНА СИСТЕМА БУФЕРУ СИГНАЛІВ
            self.logger.info("Аналіз ринку для нових угод...")
            signal_buffer_config = STRATEGY_CONFIG.get('signal_buffer', {})
            
            if signal_buffer_config.get('enable', True) and signal_buffer_config.get('collect_all_signals_first', True):
                # Збираємо ВСІ сигнали спочатку в буфер
                all_signals_buffer = []
                analysis_results = {}
                
                self.logger.info("📊 Збирання всіх сигналів в буфер...")
                signals_collected = 0
                max_buffer_size = signal_buffer_config.get('max_buffer_size', 20)
                
                for symbol in TRADING_CONFIG['trade_pairs']:
                    if symbol not in self.positions: # Аналізуємо тільки якщо немає активної позиції по символу
                        result = await self.analyze_and_trade(symbol, execute_trade=False)  # Не виконуємо торгівлю одразу
                        analysis_results[symbol] = result
                        
                        # Збираємо сигнали в буфер
                        if (result and result.get('signal_data', {}).get('signal') in ['BUY', 'SELL'] and
                            result.get('action') not in ['MAX_ORDERS_REACHED', 'ERROR']):
                            signal_data = result.get('signal_data', {})
                            signal_data['symbol'] = symbol
                            signal_data['analysis_result'] = result
                            all_signals_buffer.append(signal_data)
                            signals_collected += 1
                            
                            if signals_collected >= max_buffer_size:
                                self.logger.info(f"🛑 Досягнуто максимум буфера сигналів: {max_buffer_size}")
                                break
                        
                        await asyncio.sleep(TRADING_CONFIG.get('delay_between_symbols_ms', 500) / 1000.0)
                    else:
                        self.logger.debug(f"Пропускаємо аналіз на вхід для {symbol}, є активна позиція.")
                
                self.logger.info(f"📈 Зібрано {len(all_signals_buffer)} сигналів в буфер")
                
                # Обробляємо сигнали з буфера з розумною фільтрацією
                if all_signals_buffer:
                    processed_signals = await self._process_signals_with_buffer_prioritization(all_signals_buffer)
                    self.logger.info(f"✅ Оброблено {len(processed_signals)} відфільтрованих сигналів з буфера")
                else:
                    self.logger.info("📭 Буфер сигналів порожній - немає сигналів для торгівлі")
            else:
                # Стара система - збираємо та обробляємо одночасно (для сумісності)
                analysis_results = {}
                all_signals = []  # Збираємо всі сигнали для пріоритизації
                
                for symbol in TRADING_CONFIG['trade_pairs']:
                    if symbol not in self.positions: # Аналізуємо тільки якщо немає активної позиції по символу
                        result = await self.analyze_and_trade(symbol)
                        analysis_results[symbol] = result
                        
                        # Збираємо сигнали для пріоритизації
                        if (result and result.get('signal_data', {}).get('signal') in ['BUY', 'SELL'] and
                            result.get('action') not in ['MAX_ORDERS_REACHED']):
                            signal_data = result.get('signal_data', {})
                            signal_data['symbol'] = symbol
                            signal_data['analysis_result'] = result
                            all_signals.append(signal_data)
                        
                        await asyncio.sleep(TRADING_CONFIG.get('delay_between_symbols_ms', 500) / 1000.0)
                    else:
                        self.logger.debug(f"Пропускаємо аналіз на вхід для {symbol}, є активна позиція.")
                
                # Обробляємо сигнали з пріоритизацією (стара система)
                if all_signals:
                    processed_signals = await self._process_signals_with_prioritization(all_signals)
                    self.logger.info(f"Оброблено сигналів з пріоритизацією: {len(processed_signals)}")
                
            self.logger.info("-" * 30 + " Завершення торгового циклу " + "-" * 30 + "\n")
        except Exception as e:
            self.logger.error(f"Помилка в циклі торгівлі: {e}", exc_info=True)
            await self.telegram.send_error_notification({'type': 'TRADING_CYCLE_ERROR', 'message': str(e)})
    
    async def run(self):
        try:
            init_success = await self.initialize()
            if not init_success:
                self.logger.error("Ініціалізація не вдалася, зупинка бота")
                return
            
            self.is_running = True
            
            # Створюємо задачі з proper task management
            update_task = self._create_task(
                self.preprocessor.start_continuous_update(),
                name="data_preprocessor",
                group="data_processing"
            )
            
            self.logger.info("Торговий бот запущено з оптимізованим task management!")
            await self.telegram.send_bot_status("RUNNING")
            
            # Ініціалізація часових міток
            last_balance_report_time = datetime.now(timezone.utc) - timedelta(
                minutes=TRADING_CONFIG.get('balance_report_interval_minutes', 15) + 1
            )
            last_quick_check_time = datetime.now(timezone.utc)
            last_sync_time = datetime.now(timezone.utc) - timedelta(
                minutes=TRADING_CONFIG.get('sync_check_interval_minutes', 2) + 1
            )

            # Початкові значення змінних
            usdt_balance_val = 0.0
            num_open_positions = 0

            # Основний цикл з покращеним error handling
            while self.is_running:
                try:
                    current_time = datetime.now(timezone.utc)
                    
                    # 🆕 ШВИДКА ПЕРЕВІРКА КОЖНІ 30 СЕКУНД
                    if ((current_time - last_quick_check_time).total_seconds() >= 
                        TRADING_CONFIG.get('position_check_interval_seconds', 30)):
                        
                        if self.positions:  # Тільки якщо є активні позиції
                            await self.quick_check_active_positions()
                        
                        last_quick_check_time = current_time

                    # 🆕 СИНХРОНІЗАЦІЯ З ІСТОРІЄЮ КОЖНІ 2 ХВИЛИНИ
                    if ((current_time - last_sync_time).total_seconds() >= 
                        TRADING_CONFIG.get('sync_check_interval_minutes', 2) * 60):
                        
                        if self.positions:  # Якщо є активні позиції
                            self.logger.info(f"🔄 Початок періодичної синхронізації позицій з історією виконання. Активних позицій: {len(self.positions)}")
                            try:
                                await self.sync_positions_with_execution_history()
                                self.logger.info("✅ Періодична синхронізація позицій завершена успішно")
                            except Exception as e_sync:
                                self.logger.error(f"❌ Помилка під час періодичної синхронізації позицій: {e_sync}", exc_info=True)
                                await self.telegram.send_error_notification({
                                    'type': 'PERIODIC_SYNC_ERROR',
                                    'message': f"Помилка періодичної синхронізації позицій: {e_sync}"
                                })
                        else:
                            self.logger.debug("🔄 Періодична синхронізація пропущена: немає активних позицій")
                        
                        last_sync_time = current_time

                    # 🆕 ЗВІТ БАЛАНСУ КОЖНІ 15 ХВИЛИН З ВАЛІДАЦІЄЮ СТАТИСТИКИ
                    if ((current_time - last_balance_report_time).total_seconds() >= 
                        TRADING_CONFIG.get('balance_report_interval_minutes', 15) * 60):
                        
                        # ✅ ДОДАНО: Валідація та корекція статистики перед звітом
                        await self._validate_and_correct_trade_statistics()
                        await self._log_detailed_statistics("balance_report")
                        
                        try:
                            usdt_balance_val = await self.api_manager.get_usdt_balance()
                        except Exception as e:
                            self.logger.error(f"Помилка отримання балансу USDT: {e}")
                            usdt_balance_val = 0.0  # Використовуємо 0 як fallback
                        
                        num_open_positions = len(self.positions)

                        if self.trade_stats['initial_balance'] is None and usdt_balance_val > 0:
                            self.trade_stats['initial_balance'] = usdt_balance_val
                            self.logger.info(f"Встановлено початковий баланс: {usdt_balance_val:.2f} USDT")

                        # ✅ ВАЛІДАЦІЯ СТАТИСТИКИ ПЕРЕД ВІДПРАВКОЮ
                        is_stats_valid = self.validate_trade_stats()
                        if not is_stats_valid:
                            self.logger.warning("🔄 Виправлення статистики перед відправкою balance update")
                            calculated_total = self.trade_stats['winning_trades'] + self.trade_stats['losing_trades']
                            if calculated_total != self.trade_stats['total_trades']:
                                self.trade_stats['total_trades'] = calculated_total
                                self.logger.info(f"✅ Статистика виправлена: total_trades = {calculated_total}")

                        await self.telegram.send_balance_update({
                            'usdt_balance': usdt_balance_val,
                            'initial_balance': self.trade_stats['initial_balance'],
                            'total_pnl': self.trade_stats['total_pnl'],
                            'open_positions_count': num_open_positions,
                            'total_trades': self.trade_stats['total_trades'],
                            'winning_trades': self.trade_stats['winning_trades'],
                            'losing_trades': self.trade_stats['losing_trades']
                        })
                        last_balance_report_time = current_time

                    # 🆕 ОСНОВНИЙ ТОРГОВИЙ ЦИКЛ (НА ОСНОВІ ЧАСУ СВІЧОК)
                    next_trade_cycle_start_time = self.preprocessor.calculate_next_update_time()
                    now_utc = datetime.now(timezone.utc)
                    
                    actual_trade_cycle_start_time = next_trade_cycle_start_time + timedelta(
                        seconds=TRADING_CONFIG.get('trade_cycle_buffer_seconds', 15)
                    )
                    
                    # 🆕 Pre-Cycle Position Validation Timing
                    pre_cycle_config = STRATEGY_CONFIG.get('pre_cycle_validation', {})
                    if pre_cycle_config.get('enable', True):
                        pre_cycle_validation_time = actual_trade_cycle_start_time - timedelta(
                            seconds=pre_cycle_config.get('seconds_before_cycle', 7)
                        )
                    else:
                        pre_cycle_validation_time = None
                    
                    sleep_seconds = (actual_trade_cycle_start_time - now_utc).total_seconds()
                    pre_cycle_sleep_seconds = (pre_cycle_validation_time - now_utc).total_seconds() if pre_cycle_validation_time else None
                    
                    if sleep_seconds > 0:
                        # Використовуємо короткі сни для перевірки інших завдань
                        if sleep_seconds > 30:
                            self.logger.info(
                                f"Наступний торговий цикл о ~{actual_trade_cycle_start_time.strftime('%Y-%m-%d %H:%M:%S')} UTC. "
                                f"Очікування: {sleep_seconds:.0f} секунд"
                            )
                            
                            # Pre-cycle validation timing
                            pre_cycle_executed = False
                            
                            # Спимо по 10 секунд для гнучкості
                            while sleep_seconds > 10 and self.is_running:
                                # Перевіряємо чи час для pre-cycle validation
                                current_time = datetime.now(timezone.utc)
                                if (pre_cycle_validation_time and not pre_cycle_executed and 
                                    current_time >= pre_cycle_validation_time):
                                    await self.pre_cycle_position_validation()
                                    pre_cycle_executed = True
                                
                                await asyncio.sleep(10)
                                sleep_seconds -= 10
                                current_time = datetime.now(timezone.utc)
                                
                                # Перевіряємо швидку перевірку під час очікування
                                if ((current_time - last_quick_check_time).total_seconds() >= 
                                    TRADING_CONFIG.get('position_check_interval_seconds', 30)):
                                    if self.positions:
                                        await self.quick_check_active_positions()
                                    last_quick_check_time = current_time
                            
                            # Final pre-cycle validation check
                            if (pre_cycle_validation_time and not pre_cycle_executed and 
                                self.is_running and datetime.now(timezone.utc) >= pre_cycle_validation_time):
                                await self.pre_cycle_position_validation()
                                pre_cycle_executed = True
                            
                            if self.is_running and sleep_seconds > 0:
                                await asyncio.sleep(sleep_seconds)
                        else:
                            # Short sleep - check if we need pre-cycle validation
                            if pre_cycle_sleep_seconds and pre_cycle_sleep_seconds > 0:
                                if pre_cycle_sleep_seconds < sleep_seconds:
                                    await asyncio.sleep(pre_cycle_sleep_seconds)
                                    await self.pre_cycle_position_validation()
                                    remaining_sleep = sleep_seconds - pre_cycle_sleep_seconds
                                    if remaining_sleep > 0:
                                        await asyncio.sleep(remaining_sleep)
                                else:
                                    await asyncio.sleep(sleep_seconds)
                            else:
                                await asyncio.sleep(sleep_seconds)
                    else:
                        self.logger.warning(
                            f"Час наступного торгового циклу ({actual_trade_cycle_start_time.strftime('%Y-%m-%d %H:%M:%S')}) "
                            f"вже минув ({abs(sleep_seconds):.1f} сек тому). Поточний час: {now_utc.strftime('%Y-%m-%d %H:%M:%S')} UTC. Запуск..."
                        )
                        await asyncio.sleep(1)

                    if self.is_running:
                        await self.run_trading_cycle()
                    
                except KeyboardInterrupt:
                    self.logger.info("Отримано сигнал переривання KeyboardInterrupt в основному циклі")
                    self.is_running = False 
                    break 
                except asyncio.CancelledError:
                    self.logger.info("Основний цикл скасовано.")
                    self.is_running = False
                    break
                except Exception as e:
                    self.logger.error(f"Помилка в основному циклі: {e}", exc_info=True)
                    await self.telegram.send_error_notification({'type': 'MAIN_LOOP_ERROR', 'message': str(e)})
                    await asyncio.sleep(TRADING_CONFIG.get('main_loop_error_sleep_seconds', 60))
            
            self.logger.info("Зупинка торгового бота...")
            await self.telegram.send_bot_status("STOPPING")
            
            # Використовуємо покращене управління задачами
            await self._shutdown_all_tasks()

            # Закриваємо компоненти в правильному порядку
            await self.preprocessor.close()
            await self.api_manager.close()
            await self.db_manager.close()

            await self.telegram.send_bot_status("STOPPED")
            self.logger.info("Торговий бот зупинено з використанням оптимізованого task management")
            
        except Exception as e:
            self.logger.critical(f"Критична помилка під час запуску/зупинки бота: {e}", exc_info=True)
            if hasattr(self, 'telegram') and self.telegram and self.telegram.bot:
                try:
                    await self.telegram.send_error_notification({
                        'type': 'CRITICAL_BOT_FAILURE', 
                        'message': f"Критична помилка бота: {str(e)}"
                    })
                except Exception as e_telegram_critical:
                    self.logger.error(f"Не вдалося відправити критичну помилку через Telegram: {e_telegram_critical}")

    async def analyze_and_notify_position_closure(self, symbol: str, local_position: Dict) -> bool:
        """
        ✅ ПОКРАЩЕНО: Аналізує закриття позиції, оновлює статистику та відправляє сповіщення
        
        Args:
            symbol: Торгова пара
            local_position: Локальні дані позиції
            
        Returns:
            bool: True якщо аналіз виконано успішно та сповіщення відправлено
        """
        try:
            self.logger.info(f"🔍 Початок аналізу закриття позиції {symbol}")
            
            # Використовуємо нову функцію з API менеджера для аналізу закриття
            closure_details = await self.api_manager.analyze_and_get_position_closure_details(symbol, local_position)
            
            if closure_details:
                # ✅ ДОДАНО: Перевірка чи було вже оброблено це закриття
                if self._is_closure_already_processed(symbol, closure_details):
                    self.logger.debug(f"Закриття позиції {symbol} вже було оброблено раніше, пропускаємо")
                    return True
                
                # ✅ ПОКРАЩЕНО: Більш детальний аналіз причини закриття
                enhanced_closure_details = self._enhance_closure_analysis(closure_details, local_position)
                
                # ✅ КРИТИЧНЕ ВИПРАВЛЕННЯ: Оновлюємо торгову статистику
                pnl_usdt = enhanced_closure_details.get('pnl_usdt', 0)
                
                # Створюємо дані для оновлення статистики
                trade_data_for_stats = {
                    'action': f"EXTERNAL_CLOSE_{enhanced_closure_details.get('side', 'UNKNOWN')}",
                    'symbol': symbol,
                    'price': enhanced_closure_details.get('exit_price', 0),
                    'quantity': enhanced_closure_details.get('quantity', 0),
                    'entry_price': enhanced_closure_details.get('entry_price', 0),
                    'side': enhanced_closure_details.get('side', 'UNKNOWN')
                }
                
                # Оновлюємо статистику через централізований метод
                stats_updated = await self._update_trade_statistics(
                    symbol=symbol,
                    pnl=pnl_usdt,
                    trade_data=trade_data_for_stats,
                    reason=f"external_closure_{enhanced_closure_details.get('closure_type', 'unknown')}"
                )
                
                self.logger.info(f"📊 Статистика {'оновлена' if stats_updated else 'НЕ оновлена (можливо дублікат)'} для зовнішнього закриття {symbol}")
                
                # ✅ ПОКРАЩЕНО: Негайне видалення позиції з пам'яті після аналізу закриття
                if symbol in self.positions:
                    self.trade_logger.info(f"🔄 Негайне видалення {symbol} з пам'яті після аналізу зовнішнього закриття")
                    del self.positions[symbol]
                
                # Відправляємо детальне сповіщення про закриття
                if hasattr(self, 'telegram') and self.telegram:
                    notification_sent = await self.telegram.send_position_closure_notification(enhanced_closure_details)
                    if notification_sent:
                        # ✅ ДОДАНО: Позначаємо закриття як оброблене
                        self._mark_closure_as_processed(symbol, enhanced_closure_details)
                        
                        # ✅ ПОКРАЩЕНО: Запобігання дублюванню повідомлень про аналіз закриття
                        closure_analysis_key = f"{symbol}_{enhanced_closure_details['closure_display_type']}_{enhanced_closure_details['exit_price']:.6f}"
                        if not self._is_closure_already_processed(closure_analysis_key, enhanced_closure_details):
                            self.trade_logger.info(
                                f"✅ {symbol} Closure Analysis: {enhanced_closure_details['closure_display_type']} - "
                                f"P&L: {enhanced_closure_details['pnl_usdt']:.3f} USDT ({enhanced_closure_details['pnl_percentage']:.2f}%)"
                            )
                            # Позначаємо як оброблене з унікальним ключем
                            self._mark_closure_as_processed(closure_analysis_key, enhanced_closure_details)
                    else:
                        self.trade_logger.warning(f"📤 Не вдалося відправити сповіщення про закриття {symbol}")
                
                # Логуємо детальну інформацію для внутрішнього використання
                self.logger.info(
                    f"📊 {symbol} закрито: {enhanced_closure_details['closure_display_type']} @ {enhanced_closure_details['exit_price']:.6f} "
                    f"(entry: {enhanced_closure_details['entry_price']:.6f}, P&L: {enhanced_closure_details['pnl_usdt']:.3f} USDT)"
                )
                
                return True
            else:
                # Якщо не вдалося отримати деталі закриття, показуємо оригінальне попередження
                self.trade_logger.warning(f"⚠️ ШВИДКА ПЕРЕВІРКА: {symbol} відсутня на біржі (аналіз закриття неможливий)")
                return False
                
        except Exception as e:
            self.logger.error(f"Помилка аналізу закриття позиції {symbol}: {e}", exc_info=True)
            # У випадку помилки показуємо оригінальне попередження
            self.trade_logger.warning(f"⚠️ ШВИДКА ПЕРЕВІРКА: {symbol} відсутня на біржі (помилка аналізу закриття)")
            return False

    def _enhance_closure_analysis(self, closure_details: Dict, local_position: Dict) -> Dict:
        """Покращує аналіз причини закриття позиції з додатковими деталями"""
        try:
            enhanced_details = closure_details.copy()
            
            # ✅ ДОДАНО: Аналізуємо тип закриття більш детально
            close_reason = closure_details.get('close_reason', 'unknown')
            close_price = closure_details.get('exit_price', 0)
            entry_price = closure_details.get('entry_price', 0)
            side = closure_details.get('side', 'BUY')
            
            # Отримуємо локальні дані для аналізу
            local_sl = local_position.get('current_stop_loss', local_position.get('stop_loss', 0))
            local_tps = local_position.get('take_profit_levels', [])
            entry_timestamp = local_position.get('entry_timestamp')
            
            # ✅ ПОКРАЩЕНО: Детальний аналіз причини закриття
            probable_trigger = "Зовнішнє закриття"
            closure_emoji = "💨"
            
            if close_reason == 'StopLoss' or self._is_sl_triggered(close_price, local_sl, side):
                probable_trigger = "Stop Loss спрацював"
                closure_emoji = "🛑"
                enhanced_details['closure_type'] = 'stop_loss'
            elif close_reason == 'TakeProfit' or self._is_tp_triggered(close_price, local_tps, side):
                tp_level = self._find_triggered_tp_level(close_price, local_tps, side)
                if any(level in tp_level.lower() for level in ['1', 'partial_1', 'partial 1']):
                    probable_trigger = "Take Profit 1 досягнуто"
                    closure_emoji = "💎"
                elif any(level in tp_level.lower() for level in ['2', 'partial_2', 'partial 2']):
                    probable_trigger = "Take Profit 2 досягнуто"
                    closure_emoji = "💎"
                elif any(level in tp_level.lower() for level in ['3', 'partial_3', 'partial 3']):
                    probable_trigger = "Take Profit 3 досягнуто"
                    closure_emoji = "💎"
                else:
                    probable_trigger = "Фінальний Take Profit досягнуто"
                    closure_emoji = "🏆"
                enhanced_details['closure_type'] = 'take_profit'
            elif close_reason == 'UserCancel':
                probable_trigger = "Мануальне закриття"
                closure_emoji = "👤"
                enhanced_details['closure_type'] = 'manual_close'
            elif close_reason == 'Liquidation':
                probable_trigger = "Ліквідація позиції"
                closure_emoji = "🚨"
                enhanced_details['closure_type'] = 'liquidation'
            elif 'trailing' in close_reason.lower():
                probable_trigger = "Trailing Stop спрацював"
                closure_emoji = "⚡"
                enhanced_details['closure_type'] = 'trailing_stop'
            elif 'breakeven' in close_reason.lower():
                probable_trigger = "Переведення в беззбиток"
                closure_emoji = "⚖️"
                enhanced_details['closure_type'] = 'breakeven'
            elif 'price_mismatch' in close_reason.lower():
                probable_trigger = "Розбіжність цін з біржею"
                closure_emoji = "⚠️"
                enhanced_details['closure_type'] = 'price_mismatch'
            else:
                enhanced_details['closure_type'] = 'external_close'
            
            enhanced_details['closure_display_type'] = probable_trigger
            enhanced_details['closure_emoji'] = closure_emoji
            
            # ✅ ДОДАНО: Розрахунок залишку для часткових закриттів
            closure_quantity = closure_details.get('quantity', 0)
            initial_quantity = local_position.get('initial_quantity', local_position.get('quantity', 0))
            current_quantity = local_position.get('quantity', 0)
            
            # Визначаємо залишок після закриття
            remaining_quantity = max(0, current_quantity - closure_quantity)
            is_full_closure = (remaining_quantity <= 0.000001 or 
                             closure_details.get('is_full_closure', False) or
                             'Повне закриття' in probable_trigger)
            
            enhanced_details['remaining_quantity'] = remaining_quantity
            enhanced_details['is_full_closure'] = is_full_closure
            enhanced_details['initial_quantity'] = initial_quantity
            
            # ✅ ДОДАНО: Час тримання позиції
            if entry_timestamp:
                try:
                    if isinstance(entry_timestamp, str):
                        entry_time = datetime.fromisoformat(entry_timestamp.replace('Z', '+00:00'))
                    else:
                        entry_time = entry_timestamp
                    
                    closure_time = datetime.now(timezone.utc)
                    enhanced_details['entry_time'] = entry_time
                    enhanced_details['closure_time'] = closure_time
                    
                except Exception as e:
                    self.logger.debug(f"Помилка обробки часу входу: {e}")
            
            # ✅ ДОДАНО: Додаткова інформація про продуктивність
            if entry_price > 0:
                price_change_pct = ((close_price - entry_price) / entry_price * 100) if side == 'BUY' else ((entry_price - close_price) / entry_price * 100)
                enhanced_details['price_change_percentage'] = price_change_pct
            
            return enhanced_details
            
        except Exception as e:
            self.logger.error(f"Помилка покращення аналізу закриття: {e}")
            return closure_details

    async def sync_single_position_with_history(self, symbol: str):
        """Синхронізує одну позицію з історією виконання з покращеним відновленням."""
        if symbol not in self.positions:
            self.logger.debug(f"sync_single_position_with_history: Локальної позиції для {symbol} не знайдено для синхронізації.")
            
            # ✅ ПОКРАЩЕНЕ ВІДНОВЛЕННЯ ПОЗИЦІЙ
            # Додатково перевіряємо історію з розширеним lookback для demo mode
            lookback_hours = TRADING_CONFIG.get('sync_lookback_hours_short', 24)
            if TRADING_CONFIG.get('mode', 'DEMO').upper() == 'DEMO':
                lookback_hours = 72  # Більший lookback для demo mode через можливі затримки
            
            history_check_pos, execution_details, analysis_msg = await self.api_manager.analyze_position_from_execution_history(
                symbol=symbol,
                lookback_hours=lookback_hours
            )
            
            if history_check_pos:
                self.logger.warning(
                    f"🔄 ПОЗИЦІЯ ЗНАЙДЕНА НА БІРЖІ: {symbol} відсутня локально, АЛЕ активна на біржі. "
                    f"Режим: {TRADING_CONFIG.get('mode', 'DEMO')}. Деталі: {analysis_msg}"
                )
                
                # ✅ СПРОБА ВІДНОВЛЕННЯ ПОЗИЦІЇ
                try:
                    recovery_position = {
                        'symbol': symbol,
                        'side': history_check_pos.get('side', 'BUY'),
                        'quantity': float(history_check_pos.get('size', 0)),
                        'entry_price': float(history_check_pos.get('avgPrice', 0)),
                        'unrealized_pnl': float(history_check_pos.get('unrealisedPnl', 0)),
                        'created_at': datetime.now(timezone.utc).isoformat(),
                        'sync_source': 'recovered_from_exchange',
                        'recovery_timestamp': datetime.now(timezone.utc).isoformat(),
                        'recovery_reason': 'found_on_exchange_missing_locally',
                        'take_profits': [],  # Will be reconstructed later
                        'stop_loss': 0,      # Will be reconstructed later
                        'status': 'active'
                    }
                    
                    # Додаємо відновлену позицію
                    self.positions[symbol] = recovery_position
                    
                    self.logger.info(
                        f"✅ ПОЗИЦІЮ ВІДНОВЛЕНО: {symbol} {recovery_position['side']} "
                        f"{recovery_position['quantity']:.8f} @ {recovery_position['entry_price']:.6f}"
                    )
                    
                    # Сповіщення про відновлення
                    if hasattr(self, 'telegram_notifier') and self.telegram_notifier:
                        await self.telegram_notifier.send_notification(
                            f"🔄 Позицію відновлено: {symbol}\n"
                            f"Сторона: {recovery_position['side']}\n"
                            f"Розмір: {recovery_position['quantity']:.8f}\n"
                            f"Ціна входу: {recovery_position['entry_price']:.6f}\n"
                            f"Режим: {TRADING_CONFIG.get('mode', 'DEMO')}",
                            message_type='position_recovery'
                        )
                    
                except Exception as e_recovery:
                    self.logger.error(f"❌ Помилка відновлення позиції {symbol}: {e_recovery}")
                    
            else:
                self.logger.debug(f"📭 {symbol}: позиція не знайдена ні локально, ні на біржі")
            return

        local_pos_copy = self.positions[symbol].copy()
        self.logger.info(f"Розпочато синхронізацію для активної позиції: {symbol}")

        is_synced, updated_data_from_reconcile, sync_status_msg = await self.api_manager.reconcile_position_with_history(
            symbol, local_pos_copy
        )

        self.logger.info(f"Результат reconcile_position_with_history для {symbol}: is_synced={is_synced}, status_msg='{sync_status_msg}'")
        if updated_data_from_reconcile:
             self.logger.debug(f"Дані від reconcile: {updated_data_from_reconcile}")


        if not is_synced:
            self.logger.warning(f"Розбіжність виявлено для {symbol}: {sync_status_msg}")
            if updated_data_from_reconcile and 'closed_externally_details' in updated_data_from_reconcile:
                ext_details = updated_data_from_reconcile['closed_externally_details']
                
                # ✅ ПОКРАЩЕНИЙ АНАЛІЗ ПРИЧИН ЗАКРИТТЯ ПОЗИЦІЙ
                close_reason = ext_details.get('close_reason', 'unknown')
                close_price = ext_details.get('close_price', 0)
                entry_price = ext_details.get('entry_price', 0)
                local_sl = local_pos_copy.get('stop_loss', 0)
                local_tps = local_pos_copy.get('take_profits', [])
                side = ext_details.get('side', 'BUY')
                
                # Аналіз причини закриття
                probable_trigger = "Unknown"
                if close_reason == 'StopLoss' or (local_sl > 0 and self._is_sl_triggered(close_price, local_sl, side)):
                    probable_trigger = f"Stop Loss (SL: {local_sl:.6f}, Close: {close_price:.6f})"
                elif close_reason == 'TakeProfit' or self._is_tp_triggered(close_price, local_tps, side):
                    tp_level = self._find_triggered_tp_level(close_price, local_tps, side)
                    probable_trigger = f"Take Profit Level {tp_level} (Close: {close_price:.6f})"
                elif close_reason == 'UserCancel':
                    probable_trigger = "Manual Close/Cancel"
                elif close_reason == 'Liquidation':
                    probable_trigger = "Liquidation"
                else:
                    probable_trigger = f"External Close ({close_reason})"
                
                self.logger.warning(
                    f"📊 ПОЗИЦІЯ ЗАКРИТА: {symbol} визначена як закрита зовнішньо під час reconcile.\n"
                    f"   Причина: {probable_trigger}\n"
                    f"   Сторона: {side}, Кількість: {ext_details.get('quantity', 0):.8f}\n"
                    f"   Вхід: {entry_price:.6f} → Вихід: {close_price:.6f}\n"
                    f"   Режим торгівлі: {TRADING_CONFIG.get('mode', 'DEMO')}"
                )

                pnl = self.pnl_calculator.calculate_simple_pnl(
                    entry_price=ext_details['entry_price'],
                    close_price=ext_details['close_price'],
                    quantity=ext_details['quantity'], # Повна кількість позиції, яка була закрита
                    side=ext_details['side']
                )
                
                pnl_percentage = 0
                if ext_details['entry_price'] > 0:
                    if ext_details['side'] == 'BUY':
                        pnl_percentage = ((ext_details['close_price'] - ext_details['entry_price']) / ext_details['entry_price']) * 100
                    else: # SELL
                        pnl_percentage = ((ext_details['entry_price'] - ext_details['close_price']) / ext_details['entry_price']) * 100
                
                trade_update_data = {
                    'symbol': symbol,
                    'action': f'EXTERNAL_SYNC_CLOSE_{ext_details["side"].upper()}',
                    'price': ext_details['close_price'],
                    'quantity': ext_details['quantity'], 
                    'quantity_float': ext_details['quantity'],
                    'side': ext_details['side'],
                    'reason': f"External/Sync Close: {ext_details.get('reason', 'N/A')}",
                    'detailed_close_reason': f"Source: {ext_details.get('source', 'reconcile')}. Details: {ext_details.get('reason', 'N/A')}",
                    'pnl': pnl,
                    'pnl_percentage': pnl_percentage,
                    'entry_price': ext_details['entry_price'],
                    'remaining_quantity': 0 
                }
                
                # ✅ ПОКРАЩЕНО: Використовуємо централізоване оновлення статистики
                stats_updated = await self._update_trade_statistics(
                    symbol=symbol,
                    pnl=pnl,
                    trade_data=trade_update_data,
                    reason=f"reconcile_{ext_details.get('source', 'external')}"
                )
                
                if not stats_updated:
                    # Fallback до старого методу тільки якщо централізований не спрацював
                    self.trade_stats['total_trades'] += 1
                    if pnl > 0:
                        self.trade_stats['winning_trades'] += 1
                    elif pnl < 0:
                        self.trade_stats['losing_trades'] += 1
                    self.trade_stats['total_pnl'] += pnl
                    self.strategy.update_risk_management(pnl)
                    await self.db_manager.save_trade_stats(self.trade_stats)
                    self.logger.warning(f"📊 Використано fallback статистику для reconcile {symbol}")
                
                await self.telegram.send_trade_notification(trade_update_data)
                
                if symbol in self.positions:
                    del self.positions[symbol]
                
                self.logger.info(f"Позиція {symbol} видалена з локального стану через зовнішнє закриття, підтверджене reconcile.")

            elif updated_data_from_reconcile: # Позиція оновлена, але не закрита
                self.logger.info(f"Позиція {symbol} оновлена згідно reconcile: {sync_status_msg}")
                # Переконуємось, що ми не перезаписуємо важливі локальні дані, якщо вони не прийшли від reconcile
                # Наприклад, 'take_profit_levels', 'initial_quantity' тощо.
                original_tp_levels = self.positions[symbol].get('take_profit_levels')
                original_initial_quantity = self.positions[symbol].get('initial_quantity')
                # ✅ CRITICAL FIX: Preserve backup entry price fields
                original_initial_entry_price = self.positions[symbol].get('initial_entry_price')
                original_signal_data = self.positions[symbol].get('original_signal_data')
                
                self.positions[symbol].update(updated_data_from_reconcile)
                
                # Відновлюємо важливі локальні поля, якщо вони не були частиною updated_data_from_reconcile
                if 'take_profit_levels' not in updated_data_from_reconcile and original_tp_levels:
                    self.positions[symbol]['take_profit_levels'] = original_tp_levels
                if 'initial_quantity' not in updated_data_from_reconcile and original_initial_quantity:
                    self.positions[symbol]['initial_quantity'] = original_initial_quantity
                
                # ✅ CRITICAL FIX: Preserve entry price backup fields to prevent data loss
                if 'initial_entry_price' not in updated_data_from_reconcile and original_initial_entry_price:
                    self.positions[symbol]['initial_entry_price'] = original_initial_entry_price
                if 'original_signal_data' not in updated_data_from_reconcile and original_signal_data:
                    self.positions[symbol]['original_signal_data'] = original_signal_data
                
                # ✅ VALIDATION: Verify entry price integrity after update
                updated_entry_price = self.positions[symbol].get('entry_price', 0)
                if updated_entry_price <= 0:
                    # Try to recover from backup fields
                    if original_initial_entry_price and original_initial_entry_price > 0:
                        self.positions[symbol]['entry_price'] = original_initial_entry_price
                        self.logger.warning(f"🔧 ENTRY PRICE RECOVERY: Restored entry price {original_initial_entry_price:.6f} for {symbol} after reconcile")
                    elif original_signal_data and 'entry_price' in original_signal_data:
                        signal_entry = float(original_signal_data['entry_price'])
                        if signal_entry > 0:
                            self.positions[symbol]['entry_price'] = signal_entry
                            self.logger.warning(f"🔧 ENTRY PRICE RECOVERY: Restored entry price {signal_entry:.6f} from signal data for {symbol}")
                
                # Оновлюємо 'current_active_tp_price' на основі оновлених TP рівнів, якщо вони є
                active_tp_price_reconciled = None
                if self.positions[symbol].get('take_profit_levels'):
                    for tp_level in self.positions[symbol]['take_profit_levels']:
                        if not tp_level.get('hit', False) and tp_level.get('price'):
                            active_tp_price_reconciled = float(tp_level['price'])
                            break
                self.positions[symbol]['current_active_tp_price'] = active_tp_price_reconciled

                self.logger.info(f"Оновлена позиція {symbol} після reconcile: {self.positions[symbol]}")
                # Можливо, потрібно оновити і в БД, якщо зберігаєте там позиції
            else:
                # updated_data_from_reconcile is None, але is_synced is False.
                # Це може бути критична помилка в reconcile або непередбачений стан.
                self.logger.error(f"Критична розбіжність для {symbol} або помилка reconcile: {sync_status_msg}. Дані не оновлено.")
        
        elif is_synced: # Позиція синхронізована
            if updated_data_from_reconcile: # Можуть бути невеликі оновлення (наприклад, unrealized PnL, latest_execution_time)
                self.positions[symbol].update(updated_data_from_reconcile)
                # Оновлюємо 'current_active_tp_price' на основі оновлених TP рівнів
                active_tp_price_synced = None
                if self.positions[symbol].get('take_profit_levels'):
                    for tp_level in self.positions[symbol]['take_profit_levels']:
                        if not tp_level.get('hit', False) and tp_level.get('price'):
                            active_tp_price_synced = float(tp_level['price'])
                            break
                self.positions[symbol]['current_active_tp_price'] = active_tp_price_synced
                self.logger.info(f"Позиція {symbol} підтверджена та синхронізована (з оновленнями): {sync_status_msg}")
                self.logger.debug(f"Деталі синхронізованої позиції {symbol}: {self.positions[symbol]}")
            else:
                # is_synced is True, and updated_data_from_reconcile is None.
                # Це означає, що локальна позиція повністю відповідає історії, або історія підтверджує, що позиції немає (і локально теж).
                self.logger.info(f"Позиція {symbol} підтверджена як синхронізована (без оновлень даних): {sync_status_msg}")

    async def _process_signals_with_prioritization(self, all_signals: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Обробляє сигнали з пріоритизацією на основі сили сигналу.
        Вибирає найсильніші сигнали, якщо досягнуто max_orders_qty.
        """
        try:
            if not all_signals:
                return []
            
            max_orders = TRADING_CONFIG.get('max_orders_qty', 3)
            current_positions = len(self.positions)
            available_slots = max_orders - current_positions
            
            self.logger.info(f"Пріоритизація сигналів: {len(all_signals)} сигналів, "
                           f"{current_positions}/{max_orders} позицій, {available_slots} вільних слотів")
            
            # Якщо є вільні слоти для всіх сигналів, виконуємо всі
            if available_slots >= len(all_signals):
                self.logger.info("Достатньо вільних слотів для всіх сигналів")
                processed_signals = []
                for signal_data in all_signals:
                    symbol = signal_data.get('symbol')
                    if symbol and symbol not in self.positions:  # Перевіряємо, чи позиція не з'явилася тим часом
                        try:
                            trade_result = await self.execute_trade(symbol, signal_data)
                            processed_signals.append(trade_result)
                            self.logger.info(f"Виконано торгівлю для {symbol}: {trade_result.get('action', 'N/A')}")
                        except Exception as e:
                            self.logger.error(f"Помилка виконання торгівлі для {symbol}: {e}")
                return processed_signals
            
            # Сортуємо сигнали за силою (спочатку найсильніші)
            sorted_signals = sorted(all_signals, 
                                  key=lambda x: x.get('signal_strength', 0.0), 
                                  reverse=True)
            
            self.logger.info("Ранжування сигналів за силою:")
            for i, signal in enumerate(sorted_signals[:10], 1):  # Показуємо топ-10
                symbol = signal.get('symbol', 'N/A')
                strength = signal.get('signal_strength', 0.0)
                signal_type = signal.get('signal', 'N/A')
                self.logger.info(f"  {i}. {symbol}: {signal_type} (Сила: {strength:.1f})")
            
            # Вибираємо найсильніші сигнали в межах доступних слотів
            selected_signals = sorted_signals[:available_slots] if available_slots > 0 else []
            rejected_signals = sorted_signals[available_slots:] if available_slots > 0 else sorted_signals
            
            if selected_signals:
                self.logger.info(f"Вибрано {len(selected_signals)} найсильніших сигналів для виконання:")
                for signal in selected_signals:
                    symbol = signal.get('symbol', 'N/A')
                    strength = signal.get('signal_strength', 0.0)
                    signal_type = signal.get('signal', 'N/A')
                    self.logger.info(f"  ✅ {symbol}: {signal_type} (Сила: {strength:.1f})")
            
            if rejected_signals:
                self.logger.info(f"Відхилено {len(rejected_signals)} сигналів через обмеження max_orders_qty:")
                for signal in rejected_signals:
                    symbol = signal.get('symbol', 'N/A')
                    strength = signal.get('signal_strength', 0.0)
                    signal_type = signal.get('signal', 'N/A')
                    self.logger.info(f"  ❌ {symbol}: {signal_type} (Сила: {strength:.1f})")
            
            # Виконуємо вибрані сигнали
            processed_signals = []
            for signal_data in selected_signals:
                symbol = signal_data.get('symbol')
                if symbol and symbol not in self.positions:  # Перевіряємо, чи позиція не з'явилася тим часом
                    try:
                        trade_result = await self.execute_trade(symbol, signal_data)
                        processed_signals.append(trade_result)
                        self.logger.info(f"Виконано пріоритетну торгівлю для {symbol}: {trade_result.get('action', 'N/A')}")
                        
                        # Оновлюємо кількість позицій для наступних ітерацій
                        if trade_result.get('success'):
                            current_positions += 1
                            
                    except Exception as e:
                        self.logger.error(f"Помилка виконання пріоритетної торгівлі для {symbol}: {e}")
                else:
                    self.logger.debug(f"Пропускаємо {symbol}, позиція вже існує")
            
            # Відправляємо сповіщення про пріоритизацію
            if rejected_signals and hasattr(self, 'telegram'):
                prioritization_msg = (
                    f"🎯 Пріоритизація сигналів:\n"
                    f"• Згенеровано: {len(all_signals)} сигналів\n"
                    f"• Вибрано: {len(selected_signals)} найсильніших\n"
                    f"• Відхилено: {len(rejected_signals)} слабших\n"
                    f"• Позицій: {current_positions}/{max_orders}"
                )
                await self.telegram.send_notification(prioritization_msg, message_type='signal_prioritization')
            
            return processed_signals
            
        except Exception as e:
            self.logger.error(f"Помилка пріоритизації сигналів: {e}", exc_info=True)
            return []
    
    async def _process_signals_with_buffer_prioritization(self, signal_buffer: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        🆕 НОВА СИСТЕМА: Обробка сигналів з буфера з розумною фільтрацією.
        Вибирає найсильніші сигнали на основі доступних слотів та відправляє 
        Telegram сповіщення тільки для обраних сигналів.
        """
        try:
            if not signal_buffer:
                return []
            
            max_orders = TRADING_CONFIG.get('max_orders_qty', 3)
            current_positions = len(self.positions)
            available_slots = max_orders - current_positions
            
            self.logger.info(
                f"🎯 Розумна фільтрація сигналів з буфера: {len(signal_buffer)} сигналів, "
                f"{current_positions}/{max_orders} позицій, {available_slots} вільних слотів"
            )
            
            if available_slots <= 0:
                self.logger.warning(f"❌ Немає вільних слотів для нових позицій ({current_positions}/{max_orders})")
                
                # Відправляємо статистику фільтрації
                telegram_config = STRATEGY_CONFIG.get('telegram_filtering', {})
                if telegram_config.get('include_filtering_stats', True):
                    await self.telegram.send_notification(
                        f"📊 Фільтрація сигналів: всі {len(signal_buffer)} сигналів відхилено через відсутність вільних слотів "
                        f"(позицій: {current_positions}/{max_orders})",
                        message_type='signal_filtering'
                    )
                return []
            
            # Сортуємо сигнали за силою (найсильніші першими)
            sorted_signals = sorted(signal_buffer, 
                                  key=lambda x: x.get('signal_strength', 0.0), 
                                  reverse=True)
            
            self.logger.info("📊 Ранжування сигналів в буфері за силою:")
            for i, signal in enumerate(sorted_signals, 1):
                symbol = signal.get('symbol', 'N/A')
                strength = signal.get('signal_strength', 0.0)
                signal_type = signal.get('signal', 'N/A')
                volume_info = signal.get('volume_boost_data', {})
                volume_surge = " 🔥" if volume_info.get('volume_surge_active', False) else ""
                super_volume = " ⚡" if volume_info.get('super_volume_surge_active', False) else ""
                
                self.logger.info(f"  {i:2d}. {symbol}: {signal_type} (Сила: {strength:.1f}){volume_surge}{super_volume}")
            
            # Вибираємо найсильніші сигнали в межах доступних слотів
            selected_signals = sorted_signals[:available_slots]
            rejected_signals = sorted_signals[available_slots:]
            
            if selected_signals:
                self.logger.info(f"✅ Обрано {len(selected_signals)} найсильніших сигналів для виконання:")
                for signal in selected_signals:
                    symbol = signal.get('symbol', 'N/A')
                    strength = signal.get('signal_strength', 0.0)
                    signal_type = signal.get('signal', 'N/A')
                    self.logger.info(f"  ✅ {symbol}: {signal_type} (Сила: {strength:.1f})")
            
            if rejected_signals:
                self.logger.info(f"❌ Відхилено {len(rejected_signals)} слабших сигналів через обмеження слотів:")
                for signal in rejected_signals[:5]:  # Показуємо тільки топ-5 відхилених
                    symbol = signal.get('symbol', 'N/A')
                    strength = signal.get('signal_strength', 0.0)
                    signal_type = signal.get('signal', 'N/A')
                    self.logger.info(f"  ❌ {symbol}: {signal_type} (Сила: {strength:.1f})")
                if len(rejected_signals) > 5:
                    self.logger.info(f"     ... та ще {len(rejected_signals) - 5} сигналів")
            
            # Виконуємо вибрані сигнали та відправляємо Telegram сповіщення
            executed_signals = []
            telegram_config = STRATEGY_CONFIG.get('telegram_filtering', {})
            
            for signal_data in selected_signals:
                symbol = signal_data.get('symbol')
                if symbol and symbol not in self.positions:  # Перевіряємо, чи позиція не з'явилася тим часом
                    try:
                        # Відправляємо Telegram сповіщення ПЕРЕД виконанням торгівлі
                        if (telegram_config.get('notify_only_selected_signals', True) and 
                            signal_data.get('entry_price', 0) > 0):
                            await self.telegram.send_signal_notification(signal_data)
                        
                        # Виконуємо торгівлю
                        trade_result = await self.execute_trade(symbol, signal_data)
                        executed_signals.append(trade_result)
                        self.logger.info(f"✅ Виконано відфільтровану торгівлю для {symbol}: {trade_result.get('action', 'N/A')}")
                        
                        # Оновлюємо кількість позицій для наступних ітерацій
                        if trade_result.get('success'):
                            current_positions += 1
                            
                    except Exception as e:
                        self.logger.error(f"❌ Помилка виконання відфільтрованої торгівлі для {symbol}: {e}")
                else:
                    self.logger.debug(f"⏭️  Пропускаємо {symbol}, позиція вже існує")
            
            # Відправляємо статистику фільтрації
            if telegram_config.get('include_filtering_stats', True):
                filtering_msg = (
                    f"🎯 Розумна фільтрація сигналів:\n"
                    f"• Згенеровано: {len(signal_buffer)} сигналів\n"
                    f"• Обрано: {len(selected_signals)} найсильніших\n"
                    f"• Відхилено: {len(rejected_signals)} слабших\n"
                    f"• Виконано: {len(executed_signals)} угод\n"
                    f"• Позицій: {current_positions}/{max_orders}"
                )
                
                if selected_signals:
                    filtering_msg += f"\n\nОбрані сигнали:"
                    for signal in selected_signals:
                        symbol = signal.get('symbol', 'N/A')
                        strength = signal.get('signal_strength', 0.0)
                        signal_type = signal.get('signal', 'N/A')
                        filtering_msg += f"\n✅ {symbol}: {signal_type} ({strength:.1f})"
                
                await self.telegram.send_notification(filtering_msg, message_type='signal_filtering')
            
            return executed_signals
            
        except Exception as e:
            self.logger.error(f"Помилка в обробці буфера сигналів: {e}", exc_info=True)
            return []

    async def _cleanup_missing_positions(self, positions_to_cleanup: List[str]):
        """
        ✅ ПОКРАЩЕНО: Автоматично видаляє локальні позиції, які відсутні на біржі з поліпшеною статистикою
        """
        try:
            for symbol in positions_to_cleanup:
                if symbol in self.positions:
                    local_position = self.positions[symbol].copy()
                    
                    # ✅ ЗМІНЕНО: Знижено рівень логування з WARNING на INFO для менш тривожних повідомлень
                    self.logger.info(
                        f"🧹 Автоматичне очищення позиції {symbol}: позиція відсутня на біржі, "
                        f"але є в локальному стані. Аналізуємо та видаляємо з локального стану."
                    )
                    
                    # ✅ ПОКРАЩЕНО: Спробуємо проаналізувати закриття перед видаленням
                    closure_analyzed = await self.analyze_and_notify_position_closure(symbol, local_position)
                    
                    if not closure_analyzed:
                        # Якщо аналіз не вдався, просто видаляємо позицію
                        entry_price = local_position.get('entry_price', 0)
                        quantity = local_position.get('quantity', 0)
                        side = local_position.get('side', 'UNKNOWN')
                        
                        # Перевіряємо чи позиція була закрита внутрішньо через стоп, тейк або дивергенцію
                        closure_reason = local_position.get('closure_reason', '')
                        last_action = local_position.get('last_action', '')
                        
                        # Видаляємо позицію з локального стану
                        del self.positions[symbol]
                        
                        # Оновлюємо статистику
                        self.trade_stats['automatic_cleanups'] = self.trade_stats.get('automatic_cleanups', 0) + 1
                        
                        # ✅ ПОКРАЩЕНО: Не відправляємо сповіщення для інформативних очищень, якщо позиція була закрита внутрішньо
                        should_send_cleanup_notification = True
                        
                        # Перевіряємо чи це була очікувана закриття (стоп, тейк, дивергенція)
                        if any(reason in closure_reason.lower() for reason in ['stop loss', 'take profit', 'volume divergence', 'tp hit', 'sl hit']) or \
                           any(action in last_action.lower() for action in ['tp_hit', 'sl_hit', 'vol_div_exit', 'divergence']):
                            should_send_cleanup_notification = False
                            self.logger.info(f"✅ Позицію {symbol} очищено (була закрита внутрішньо: {closure_reason or last_action})")
                        
                        if should_send_cleanup_notification:
                            # ✅ СПРОЩЕНО: Менш детальне повідомлення про очищення
                            cleanup_msg = f"🧹 Позиція {symbol} очищена (відсутня на біржі)"
                            
                            if hasattr(self, 'telegram'):
                                await self.telegram.send_notification(cleanup_msg, message_type='position_cleanup')
                        
                        if not should_send_cleanup_notification:
                            self.logger.info(f"✅ Позицію {symbol} успішно видалено з локального стану (без сповіщення)")
                        else:
                            self.logger.info(f"✅ Позицію {symbol} успішно видалено з локального стану")
                    else:
                        # Аналіз пройшов успішно, позиція вже видалена в analyze_and_notify_position_closure
                        self.logger.info(f"✅ Позицію {symbol} проаналізовано та видалено через analyze_and_notify_position_closure")
                else:
                    self.logger.debug(f"Позиція {symbol} вже відсутня в локальному стані")
                    
        except Exception as e:
            self.logger.error(f"Помилка автоматичного очищення позицій: {e}", exc_info=True)

    async def _ensure_position_cleanup(self, symbol: str, reason: str = "generic_cleanup") -> bool:
        """
        ✅ НОВА ФУНКЦІЯ: Забезпечує негайне очищення позиції з пам'яті
        
        Args:
            symbol: Торгова пара
            reason: Причина очищення
            
        Returns:
            bool: True якщо позиція була очищена
        """
        try:
            if symbol in self.positions:
                self.trade_logger.info(f"🔄 Негайне очищення позиції {symbol} з пам'яті. Причина: {reason}")
                del self.positions[symbol]
                return True
            return False
        except Exception as e:
            self.logger.error(f"Помилка очищення позиції {symbol}: {e}")
            return False

    async def validate_position_consistency(self) -> Dict[str, Any]:
        """
        Валідує узгодженість між локальними позиціями та станом на біржі.
        Повертає детальний звіт про виявлені розбіжності.
        """
        validation_report = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'local_positions_count': len(self.positions),
            'exchange_positions_count': 0,
            'consistent_positions': [],
            'discrepancies': [],
            'missing_on_exchange': [],
            'missing_locally': [],
            'validation_status': 'unknown'
        }
        
        try:
            # Отримуємо позиції з біржі
            exchange_positions_raw = await self.api_manager.get_positions()
            exchange_positions = {
                pos.get('symbol'): {
                    'side': pos.get('side'),
                    'size': float(pos.get('size', 0)),
                    'avg_price': float(pos.get('avgPrice', 0)),
                    'unrealized_pnl': float(pos.get('unrealisedPnl', 0))
                }
                for pos in exchange_positions_raw 
                if float(pos.get('size', 0)) > TRADING_CONFIG.get('min_trade_quantity_threshold', 0.000001)
            }
            
            validation_report['exchange_positions_count'] = len(exchange_positions)
            
            local_symbols = set(self.positions.keys())
            exchange_symbols = set(exchange_positions.keys())
            
            # Позиції, які є локально, але відсутні на біржі
            validation_report['missing_on_exchange'] = list(local_symbols - exchange_symbols)
            
            # Позиції, які є на біржі, але відсутні локально
            validation_report['missing_locally'] = list(exchange_symbols - local_symbols)
            
            # Перевіряємо узгодженість спільних позицій
            common_symbols = local_symbols & exchange_symbols
            qty_tolerance = TRADING_CONFIG.get('sync_tolerance_qty', 0.0000001)
            price_tolerance = TRADING_CONFIG.get('sync_tolerance_price_percentage', 0.002)
            
            for symbol in common_symbols:
                local_pos = self.positions[symbol]
                exchange_pos = exchange_positions[symbol]
                
                local_qty = local_pos.get('quantity', 0)
                local_side = local_pos.get('side', '')
                local_price = local_pos.get('entry_price', 0)
                
                exchange_qty = exchange_pos['size']
                exchange_side = exchange_pos['side']
                exchange_price = exchange_pos['avg_price']
                
                discrepancies = []
                
                # Перевірка кількості
                if abs(local_qty - exchange_qty) > qty_tolerance:
                    discrepancies.append(f"quantity: local={local_qty:.8f}, exchange={exchange_qty:.8f}")
                
                # Перевірка сторони
                if local_side.upper() != exchange_side.upper():
                    discrepancies.append(f"side: local={local_side}, exchange={exchange_side}")
                
                # Перевірка ціни
                if local_price > 0 and exchange_price > 0:
                    price_diff_pct = abs(local_price - exchange_price) / local_price
                    if price_diff_pct > price_tolerance:
                        discrepancies.append(f"price: local={local_price:.6f}, exchange={exchange_price:.6f} (diff: {price_diff_pct*100:.3f}%)")
                
                if discrepancies:
                    validation_report['discrepancies'].append({
                        'symbol': symbol,
                        'issues': discrepancies
                    })
                else:
                    validation_report['consistent_positions'].append(symbol)
            
            # Визначаємо загальний статус
            if (not validation_report['missing_on_exchange'] and 
                not validation_report['missing_locally'] and 
                not validation_report['discrepancies']):
                validation_report['validation_status'] = 'fully_consistent'
            elif validation_report['missing_on_exchange'] or validation_report['missing_locally']:
                validation_report['validation_status'] = 'positions_missing'
            elif validation_report['discrepancies']:
                validation_report['validation_status'] = 'data_discrepancies'
            else:
                validation_report['validation_status'] = 'unknown_issues'
            
            # Логування результатів
            if validation_report['validation_status'] == 'fully_consistent':
                self.logger.info("✅ Валідація позицій: повна узгодженість між локальним станом та біржею")
            else:
                self.logger.warning(f"⚠️ Валідація позицій виявила проблеми: {validation_report['validation_status']}")
                if validation_report['missing_on_exchange']:
                    self.logger.warning(f"   Відсутні на біржі: {validation_report['missing_on_exchange']}")
                if validation_report['missing_locally']:
                    self.logger.warning(f"   Відсутні локально: {validation_report['missing_locally']}")
                if validation_report['discrepancies']:
                    self.logger.warning(f"   Розбіжності даних: {len(validation_report['discrepancies'])} позицій")
            
            return validation_report
            
        except Exception as e:
            self.logger.error(f"Помилка валідації узгодженості позицій: {e}", exc_info=True)
            validation_report['validation_status'] = 'validation_error'
            validation_report['error'] = str(e)
            return validation_report

        # Зберігаємо стан позицій після синхронізації, якщо це передбачено
        # await self.db_manager.save_active_positions(self.positions)

    def _is_sl_triggered(self, close_price: float, stop_loss: float, side: str) -> bool:
        """
        Перевіряє, чи була спрацьована Stop Loss ордер
        
        Args:
            close_price: Ціна закриття позиції
            stop_loss: Ціна Stop Loss
            side: Сторона позиції (BUY/SELL)
            
        Returns:
            bool: True якщо SL була спрацьована
        """
        if stop_loss <= 0:
            return False
            
        tolerance = 0.0001  # Допуск для порівняння цін
        
        if side.upper() == 'BUY':
            # Для BUY позиції SL спрацьовує коли ціна падає нижче SL
            return close_price <= (stop_loss + tolerance)
        else:  # SELL
            # Для SELL позиції SL спрацьовує коли ціна росте вище SL
            return close_price >= (stop_loss - tolerance)
    
    def _is_tp_triggered(self, close_price: float, take_profits: List[Dict], side: str) -> bool:
        """
        Перевіряє, чи була спрацьована якась Take Profit ордер
        
        Args:
            close_price: Ціна закриття позиції
            take_profits: Список Take Profit рівнів
            side: Сторона позиції (BUY/SELL)
            
        Returns:
            bool: True якщо будь-який TP був спрацьований
        """
        if not take_profits:
            return False
            
        tolerance = 0.0001  # Допуск для порівняння цін
        
        for tp in take_profits:
            tp_price = tp.get('price', 0)
            if tp_price <= 0:
                continue
                
            if side.upper() == 'BUY':
                # Для BUY позиції TP спрацьовує коли ціна росте до TP
                if close_price >= (tp_price - tolerance):
                    return True
            else:  # SELL
                # Для SELL позиції TP спрацьовує коли ціна падає до TP
                if close_price <= (tp_price + tolerance):
                    return True
        
        return False
    
    def _find_triggered_tp_level(self, close_price: float, take_profits: List[Dict], side: str) -> str:
        """
        Знаходить який саме TP рівень був спрацьований
        
        Args:
            close_price: Ціна закриття позиції
            take_profits: Список Take Profit рівнів
            side: Сторона позиції (BUY/SELL)
            
        Returns:
            str: Опис спрацьованого TP рівня
        """
        if not take_profits:
            return "Unknown"
            
        tolerance = 0.0001
        triggered_levels = []
        
        for i, tp in enumerate(take_profits):
            tp_price = tp.get('price', 0)
            tp_type = tp.get('type', f'TP{i+1}')
            
            if tp_price <= 0:
                continue
                
            is_triggered = False
            if side.upper() == 'BUY':
                is_triggered = close_price >= (tp_price - tolerance)
            else:  # SELL
                is_triggered = close_price <= (tp_price + tolerance)
                
            if is_triggered:
                triggered_levels.append(f"{tp_type} ({tp_price:.6f})")
        
        if triggered_levels:
            return ", ".join(triggered_levels)
        else:
            return "None (price analysis inconclusive)"

async def main_async():
    setup_logging()
    logger = logging.getLogger(__name__) # Використовуємо __name__ для логера модуля
    logger.info("=" * 60)
    logger.info("Запуск торгового бота Lightning Volume")
    logger.info(f"Час запуску: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Режим торгівлі: {TRADING_CONFIG['mode']}")
    logger.info(f"Торгові пари: {', '.join(TRADING_CONFIG['trade_pairs'])}")
    logger.info(f"Таймфрейм: {TRADING_CONFIG['timeframe']} хвилин")
    logger.info("=" * 60)
    
    bot = TradingBot()
    try:
        await bot.run()
    except KeyboardInterrupt: # Обробка KeyboardInterrupt тут також
        logger.info("Переривання користувача в main_async. Ініціюю зупинку бота...")
        if bot.is_running: # Якщо бот ще працює, зупиняємо його коректно
            bot.is_running = False 
            # Даємо боту час на завершення поточних операцій, якщо це можливо
            # await asyncio.sleep(5) # Можна додати невелику затримку, якщо потрібно
    except Exception as e:
        logger.error(f"Неочікувана помилка в main_async: {e}", exc_info=True)
    finally:
        logger.info("Завершення роботи програми (main_async finally)")
        # Додаткові дії по очищенню, якщо потрібно, хоча bot.run() має це робити


if __name__ == "__main__":
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        # Логування вже має відбутися всередині main_async
        print("\nПрограма зупинена користувачем (зовнішній KeyboardInterrupt).")
    except Exception as e: # Загальний обробник на випадок непередбачених помилок
        print(f"Критична помилка на верхньому рівні програми: {e}")
        logging.getLogger(__name__).critical(f"Критична помилка на верхньому рівні: {e}", exc_info=True)