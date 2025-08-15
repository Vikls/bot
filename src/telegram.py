# -*- coding: utf-8 -*-
"""
Telegram бот для сповіщень торгового бота
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Tuple
from telegram import Bot
from telegram.error import TelegramError
from config.settings import (
    TELEGRAM_CONFIG, API_CONFIG, PRECISION_CONFIG, DISPLAY_CONFIG
)

logger = logging.getLogger(__name__)

class TelegramNotifier:
    """Клас для відправки сповіщень через Telegram"""
    
    def __init__(self):
        self.config = TELEGRAM_CONFIG
        self.bot = None
        self.chat_id = self.config.get('chat_id')
        
        # ✅ ДОДАНО: Система запобігання дублюванню сповіщень
        dedup_config = self.config.get('deduplication', {})
        self.enable_deduplication = dedup_config.get('enable_deduplication', True)
        self.sent_notifications = {}  # Ключ: унікальний ID, Значення: час відправки
        self.notification_expiry_hours = dedup_config.get('notification_expiry_hours', 24)
        self.cleanup_threshold = dedup_config.get('cleanup_threshold', 100)
        
        # ✅ ДОДАНО: Налаштування покращення повідомлень  
        enhancement_config = self.config.get('message_enhancement', {})
        self.improve_closure_reasons = enhancement_config.get('improve_closure_reasons', True)
        self.add_holding_time = enhancement_config.get('add_holding_time', True)
        self.enhanced_formatting = enhancement_config.get('enhanced_formatting', True)
        
        if self.config.get('bot_token') and self.config.get('enable_notifications', True):
            try:
                self.bot = Bot(token=self.config['bot_token'])
                logger.info("Telegram бот ініціалізовано з системою запобігання дублюванню")
            except Exception as e:
                logger.error(f"Помилка ініціалізації Telegram бота: {e}")
                self.bot = None
        else:
            logger.warning("Telegram сповіщення вимкнено")
    
    def get_detailed_action_info(self, action: str, reason: str = "", side: str = "") -> Tuple[str, str]:
        """Визначає детальний тип дії та емодзі для торгових операцій"""

        action_lower = action.lower()
        reason_lower = reason.lower() if reason else ""
        side_upper = side.upper() if side else "UNKNOWN_SIDE"

        position_suffix = ""
        if side_upper == 'BUY':
            position_suffix = " (LONG)"
        elif side_upper == 'SELL':
            position_suffix = " (SHORT)"

        # Пріоритетний аналіз за reason (більш точний, якщо він детальний)
        if 'stop loss hit' in reason_lower or 'sl_hit' in action_lower: # Додано перевірку action_lower
            return f"STOP LOSS HIT{position_suffix}", "🛑"
        elif 'partial_1 hit' in reason_lower or 'partial_tp1_hit' in action_lower:
            return f"PARTIAL TP 1 HIT{position_suffix}", "💎"
        elif 'partial_2 hit' in reason_lower or 'partial_tp2_hit' in action_lower:
            return f"PARTIAL TP 2 HIT{position_suffix}", "💎"
        elif 'partial_3 hit' in reason_lower or 'partial_tp3_hit' in action_lower:
            return f"PARTIAL TP 3 HIT{position_suffix}", "💎"
        elif 'final tp hit' in reason_lower or ('take profit' in reason_lower and 'partial' not in reason_lower) or 'final_tp_hit' in action_lower:
            return f"FINAL TP HIT{position_suffix}", "🏆"
        elif 'volume divergence exit' in reason_lower or 'vol_div_exit' in action_lower:
            return f"VOLUME DIVERGENCE EXIT{position_suffix}", "📊"
        elif 'breakeven' in reason_lower or 'breakeven_close' in action_lower:
            return f"BREAKEVEN CLOSE{position_suffix}", "⚖️"
        elif 'trailing stop' in reason_lower or 'trailing_sl_hit' in action_lower:
            return f"TRAILING STOP HIT{position_suffix}", "⚡"
        elif 'pos_closed_on_tpsl_update_fail' in action_lower: # Новий тип
            return f"CLOSED (TP/SL Update Fail){position_suffix}", "⚠️"
        elif 'closed externally' in reason_lower or 'external_close' in action_lower or 'closed_externally' in action_lower:
            return f"EXTERNAL CLOSE{position_suffix}", "💨"
        elif 'already_closed' in action_lower: # Змінено з 'already closed' in reason_lower
            return f"ALREADY CLOSED{position_suffix}", "💨"

        # Аналіз за action, якщо reason не дав точного результату
        if 'open' in action_lower:
            return f"OPEN{position_suffix}", "🟢" if side_upper == "BUY" else "🔴"

        # Розбираємо action, якщо він має складний формат (наприклад, з _close_position)
        # PARTIAL_SL_HIT_BUY, PARTIAL_TP1_HIT_SELL etc.
        if "partial" in action_lower and "close" not in action_lower: # Наприклад, PARTIAL_TP1_HIT_BUY
            tp_type_part = "UNKNOWN_TP"
            if "tp1" in action_lower or "partial_1" in action_lower : tp_type_part = "PARTIAL TP 1"
            elif "tp2" in action_lower or "partial_2" in action_lower: tp_type_part = "PARTIAL TP 2"
            elif "tp3" in action_lower or "partial_3" in action_lower: tp_type_part = "PARTIAL TP 3"
            elif "final" in action_lower : tp_type_part = "FINAL TP"

            if "hit" in action_lower:
                return f"{tp_type_part} HIT{position_suffix}", "💎" if "final" not in tp_type_part.lower() else "🏆"

        # Загальні випадки закриття, якщо action містить 'close'
        if 'close' in action_lower or 'closed' in action_lower:
            if 'partial' in action_lower: # Наприклад, MANUAL_PARTIAL_CLOSE_BUY
                return f"PARTIAL CLOSE{position_suffix}", "📊"
            else: # Наприклад, MANUAL_CLOSE_BUY
                return f"CLOSE{position_suffix}", "🎯"

        # Якщо нічого не підійшло, повертаємо базову інформацію
        default_action_text = action.replace('_', ' ').upper()
        default_emoji = "ℹ️"
        if side_upper == "BUY": default_emoji = "📈"
        elif side_upper == "SELL": default_emoji = "📉"

        return f"{default_action_text}{position_suffix}", default_emoji
    
    def _generate_notification_id(self, message_type: str, symbol: str, unique_data: Dict) -> str:
        """Генерує унікальний ID для сповіщення щоб запобігти дублюванню"""
        try:
            # Створюємо ключ на основі типу повідомлення, символу та унікальних даних
            key_parts = [message_type, symbol]
            
            if message_type == 'position_closure':
                # Для закриття позицій використовуємо ціну закриття та причину
                key_parts.extend([
                    str(unique_data.get('exit_price', 0)),
                    str(unique_data.get('closure_type', '')),
                    str(unique_data.get('quantity', 0))
                ])
            elif message_type == 'trade_notification':
                # Для торгових сповіщень використовуємо дію та ціну
                key_parts.extend([
                    str(unique_data.get('action', '')),
                    str(unique_data.get('price', 0)),
                    str(unique_data.get('reason', ''))
                ])
            
            # Створюємо хеш
            import hashlib
            key_string = '|'.join(key_parts)
            return hashlib.md5(key_string.encode()).hexdigest()[:16]
            
        except Exception as e:
            logger.error(f"Помилка генерації ID сповіщення: {e}")
            # Fallback до простого ID
            return f"{message_type}_{symbol}_{int(datetime.now().timestamp())}"
    
    def _is_notification_already_sent(self, notification_id: str) -> bool:
        """Перевіряє чи було вже відправлено це сповіщення"""
        try:
            if notification_id not in self.sent_notifications:
                return False
            
            # Перевіряємо чи не застаріло сповіщення
            sent_time = self.sent_notifications[notification_id]
            hours_passed = (datetime.now() - sent_time).total_seconds() / 3600
            
            if hours_passed > self.notification_expiry_hours:
                # Видаляємо застарілий запис
                del self.sent_notifications[notification_id]
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"Помилка перевірки дублювання сповіщення: {e}")
            return False
    
    def _mark_notification_as_sent(self, notification_id: str):
        """Позначає сповіщення як відправлене"""
        try:
            self.sent_notifications[notification_id] = datetime.now()
            
            # Очищуємо застарілі записи (не частіше ніж раз на годину)
            if len(self.sent_notifications) > self.cleanup_threshold:  # Якщо багато записів
                self._cleanup_old_notifications()
                
        except Exception as e:
            logger.error(f"Помилка збереження статусу сповіщення: {e}")
    
    def _cleanup_old_notifications(self):
        """Очищає застарілі записи про відправлені сповіщення"""
        try:
            current_time = datetime.now()
            expired_ids = []
            
            for notification_id, sent_time in self.sent_notifications.items():
                hours_passed = (current_time - sent_time).total_seconds() / 3600
                if hours_passed > self.notification_expiry_hours:
                    expired_ids.append(notification_id)
            
            for expired_id in expired_ids:
                del self.sent_notifications[expired_id]
            
            if expired_ids:
                logger.info(f"Очищено {len(expired_ids)} застарілих записів сповіщень")
                
        except Exception as e:
            logger.error(f"Помилка очищення старих сповіщень: {e}")
    
    async def send_message(self, message: str, parse_mode: str = 'HTML') -> bool:
        """Відправка повідомлення в Telegram"""
        if not self.bot or not self.chat_id:
            logger.debug("Telegram бот не налаштовано")
            return False
        
        try:
            await self.bot.send_message(
                chat_id=self.chat_id,
                text=message,
                parse_mode=parse_mode
            )
            return True
            
        except TelegramError as e:
            logger.error(f"Помилка відправки Telegram повідомлення: {e}")
            return False
        except Exception as e:
            logger.error(f"Неочікувана помилка Telegram: {e}")
            return False
    
    async def send_bot_status(self, status: str, additional_info: Dict = None) -> bool:
        """Відправка статусу бота"""
        if not self.config.get('notification_types', {}).get('status', True):
            return False
        
        try:
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            message = f"🤖 <b>Статус бота</b>\n"
            message += f"⏰ Час: {timestamp}\n"
            message += f"📊 Статус: <b>{status}</b>\n"
            
            if additional_info:
                message += "\n📋 <b>Додаткова інформація:</b>\n"
                for key, value in additional_info.items():
                    message += f"• {key}: {value}\n"
            
            return await self.send_message(message)
            
        except Exception as e:
            logger.error(f"Помилка відправки статусу бота: {e}")
            return False
    
    async def send_balance_update(self, balance_data: Dict[str, Any]) -> bool:
        """Відправка оновлення балансу з правильним розрахунком P&L через різницю балансів"""
        if not self.config.get('notification_types', {}).get('balance', True):
            return False
        
        try:
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            message = f"💰 <b>Баланс акаунта</b>\n"
            message += f"⏰ Час: {timestamp}\n"
            
            # Поточний баланс USDT
            current_usdt_balance = balance_data.get('usdt_balance')
            if isinstance(current_usdt_balance, (int, float)):
                message += f"💵 USDT: <b>{float(current_usdt_balance):.2f}</b>\n"
            else:
                message += f"💵 USDT: <b>{current_usdt_balance or '0.00'}</b>\n"

            # ✅ ВИПРАВЛЕНО: P&L розраховується як різниця між поточним та початковим балансом
            initial_balance = balance_data.get('initial_balance')
            if isinstance(current_usdt_balance, (int, float)) and isinstance(initial_balance, (int, float)):
                pnl_from_balance_diff = float(current_usdt_balance) - float(initial_balance)
                pnl_emoji = "📈" if pnl_from_balance_diff >= 0 else "📉"
                message += f"{pnl_emoji} P&L: <b>{pnl_from_balance_diff:+.4f} USDT</b>\n"
            else:
                # Fallback на старий спосіб, якщо немає початкового балансу
                total_pnl_val = balance_data.get('total_pnl', 0)
                if isinstance(total_pnl_val, (int, float)):
                    pnl_val_num = float(total_pnl_val)
                    pnl_emoji = "📈" if pnl_val_num >= 0 else "📉"
                    message += f"{pnl_emoji} P&L: <b>{pnl_val_num:+.4f} USDT</b>\n"
                else:
                    message += f"📊 P&L: <b>{total_pnl_val or 'N/A'}</b>\n"
            
            # Кількість позицій
            open_positions_count = balance_data.get('open_positions_count', 0)
            message += f"📍 Відкритих позицій: <b>{open_positions_count}</b>\n"
            
            # ✅ ВИПРАВЛЕНО: Статистика торгівлі
            total_trades = balance_data.get('total_trades', 0)
            winning_trades = balance_data.get('winning_trades', 0)
            losing_trades = balance_data.get('losing_trades', 0)
            
            if total_trades > 0:
                win_rate = (winning_trades / total_trades) * DISPLAY_CONFIG['percentage_multiplier']
                message += f"\n📊 <b>Статистика торгівлі:</b>\n"
                message += f"🎯 Всього угод: <b>{total_trades}</b>\n"
                message += f"✅ Виграшних: <b>{winning_trades}</b>\n"
                message += f"❌ Програшних: <b>{losing_trades}</b>\n"
                message += f"📈 Вінрейт: <b>{win_rate:.1f}%</b>\n"
                
                # ✅ ВИПРАВЛЕНО: P&L від угод окремо (сума P&L з торгових записів)
                total_pnl_from_trades = balance_data.get('total_pnl', 0)
                if isinstance(total_pnl_from_trades, (int, float)):
                    avg_trade = total_pnl_from_trades / total_trades if total_trades > 0 else 0
                    message += f"💰 P&L від торгівлі: <b>{total_pnl_from_trades:+.4f} USDT</b>\n"
                    message += f"📊 Середня угода: <b>{avg_trade:+.4f} USDT</b>\n"
                    
                    # Показуємо різницю між P&L від балансу та угод (комісії, слипадж тощо)
                    if isinstance(current_usdt_balance, (int, float)) and isinstance(initial_balance, (int, float)):
                        balance_pnl = float(current_usdt_balance) - float(initial_balance)
                        difference = balance_pnl - total_pnl_from_trades
                        if abs(difference) > 0.0001:  # Показуємо тільки якщо різниця значна
                            message += f"⚖️ Різниця (комісії/слипадж): <b>{difference:+.4f} USDT</b>\n"
            
            return await self.send_message(message)
            
        except Exception as e:
            logger.error(f"Помилка відправки балансу: {e}", exc_info=True)
            return False
    
    async def send_trade_notification(self, trade_data: Dict[str, Any]) -> bool:
        """Відправка сповіщення про торгівлю з покращеними заголовками та точним P&L"""
        if not self.config.get('notification_types', {}).get('trades', True):
            return False

        try:
            # ✅ ДОДАНО: Перевірка на дублювання для торгових сповіщень
            if self.enable_deduplication and self.config.get('deduplication', {}).get('trade_notification_deduplication', True):
                notification_id = self._generate_notification_id('trade_notification', 
                                                               trade_data.get('symbol', 'UNKNOWN'), 
                                                               trade_data)
                
                if self._is_notification_already_sent(notification_id):
                    logger.debug(f"Торгове сповіщення вже відправлено (ID: {notification_id}), пропускаємо")
                    return True  # Повертаємо True оскільки сповіщення технічно "успішне"
            
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            action = trade_data.get('action', 'Unknown Trade Action')
            symbol = trade_data.get('symbol', 'N/A')
            side = trade_data.get('side', 'N/A')
            reason = trade_data.get('reason', '')

            detailed_action_text, action_emoji = self.get_detailed_action_info(action, reason, side)
            message = f"{action_emoji} <b>{detailed_action_text}</b>\n"

            message += f"⏰ Час: {timestamp}\n"
            message += f"📊 Пара: <b>{symbol}</b>\n"

            price_val = trade_data.get('price') # Ціна виконання або тригера
            if price_val is not None:
                try:
                    price_float = float(price_val)
                    price_str_fmt = str(price_float)
                    decimals = 0
                    if '.' in price_str_fmt:
                        decimals = len(price_str_fmt.split('.')[1])
                        if price_float > PRECISION_CONFIG['price_decimals_high'] and decimals > PRECISION_CONFIG['price_decimals_high']: 
                            decimals = PRECISION_CONFIG['price_decimals_high']
                        elif price_float > 1 and decimals > 4: decimals = 4
                        elif decimals > 6: decimals = 6
                    message += f"💰 Ціна: <b>{price_float:.{decimals}f}</b>\n"
                except (ValueError, TypeError):
                    message += f"💰 Ціна: <b>{price_val}</b>\n"
            
            # Додаємо ціну тригера, якщо вона є і відрізняється від ціни виконання
            trigger_price = trade_data.get('trigger_price_for_close')
            if trigger_price is not None and price_val is not None:
                try:
                    if abs(float(trigger_price) - float(price_val)) > 1e-9: # Якщо ціни суттєво відрізняються
                        trigger_price_float = float(trigger_price)
                        decimals_trigger = 0
                        if '.' in str(trigger_price_float):
                            decimals_trigger = len(str(trigger_price_float).split('.')[1])
                            if trigger_price_float > PRECISION_CONFIG['price_decimals_high'] and decimals_trigger > PRECISION_CONFIG['price_decimals_high']: 
                                decimals_trigger = PRECISION_CONFIG['price_decimals_high']
                            elif trigger_price_float > 1 and decimals_trigger > 4: decimals_trigger = 4
                            elif decimals_trigger > 6: decimals_trigger = 6
                        message += f"🔑 Тригер ціна: <b>{trigger_price_float:.{decimals_trigger}f}</b>\n"
                except (ValueError, TypeError):
                    pass


            quantity_display = trade_data.get('quantity_float', trade_data.get('quantity'))
            if quantity_display is not None:
                try:
                    qty_val = float(quantity_display)
                    qty_str_fmt = str(qty_val)
                    qty_decimals = 0
                    if '.' in qty_str_fmt:
                        qty_decimals = len(qty_str_fmt.split('.')[1])
                        if qty_val < 0.001 and qty_decimals > 8: qty_decimals = 8 # Для дуже малих кількостей
                        elif qty_val < 1 and qty_decimals > 6 : qty_decimals = 6
                        elif qty_val >=1 and qty_decimals > 4: qty_decimals = 4
                    
                    # ✅ ПОКРАЩЕНО: Покращена логіка відображення залишку для часткових закриттів
                    remaining_qty = trade_data.get('remaining_quantity')
                    is_partial_closure = (any(keyword in detailed_action_text.upper() for keyword in ['PARTIAL', 'TP 1', 'TP 2', 'TP 3']) and 
                                        any(keyword in detailed_action_text.upper() for keyword in ['CLOSE', 'HIT', 'EXIT'])) or \
                                       (remaining_qty is not None and float(remaining_qty or 0) > DISPLAY_CONFIG.get('min_remaining_qty_display', 0.0001))
                    
                    if is_partial_closure and remaining_qty is not None:
                        try:
                            rem_qty_float = float(remaining_qty)
                            if rem_qty_float > DISPLAY_CONFIG.get('min_remaining_qty_display', 0.0001):
                                rem_qty_decimals = 0
                                if '.' in str(rem_qty_float): 
                                    rem_qty_decimals = len(str(rem_qty_float).split('.')[1])
                                if rem_qty_float < 0.001 and rem_qty_decimals > 8: 
                                    rem_qty_decimals = 8
                                elif rem_qty_float < 1 and rem_qty_decimals > 6: 
                                    rem_qty_decimals = 6
                                elif rem_qty_float >= 1 and rem_qty_decimals > 4: 
                                    rem_qty_decimals = 4
                                # ✅ ПОКРАЩЕНО: Більш чіткий формат для часткового закриття
                                message += f"📦 Кількість: <b>{qty_val:.{qty_decimals}f}</b> (Часткове закриття)\n"
                                message += f"📦 Залишок: <b>{rem_qty_float:.{rem_qty_decimals}f}</b>\n"
                            else:
                                message += f"📦 Кількість: <b>{qty_val:.{qty_decimals}f}</b> (Повне закриття)\n"
                        except (ValueError, TypeError):
                            message += f"📦 Кількість: <b>{qty_val:.{qty_decimals}f}</b> (Часткове закриття)\n"
                            message += f"📦 Залишок: <b>{remaining_qty}</b>\n"
                    else:
                        # Для повних закриттів або відкриттів
                        closure_info = ""
                        if any(keyword in detailed_action_text.upper() for keyword in ['CLOSE', 'HIT', 'EXIT']) and not is_partial_closure:
                            closure_info = " (Повне закриття)"
                        message += f"📦 Кількість: <b>{qty_val:.{qty_decimals}f}</b>{closure_info}\n"
                        
                except (ValueError, TypeError):
                    message += f"📦 Кількість: <b>{quantity_display}</b>\n"

            if 'OPEN' in detailed_action_text.upper():
                entry_price_val = trade_data.get('entry_price', trade_data.get('price'))
                # ✅ CRITICAL FIX: Validate entry price before using in calculations
                if entry_price_val is not None and float(entry_price_val or 0) > 0 and quantity_display is not None:
                    try:
                        total_value = float(entry_price_val) * float(quantity_display)
                        message += f"💵 Сума (орієнтовно): <b>{total_value:.2f} USDT</b>\n"
                    except (ValueError, TypeError): 
                        logger.warning(f"Error calculating total value for {symbol}: entry_price={entry_price_val}, quantity={quantity_display}")
                elif entry_price_val is None or float(entry_price_val or 0) <= 0:
                    logger.warning(f"Invalid entry price in trade notification for {symbol}: {entry_price_val}")

                sl_val = trade_data.get('stop_loss')
                if sl_val is not None:
                    try:
                        sl_float = float(sl_val)
                        sl_decimals = (PRECISION_CONFIG['price_decimals_high'] if sl_float > PRECISION_CONFIG['price_decimals_high'] 
                                     else PRECISION_CONFIG['price_decimals_medium'] if sl_float > 1 
                                     else PRECISION_CONFIG['price_decimals_low'])
                        message += f"🛑 Stop Loss: <b>{sl_float:.{sl_decimals}f}</b>\n"
                    except (ValueError, TypeError):
                        message += f"🛑 Stop Loss: <b>{sl_val}</b>\n"

                tp_levels = trade_data.get('take_profits')
                if isinstance(tp_levels, list) and tp_levels:
                    message += f"🎯 Take Profits:\n"
                    for i, tp in enumerate(tp_levels):
                        if isinstance(tp, dict):
                            tp_price = tp.get('price')
                            tp_percentage = tp.get('percentage_to_close')
                            tp_type = tp.get('type', str(i+1))
                            if tp_price is not None and tp_percentage is not None:
                                try:
                                    tp_price_float = float(tp_price)
                                    tp_dec = (PRECISION_CONFIG['price_decimals_high'] if tp_price_float > PRECISION_CONFIG['price_decimals_high'] 
                                            else PRECISION_CONFIG['price_decimals_medium'] if tp_price_float > 1 
                                            else PRECISION_CONFIG['price_decimals_low'])
                                    message += f"  • TP {tp_type}: {tp_price_float:.{tp_dec}f} ({float(tp_percentage):.1f}%)\n"
                                except (ValueError, TypeError):
                                    message += f"  • TP {tp_type}: {tp_price} ({tp_percentage}%)\n"

                if trade_data.get('confidence') is not None:
                    message += f"📊 Впевненість: <b>{trade_data['confidence']}</b>\n"
                if trade_data.get('volume_surge_active'): message += f"⚡ Volume Surge активовано!\n"
                if trade_data.get('super_volume_surge_active'): message += f"🌟 SUPER Volume Surge активовано!\n"

            if any(keyword in detailed_action_text.upper() for keyword in ['CLOSE', 'HIT', 'EXIT', 'ALREADY', 'FAIL']): # Додано FAIL
                pnl_value_from_data = trade_data.get('pnl')
                pnl_display_text = "<b>N/A</b>"
                pnl_emoji_for_msg = "📊"

                if pnl_value_from_data is not None:
                    if isinstance(pnl_value_from_data, str) and "N/A" in pnl_value_from_data:
                        pass # Залишаємо N/A
                    else:
                        try:
                            pnl_float = float(pnl_value_from_data)
                            pnl_emoji_for_msg = "💚" if pnl_float >= 0 else "❤️"
                            pnl_display_text = f"<b>{pnl_float:+.4f} USDT</b>"

                            pnl_percentage = trade_data.get('pnl_percentage')
                            if pnl_percentage is not None:
                                try:
                                    pnl_display_text += f" ({float(pnl_percentage):+.2f}%)"
                                except (ValueError, TypeError): pass
                        except (ValueError, TypeError):
                            pnl_display_text = f"<b>{str(pnl_value_from_data)}</b>"

                message += f"{pnl_emoji_for_msg} P&L: {pnl_display_text}\n"

            if reason:
                reason_short = (reason[:DISPLAY_CONFIG['reason_display_limit']] + "..." 
                              if len(reason) > DISPLAY_CONFIG['reason_display_limit'] else reason)
                message += f"📝 Причина: {reason_short}\n"
            
            # ✅ ПОКРАЩЕНО: Детальна причина закриття з більш інформативними повідомленнями
            detailed_reason = trade_data.get('detailed_close_reason')
            if detailed_reason and detailed_reason.lower() != reason.lower(): # Якщо відрізняється від основної причини
                # Покращуємо читабельність детальної причини
                if self.improve_closure_reasons:
                    detailed_reason_improved = self._improve_detailed_reason(detailed_reason)
                    message += f"🔍 Деталі: {detailed_reason_improved}\n"
                else:
                    message += f"🔍 Деталі: {detailed_reason}\n"

            # ✅ ДОДАНО: Час тримання позиції для закритих позицій
            if self.add_holding_time and any(keyword in detailed_action_text.upper() for keyword in ['CLOSE', 'HIT', 'EXIT']):
                entry_timestamp = trade_data.get('entry_timestamp')
                if entry_timestamp:
                    try:
                        if isinstance(entry_timestamp, str):
                            entry_time = datetime.fromisoformat(entry_timestamp.replace('Z', '+00:00'))
                        else:
                            entry_time = entry_timestamp
                        
                        holding_duration = datetime.now(timezone.utc) - entry_time.replace(tzinfo=timezone.utc)
                        hours = int(holding_duration.total_seconds() // 3600)
                        minutes = int((holding_duration.total_seconds() % 3600) // 60)
                        
                        if hours > 0:
                            message += f"⏱️ Тримання: <b>{hours}г {minutes}хв</b>\n"
                        else:
                            message += f"⏱️ Тримання: <b>{minutes}хв</b>\n"
                    except Exception as e:
                        logger.debug(f"Помилка розрахунку часу тримання: {e}")

            exchange_order_id = trade_data.get('exchange_order_id')
            if exchange_order_id:
                message += f"🆔 ID ордера: <code>{exchange_order_id}</code>\n"


            details = trade_data.get('details') # Загальні деталі
            if details:
                details_short = str(details)[:200] + "..." if len(str(details)) > 200 else str(details)
                message += f"ℹ️ Додатково: {details_short}\n"

            # ✅ ДОДАНО: Відправляємо повідомлення та позначаємо як відправлене
            result = await self.send_message(message)
            if result and self.enable_deduplication and self.config.get('deduplication', {}).get('trade_notification_deduplication', True):
                self._mark_notification_as_sent(notification_id)
                logger.debug(f"Торгове сповіщення відправлено (ID: {notification_id})")
            
            return result

        except Exception as e:
            logger.error(f"Помилка відправки торгового сповіщення: {e}", exc_info=True)
            try:
                await self.send_message(f"🚨 Помилка формування торгового сповіщення: {str(e)[:200]}")
            except: pass
            return False
  
    def _improve_detailed_reason(self, detailed_reason: str) -> str:
        """Покращує читабельність детальної причини закриття"""
        try:
            if not detailed_reason:
                return detailed_reason
                
            reason_lower = detailed_reason.lower()
            
            # Заміни для покращення читабельності
            replacements = {
                'external_price_mismatch': 'Розбіжність цін з біржею',
                'external_sync_close': 'Синхронізація з біржею',
                'position_not_found': 'Позиція не знайдена на біржі',
                'sl_hit_detected': 'Виявлено спрацювання Stop Loss',
                'tp_hit_detected': 'Виявлено спрацювання Take Profit',
                'manual_close_detected': 'Виявлено мануальне закриття',
                'liquidation_detected': 'Виявлено ліквідацію',
                'api_error': 'Помилка API біржі',
                'insufficient_balance': 'Недостатній баланс',
                'order_rejected': 'Ордер відхилено біржею',
            }
            
            improved_reason = detailed_reason
            for old_text, new_text in replacements.items():
                if old_text in reason_lower:
                    improved_reason = improved_reason.replace(old_text, new_text)
            
            # Обмеження довжини
            if len(improved_reason) > 150:
                improved_reason = improved_reason[:147] + "..."
                
            return improved_reason
            
        except Exception as e:
            logger.error(f"Помилка покращення детальної причини: {e}")
            return detailed_reason
    
    async def send_signal_notification(self, signal_data: Dict[str, Any]) -> bool:
        """Відправка сповіщення про сигнал"""
        try:
            signal = signal_data.get('signal', 'HOLD')
            symbol = signal_data.get('symbol', 'Unknown')
            confidence_val = signal_data.get('confidence') 
            confidence_str = str(confidence_val) if confidence_val is not None else "N/A"

            if signal == 'HOLD': 
                return False 
            
            # Покращене відображення сигналів
            if signal == 'BUY':
                signal_emoji = "🟢"
                signal_text = "BUY (LONG)"
            elif signal == 'SELL':
                signal_emoji = "🔴"
                signal_text = "SELL (SHORT)"
            else:
                signal_emoji = "⚪"
                signal_text = signal
            
            # ✅ ЗМЕНШЕНО ДЕТАЛЬНІСТЬ: прибрано час, ціну входу, TP рівні
            message = f"{signal_emoji} <b>Торговий сигнал: {signal_text}</b>\n"
            message += f"📊 Пара: <b>{symbol}</b>\n"
            message += f"💪 Впевненість: <b>{confidence_str}</b>\n" 
            
            if signal_data.get('volume_surge_active'):
                message += f"⚡ <b>Volume Surge!</b>\n"
            if signal_data.get('super_volume_surge_active'):
                 message += f"🌟 <b>SUPER Volume Surge!</b>\n"
            
            market_regime_info = signal_data.get('market_regime_status', "N/A") 
            message += f"📈 Режим: <b>{market_regime_info}</b>\n"
            
            reason = signal_data.get('reason')
            if reason:
                message += f"📝 Причина: {reason}\n"
            
            return await self.send_message(message)
            
        except Exception as e:
            logger.error(f"Помилка відправки сповіщення про сигнал: {e}", exc_info=True)
            return False
    
    async def send_error_notification(self, error_data: Dict[str, Any]) -> bool:
        """Відправка сповіщення про помилку"""
        if not self.config.get('notification_types', {}).get('errors', True):
            return False
        
        try:
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            error_type = error_data.get('type', 'Unknown')
            error_message_val = error_data.get('message', 'Unknown error')
            # Обмеження довжини повідомлення про помилку
            error_message_str = str(error_message_val)
            if len(error_message_str) > DISPLAY_CONFIG['telegram_error_limit']: # Обмеження, щоб не перевищити ліміти Telegram
                error_message_str = error_message_str[:DISPLAY_CONFIG['telegram_error_limit']] + "..."

            message = f"🚨 <b>Критична помилка</b>\n"
            message += f"⏰ Час: {timestamp}\n"
            message += f"❌ Тип: <b>{error_type}</b>\n"
            message += f"📝 Повідомлення: {error_message_str}\n" # Використовуємо обмежений рядок
            
            if 'symbol' in error_data:
                message += f"📊 Пара: <b>{error_data['symbol']}</b>\n"
            
            if 'action' in error_data:
                message += f"🔧 Дія: <b>{error_data['action']}</b>\n"
            
            # Додамо API відповідь, якщо вона є і це помилка біржі
            if 'api_response' in error_data and "EXCHANGE" in error_type.upper():
                api_response_str = str(error_data['api_response'])
                if len(api_response_str) > DISPLAY_CONFIG['telegram_api_response_limit']:
                    api_response_str = api_response_str[:DISPLAY_CONFIG['telegram_api_response_limit']] + "..."
                message += f"📄 Відповідь API: {api_response_str}\n"

            return await self.send_message(message)
            
        except Exception as e:
            logger.error(f"Помилка відправки помилки: {e}") # Не exc_info, щоб уникнути рекурсії логування
            return False
    
    async def send_market_analysis(self, analysis_data: Dict[str, Any]) -> bool:
        """Відправка аналізу ринку"""
        try:
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            message = f"📊 <b>Аналіз ринку</b>\n"
            message += f"⏰ Час: {timestamp}\n\n"
            
            for symbol, data in analysis_data.items():
                if isinstance(data, dict):
                    signal = data.get('signal', 'HOLD')
                    confidence = data.get('confidence', 0)
                    
                    if signal == 'BUY':
                        signal_emoji = "🟢"
                        signal_text = "LONG"
                    elif signal == 'SELL':
                        signal_emoji = "🔴"
                        signal_text = "SHORT"
                    else:
                        signal_emoji = "⚪"
                        signal_text = "HOLD"
                    
                    message += f"{signal_emoji} <b>{symbol}</b>: {signal_text} ({confidence}/4)\n"
                    
                    if data.get('volume_surge'):
                        message += f"  ⚡ Volume Surge\n"
                    if data.get('super_volume_surge'):
                        message += f"  🌟 SUPER Volume Surge\n"
            
            return await self.send_message(message)
            
        except Exception as e:
            logger.error(f"Помилка відправки аналізу ринку: {e}", exc_info=True)
            return False
    
    async def send_daily_summary(self, summary_data: Dict[str, Any]) -> bool:
        """Відправка денного підсумку"""
        try:
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            message = f"📈 <b>Денний підсумок</b>\n"
            message += f"⏰ Дата: {timestamp[:10]}\n\n"
            
            total_trades = summary_data.get('total_trades', 0)
            winning_trades = summary_data.get('winning_trades', 0)
            losing_trades = summary_data.get('losing_trades', 0)
            
            message += f"🎯 Всього угод: <b>{total_trades}</b>\n"
            
            if total_trades > 0:
                win_rate = (winning_trades / total_trades) * DISPLAY_CONFIG['percentage_multiplier']
                message += f"✅ Прибуткових: <b>{winning_trades}</b>\n"
                message += f"❌ Збиткових: <b>{losing_trades}</b>\n"
                message += f"📊 Вінрейт: <b>{win_rate:.1f}%</b>\n\n"
            
            total_pnl = summary_data.get('total_pnl', 0)
            pnl_emoji = "💚" if total_pnl >= 0 else "❤️"
            message += f"{pnl_emoji} Загальний P&L: <b>{total_pnl:+.4f} USDT</b>\n"
            
            current_balance = summary_data.get('current_balance', 0)
            message += f"💰 Поточний баланс: <b>{current_balance:.2f} USDT</b>\n"
            
            return await self.send_message(message)
            
        except Exception as e:
            logger.error(f"Помилка відправки денного підсумку: {e}", exc_info=True)
            return False
    
    async def send_notification(self, message: str, message_type: str = 'general') -> bool:
        """Універсальний метод для відправки сповіщень з типом повідомлення"""
        if not self.config.get('enable_notifications', True):
            logger.debug(f"Сповіщення вимкнено для типу: {message_type}")
            return False
            
        try:
            # Додаємо емодзі та форматування залежно від типу повідомлення
            formatted_message = message
            
            if message_type == 'signal_prioritization':
                # Вже має форматування з емодзі 🎯
                pass
            elif message_type == 'position_cleanup':
                # Вже має форматування з емодзі ⚠️
                pass
            elif message_type == 'position_tracking':
                # Вже має форматування з емодзі ⚠️
                pass
            elif message_type == 'position_recovery':
                # Вже має форматування з емодзі 🔄
                pass
            elif message_type == 'position_closure':
                # Для закриття позицій додаємо емодзі, якщо його немає
                if not any(emoji in message for emoji in ['🛑', '💎', '🏆', '📊', '⚖️', '⚡', '💨']):
                    formatted_message = f"📍 {message}"
            else:
                # Загальні повідомлення з базовим емодзі
                if not message.startswith(('🤖', '📊', '⚠️', '✅', 'ℹ️', '🔔')):
                    formatted_message = f"ℹ️ {message}"
            
            # Відправляємо через базовий метод send_message
            return await self.send_message(formatted_message)
            
        except Exception as e:
            logger.error(f"Помилка відправки сповіщення типу '{message_type}': {e}")
            return False

    async def send_position_closure_notification(self, closure_details: Dict) -> bool:
        """Відправляє детальне сповіщення про закриття позиції з запобіганням дублюванню"""
        if not closure_details:
            return False
            
        try:
            symbol = closure_details.get('symbol', 'UNKNOWN')
            
            # ✅ ДОДАНО: Перевірка на дублювання для сповіщень про закриття позицій
            if self.enable_deduplication and self.config.get('deduplication', {}).get('position_closure_deduplication', True):
                notification_id = self._generate_notification_id('position_closure', symbol, closure_details)
                
                if self._is_notification_already_sent(notification_id):
                    logger.debug(f"Сповіщення про закриття позиції {symbol} вже відправлено (ID: {notification_id}), пропускаємо")
                    return True  # Повертаємо True оскільки сповіщення технічно "успішне"
            
            closure_type = closure_details.get('closure_display_type', 'Position Closed')
            entry_price = closure_details.get('entry_price', 0)
            exit_price = closure_details.get('exit_price', 0)
            pnl_usdt = closure_details.get('pnl_usdt', 0)
            pnl_percentage = closure_details.get('pnl_percentage', 0)
            quantity = closure_details.get('quantity', 0)
            is_full_closure = closure_details.get('is_full_closure', True)
            closure_time = closure_details.get('closure_time')
            side = closure_details.get('side', 'BUY')
            
            # ✅ CRITICAL FIX: Validate and attempt to recover entry price if corrupted
            if entry_price <= 0:
                # Try to recover entry price from alternative sources
                initial_entry = closure_details.get('initial_entry_price', 0)
                if initial_entry > 0:
                    entry_price = initial_entry
                    logger.warning(f"🔧 Entry price recovered for {symbol}: {entry_price:.6f}")
                else:
                    logger.error(f"❌ Invalid entry price for {symbol}: {entry_price}, using 'N/A' in notification")
                    entry_price = None  # Use None to display 'N/A' instead of 0.000000
            
            # ✅ ПОКРАЩЕНО: Більш детальне визначення емодзі та типу закриття
            emoji = "🎯"  # За замовчуванням
            if "SL Hit" in closure_type or "Stop Loss" in closure_type:
                emoji = "🛑"
            elif "TP" in closure_type and "Hit" in closure_type:
                if any(level in closure_type for level in ["1", "partial_1", "Partial 1"]):
                    emoji = "💎"  # Перший часткий TP
                elif any(level in closure_type for level in ["2", "partial_2", "Partial 2"]):
                    emoji = "💎"  # Другий часткий TP  
                elif any(level in closure_type for level in ["3", "partial_3", "Partial 3"]):
                    emoji = "💎"  # Третій часткий TP
                else:
                    emoji = "🏆"  # Фінальний TP
            elif "Trailing" in closure_type:
                emoji = "⚡"
            elif "Breakeven" in closure_type:
                emoji = "⚖️"
            elif any(keyword in closure_type for keyword in ["Manual", "External", "Sync"]):
                emoji = "💨"
            elif "Liquidation" in closure_type:
                emoji = "🚨"
            elif "Volume Divergence" in closure_type:
                emoji = "📊"
            
            # ✅ ПОКРАЩЕНО: Форматування P&L з кращими кольорами та точністю
            if pnl_usdt > 0:
                pnl_text = f"💰 P&L: +{pnl_usdt:.3f} USDT (+{pnl_percentage:.2f}%)"
            elif pnl_usdt < 0:
                pnl_text = f"💸 P&L: {pnl_usdt:.3f} USDT ({pnl_percentage:.2f}%)"
            else:
                pnl_text = f"💰 P&L: {pnl_usdt:.3f} USDT ({pnl_percentage:.2f}%)"
            
            # ✅ ПОКРАЩЕНО: Інформація про розмір закриття з залишком
            remaining_quantity = closure_details.get('remaining_quantity', 0)
            closure_size_info = "Повне закриття" if is_full_closure else "Часткове закриття"
            remaining_qty_str = ""
            
            if not is_full_closure and remaining_quantity and float(remaining_quantity) > 0.0001:
                try:
                    rem_qty_float = float(remaining_quantity)
                    rem_qty_decimals = 0
                    if '.' in str(rem_qty_float):
                        rem_qty_decimals = len(str(rem_qty_float).split('.')[1])
                    if rem_qty_float < 0.001 and rem_qty_decimals > 8:
                        rem_qty_decimals = 8
                    elif rem_qty_float < 1 and rem_qty_decimals > 6:
                        rem_qty_decimals = 6
                    elif rem_qty_float >= 1 and rem_qty_decimals > 4:
                        rem_qty_decimals = 4
                    remaining_qty_str = f"📦 Залишок: {rem_qty_float:.{rem_qty_decimals}f}\n"
                except (ValueError, TypeError):
                    remaining_qty_str = f"📦 Залишок: {remaining_quantity}\n"
            
            # ✅ ПОКРАЩЕНО: Час закриття з кращим форматуванням
            if closure_time:
                try:
                    if hasattr(closure_time, 'strftime'):
                        time_str = closure_time.strftime('%H:%M:%S UTC')
                    else:
                        time_str = str(closure_time)
                except:
                    time_str = "Невідомо"
            else:
                time_str = "Невідомо"
            
            # ✅ ПОКРАЩЕНО: Напрямок позиції з емодзі
            position_direction = "📈 LONG" if side == "BUY" else "📉 SHORT"
            
            # ✅ ДОДАНО: Розрахунок часу тримання, якщо є дані
            holding_time_str = ""
            if self.add_holding_time:
                entry_time = closure_details.get('entry_time')
                if entry_time and closure_time:
                    try:
                        if isinstance(entry_time, str):
                            entry_dt = datetime.fromisoformat(entry_time.replace('Z', '+00:00'))
                        else:
                            entry_dt = entry_time
                        
                        if isinstance(closure_time, str):
                            closure_dt = datetime.fromisoformat(closure_time.replace('Z', '+00:00'))
                        else:
                            closure_dt = closure_time
                        
                        holding_duration = closure_dt - entry_dt
                        hours = int(holding_duration.total_seconds() // 3600)
                        minutes = int((holding_duration.total_seconds() % 3600) // 60)
                        
                        if hours > 0:
                            holding_time_str = f"⏱️ Тримання: {hours}г {minutes}хв\n"
                        else:
                            holding_time_str = f"⏱️ Тримання: {minutes}хв\n"
                    except Exception as e:
                        logger.debug(f"Помилка розрахунку часу тримання: {e}")
            
            # ✅ CRITICAL FIX: Safe message formatting with entry price validation
            entry_price_display = "N/A" if entry_price is None or entry_price <= 0 else f"{entry_price:.6f}"
            
            message = (
                f"{emoji} <b>{symbol} - {closure_type}</b>\n"
                f"📊 {entry_price_display} → {exit_price:.6f}\n"
                f"{pnl_text}\n"
                f"📦 Кількість: {quantity:.8f} ({closure_size_info})\n"
                f"{remaining_qty_str}"
                f"{position_direction}\n"
                f"{holding_time_str}"
                f"🕐 Закрито: {time_str}"
            )
            
            # ✅ ДОДАНО: Відправляємо повідомлення та позначаємо як відправлене
            result = await self.send_notification(message, 'position_closure')
            if result and self.enable_deduplication and self.config.get('deduplication', {}).get('position_closure_deduplication', True):
                self._mark_notification_as_sent(notification_id)
                logger.debug(f"Сповіщення про закриття позиції {symbol} відправлено (ID: {notification_id})")
            
            return result
            
        except Exception as e:
            logger.error(f"Помилка відправки сповіщення про закриття позиції: {e}")
            return False

    async def test_connection(self) -> bool:
        """Тестування з'єднання з Telegram"""
        if not self.bot:
            return False
        
        try:
            test_message = "🤖 Тест з'єднання: Telegram бот працює!"
            return await self.send_message(test_message)
            
        except Exception as e:
            logger.error(f"Помилка тестування Telegram з'єднання: {e}")
            return False