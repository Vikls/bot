# Додаємо нові імпорти якщо потрібно
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional, List, Tuple
import logging
import asyncio

class TradingBot:
    async def sync_positions_with_execution_history(self) -> Dict[str, Dict]:
        """
        Оновлена функція синхронізації позицій використовуючи виключно історію торгів.
        """
        try:
            self.logger.info("🔄 Початок повної синхронізації позицій через історію торгів")
            sync_results = {}
            
            # Отримуємо повну історію торгів для всіх символів
            symbols_to_sync = set(self.positions.keys()) | set(TRADING_CONFIG['trade_pairs'])
            
            for symbol in symbols_to_sync:
                try:
                    # Використовуємо нову функцію повної синхронізації
                    position_data, history_details = await self.api_manager.full_trade_history_sync(
                        symbol=symbol,
                        lookback_hours=TRADING_CONFIG.get('sync_lookback_hours', 48)
                    )
                    
                    if position_data:
                        # Оновлюємо або створюємо позицію
                        if position_data.get('size', 0) > TRADING_CONFIG.get('min_trade_quantity_threshold', 0.000001):
                            self.positions[symbol] = {
                                'side': position_data['side'],
                                'remaining_quantity': position_data['size'],
                                'entry_price': position_data['avg_price'],
                                'current_price': position_data['latest_price'],
                                'unrealized_pnl': position_data['unrealized_pnl'],
                                'last_sync_time': datetime.now(timezone.utc),
                                'sync_source': 'full_history_sync',
                                'trade_history': history_details
                            }
                            sync_results[symbol] = {
                                'status': 'updated',
                                'action': 'position_synced_from_history',
                                'details': position_data
                            }
                        else:
                            # Видаляємо позицію якщо вона занадто мала
                            if symbol in self.positions:
                                del self.positions[symbol]
                            sync_results[symbol] = {
                                'status': 'closed',
                                'action': 'position_removed_too_small',
                                'details': position_data
                            }
                    else:
                        # Позиція не знайдена в історії - видаляємо локально
                        if symbol in self.positions:
                            del self.positions[symbol]
                        sync_results[symbol] = {
                            'status': 'closed',
                            'action': 'position_not_found_in_history',
                            'details': None
                        }
                
                except Exception as e:
                    self.logger.error(f"Помилка синхронізації {symbol}: {str(e)}", exc_info=True)
                    sync_results[symbol] = {
                        'status': 'error',
                        'action': 'sync_failed',
                        'error': str(e)
                    }
            
            self.logger.info(f"✅ Синхронізація завершена. Оброблено {len(symbols_to_sync)} символів")
            return sync_results
            
        except Exception as e:
            self.logger.error(f"Критична помилка під час синхронізації позицій: {str(e)}", exc_info=True)
            return {'status': 'error', 'error': str(e)}

    async def validate_positions(self) -> None:
        """
        Оновлена функція валідації позицій з використанням історії торгів
        """
        try:
            self.logger.info("🔍 Початок валідації позицій")
            validated_count = 0
            
            for symbol in list(self.positions.keys()):
                try:
                    position_data, _ = await self.api_manager.full_trade_history_sync(
                        symbol=symbol,
                        lookback_hours=TRADING_CONFIG.get('sync_lookback_hours_short', 24)
                    )
                    
                    if not position_data or position_data.get('size', 0) <= TRADING_CONFIG.get('min_trade_quantity_threshold', 0.000001):
                        self.logger.warning(f"❌ Позиція {symbol} не знайдена в історії або занадто мала - видалення")
                        del self.positions[symbol]
                    else:
                        validated_count += 1
                        
                except Exception as e:
                    self.logger.error(f"Помилка валідації {symbol}: {str(e)}", exc_info=True)
            
            self.logger.info(f"✅ Валідація завершена. Підтверджено {validated_count} позицій")
            
        except Exception as e:
            self.logger.error(f"Критична помилка під час валідації позицій: {str(e)}", exc_info=True)