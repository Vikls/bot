# -*- coding: utf-8 -*-
"""
Налаштування торгового бота
"""

import os
import logging
from typing import Dict, List, Any
from dotenv import load_dotenv

# Завантаження змінних середовища
load_dotenv('config/.env')

# ===== API НАЛАШТУВАННЯ =====
API_CONFIG = {
    'live': {
        'api_key': os.getenv('BYBIT_LIVE_API_KEY'),
        'api_secret': os.getenv('BYBIT_LIVE_SECRET_KEY'),
    },
    'demo': {
        'api_key': os.getenv('BYBIT_DEMO_API_KEY'),
        'api_secret': os.getenv('BYBIT_DEMO_SECRET_KEY'),
    },
    'testnet': False,  # ЗАВЖДИ False!
    'unified_account': True,
    'rate_limit': 3,  # запитів на секунду
    'retry_attempts': 3,
    'retry_delay': 1.0,
    
    # ✅ ВИПРАВЛЕНО: Реальні комісії Bybit
    'commission_rate_maker': 0.001,  # 0.055% для maker ордерів
    'commission_rate_taker': 0.001,  # 0.075% для taker ордерів
    'commission_rate_default': 0.001,  # За замовчуванням maker (більш консервативно)
}

# ===== ТОРГОВІ НАЛАШТУВАННЯ =====
TRADING_CONFIG = {
    'mode': os.getenv('TRADING_MODE', 'DEMO'),  # LIVE або DEMO
    'trade_pairs': ['STRKUSDT', 'ADAUSDT', 'ETHUSDT',  'SOLUSDT', 'BTCUSDT', 'ZKUSDT',

    # Додаткові популярні та середньо-популярні пари
    'XRPUSDT', 'DOGEUSDT', 'AVAXUSDT', 'DOTUSDT', 'TRXUSDT', 'LTCUSDT',
    'LINKUSDT', 'UNIUSDT', 'ETCUSDT', 'XLMUSDT', 'NEARUSDT', 'ALGOUSDT',
    'VETUSDT', 'ICPUSDT', 'FILUSDT', 'SANDUSDT', 'MANAUSDT', 'AXSUSDT', 'AAVEUSDT',
    'XTZUSDT', 'THETAUSDT', 'GRTUSDT', 'MKRUSDT', 'RUNEUSDT', 'EGLDUSDT', 'KSMUSDT',

    # Деякі менш популярні або новіші пари для різноманітності
    'CHZUSDT', 'ENJUSDT', 'SUSHIUSDT', 'SNXUSDT', 'CRVUSDT', 'DYDXUSDT',
    'GALAUSDT', 'APEUSDT', 'GMTUSDT', 'OPUSDT', 'APTUSDT', 'ARBUSDT', 'SUIUSDT', 'BLURUSDT',
    'SEIUSDT', 'TIAUSDT'],
    'timeframe': '5',  # хвилини
    'load_candles_amount': 500,
    'min_order_amount': 10,  # відсоток від балансу на одну угоду
    'max_orders_qty': 5,     # максимальна кількість одночасних позицій
    'leverage': 10,          # кредитне плече
    'min_order_value_usdt': 5.0,  # мінімальна вартість ордера в USDT (без врахування левереджа)
    'delay_after_market_order_ms': 2000,  # затримка після розміщення ордера
    'delay_between_symbols_ms': 500,      # затримка між символами
    'balance_report_interval_minutes': 30, # інтервал звітів про баланс
    'trade_cycle_buffer_seconds': 15,      # буфер торгового циклу
    'main_loop_error_sleep_seconds': 60,   # пауза при помилці в основному циклі
    'min_sl_market_distance_tick_multiplier': 5,  # мінімальна відстань SL від ринку в тіках
    'position_sync_enabled': True,  # Включити після виправлення
    'position_check_interval_seconds': 30,  # Швидка перевірка кожні 30 сек
    'sync_check_interval_minutes': 2,       # Повна синхронізація кожні 2 хв
    'balance_report_interval_minutes': 15,  # Звіт балансу кожні 15 хв
    'sync_lookback_hours': 72,  # Аналізувати останні 72 години
    'sync_lookback_hours_short': 24,  # Короткий lookback для швидких перевірок
    'sync_tolerance_qty': 0.0000001,  # Толерантність для кількості
    'sync_tolerance_price_percentage': 0.002,  # Толерантність для цін (0.2%)
    'sync_debug_mode': True,  # Детальне логування синхронізації
    'min_trade_quantity_threshold': 0.000001,  # Мінімальна кількість для вважання позиції активною
}

# ===== НАЛАШТУВАННЯ ІНДИКАТОРІВ =====
INDICATORS_CONFIG = {
    # RSI
    'rsi_length': 14,
    'rsi_overbought': 70,
    'rsi_oversold': 30,
    
    # EMA
    'fast_ma': 8,
    'slow_ma': 21,
    'trend_ema': 21,
    
    # MACD
    'macd_fast': 12,
    'macd_slow': 26,
    'macd_signal': 9,
    
    # ADX
    'adx_period': 14,
    'adx_threshold': 20,
    
    # ATR
    'atr_length': 14,
    
    # Volume
    'volume_lookback': 5,
    'min_volume_mult': 1.1,
    'volume_surge_mult': 4.0,
    'super_volume_mult': 8.0,
    'consecutive_vol_bars': 2,
    'vol_divergence_period': 5,
    
    # Warmup periods for indicator validation
    'warmup_periods': {
        'rsi': 14,          # RSI needs ~14 periods
        'ema_fast': 12,     # Fast EMA needs ~12 periods  
        'ema_slow': 26,     # Slow EMA needs ~26 periods
        'macd_line': 35,    # MACD line needs slow_ema + buffer (26 + 9)
        'macd_signal': 44,  # MACD signal needs macd_line + signal_ema (35 + 9)
        'macd_histogram': 44, # Same as MACD signal
        'atr': 14,          # ATR needs ~14 periods
        'adx': 28,          # ADX needs ~2x period (14 * 2)
    },
}

# ===== СТРАТЕГІЯ =====
STRATEGY_CONFIG = {
    # Часові фільтри
    'use_time_filter': False,
    'avoid_early_hours': True,
    'avoid_late_hours': True,
    'avoid_lunch_time': True,
    'early_session_start': 0,
    'early_session_end': 2,
    'late_session_start': 21,
    'late_session_end': 23,
    'lunch_time_start': 12,
    'lunch_time_end': 14,
    
    # Адаптивний ризик
    'use_adaptive_risk': True,
    'risk_reduction_percent': 25.0,
    
    # Ринкові режими
    'use_market_regime': True,
    'regime_period': 20,
    'trend_strength': 1.5,
    'volatility_threshold': 0.02,
    'momentum_period': 10,
    
    # Адаптивні параметри (СТАНДАРТИЗОВАНО для узгодженості)
    'use_adaptive_params': True,
    'trending_min_conf_long': 2,
    'trending_min_conf_short': 2,
    'trending_adx_thresh': 20,  # Стандартизовано: сильний тренд
    'trending_tp_mult': 2.8,
    'trending_sl_mult': 1.0,
    
    'ranging_min_conf_long': 2,
    'ranging_min_conf_short': 2,
    'ranging_adx_thresh': 15,  # Стандартизовано: слабкий тренд/флет
    'ranging_tp_mult': 1.5,
    'ranging_sl_mult': 0.8,
    
    'mixed_min_conf_long': 2,
    'mixed_min_conf_short': 2,
    'mixed_adx_thresh': 18,   # Стандартизовано: середній тренд
    'mixed_tp_mult': 2.2,
    'mixed_sl_mult': 1.0,
    
    # Lightning Volume System
    'use_lightning_volume': True,
    'vol_surge_tp_boost': 15.0,
    'super_volume_tp_boost': 35.0,
    'use_volume_extension': True,
    'volume_extension_mult': 1.3,
    
     # ✅ НОВА СИСТЕМА TP (без Final TP)
    'use_triple_partial_tp': True,
    'first_partial_percent': 30.0,    # Змінено з 15.0 на 30.0
    'first_partial_multiplier': 0.8,
    'second_partial_percent': 50.0,   # Змінено з 25.0 на 50.0  
    'second_partial_multiplier': 1.3,
    'third_partial_percent': 20.0,    # Змінено з 30.0 на 20.0
    'third_partial_multiplier': 1.8,
    
    # ✅ ВИМКНУТО Final TP
    'use_final_tp': False,  # Новий параметр
    
    'use_breakeven': True,
    'breakeven_buffer': 0.05,
    'breakeven_min_buffer_ticks': 3,  # мінімальний буфер в тіках для беззбитка
    
    # Trailing Stop
    'use_trailing_stop': True,
    'trail_atr_mult': 0.7,

    # Volume Divergence
    'use_volume_divergence': True,
    'volume_divergence_close_percent': 50.0, # Відсоток закриття

    # Stop Loss налаштування
    'sl_atr_multiplier': 1.5,  # множник ATR для розрахунку SL
     # ✅ НОВІ НАЛАШТУВАННЯ для кращого розрахунку TP
    'final_tp_safety_buffer': 0.2,  # Додатковий буфер для final TP (в частках ATR)
    'min_tp_distance': 0.1,         # Мінімальна відстань між TP рівнями (в частках ATR)
    'strict_tp_order_validation': True,  # Строга перевірка порядку TP

    # Налаштування пріоритизації сигналів
    'signal_strength_weights': {
        'confirmations': 25.0,      # Вага технічних підтверджень
        'volume_surge': 20.0,       # Вага об'ємного сплеску
        'adx_strength': 15.0,       # Вага силі тренду (ADX)
        'volatility': 10.0,         # Вага волатильності (ATR)
        'divergence': 15.0,         # Вага дивергенції
        'market_regime': 10.0,      # Вага ринкового режиму
        'trend_alignment': 5.0      # Вага вирівнювання тренду
    },
    'enable_signal_prioritization': True,  # Включити пріоритизацію сигналів при досягненні max_orders_qty

    # ===== НОВІ НАЛАШТУВАННЯ: Покращення логіки торгового циклу =====
    # Pre-Cycle Position Validation
    'pre_cycle_validation': {
        'enable': True,
        'seconds_before_cycle': 7,  # За скільки секунд до циклу робити валідацію
        'cleanup_invalid_positions': True,
    },
    # Signal Buffer System  
    'signal_buffer': {
        'enable': True,
        'collect_all_signals_first': True,
        'max_buffer_size': 20,  # Максимальна кількість сигналів в буфері
    },
    # Telegram Filtering
    'telegram_filtering': {
        'notify_only_selected_signals': True,
        'include_filtering_stats': True,
    }
}

# ===== БАЗА ДАНИХ =====
DATABASE_CONFIG = {
    'db_path': os.getenv('DB_PATH', 'data/candles.db'),
    'connection_timeout': 30.0,
    'isolation_level': None,
}

# ===== TELEGRAM =====
TELEGRAM_CONFIG = {
    'bot_token': os.getenv('TELEGRAM_BOT_TOKEN'),
    'chat_id': os.getenv('TELEGRAM_CHAT_ID'),
    'enable_notifications': True,
    'notification_types': {
        'trades': True,
        'errors': True,
        'status': True,
        'balance': True,
    },
    # ✅ ДОДАНО: Налаштування системи запобігання дублюванню
    'deduplication': {
        'enable_deduplication': True,  # Включити систему запобігання дублюванню
        'notification_expiry_hours': 24,  # Час зберігання записів про відправлені сповіщення
        'cleanup_threshold': 100,  # Кількість записів після якої запускається очищення
        'position_closure_deduplication': True,  # Дедуплікація для закриття позицій
        'trade_notification_deduplication': True,  # Дедуплікація для торгових сповіщень
    },
    # ✅ ДОДАНО: Налаштування покращення повідомлень
    'message_enhancement': {
        'improve_closure_reasons': True,  # Покращувати читабельність причин закриття
        'add_holding_time': True,  # Додавати час тримання позиції
        'add_price_change_info': True,  # Додавати інформацію про зміну ціни
        'enhanced_formatting': True,  # Покращене форматування з емодзі
    }
}

# ===== НАЛАШТУВАННЯ ЛОГУВАННЯ =====
LOGGING_CONFIG = {
    'level': os.getenv('LOG_LEVEL', 'DEBUG'),
    'trade_log_level': 'INFO',  # окремий рівень для торгових логів
    'enable_console': os.getenv('ENABLE_CONSOLE_LOGGING', 'True').lower() == 'true',
    'log_dir': 'logs',
    'max_file_size': 10 * 1024 * 1024,  # 10MB
    'backup_count': 5,
    'config_changes_log': True,  # логування змін конфігурації
    'profile_changes_log': True,  # логування змін профілів
}

# ===== ДИНАМІЧНІ НАЛАШТУВАННЯ НА ОСНОВІ СЕРЕДОВИЩА =====
def get_environment_multipliers():
    """Отримання множників для поточного середовища"""
    env = os.getenv('ENVIRONMENT', 'development').lower()
    env_config = get_environment_config()
    
    cache_multiplier = float(env_config.get('cache_ttl_multiplier', 1.0))
    
    # Застосовуємо множники до системних параметрів
    return {
        'cache_ttl_default': int(SYSTEM_CONFIG['cache_ttl_default'] * cache_multiplier),
        'cache_ttl_short': int(SYSTEM_CONFIG['cache_ttl_short'] * cache_multiplier),
        'cache_ttl_long': int(SYSTEM_CONFIG['cache_ttl_long'] * cache_multiplier),
    }

# ===== СИСТЕМНІ НАЛАШТУВАННЯ =====
SYSTEM_CONFIG = {
    # Кеш налаштування
    'cache_ttl_default': 300,  # 5 хвилин для загальних даних
    'cache_ttl_short': 60,     # 1 хвилина для частих запитів
    'cache_ttl_long': 3600,    # 1 година для рідкісних даних
    
    # Пам'ять та продуктивність
    'max_memory_usage_mb': 200,
    'critical_memory_usage_mb': 300,
    'chunk_size_default': 500,
    'chunk_size_large': 100,
    'max_concurrent_connections': 100,
    'max_connections_per_host': 20,
    'keepalive_timeout': 300,
    'dns_cache_ttl': 300,
    
    # Тайм-аути та затримки
    'default_timeout': 30,
    'connect_timeout': 10,
    'session_lifetime': 3600,  # 1 година
    'max_retry_delay': 60.0,
    'circuit_breaker_recovery_timeout': 300,
    'circuit_breaker_failure_threshold': 5,
    
    # Обмеження запитів
    'max_concurrent_requests': 5,
    'default_request_limit': 1000,
    'max_request_limit': 10000,
    'min_request_limit': 50,
    'execution_history_limit': 100,
    
    # Часові константи (в мілісекундах)
    'ms_per_second': 1000,
    'ms_per_minute': 60000,
    'ms_per_hour': 3600000,
    'ms_per_day': 86400000,
    'ms_per_week': 604800000,
}

# ===== НАЛАШТУВАННЯ ТОЧНОСТІ ТА ФОРМАТУВАННЯ =====
PRECISION_CONFIG = {
    # Числова точність
    'price_tolerance': 0.000001,
    'qty_tolerance': 0.0000001,
    'percentage_tolerance': 0.1,
    'price_diff_threshold': 0.000001,
    
    # Форматування цін
    'price_decimals_high': 2,    # для цін > 100
    'price_decimals_medium': 4,  # для цін 1-100
    'price_decimals_low': 6,     # для цін < 1
    'default_decimals': 6,
    
    # Відображення відсотків
    'percentage_precision': 3,
    'win_rate_precision': 1,
    
    # Часові формати
    'datetime_format': '%Y-%m-%d %H:%M:%S UTC',
    'datetime_format_short': '%Y-%m-%d %H:%M:%S',
    'date_format': '%Y-%m-%d',
    'time_format': '%H:%M:%S',
}

# ===== НАЛАШТУВАННЯ ВІДОБРАЖЕННЯ ТА ПОВІДОМЛЕНЬ =====
DISPLAY_CONFIG = {
    # Обмеження довжини повідомлень
    'telegram_message_limit': 1000,
    'telegram_error_limit': 1000,
    'telegram_api_response_limit': 500,
    'reason_display_limit': 250,
    'max_variable_display_length': 100,
    
    # Показники ефективності
    'memory_effectiveness_multiplier': 100,
    'percentage_multiplier': 100,
    
    # Відображення позицій
    'min_position_size_display': 0.0000001,
    'min_remaining_qty_display': 0.0000001,
    
    # Налаштування для аналізу
    'min_records_for_analysis': 50,
    'min_records_for_reliable_check': 100,
    'min_total_records_multiplier': 2,
    'base_minimum_records': 50,
    
    # Налаштування стратегії
    'rsi_neutral_value': 50.0,
    'min_candles_for_strategy': 100,
    'num_candles_for_recalc_base': 100,
    'recalc_candles_limit': 150,
}

# ===== ПРОФІЛІ КОНФІГУРАЦІЇ =====
CONFIG_PROFILES = {
    'conservative': {
        'description': 'Консервативна стратегія з мінімальним ризиком',
        'trading_overrides': {
            'min_order_amount': 5,  # менший розмір позиції
            'leverage': 5,          # менший леверидж
            'max_orders_qty': 3,    # менше одночасних позицій
        },
        'strategy_overrides': {
            'sl_atr_multiplier': 2.0,     # більший стоп-лосс
            'trending_tp_mult': 2.0,      # менші тейк-профіти
            'ranging_tp_mult': 1.2,
            'mixed_tp_mult': 1.8,
            'use_trailing_stop': True,
            'trail_atr_mult': 1.0,        # більший трейлінг
        },
        'indicators_overrides': {
            'rsi_overbought': 75,         # більш консервативні рівні RSI
            'rsi_oversold': 25,
            'adx_threshold': 25,          # вищий поріг тренду
        }
    },
    'balanced': {
        'description': 'Збалансована стратегія з помірним ризиком',
        'trading_overrides': {
            'min_order_amount': 10,
            'leverage': 10,
            'max_orders_qty': 5,
        },
        'strategy_overrides': {
            'sl_atr_multiplier': 1.5,
            'trending_tp_mult': 2.8,
            'ranging_tp_mult': 1.5,
            'mixed_tp_mult': 2.2,
        },
        'indicators_overrides': {
            'rsi_overbought': 70,
            'rsi_oversold': 30,
            'adx_threshold': 20,
        }
    },
    'aggressive': {
        'description': 'Агресивна стратегія з високим ризиком',
        'trading_overrides': {
            'min_order_amount': 15,  # більший розмір позиції
            'leverage': 15,          # більший леверидж
            'max_orders_qty': 8,     # більше одночасних позицій
        },
        'strategy_overrides': {
            'sl_atr_multiplier': 1.0,     # менший стоп-лосс
            'trending_tp_mult': 3.5,      # більші тейк-профіти
            'ranging_tp_mult': 2.0,
            'mixed_tp_mult': 3.0,
            'trail_atr_mult': 0.5,        # агресивніший трейлінг
        },
        'indicators_overrides': {
            'rsi_overbought': 65,         # більш агресивні рівні RSI
            'rsi_oversold': 35,
            'adx_threshold': 15,          # нижчий поріг тренду
        }
    }
}

def get_configuration_summary() -> Dict[str, Any]:
    """Отримання повного опису поточної конфігурації"""
    active_profile = get_active_profile()
    env_config = get_environment_config()
    env_multipliers = get_environment_multipliers()
    
    return {
        'profile': {
            'name': active_profile,
            'description': CONFIG_PROFILES.get(active_profile, {}).get('description', ''),
            'available_profiles': list(CONFIG_PROFILES.keys())
        },
        'environment': {
            'name': os.getenv('ENVIRONMENT', 'development'),
            'config': env_config,
            'multipliers': env_multipliers
        },
        'system': {
            'memory_limit_mb': SYSTEM_CONFIG['max_memory_usage_mb'],
            'cache_ttl_default': env_multipliers['cache_ttl_default'],
            'chunk_size': SYSTEM_CONFIG['chunk_size_default']
        },
        'precision': {
            'price_tolerance': PRECISION_CONFIG['price_tolerance'],
            'qty_tolerance': PRECISION_CONFIG['qty_tolerance']
        },
        'display': {
            'message_limits': {
                'telegram': DISPLAY_CONFIG['telegram_message_limit'],
                'error': DISPLAY_CONFIG['telegram_error_limit'],
                'api_response': DISPLAY_CONFIG['telegram_api_response_limit']
            }
        }
    }

def log_configuration_startup():
    """Логування конфігурації при запуску системи"""
    logger = logging.getLogger(__name__)
    
    if LOGGING_CONFIG.get('config_changes_log', True):
        summary = get_configuration_summary()
        
        logger.info("=== КОНФІГУРАЦІЯ ТОРГОВОГО БОТА ===")
        logger.info(f"Профіль: {summary['profile']['name']} - {summary['profile']['description']}")
        logger.info(f"Середовище: {summary['environment']['name']}")
        logger.info(f"Ліміт пам'яті: {summary['system']['memory_limit_mb']} MB")
        logger.info(f"Кеш TTL: {summary['system']['cache_ttl_default']}s")
        logger.info(f"Розмір чанка: {summary['system']['chunk_size']}")
        logger.info(f"Толерантність цін: {summary['precision']['price_tolerance']}")
        logger.info("=====================================")

# ===== ВАЛІДАЦІЯ КОНФІГУРАЦІЇ =====
def validate_config():
    """Валідація конфігурації при запуску"""
    errors = []
    
    # Перевірка API конфігурації
    mode = TRADING_CONFIG['mode'].upper()
    if mode not in ['LIVE', 'DEMO']:
        errors.append(f"Невірний trading mode: {mode}. Має бути LIVE або DEMO")
    
    api_creds = API_CONFIG['demo'] if mode == 'DEMO' else API_CONFIG['live']
    if not api_creds['api_key'] or not api_creds['api_secret']:
        errors.append(f"API ключі не налаштовані для режиму {mode}")
    
    # Перевірка торгових пар
    if not TRADING_CONFIG['trade_pairs'] or len(TRADING_CONFIG['trade_pairs']) == 0:
        errors.append("Не вказано торгові пари")
    
    # Перевірка числових параметрів
    if TRADING_CONFIG['leverage'] <= 0 or TRADING_CONFIG['leverage'] > 100:
        errors.append(f"Невірний леверидж: {TRADING_CONFIG['leverage']}")
    
    if TRADING_CONFIG['min_order_amount'] <= 0 or TRADING_CONFIG['min_order_amount'] > 100:
        errors.append(f"Невірний розмір ордера: {TRADING_CONFIG['min_order_amount']}%")
    
    # Перевірка Telegram конфігурації
    if TELEGRAM_CONFIG['enable_notifications']:
        if not TELEGRAM_CONFIG['bot_token'] or not TELEGRAM_CONFIG['chat_id']:
            errors.append("Telegram налаштування не повні при включених сповіщеннях")
    
    # Перевірка системних параметрів
    if SYSTEM_CONFIG['max_memory_usage_mb'] <= 0:
        errors.append("Максимальне використання пам'яті має бути більше 0")
    
    if SYSTEM_CONFIG['chunk_size_default'] <= 0:
        errors.append("Розмір чанка має бути більше 0")
    
    # Перевірка параметрів точності
    if PRECISION_CONFIG['price_tolerance'] <= 0:
        errors.append("Толерантність цін має бути більше 0")
    
    # Перевірка параметрів відображення
    if DISPLAY_CONFIG['telegram_message_limit'] <= 0:
        errors.append("Ліміт повідомлень Telegram має бути більше 0")
    
    return errors

# ===== ФУНКЦІЇ КЕРУВАННЯ КОНФІГУРАЦІЄЮ =====
def get_active_profile() -> str:
    """Отримання активного профілю конфігурації"""
    return os.getenv('CONFIG_PROFILE', 'balanced').lower()

def apply_config_profile(profile_name: str = None) -> Dict[str, Any]:
    """Застосування профілю конфігурації з логуванням"""
    if profile_name is None:
        profile_name = get_active_profile()
    
    logger = logging.getLogger(__name__)
    
    if profile_name not in CONFIG_PROFILES:
        logger.warning(f"Профіль '{profile_name}' не знайдено. Використовується 'balanced'")
        profile_name = 'balanced'
    
    profile = CONFIG_PROFILES[profile_name]
    
    # Логуємо застосування профілю
    if LOGGING_CONFIG.get('profile_changes_log', True):
        logger.info(f"Застосування профілю конфігурації: {profile_name}")
        logger.info(f"Опис профілю: {profile.get('description', 'Без опису')}")
    
    # Копіюємо базові конфігурації
    trading_config = TRADING_CONFIG.copy()
    strategy_config = STRATEGY_CONFIG.copy()
    indicators_config = INDICATORS_CONFIG.copy()
    
    # Логуємо зміни
    changes_log = []
    
    # Застосовуємо overrides з логуванням
    if 'trading_overrides' in profile:
        for key, value in profile['trading_overrides'].items():
            old_value = trading_config.get(key)
            trading_config[key] = value
            if old_value != value:
                changes_log.append(f"trading.{key}: {old_value} → {value}")
    
    if 'strategy_overrides' in profile:
        for key, value in profile['strategy_overrides'].items():
            old_value = strategy_config.get(key)
            strategy_config[key] = value
            if old_value != value:
                changes_log.append(f"strategy.{key}: {old_value} → {value}")
    
    if 'indicators_overrides' in profile:
        for key, value in profile['indicators_overrides'].items():
            old_value = indicators_config.get(key)
            indicators_config[key] = value
            if old_value != value:
                changes_log.append(f"indicators.{key}: {old_value} → {value}")
    
    # Логуємо всі зміни
    if changes_log and LOGGING_CONFIG.get('config_changes_log', True):
        logger.info(f"Зміни конфігурації для профілю '{profile_name}':")
        for change in changes_log:
            logger.info(f"  - {change}")
    
    return {
        'profile': profile_name,
        'description': profile.get('description', ''),
        'trading': trading_config,
        'strategy': strategy_config,
        'indicators': indicators_config,
        'changes': changes_log
    }

def get_environment_config() -> Dict[str, str]:
    """Отримання конфігурації для поточного середовища"""
    env = os.getenv('ENVIRONMENT', 'development').lower()
    
    env_configs = {
        'development': {
            'log_level': 'DEBUG',
            'enable_console_logging': 'True',
            'cache_ttl_multiplier': 0.5,  # Коротший кеш для розробки
        },
        'staging': {
            'log_level': 'INFO',
            'enable_console_logging': 'True',
            'cache_ttl_multiplier': 1.0,
        },
        'production': {
            'log_level': 'WARNING',
            'enable_console_logging': 'False',
            'cache_ttl_multiplier': 2.0,  # Довший кеш для продакшну
        }
    }
    
    return env_configs.get(env, env_configs['development'])

def reload_configuration():
    """Динамічне перезавантаження конфігурації з детальним логуванням"""
    import importlib
    import sys
    
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("Початок перезавантаження конфігурації...")
        
        # Зберігаємо поточний стан для порівняння
        old_profile = get_active_profile()
        old_env = os.getenv('ENVIRONMENT', 'development').lower()
        
        # Перезавантажуємо змінні середовища
        load_dotenv('config/.env', override=True)
        
        # Перезавантажуємо модуль конфігурації
        if 'config.settings' in sys.modules:
            importlib.reload(sys.modules['config.settings'])
        
        # Отримуємо новий стан
        new_profile = get_active_profile()
        new_env = os.getenv('ENVIRONMENT', 'development').lower()
        
        # Логуємо зміни
        changes = []
        if old_profile != new_profile:
            changes.append(f"Профіль: {old_profile} → {new_profile}")
        if old_env != new_env:
            changes.append(f"Середовище: {old_env} → {new_env}")
        
        if changes:
            logger.info("Зміни після перезавантаження:")
            for change in changes:
                logger.info(f"  - {change}")
        else:
            logger.info("Конфігурація перезавантажена без змін")
        
        # Застосовуємо новий профіль
        profile_config = apply_config_profile()
        
        logger.info(f"Конфігурацію успішно перезавантажено. Активний профіль: {new_profile}")
        
        return {
            'success': True,
            'old_profile': old_profile,
            'new_profile': new_profile,
            'changes': changes,
            'profile_config': profile_config
        }
        
    except Exception as e:
        logger.error(f"Помилка при перезавантаженні конфігурації: {e}")
        return {
            'success': False,
            'error': str(e)
        }

def validate_config_section(section_name: str, section_config: Dict[str, Any]) -> List[str]:
    """Валідація окремої секції конфігурації"""
    errors = []
    
    if section_name == 'system':
        if section_config.get('max_memory_usage_mb', 0) <= 0:
            errors.append("max_memory_usage_mb має бути більше 0")
        if section_config.get('chunk_size_default', 0) <= 0:
            errors.append("chunk_size_default має бути більше 0")
    
    elif section_name == 'precision':
        if section_config.get('price_tolerance', 0) <= 0:
            errors.append("price_tolerance має бути більше 0")
        if section_config.get('qty_tolerance', 0) <= 0:
            errors.append("qty_tolerance має бути більше 0")
    
    elif section_name == 'display':
        if section_config.get('telegram_message_limit', 0) <= 0:
            errors.append("telegram_message_limit має бути більше 0")
    
    return errors

# Валідуємо конфігурацію при імпорті
_config_errors = validate_config()
if _config_errors:
    import logging
    logger = logging.getLogger(__name__)
    for error in _config_errors:
        logger.error(f"Помилка конфігурації: {error}")
    raise ValueError(f"Знайдено {len(_config_errors)} помилок конфігурації")

# Логуємо конфігурацію при завантаженні
try:
    log_configuration_startup()
except Exception as e:
    # Не викидаємо помилку, якщо логування не вдалося
    print(f"Попередження: Не вдалося залогувати конфігурацію при запуску: {e}")