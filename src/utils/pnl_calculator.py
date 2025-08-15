# -*- coding: utf-8 -*-
"""
Unified P&L Calculator
Consolidates all P&L calculation logic into a single, well-tested implementation.
"""

import logging
from typing import Dict, Any, Tuple, Union, Optional
from dataclasses import dataclass


@dataclass
class PnLResult:
    """Result of P&L calculation with detailed information"""
    pnl_raw: float  # P&L without commission
    pnl_net: float  # P&L after commission
    commission: float  # Total commission paid
    status: str  # Calculation status/message
    is_valid: bool  # Whether calculation was successful


class PnLCalculator:
    """
    Unified P&L calculator for trading positions.
    
    Handles both BUY and SELL positions with configurable commission rates.
    Thread-safe and async-compatible.
    """
    
    def __init__(self, api_config: Optional[Dict[str, Any]] = None, logger: Optional[logging.Logger] = None):
        """
        Initialize the P&L calculator.
        
        Args:
            api_config: API configuration dictionary containing commission rates
            logger: Logger instance for error reporting
        """
        self.logger = logger or logging.getLogger(__name__)
        
        # Set commission rates from config or use defaults
        if api_config:
            self.commission_rate_default = api_config.get('commission_rate_default', 0.001)
            self.commission_rate_maker = api_config.get('commission_rate_maker', 0.001)
            self.commission_rate_taker = api_config.get('commission_rate_taker', 0.001)
        else:
            self.commission_rate_default = 0.001
            self.commission_rate_maker = 0.001
            self.commission_rate_taker = 0.001
    
    def _validate_inputs(self, entry_price: float, close_price: float, 
                        quantity: float, side: str) -> Tuple[bool, str]:
        """
        Validate input parameters for P&L calculation.
        
        Returns:
            Tuple of (is_valid, error_message)
        """
        # Check if all parameters are provided
        if any(param is None for param in [entry_price, close_price, quantity, side]):
            return False, "Missing required parameters"
        
        # Validate numeric parameters
        try:
            entry_price = float(entry_price)
            close_price = float(close_price)
            quantity = float(quantity)
        except (ValueError, TypeError):
            return False, "Invalid numeric parameters"
        
        # ✅ ENHANCED: Check for positive values with better error messages
        if entry_price <= 0:
            return False, f"Invalid entry_price: {entry_price} (must be > 0). This often indicates position data corruption."
        
        if close_price <= 0:
            return False, f"Invalid close_price: {close_price} (must be > 0)"
        
        if quantity <= 0:
            return False, f"Invalid quantity: {quantity} (must be > 0)"
        
        # Validate side
        if not isinstance(side, str) or side.upper() not in ["BUY", "SELL"]:
            return False, f"Invalid side: {side} (must be 'BUY' or 'SELL')"
        
        # ✅ NEW: Additional sanity checks
        if entry_price > 1000000 or close_price > 1000000:
            return False, f"Suspicious price values - entry: {entry_price}, close: {close_price}"
        
        if quantity > 1000000000:  # 1 billion
            return False, f"Suspicious quantity value: {quantity}"
        
        return True, "Valid"
    
    def _calculate_raw_pnl(self, entry_price: float, close_price: float, 
                          quantity: float, side: str) -> float:
        """
        Calculate raw P&L without commission.
        
        Uses the same logic as existing functions:
        - BUY: (close_price - entry_price) * quantity
        - SELL: (entry_price - close_price) * quantity
        """
        side_upper = side.upper()
        
        if side_upper == "BUY":
            return (close_price - entry_price) * quantity
        elif side_upper == "SELL":
            return (entry_price - close_price) * quantity
        else:
            return 0.0
    
    def _calculate_commission(self, entry_price: float, close_price: float, 
                             quantity: float, commission_rate: Optional[float] = None) -> float:
        """
        Calculate total commission for entry and exit trades.
        
        Commission is calculated on both entry and exit:
        - Entry commission: entry_price * quantity * commission_rate
        - Exit commission: close_price * quantity * commission_rate
        """
        if commission_rate is None:
            commission_rate = self.commission_rate_default
        
        entry_commission = entry_price * quantity * commission_rate
        exit_commission = close_price * quantity * commission_rate
        
        return entry_commission + exit_commission
    
    def calculate_pnl(self, entry_price: float, close_price: float, 
                     quantity: float, side: str, 
                     include_commission: bool = True,
                     commission_rate: Optional[float] = None) -> PnLResult:
        """
        Calculate comprehensive P&L with detailed result.
        
        Args:
            entry_price: Entry price of the position
            close_price: Close price of the position
            quantity: Quantity of the position
            side: Position side ('BUY' or 'SELL')
            include_commission: Whether to include commission in calculation
            commission_rate: Custom commission rate (uses default if None)
            
        Returns:
            PnLResult with detailed calculation information
        """
        # Validate inputs
        is_valid, error_msg = self._validate_inputs(entry_price, close_price, quantity, side)
        if not is_valid:
            self.logger.error(f"P&L calculation failed: {error_msg}")
            return PnLResult(
                pnl_raw=0.0,
                pnl_net=0.0,
                commission=0.0,
                status=f"Error: {error_msg}",
                is_valid=False
            )
        
        try:
            # Convert to appropriate types
            entry_price = float(entry_price)
            close_price = float(close_price)
            quantity = float(quantity)
            side = str(side).upper()
            
            # Calculate raw P&L
            pnl_raw = self._calculate_raw_pnl(entry_price, close_price, quantity, side)
            
            # Calculate commission if requested
            commission = 0.0
            if include_commission:
                commission = self._calculate_commission(
                    entry_price, close_price, quantity, commission_rate
                )
            
            # Calculate net P&L
            pnl_net = pnl_raw - commission
            
            return PnLResult(
                pnl_raw=round(pnl_raw, 8),
                pnl_net=round(pnl_net, 8),
                commission=round(commission, 8),
                status="Calculated successfully",
                is_valid=True
            )
            
        except Exception as e:
            error_msg = f"Unexpected error in P&L calculation: {str(e)}"
            self.logger.error(error_msg)
            return PnLResult(
                pnl_raw=0.0,
                pnl_net=0.0,
                commission=0.0,
                status=error_msg,
                is_valid=False
            )
    
    def calculate_simple_pnl(self, entry_price: float, close_price: float, 
                           quantity: float, side: str) -> float:
        """
        Calculate simple P&L without commission (backward compatibility).
        
        This method maintains the same interface as the original 
        _calculate_correct_pnl() function in main.py.
        
        Returns:
            Float P&L value (0.0 if calculation fails)
        """
        result = self.calculate_pnl(
            entry_price=entry_price,
            close_price=close_price,
            quantity=quantity,
            side=side,
            include_commission=False
        )
        
        return result.pnl_raw if result.is_valid else 0.0
    
    def calculate_accurate_pnl(self, trade_data: Dict[str, Any]) -> Tuple[float, str]:
        """
        Calculate accurate P&L with commission from trade data dictionary.
        
        This method maintains the same interface as the original
        calculate_accurate_pnl() function in telegram.py.
        
        Args:
            trade_data: Dictionary containing trade information
            
        Returns:
            Tuple of (pnl_net, status_message)
        """
        try:
            # Extract data from trade_data dictionary
            side = trade_data.get('side', '').upper()
            entry_price = float(trade_data.get('entry_price', trade_data.get('price', 0)))
            exit_price = float(trade_data.get('price', 0))
            quantity = float(trade_data.get('quantity_float', trade_data.get('quantity', 0)))
            
            # ✅ ENHANCED: Better entry price recovery for corrupted data
            if entry_price <= 0:
                # Try alternative sources for entry price
                fallback_entry = float(trade_data.get('initial_entry_price', 0))
                if fallback_entry <= 0 and 'original_signal_data' in trade_data:
                    fallback_entry = float(trade_data['original_signal_data'].get('entry_price', 0))
                
                if fallback_entry > 0:
                    entry_price = fallback_entry
                    self.logger.warning(f"Entry price recovered from fallback: {entry_price:.6f}")
                else:
                    self.logger.error(f"Cannot recover entry price from trade data: {trade_data}")
                    return 0.0, "Entry price corrupted - cannot calculate P&L"
            
            # Check if we have sufficient data
            if not all([entry_price, exit_price, quantity]):
                return 0.0, f"Insufficient data - entry: {entry_price}, exit: {exit_price}, qty: {quantity}"
            
            if not side:
                return 0.0, "Unknown side"
            
            # Calculate P&L with commission
            result = self.calculate_pnl(
                entry_price=entry_price,
                close_price=exit_price,
                quantity=quantity,
                side=side,
                include_commission=True
            )
            
            if result.is_valid:
                return round(result.pnl_net, 4), "Calculated"
            else:
                return 0.0, result.status
                
        except Exception as e:
            error_msg = f"Error: {str(e)}"
            self.logger.error(f"P&L calculation error: {error_msg}")
            return 0.0, error_msg


# Global instance for easy access (will be initialized in main.py)
_global_calculator: Optional[PnLCalculator] = None


def get_pnl_calculator() -> PnLCalculator:
    """
    Get the global P&L calculator instance.
    
    Returns:
        Global PnLCalculator instance
    """
    global _global_calculator
    if _global_calculator is None:
        # Initialize with default settings if not set
        _global_calculator = PnLCalculator()
    return _global_calculator


def initialize_pnl_calculator(api_config: Dict[str, Any], logger: logging.Logger) -> None:
    """
    Initialize the global P&L calculator with configuration.
    
    Args:
        api_config: API configuration dictionary
        logger: Logger instance
    """
    global _global_calculator
    _global_calculator = PnLCalculator(api_config=api_config, logger=logger)