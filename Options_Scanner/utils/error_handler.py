"""
Centralized error handling utilities for the options trading system.
"""
import logging
import traceback
import sys
import json
from datetime import datetime
from typing import Dict, Any, Optional, Callable, Union, Type
import functools

# Configure logger
logger = logging.getLogger(__name__)

# Define common error types
class SystemError(Exception):
    """Base class for system-level errors."""
    def __init__(self, message: str, error_code: str = None):
        self.error_code = error_code
        super().__init__(message)

class APIError(SystemError):
    """Error in external API communications."""
    pass

class DatabaseError(SystemError):
    """Error in database operations."""
    pass

class ConfigurationError(SystemError):
    """Error in system configuration."""
    pass

class ValidationError(SystemError):
    """Error in data validation."""
    pass

# Error tracking
_error_counts: Dict[str, int] = {}
_error_registry: Dict[str, Dict[str, Any]] = {}

def register_error(error: Exception, context: Dict[str, Any] = None) -> str:
    """
    Register an error in the central error registry with a unique ID.
    
    Args:
        error: The exception that occurred
        context: Additional context about where/why the error occurred
        
    Returns:
        Unique error ID for reference
    """
    error_type = error.__class__.__name__
    timestamp = datetime.now()
    error_id = f"{error_type}_{timestamp.strftime('%Y%m%d%H%M%S')}_{id(error)}"
    
    # Update error counts
    if error_type not in _error_counts:
        _error_counts[error_type] = 0
    _error_counts[error_type] += 1
    
    # Create error record
    error_record = {
        "error_type": error_type,
        "message": str(error),
        "timestamp": timestamp.isoformat(),
        "traceback": traceback.format_exception(type(error), error, error.__traceback__),
        "context": context or {},
    }
    
    # Add to registry
    _error_registry[error_id] = error_record
    
    # Log the error
    logger.error(
        f"Error registered [ID: {error_id}]: {error_type}: {str(error)}", 
        extra={"error_id": error_id, "context": context}
    )
    
    # Prune old errors if registry gets too large
    if len(_error_registry) > 1000:
        _prune_error_registry()
    
    return error_id

def get_error(error_id: str) -> Optional[Dict[str, Any]]:
    """
    Retrieve an error record by ID.
    
    Args:
        error_id: Unique error ID
        
    Returns:
        Error record dictionary or None if not found
    """
    return _error_registry.get(error_id)

def get_error_stats() -> Dict[str, Any]:
    """
    Get error statistics for monitoring.
    
    Returns:
        Dictionary with error statistics
    """
    return {
        "total_errors": sum(_error_counts.values()),
        "error_types": _error_counts,
        "active_errors": len(_error_registry)
    }

def export_errors(filepath: str) -> bool:
    """
    Export error registry to a JSON file.
    
    Args:
        filepath: Path to save JSON file
        
    Returns:
        True if successful, False otherwise
    """
    try:
        # Convert datetime objects to strings for JSON serialization
        sanitized_registry = {}
        for error_id, record in _error_registry.items():
            sanitized_record = record.copy()
            sanitized_registry[error_id] = sanitized_record
        
        with open(filepath, 'w') as f:
            json.dump(sanitized_registry, f, indent=2)
        return True
    except Exception as e:
        logger.error(f"Failed to export errors: {e}")
        return False

def _prune_error_registry():
    """Remove oldest errors when registry gets too large."""
    # Sort by timestamp
    sorted_errors = sorted(
        _error_registry.items(), 
        key=lambda x: datetime.fromisoformat(x[1]["timestamp"])
    )
    
    # Keep only the most recent 500 errors
    to_keep = sorted_errors[-500:]
    
    # Update registry
    _error_registry.clear()
    for error_id, record in to_keep:
        _error_registry[error_id] = record

def handle_errors(
    default_return: Any = None,
    reraise: bool = False,
    handled_exceptions: Union[Type[Exception], tuple] = Exception,
    error_handler: Optional[Callable[[Exception, Dict], None]] = None
):
    """
    Decorator for standardized error handling across the system.
    
    Args:
        default_return: Value to return on error
        reraise: Whether to re-raise the exception after handling
        handled_exceptions: Exception types to catch
        error_handler: Custom function to handle errors
        
    Returns:
        Decorator function
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except handled_exceptions as e:
                # Create context with function info
                context = {
                    "function": func.__name__,
                    "module": func.__module__,
                    "args": repr(args),
                    "kwargs": repr(kwargs)
                }
                
                # Register error
                error_id = register_error(e, context)
                
                # Call custom error handler if provided
                if error_handler:
                    error_handler(e, context)
                
                # Re-raise if requested
                if reraise:
                    raise
                
                # Otherwise return default
                return default_return
        return wrapper
    return decorator

def async_handle_errors(
    default_return: Any = None,
    reraise: bool = False,
    handled_exceptions: Union[Type[Exception], tuple] = Exception,
    error_handler: Optional[Callable[[Exception, Dict], None]] = None
):
    """
    Decorator for standardized error handling in async functions.
    
    Args:
        default_return: Value to return on error
        reraise: Whether to re-raise the exception after handling
        handled_exceptions: Exception types to catch
        error_handler: Custom function to handle errors
        
    Returns:
        Decorator function
    """
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            try:
                return await func(*args, **kwargs)
            except handled_exceptions as e:
                # Create context with function info
                context = {
                    "function": func.__name__,
                    "module": func.__module__,
                    "args": repr(args),
                    "kwargs": repr(kwargs)
                }
                
                # Register error
                error_id = register_error(e, context)
                
                # Call custom error handler if provided
                if error_handler:
                    error_handler(e, context)
                
                # Re-raise if requested
                if reraise:
                    raise
                
                # Otherwise return default
                return default_return
        return wrapper
    return decorator

# Add a global exception handler
def setup_global_exception_handler():
    """
    Set up a global exception handler to catch unhandled exceptions.
    """
    def global_exception_handler(exctype, value, tb):
        """Handle uncaught exceptions."""
        context = {"unhandled": True}
        register_error(value, context)
        
        # Still use the default exception handler
        sys.__excepthook__(exctype, value, tb)
    
    # Set the exception hook
    sys.excepthook = global_exception_handler