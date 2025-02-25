"""
Logging configuration for the TradeLab project.
"""

import logging
import sys
from pathlib import Path
from datetime import datetime
from typing import Optional
from ..config.settings import settings

def setup_logger(
    name: str,
    log_file: Optional[Path] = None,
    level: int = logging.INFO,
    log_format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
) -> logging.Logger:
    """
    Set up a logger with both file and console handlers.
    
    Args:
        name: Name of the logger (usually __name__)
        log_file: Optional path to log file
        level: Logging level (default: INFO)
        log_format: Format string for log messages
        
    Returns:
        Configured logger instance
    """
    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Avoid duplicate handlers
    if logger.handlers:
        return logger
    
    # Create formatter
    formatter = logging.Formatter(log_format)
    
    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # Create file handler if log_file is specified
    if log_file:
        # Ensure log directory exists
        log_file.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger

def get_logger(
    name: str,
    include_file_logging: bool = True
) -> logging.Logger:
    """
    Get a configured logger instance.
    
    Args:
        name: Name of the logger (usually __name__)
        include_file_logging: Whether to include file logging
        
    Returns:
        Configured logger instance
    """
    # Create logs directory in project root
    log_dir = settings.PROJECT_ROOT / "logs"
    
    if include_file_logging:
        # Create dated log filename
        date_str = datetime.now().strftime("%Y%m%d")
        module_name = name.split('.')[-1]  # Get the last part of the module path
        log_file = log_dir / f"{date_str}_{module_name}.log"
    else:
        log_file = None
    
    return setup_logger(name, log_file)

# Example usage:
# from tradelab.utils.logger import get_logger
# logger = get_logger(__name__)
# logger.info("This is a log message")