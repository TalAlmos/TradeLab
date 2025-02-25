"""
Configuration settings for the TradeLab project.
"""

from pathlib import Path
from typing import Dict, Any
from zoneinfo import ZoneInfo  # for Python 3.9+
# OR from datetime import timezone, timedelta  # for older Python versions

class Settings:
    def __init__(self):
        # Get project root directory (3 levels up from this file)
        self.PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
        
        # Data directories
        self.DATA_DIR = self.PROJECT_ROOT / "data"
        self.RAW_DATA_DIR = self.DATA_DIR / "raw"
        self.PROCESSED_DATA_DIR = self.DATA_DIR / "processed"
        
        # Market settings with timezone
        self.MARKET_HOURS = {
            "start": "09:30",
            "end": "16:00",
            "timezone": "America/New_York",  # NYSE timezone
            "trading_days": "Mon Tue Wed Thu Fri"  # Trading days
        }
        
        # Data fetching settings
        self.DATA_SETTINGS = {
            "interval": "1m",  # 1-minute intervals
            "required_columns": [
                "Datetime",
                "Open",
                "High",
                "Low",
                "Close",
                "Volume"
            ],
            "default_retries": 3,
            "retry_delay": 5  # seconds
        }
        
        # Ensure directories exist
        self._create_directories()
    
    def _create_directories(self) -> None:
        """Create necessary directories if they don't exist."""
        self.RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
        self.PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)
    
    def get(self, *keys: str, default: Any = None) -> Any:
        """
        Get a configuration value using dot notation.
        Example: settings.get('MARKET_HOURS', 'start')
        """
        current = self.__dict__
        for key in keys:
            if isinstance(current, dict):
                current = current.get(key, default)
            else:
                return default
        return current
    
    def validate(self) -> bool:
        """
        Validate the settings configuration.
        Returns True if all required settings are present and valid.
        """
        required_attrs = [
            'PROJECT_ROOT',
            'DATA_DIR',
            'RAW_DATA_DIR',
            'PROCESSED_DATA_DIR',
            'MARKET_HOURS',
            'DATA_SETTINGS'
        ]
        
        return all(hasattr(self, attr) for attr in required_attrs)

# Create a global settings instance
settings = Settings()

# Example usage:
# from tradelab.config.settings import settings
# raw_dir = settings.RAW_DATA_DIR
# market_start = settings.get('MARKET_HOURS', 'start')
