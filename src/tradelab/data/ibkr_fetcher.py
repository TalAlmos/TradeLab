"""
Data fetching module using Interactive Brokers TWS API
"""

from ib_insync import *
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import sys
from typing import Optional, Dict, Union, List
from zoneinfo import ZoneInfo
import time

from ..config.settings import settings
from ..utils.logger import get_logger

logger = get_logger(__name__)

class IBKRFetcher:
    def __init__(self, save_dir: Optional[Path] = None):
        """Initialize IBKR fetcher with configuration"""
        # Initialize IB connection
        self.ib = IB()
        self.connected = False
        
        # Set up directories
        if save_dir:
            self.save_dir = Path(save_dir)
        else:
            self.save_dir = settings.RAW_DATA_DIR
        
        logger.info(f"Data will be saved to: {self.save_dir.absolute()}")
        self.save_dir.mkdir(parents=True, exist_ok=True)
        
        # Load market hours from settings
        market_hours = settings.MARKET_HOURS
        self.market_start = tuple(map(int, market_hours['start'].split(':')))
        self.market_end = tuple(map(int, market_hours['end'].split(':')))
        self.market_timezone = ZoneInfo(market_hours['timezone'])
        
    def connect(self, port: int = 7496, client_id: int = 1) -> bool:
        """
        Connect to TWS or IB Gateway
        
        Args:
            port: TWS/Gateway port (7497 for TWS paper, 7496 for Gateway paper)
            client_id: Unique client identifier
        """
        try:
            self.ib.connect('127.0.0.1', port, clientId=client_id)
            self.connected = True
            logger.info("Successfully connected to IBKR")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to IBKR: {str(e)}")
            return False
            
    def disconnect(self):
        """Disconnect from TWS/Gateway"""
        if self.connected:
            self.ib.disconnect()
            self.connected = False
            
    def fetch_date_range(self,
                        ticker: str,
                        start_date: Union[str, datetime],
                        end_date: Optional[Union[str, datetime]] = None) -> Dict[str, pd.DataFrame]:
        """Fetch data for a date range"""
        if not self.connected:
            logger.error("Not connected to IBKR")
            return {}
            
        try:
            # Convert dates to datetime with timezone
            start = pd.to_datetime(start_date).tz_localize(self.market_timezone)
            if end_date:
                end = pd.to_datetime(end_date).tz_localize(self.market_timezone)
            else:
                end = start
            
            # Generate list of trading days
            date_range = pd.date_range(start=start, end=end, freq='B')
            results = {}
            
            for date in date_range:
                try:
                    # Request historical data for each day
                    bars = self.ib.reqHistoricalData(
                        contract=Stock(ticker, 'SMART', 'USD'),
                        endDateTime=date.replace(
                            hour=self.market_end[0],
                            minute=self.market_end[1]
                        ),
                        durationStr='1 D',
                        barSizeSetting='1 min',
                        whatToShow='TRADES',
                        useRTH=True,
                        formatDate=1
                    )
                    
                    if not bars:
                        logger.warning(f"No data available for {ticker} on {date.strftime('%Y-%m-%d')}")
                        continue
                        
                    # Convert to DataFrame
                    df = util.df(bars)
                    
                    # Save data for this date
                    date_str = date.strftime('%Y-%m-%d')
                    if len(df) > 0:
                        if self._save_data(df, ticker, date_str):
                            results[date_str] = df
                            logger.info(f"Successfully saved {len(df)} records for {date_str}")
                        
                    # Add delay between requests
                    time.sleep(0.5)
                    
                except Exception as e:
                    logger.error(f"Error fetching {ticker} for {date.strftime('%Y-%m-%d')}: {str(e)}")
                    continue
                
            return results
            
        except Exception as e:
            logger.error(f"Error in fetch_date_range for {ticker}: {str(e)}")
            return {}
            
    def _save_data(self,
                   data: pd.DataFrame,
                   ticker: str,
                   date: str) -> Optional[Path]:
        """Save data to CSV using specified naming convention"""
        try:
            date_str = pd.to_datetime(date).strftime('%Y-%m-%d')
            filename = f"raw_{ticker}_{date_str}.csv"
            
            ticker_dir = self.save_dir / ticker
            ticker_dir.mkdir(exist_ok=True)
            
            filepath = ticker_dir / filename
            data.to_csv(filepath, index=False)
            logger.info(f"Data saved to: {filepath}")
            
            return filepath
            
        except Exception as e:
            logger.error(f"Error saving data: {str(e)}")
            return None

def main():
    """Main function for command line usage"""
    try:
        if len(sys.argv) < 2:
            logger.error("Please provide at least a start date (YYYYMMDD)")
            logger.error("Usage: python ibkr_fetcher.py START_DATE [END_DATE] [--ticker SYMBOL]")
            sys.exit(1)
            
        # Parse dates
        start_date = sys.argv[1]
        if len(start_date) != 8:
            raise ValueError("Start date must be in YYYYMMDD format")
            
        start = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:]}"
        
        # Parse end date if provided
        end_date = None
        if len(sys.argv) > 2 and not sys.argv[2].startswith('--'):
            end_input = sys.argv[2]
            if len(end_input) != 8:
                raise ValueError("End date must be in YYYYMMDD format")
            end_date = f"{end_input[:4]}-{end_input[4:6]}-{end_input[6:]}"
            
        # Get ticker
        ticker = "NNE"  # default ticker
        if '--ticker' in sys.argv:
            ticker_idx = sys.argv.index('--ticker')
            if ticker_idx + 1 < len(sys.argv):
                ticker = sys.argv[ticker_idx + 1]
                
        # Create fetcher and connect
        fetcher = IBKRFetcher()
        if not fetcher.connect():
            sys.exit(1)
            
        try:
            results = fetcher.fetch_date_range(ticker, start, end_date)
            
            # Print summary
            logger.info(f"\nFetched data for {ticker}:")
            logger.info(f"Total dates processed: {len(results)}")
            logger.info(f"Files saved to: {fetcher.save_dir.absolute()}")
            for date, data in results.items():
                filename = f"raw_{ticker}_{date}.csv"
                logger.info(f"{date}: {len(data)} records -> {filename}")
                
        finally:
            fetcher.disconnect()
            
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 