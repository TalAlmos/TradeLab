"""
Batch data fetching for multiple tickers using stock_tickers.csv
"""

import pandas as pd
from pathlib import Path
from typing import List, Optional
import sys
import time

from ..config.settings import settings
from ..utils.logger import get_logger
from .ibkr_fetcher import IBKRFetcher

logger = get_logger(__name__)

class BatchFetcher:
    def __init__(self):
        """Initialize batch fetcher"""
        self.tickers_file = settings.PROJECT_ROOT / "data" / "beta_stock" / "stock_tickers.csv"
        self.fetcher = IBKRFetcher()
        
    def load_tickers(self) -> List[str]:
        """Load tickers from CSV file"""
        try:
            df = pd.read_csv(self.tickers_file)
            tickers = df['Ticker'].tolist()
            logger.info(f"Loaded {len(tickers)} tickers from {self.tickers_file}")
            return tickers
        except Exception as e:
            logger.error(f"Error loading tickers: {str(e)}")
            return []
            
    def fetch_all(self, start_date: str, end_date: str) -> dict:
        """
        Fetch data for all tickers for the specified date range
        
        Args:
            start_date: Start date in YYYYMMDD format
            end_date: End date in YYYYMMDD format
        """
        # Format dates
        start = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:]}"
        end = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:]}"
        
        # Connect to IBKR
        if not self.fetcher.connect():
            logger.error("Failed to connect to IBKR")
            return {}
            
        try:
            tickers = self.load_tickers()
            results = {}
            
            total_tickers = len(tickers)
            for idx, ticker in enumerate(tickers, 1):
                try:
                    logger.info(f"Processing {ticker} ({idx}/{total_tickers})")
                    data = self.fetcher.fetch_date_range(ticker, start, end)
                    results[ticker] = data
                    
                    # Log results
                    if data:
                        dates_fetched = len(data)
                        total_records = sum(len(df) for df in data.values())
                        logger.info(f"Completed {ticker}: {dates_fetched} days, {total_records} records")
                    else:
                        logger.warning(f"No data retrieved for {ticker}")
                        
                    # Add a delay between tickers
                    time.sleep(1)
                    
                except Exception as e:
                    logger.error(f"Error processing {ticker}: {str(e)}")
                    continue
                    
            return results
            
        finally:
            self.fetcher.disconnect()

def main():
    """Main function for command line usage"""
    try:
        # Parse command line arguments
        if len(sys.argv) < 2:
            logger.error("Please provide at least a start date (YYYYMMDD)")
            logger.error("Usage: python batch_fetcher.py START_DATE [END_DATE]")
            sys.exit(1)
            
        start_date = sys.argv[1]
        if len(start_date) != 8:
            raise ValueError("Start date must be in YYYYMMDD format")
            
        # Parse end date if provided
        end_date = start_date  # default to same day
        if len(sys.argv) > 2:
            end_date = sys.argv[2]
            if len(end_date) != 8:
                raise ValueError("End date must be in YYYYMMDD format")
            
        logger.info(f"Starting batch fetch from {start_date} to {end_date}")
        
        batch_fetcher = BatchFetcher()
        results = batch_fetcher.fetch_all(start_date, end_date)
        
        # Print summary
        successful = len([t for t in results if results[t]])
        total = len(results)
        logger.info(f"\nBatch processing complete:")
        logger.info(f"Successfully processed: {successful}/{total} tickers")
        logger.info(f"Failed: {total - successful} tickers")
        
        # Print detailed statistics
        total_records = 0
        for ticker, data in results.items():
            if data:
                records = sum(len(df) for df in data.values())
                total_records += records
                logger.info(f"{ticker}: {len(data)} days, {records} records")
        logger.info(f"Total records fetched: {total_records}")
        
    except Exception as e:
        logger.error(f"Batch processing error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 