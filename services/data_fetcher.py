"""
Data fetcher module for downloading hurricane track data from Google DeepMind WeatherLab.
"""

import os
import io
import pandas as pd
import requests
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import logging

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Base URL pattern for Google DeepMind WeatherLab hurricane data
WEATHERLAB_BASE_URL = "https://deepmind.google.com/science/weatherlab/download/cyclones/FNV3/ensemble_mean/paired/csv"
WEATHERLAB_URL_PATTERN = f"{WEATHERLAB_BASE_URL}/FNV3_{{date}}T00_00_paired.csv"



def get_weatherlab_url(date_str: str) -> str:
    """Generate WeatherLab URL for a specific date."""
    date_str_replaced = date_str.replace('-', '_')
    return WEATHERLAB_URL_PATTERN.format(date=date_str_replaced)

def get_date_range(start_date: str, end_date: str) -> List[str]:
    """Generate list of dates between start and end date."""
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')
    dates = []
    current = start
    while current <= end:
        dates.append(current.strftime('%Y-%m-%d'))
        current += timedelta(days=1)
    return dates



class HurricaneDataFetcher:
    """Downloads and caches hurricane track data from Google DeepMind WeatherLab."""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Hurricane Impact Analysis/1.0 (Research Purpose)'
        })
    
    def download_hurricane_data(self, date: str, force_download: bool = False) -> Optional[pd.DataFrame]:
        """
        Download hurricane data for a specific date from WeatherLab.
        
        Args:
            date: Date in YYYY-MM-DD format
            force_download: If True, re-download even if cached (not used in API version)
            
        Returns:
            DataFrame with hurricane track data or None if download fails
        """
        url = get_weatherlab_url(date)
        try:
            logger.info(f"Downloading hurricane data for {date} from {url}")
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            # Convert bytes to string and filter out comment lines efficiently
            content_str = response.content.decode('utf-8')
            
            # Process line by line to avoid memory issues with very large files
            data_lines = []
            for line in content_str.split('\n'):
                if line.strip() and not line.strip().startswith('#'):
                    data_lines.append(line)
            
            # Join only the data lines
            csv_content = '\n'.join(data_lines)
            
            # Create file-like object and read CSV
            csv_buffer = io.StringIO(csv_content)
            df = pd.read_csv(csv_buffer)
            logger.info(f"Downloaded {len(df)} records for {date}")
            # Force Railway redeploy
            return df
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to download data for {date}: {e}")
            return None
        except Exception as e:
            logger.error(f"Error processing data for {date}: {e}")
            logger.error(f"Error type: {type(e).__name__}")
            if hasattr(e, 'errno'):
                logger.error(f"Error number: {e.errno}")
            return None
    
    
    def get_hurricane_summary(self, date: str) -> Dict:
        """Get summary information about hurricanes for a specific date."""
        df = self.download_hurricane_data(date)
        if df is None or df.empty:
            return {}
        
        return self._create_summary_from_dataframe(df, date)
    
    def _create_summary_from_dataframe(self, df: pd.DataFrame, date: str = None) -> Dict:
        """Create summary from an existing DataFrame."""
        if df is None or df.empty:
            return {}
        
        summary = {
            'date': date,
            'total_records': len(df),
            'hurricanes': {},
            'data_quality': {}
        }
        
        # Group by track_id to get hurricane summaries
        for track_id, group in df.groupby('track_id'):
            # Convert valid_time to datetime for proper min/max operations
            valid_times = pd.to_datetime(group['valid_time'])
            
            hurricane_info = {
                'track_id': track_id,
                'records': len(group),
                'max_wind_speed': group['maximum_sustained_wind_speed_knots'].max(),
                'min_pressure': group['minimum_sea_level_pressure_hpa'].min(),
                'lat_range': (group['lat'].min(), group['lat'].max()),
                'lon_range': (group['lon'].min(), group['lon'].max()),
                'time_range': (valid_times.min().strftime('%Y-%m-%d %H:%M:%S'), 
                              valid_times.max().strftime('%Y-%m-%d %H:%M:%S')),
                'has_radius_data': any(group['radius_34_knot_winds_ne_km'].notna())
            }
            summary['hurricanes'][track_id] = hurricane_info
        
        # Data quality metrics
        summary['data_quality'] = {
            'has_coordinates': df[['lat', 'lon']].notna().all().all(),
            'has_wind_data': df['maximum_sustained_wind_speed_knots'].notna().all(),
            'has_pressure_data': df['minimum_sea_level_pressure_hpa'].notna().all(),
            'has_radius_data': df['radius_34_knot_winds_ne_km'].notna().any(),
            'coordinate_range': {
                'lat': (df['lat'].min(), df['lat'].max()),
                'lon': (df['lon'].min(), df['lon'].max())
            }
        }
        
        return summary
    
    def get_available_dates(self) -> List[str]:
        """
        Get list of available dates for hurricane data.
        For the API version, this returns a sample of recent dates.
        In a real implementation, this could query WeatherLab's available data.
        """
        # Generate last 30 days as available dates
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)
        
        dates = []
        current = start_date
        while current <= end_date:
            dates.append(current.strftime('%Y-%m-%d'))
            current += timedelta(days=1)
        
        return dates

def main():
    """Example usage of HurricaneDataFetcher."""
    fetcher = HurricaneDataFetcher()
    
    # Download Hurricane Helene data (2024-09-23)
    print("Downloading Hurricane Helene data...")
    data = fetcher.download_hurricane_data("2024-09-23")
    
    if data is not None:
        print(f"Downloaded {len(data)} records")
        print("\nColumns:", data.columns.tolist())
        print("\nFirst few records:")
        print(data.head())
        
        # Get summary
        summary = fetcher.get_hurricane_summary("2024-09-23")
        print("\nHurricane Summary:")
        for track_id, info in summary['hurricanes'].items():
            print(f"  {track_id}: Max wind {info['max_wind_speed']:.1f} knots, "
                  f"Min pressure {info['min_pressure']:.1f} hPa, "
                  f"{info['records']} records")
    else:
        print("Failed to download data")

if __name__ == "__main__":
    main()
