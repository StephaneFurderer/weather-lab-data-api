"""
Data fetcher module for downloading hurricane track data from Google DeepMind WeatherLab.
"""

import os
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
DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')


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
    
    def __init__(self, data_dir: str = DATA_DIR):
        self.data_dir = data_dir
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Hurricane Impact Analysis/1.0 (Research Purpose)'
        })
    
    def download_hurricane_data(self, date: str, force_download: bool = False) -> Optional[pd.DataFrame]:
        """
        Download hurricane data for a specific date.
        
        Args:
            date: Date in YYYY-MM-DD format
            force_download: If True, re-download even if file exists
            
        Returns:
            DataFrame with hurricane track data or None if download fails
        """
        # Try different filename patterns for local files
        possible_filenames = [
            f"FNV3_{date.replace('-', '_')}T00_00_paired.csv",
            f"FNV3_{date.replace('-', '_')}T12_00_paired.csv",
            f"FNV3_{date.replace('-', '_')}_hurricane_data.csv"
        ]
        
        # Check for existing local files first
        for filename in possible_filenames:
            filepath = os.path.join(self.data_dir, filename)
            if os.path.exists(filepath):
                logger.info(f"Using local data file: {filename}")
                return self._load_csv(filepath)
        
        # If no local file found, try to download
        url = get_weatherlab_url(date)
        filename = f"FNV3_{date.replace('-', '_')}T00_00_paired.csv"
        filepath = os.path.join(self.data_dir, filename)
        
        # Check if file already exists and is recent (unless force download)
        if not force_download and os.path.exists(filepath):
            file_age = datetime.now() - datetime.fromtimestamp(os.path.getmtime(filepath))
            if file_age.days < 1:  # File is less than 1 day old
                logger.info(f"Using cached data for {date}")
                return self._load_csv(filepath)
        
        try:
            logger.info(f"Downloading hurricane data for {date} from {url}")
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            # Save raw data
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(response.text)
            
            logger.info(f"Downloaded and saved data to {filepath}")
            return self._load_csv(filepath)
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to download data for {date}: {e}")
            return None
        except Exception as e:
            logger.error(f"Error processing data for {date}: {e}")
            return None
    
    def download_date_range(self, start_date: str, force_download: bool = False) -> Dict[str, pd.DataFrame]:
        """
        Download hurricane data for a range of dates.
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            force_download: If True, re-download even if files exist
            
        Returns:
            Dictionary mapping dates to DataFrames
        """
        data_by_date = {}
        df = self.download_hurricane_data(start_date, force_download)
        if df is not None and not df.empty:
            data_by_date[start_date] = df
            logger.info(f"Successfully loaded data for {start_date}: {len(df)} records")
        else:
            logger.warning(f"No data available for {start_date}")
        
        return data_by_date
    
    def _load_csv(self, filepath: str) -> pd.DataFrame:
        """Load and parse CSV file with hurricane data."""
        try:
            # Read the file and find where the actual data starts
            with open(filepath, 'r') as f:
                lines = f.readlines()
            
            # Find the line that contains "# BEGIN DATA" and extract the header
            header_line = None
            for i, line in enumerate(lines):
                if 'BEGIN DATA' in line.upper():
                    header_line = i  # Mark that we found the line
                    
                    # Check if header is on the same line or next line
                    if 'BEGIN DATA ' in line and len(line.split('BEGIN DATA ')) > 1:
                        # Header is on the same line after "# BEGIN DATA"
                        header_part = line.split('BEGIN DATA ')[1].strip()
                        data_start = i + 1
                    else:
                        # Header is on the next line
                        if i + 1 < len(lines):
                            header_part = lines[i + 1].strip()
                            data_start = i + 2
                        else:
                            logger.error(f"No header found after BEGIN DATA in {filepath}")
                            return pd.DataFrame()
                    
                    # Create a temporary file with proper header and data
                    temp_content = header_part + '\n'
                    # Add all subsequent lines (data rows)
                    for j in range(data_start, len(lines)):
                        temp_content += lines[j]
                    
                    # Write to a temporary file and read it
                    import tempfile
                    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as temp_file:
                        temp_file.write(temp_content)
                        temp_filename = temp_file.name
                    
                    # Read the temporary CSV file
                    df = pd.read_csv(temp_filename)
                    
                    # Clean up temporary file
                    import os
                    os.unlink(temp_filename)
                    break
            
            if header_line is None:
                logger.error(f"Could not find 'BEGIN DATA' marker in {filepath}")
                return pd.DataFrame()
            
            # Remove any empty columns
            df = df.dropna(axis=1, how='all')
            
            # Convert datetime columns
            datetime_columns = ['init_time', 'valid_time']
            for col in datetime_columns:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col])
            
            # Convert lead_time to timedelta
            if 'lead_time' in df.columns:
                df['lead_time'] = pd.to_timedelta(df['lead_time'])
            
            # Convert numeric columns
            numeric_columns = [
                'lat', 'lon', 'minimum_sea_level_pressure_hpa', 
                'maximum_sustained_wind_speed_knots', 'radius_of_maximum_winds_km',
                'radius_34_knot_winds_ne_km', 'radius_34_knot_winds_se_km',
                'radius_34_knot_winds_sw_km', 'radius_34_knot_winds_nw_km',
                'radius_50_knot_winds_ne_km', 'radius_50_knot_winds_se_km',
                'radius_50_knot_winds_sw_km', 'radius_50_knot_winds_nw_km',
                'radius_64_knot_winds_ne_km', 'radius_64_knot_winds_se_km',
                'radius_64_knot_winds_sw_km', 'radius_64_knot_winds_nw_km'
            ]
            
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            logger.info(f"Loaded {len(df)} records from {filepath}")
            return df
            
        except Exception as e:
            logger.error(f"Error loading CSV file {filepath}: {e}")
            return pd.DataFrame()
    
    def get_available_dates(self) -> List[str]:
        """Get list of dates for which data files exist."""
        if not os.path.exists(self.data_dir):
            return []
        
        files = [f for f in os.listdir(self.data_dir) if f.startswith('FNV3_') and f.endswith('.csv')]
        dates = []
        
        for file in files:
            # Extract date from filename: FNV3_YYYY_MM_DD_hurricane_data.csv
            try:
                date_part = file.split('_')[1:4]  # Get YYYY, MM, DD parts
                date_str = '-'.join(date_part)
                dates.append(date_str)
            except (IndexError, ValueError):
                continue
        
        return sorted(dates)
    
    def get_hurricane_summary(self, date: str) -> Dict:
        """Get summary information about hurricanes for a specific date."""
        df = self.download_hurricane_data(date)
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
            hurricane_info = {
                'track_id': track_id,
                'records': len(group),
                'max_wind_speed': group['maximum_sustained_wind_speed_knots'].max(),
                'min_pressure': group['minimum_sea_level_pressure_hpa'].min(),
                'lat_range': (group['lat'].min(), group['lat'].max()),
                'lon_range': (group['lon'].min(), group['lon'].max()),
                'time_range': (group['valid_time'].min(), group['valid_time'].max()),
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
