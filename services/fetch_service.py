from __future__ import annotations

from typing import Optional, Dict, Any
from datetime import datetime, timedelta

from services.data_fetcher import HurricaneDataFetcher

from utils.serialize import dataframe_to_records


class FetchService:
    def __init__(self):
        self.fetcher = HurricaneDataFetcher()

    def get_available_dates(self) -> list[str]:
        return self.fetcher.get_available_dates()

    def get_data_for_date(self, date: str, force: bool = False) -> dict:
        df = self.fetcher.download_hurricane_data(date, force_download=force)
        records = dataframe_to_records(df)
        meta = {
            'date': date,
            'record_count': len(records),
            'source': 'local_or_remote',
            'cached': not force,
        }
        return {'meta': meta, 'records': records}

    def get_data_range(self, start: str, end: Optional[str] = None, days: Optional[int] = None, force: bool = False) -> dict:
        """Get hurricane data for a date range"""
        start_date = datetime.strptime(start, '%Y-%m-%d').date()
        
        if end:
            end_date = datetime.strptime(end, '%Y-%m-%d').date()
        elif days:
            end_date = start_date + timedelta(days=days-1)
        else:
            end_date = start_date
        
        data = {}
        current_date = start_date
        
        while current_date <= end_date:
            date_str = current_date.strftime('%Y-%m-%d')
            try:
                result = self.get_data_for_date(date_str, force)
                data[date_str] = {
                    'record_count': result['meta']['record_count'],
                    'records': result['records']
                }
            except Exception as e:
                data[date_str] = {
                    'record_count': 0,
                    'records': [],
                    'error': str(e)
                }
            current_date += timedelta(days=1)
        
        meta = {
            'start_date': start,
            'end_date': end_date.strftime('%Y-%m-%d'),
            'total_dates': len(data),
            'total_records': sum(d.get('record_count', 0) for d in data.values())
        }
        
        return {'meta': meta, 'data': data}

    def get_summary_for_date(self, date: str) -> dict:
        """Get hurricane summary for a specific date"""
        try:
            df = self.fetcher.download_hurricane_data(date)
            summary = self.fetcher.get_hurricane_summary(df)
            return {
                'date': date,
                'summary': summary
            }
        except Exception as e:
            return {
                'date': date,
                'summary': None,
                'error': str(e)
            }


