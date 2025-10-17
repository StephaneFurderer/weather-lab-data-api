from __future__ import annotations

from typing import Optional

from data_fetcher import HurricaneDataFetcher

from ..utils.serialize import dataframe_to_records


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


