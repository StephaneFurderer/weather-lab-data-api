from __future__ import annotations

import pandas as pd


def dataframe_to_records(df: pd.DataFrame) -> list[dict]:
    if df is None or df.empty:
        return []
    # Convert timedeltas to seconds for JSON
    result = df.copy()
    if 'lead_time' in result.columns and pd.api.types.is_timedelta64_dtype(result['lead_time']):
        result['lead_time'] = result['lead_time'].dt.total_seconds()
    # Datetime to ISO
    for col in ['init_time', 'valid_time']:
        if col in result.columns and pd.api.types.is_datetime64_any_dtype(result[col]):
            result[col] = result[col].dt.strftime('%Y-%m-%d %H:%M:%S')
    # NaNs -> None
    return result.where(pd.notnull(result), None).to_dict(orient='records')


