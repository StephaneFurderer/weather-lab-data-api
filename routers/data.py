"""Data endpoints: health, available dates, and fetch by date."""

from fastapi import APIRouter, Query
from datetime import datetime

from services.fetch_service import FetchService


router = APIRouter()


@router.get("/health")
def health() -> dict:
    return {"status": "ok", "time": datetime.utcnow().isoformat() + "Z"}


@router.get("/available-dates")
def available_dates() -> dict:
    service = FetchService()
    dates = service.get_available_dates()
    return {"dates": dates}


@router.get("/data")
def data(date: str = Query(..., description="YYYY-MM-DD"), force: bool = False) -> dict:
    service = FetchService()
    return service.get_data_for_date(date, force)


