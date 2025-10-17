from fastapi import APIRouter, Query, HTTPException
from datetime import datetime
from typing import Optional
from services.fetch_service import FetchService

router = APIRouter()

@router.get("/health")
def health() -> dict:
    """Health check endpoint"""
    return {"status": "ok", "time": datetime.utcnow().isoformat() + "Z"}

@router.get("/available-dates")
def available_dates() -> dict:
    """Get list of available dates for hurricane data"""
    service = FetchService()
    dates = service.get_available_dates()
    return {"dates": dates}

@router.get("/data")
def get_data(
    date: str = Query(..., description="Date in YYYY-MM-DD format"),
    force: bool = Query(False, description="Force re-download even if cached")
) -> dict:
    """Get hurricane data for a specific date"""
    service = FetchService()
    try:
        result = service.get_data_for_date(date, force)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/data-range")
def get_data_range(
    start: str = Query(..., description="Start date in YYYY-MM-DD format"),
    end: Optional[str] = Query(None, description="End date in YYYY-MM-DD format"),
    days: Optional[int] = Query(None, description="Number of days from start date"),
    force: bool = Query(False, description="Force re-download even if cached")
) -> dict:
    """Get hurricane data for a date range"""
    service = FetchService()
    try:
        result = service.get_data_range(start, end, days, force)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/summary")
def get_summary(
    date: str = Query(..., description="Date in YYYY-MM-DD format")
) -> dict:
    """Get hurricane summary for a specific date"""
    service = FetchService()
    try:
        result = service.get_summary_for_date(date)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))