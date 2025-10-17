from pydantic import BaseModel


class Meta(BaseModel):
    date: str | None = None
    record_count: int | None = None
    source: str | None = None
    cached: bool | None = None


