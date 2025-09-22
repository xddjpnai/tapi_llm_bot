from typing import List, Optional
from pydantic import BaseModel


class SummaryRequest(BaseModel):
    user_id: int
    tickers: List[str]


class SummaryChunk(BaseModel):
    ticker: str
    html: str


class NotificationOutbound(BaseModel):
    user_id: int
    items: List[SummaryChunk]


