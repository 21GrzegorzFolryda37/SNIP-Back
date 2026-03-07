from __future__ import annotations

import re
from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, field_validator


class SnipeStatus(str, Enum):
    waiting = "waiting"
    active = "active"
    executing = "executing"
    won = "won"
    lost = "lost"
    error = "error"
    cancelled = "cancelled"


# ---------- User ----------

class UserCreate(BaseModel):
    allegro_user_id: str
    allegro_login: str


class UserResponse(BaseModel):
    id: str
    allegro_user_id: str
    allegro_login: str
    email: Optional[str] = None
    plan: Optional[str] = None
    created_at: datetime


# ---------- Snipe ----------

class SnipeCreate(BaseModel):
    allegro_offer_url: str
    max_bid_amount: float

    @field_validator("allegro_offer_url")
    @classmethod
    def validate_allegro_url(cls, v: str) -> str:
        pattern = r"allegro\.pl/.+-(\d+)"
        if not re.search(pattern, v):
            raise ValueError("URL must be a valid Allegro offer link")
        return v.strip()

    @field_validator("max_bid_amount")
    @classmethod
    def validate_amount(cls, v: float) -> float:
        if v <= 0:
            raise ValueError("max_bid_amount must be positive")
        return round(v, 2)


class SnipeResponse(BaseModel):
    id: str
    user_id: str
    allegro_offer_id: str
    allegro_offer_url: str
    offer_title: Optional[str] = None
    offer_image_url: Optional[str] = None
    offer_end_time: Optional[datetime] = None
    current_price: Optional[float] = None
    max_bid_amount: float
    status: SnipeStatus
    result_message: Optional[str] = None
    executed_at: Optional[datetime] = None
    created_at: datetime
    updated_at: Optional[datetime] = None


class SnipeUpdate(BaseModel):
    max_bid_amount: Optional[float] = None
    status: Optional[SnipeStatus] = None


# ---------- Auth ----------

class TokenResponse(BaseModel):
    message: str
    user_login: str


# ---------- Health ----------

class HealthResponse(BaseModel):
    status: str
    environment: str
    ntp_synced: bool
    ntp_offset_ms: Optional[float]
    active_snipes: int
    scheduler_running: bool


# ---------- Snipe Log ----------

class SnipeLogEntry(BaseModel):
    id: str
    snipe_id: str
    action: str
    details: Optional[str]
    created_at: datetime
