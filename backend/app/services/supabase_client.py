"""Supabase client — CRUD for users, snipes, snipe_logs."""

from __future__ import annotations

import logging
from typing import Any, Optional

from supabase import create_client, Client

from app.config import settings
from app.models.schemas import SnipeStatus

logger = logging.getLogger(__name__)

_client: Optional[Client] = None


def get_client() -> Client:
    global _client
    if _client is None:
        _client = create_client(settings.supabase_url, settings.supabase_service_key)
    return _client


# ---------- Users ----------

async def upsert_user(
    allegro_user_id: str,
    allegro_login: str,
    encrypted_access_token: str,
    encrypted_refresh_token: str,
    token_expires_at: str,
    email: Optional[str] = None,
) -> dict[str, Any]:
    db = get_client()
    data: dict[str, Any] = {
        "allegro_user_id": allegro_user_id,
        "allegro_login": allegro_login,
        "encrypted_access_token": encrypted_access_token,
        "encrypted_refresh_token": encrypted_refresh_token,
        "token_expires_at": token_expires_at,
    }
    if email is not None:
        data["email"] = email
    try:
        result = (
            db.table("users")
            .upsert(data, on_conflict="allegro_user_id")
            .execute()
        )
        return result.data[0]
    except Exception as exc:
        logger.error("upsert_user failed: %s | data keys: %s", exc, list(data.keys()))
        raise


async def get_user_by_allegro_id(allegro_user_id: str) -> Optional[dict[str, Any]]:
    db = get_client()
    result = (
        db.table("users")
        .select("*")
        .eq("allegro_user_id", allegro_user_id)
        .single()
        .execute()
    )
    return result.data


async def update_user_tokens(
    user_id: str,
    encrypted_access_token: str,
    encrypted_refresh_token: str,
    token_expires_at: str,
) -> None:
    db = get_client()
    db.table("users").update({
        "encrypted_access_token": encrypted_access_token,
        "encrypted_refresh_token": encrypted_refresh_token,
        "token_expires_at": token_expires_at,
    }).eq("id", user_id).execute()


async def update_user_plan(
    user_id: str,
    plan: str,
    stripe_customer_id: Optional[str] = None,
    stripe_subscription_id: Optional[str] = None,
) -> None:
    db = get_client()
    payload: dict[str, Any] = {"plan": plan}
    if stripe_customer_id is not None:
        payload["stripe_customer_id"] = stripe_customer_id
    if stripe_subscription_id is not None:
        payload["stripe_subscription_id"] = stripe_subscription_id
    db.table("users").update(payload).eq("id", user_id).execute()


# ---------- Snipes ----------

async def create_snipe(
    user_id: str,
    allegro_offer_id: str,
    allegro_offer_url: str,
    max_bid_amount: float,
    offer_image_url: Optional[str] = None,
    current_price: Optional[float] = None,
) -> dict[str, Any]:
    db = get_client()
    data: dict[str, Any] = {
        "user_id": user_id,
        "allegro_offer_id": allegro_offer_id,
        "allegro_offer_url": allegro_offer_url,
        "max_bid_amount": max_bid_amount,
        "status": SnipeStatus.waiting.value,
    }
    if offer_image_url is not None:
        data["offer_image_url"] = offer_image_url
    if current_price is not None:
        data["current_price"] = current_price
    result = db.table("snipes").insert(data).execute()
    return result.data[0]


async def get_active_snipes() -> list[dict[str, Any]]:
    """Return snipes with status 'waiting' or 'active', joined with user tokens."""
    db = get_client()
    result = (
        db.table("snipes")
        .select("*, users(id, allegro_login, encrypted_access_token, encrypted_refresh_token, token_expires_at)")
        .in_("status", [SnipeStatus.waiting.value, SnipeStatus.active.value])
        .execute()
    )
    return result.data or []


async def get_snipes_for_user(user_id: str) -> list[dict[str, Any]]:
    db = get_client()
    result = (
        db.table("snipes")
        .select("*")
        .eq("user_id", user_id)
        .order("created_at", desc=True)
        .execute()
    )
    return result.data or []


async def get_snipe_by_id(snipe_id: str) -> Optional[dict[str, Any]]:
    db = get_client()
    result = db.table("snipes").select("*").eq("id", snipe_id).single().execute()
    return result.data


async def update_snipe_status(
    snipe_id: str,
    status: SnipeStatus,
    result_message: Optional[str] = None,
    offer_title: Optional[str] = None,
    offer_end_time: Optional[str] = None,
    current_price: Optional[float] = None,
    executed_at: Optional[str] = None,
) -> None:
    db = get_client()
    payload: dict[str, Any] = {"status": status.value}
    if result_message is not None:
        payload["result_message"] = result_message
    if offer_title is not None:
        payload["offer_title"] = offer_title
    if offer_end_time is not None:
        payload["offer_end_time"] = offer_end_time
    if current_price is not None:
        payload["current_price"] = current_price
    if executed_at is not None:
        payload["executed_at"] = executed_at
    db.table("snipes").update(payload).eq("id", snipe_id).execute()


async def delete_snipe(snipe_id: str, user_id: str) -> bool:
    db = get_client()
    result = (
        db.table("snipes")
        .delete()
        .eq("id", snipe_id)
        .eq("user_id", user_id)
        .execute()
    )
    return bool(result.data)


# ---------- Snipe Logs ----------

async def log_action(snipe_id: str, action: str, details: Optional[str] = None) -> None:
    db = get_client()
    try:
        db.table("snipe_logs").insert({
            "snipe_id": snipe_id,
            "action": action,
            "details": details,
        }).execute()
    except Exception as exc:
        logger.error("Failed to write snipe log: %s", exc)


async def get_snipe_logs(snipe_id: str) -> list[dict[str, Any]]:
    db = get_client()
    result = (
        db.table("snipe_logs")
        .select("*")
        .eq("snipe_id", snipe_id)
        .order("created_at", desc=True)
        .execute()
    )
    return result.data or []
