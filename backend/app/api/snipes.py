"""CRUD endpoints for snipes."""

from __future__ import annotations

import logging
import re
from typing import Any, Optional

logger = logging.getLogger(__name__)

from fastapi import APIRouter, Depends, HTTPException, Query

from app.api.auth import decode_session_token
from app.models.schemas import SnipeCreate, SnipeResponse, SnipeStatus
from app.services import allegro_client, supabase_client, token_manager

router = APIRouter(prefix="/snipes", tags=["snipes"])


def _extract_offer_id(url: str) -> str:
    """Extract numeric offer ID from an Allegro URL."""
    qs_match = re.search(r"[?&]offerId=(\d+)", url)
    if qs_match:
        return qs_match.group(1)
    match = re.search(r"-(\d+)$", url.rstrip("/").split("?")[0])
    if not match:
        raise ValueError(f"Cannot extract offer ID from URL: {url}")
    return match.group(1)


async def _require_user(token: str = Query(...)) -> dict[str, Any]:
    """Validate session token and return the user dict from DB."""
    allegro_user_id = decode_session_token(token)
    if not allegro_user_id:
        raise HTTPException(status_code=401, detail="Invalid or expired session token. Please login again via /auth/login")
    user = await supabase_client.get_user_by_allegro_id(allegro_user_id)
    if not user:
        raise HTTPException(status_code=401, detail="User not found. Please login again via /auth/login")
    return user


@router.post("", response_model=SnipeResponse)
async def create_snipe(payload: SnipeCreate, user: dict = Depends(_require_user)):
    """Create a new snipe for the authenticated user."""
    try:
        offer_id = _extract_offer_id(payload.allegro_offer_url)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    offer_title: Optional[str] = None
    offer_end_time: Optional[str] = None
    offer_image_url: Optional[str] = None
    current_price: Optional[float] = None
    try:
        access_token = token_manager.decrypt_token(user["encrypted_access_token"])
        offer = await allegro_client.get_offer(offer_id, access_token=access_token, offer_url=payload.allegro_offer_url)
        offer_title = offer.get("name") or offer.get("title")
        offer_end_time = offer.get("endingAt") or offer.get("endTime")
        images = offer.get("images") or []
        if images:
            offer_image_url = images[0].get("url")
        try:
            price_raw = (
                offer.get("sellingMode", {}).get("price", {}).get("amount")
                or offer.get("price", {}).get("amount")
            )
            if price_raw is not None:
                current_price = float(price_raw)
        except (TypeError, ValueError):
            pass
    except allegro_client.AllegroNotFoundError:
        raise HTTPException(status_code=404, detail=f"Allegro offer {offer_id} not found")
    except (allegro_client.AllegroAccessDeniedError, Exception) as exc:
        logger.warning("Failed to fetch offer details for %s (will create snipe anyway): %s", offer_id, exc)

    db_snipe = await supabase_client.create_snipe(
        user_id=user["id"],
        allegro_offer_id=offer_id,
        allegro_offer_url=payload.allegro_offer_url,
        max_bid_amount=payload.max_bid_amount,
        offer_image_url=offer_image_url,
        current_price=current_price,
    )

    if offer_title or offer_end_time:
        await supabase_client.update_snipe_status(
            db_snipe["id"],
            SnipeStatus.waiting,
            offer_title=offer_title,
            offer_end_time=offer_end_time,
        )
        db_snipe["offer_title"] = offer_title
        db_snipe["offer_end_time"] = offer_end_time

    return db_snipe


@router.get("", response_model=list[SnipeResponse])
async def list_snipes(user: dict = Depends(_require_user)):
    snipes = await supabase_client.get_snipes_for_user(user["id"])
    return snipes


@router.get("/{snipe_id}", response_model=SnipeResponse)
async def get_snipe(snipe_id: str, user: dict = Depends(_require_user)):
    snipe = await supabase_client.get_snipe_by_id(snipe_id)
    if not snipe or snipe.get("user_id") != user["id"]:
        raise HTTPException(status_code=404, detail="Snipe not found")
    return snipe


@router.delete("/{snipe_id}")
async def delete_snipe(snipe_id: str, user: dict = Depends(_require_user)):
    snipe = await supabase_client.get_snipe_by_id(snipe_id)
    if not snipe:
        raise HTTPException(status_code=404, detail="Snipe not found")
    if snipe.get("status") == SnipeStatus.executing:
        raise HTTPException(status_code=409, detail="Cannot delete a snipe that is currently executing")

    deleted = await supabase_client.delete_snipe(snipe_id, user["id"])
    if not deleted:
        raise HTTPException(status_code=404, detail="Snipe not found or access denied")
    return {"message": "Snipe deleted"}


@router.post("/{snipe_id}/cancel", response_model=SnipeResponse)
async def cancel_snipe(snipe_id: str, user: dict = Depends(_require_user)):
    """Cancel a pending or active snipe."""
    snipe = await supabase_client.get_snipe_by_id(snipe_id)
    if not snipe or snipe.get("user_id") != user["id"]:
        raise HTTPException(status_code=404, detail="Snipe not found")

    cancellable = {SnipeStatus.waiting.value, SnipeStatus.active.value}
    if snipe.get("status") not in cancellable:
        raise HTTPException(
            status_code=409,
            detail=f"Cannot cancel snipe with status '{snipe.get('status')}'"
        )

    await supabase_client.update_snipe_status(snipe_id, SnipeStatus.cancelled)
    snipe["status"] = SnipeStatus.cancelled.value
    return snipe
