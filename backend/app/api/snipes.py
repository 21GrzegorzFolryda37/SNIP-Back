"""CRUD endpoints for snipes."""

from __future__ import annotations

import re
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from app.models.schemas import SnipeCreate, SnipeResponse, SnipeStatus
from app.services import allegro_client, supabase_client

router = APIRouter(prefix="/snipes", tags=["snipes"])


def _extract_offer_id(url: str) -> str:
    """Extract numeric offer ID from an Allegro URL."""
    # Try ?offerId=... query param first (produkt URLs)
    qs_match = re.search(r"[?&]offerId=(\d+)", url)
    if qs_match:
        return qs_match.group(1)
    # Fall back to trailing numeric ID in path (standard oferta URLs)
    match = re.search(r"-(\d+)$", url.rstrip("/").split("?")[0])
    if not match:
        raise ValueError(f"Cannot extract offer ID from URL: {url}")
    return match.group(1)


@router.post("", response_model=SnipeResponse)
async def create_snipe(payload: SnipeCreate, user_id: str = Query(...)):
    """Create a new snipe for the given user."""
    user = await supabase_client.get_user_by_allegro_id(user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found. Authenticate first via /auth/login")

    try:
        offer_id = _extract_offer_id(payload.allegro_offer_url)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    # Fetch offer metadata upfront so we can store title + end time + image + price
    offer_title: Optional[str] = None
    offer_end_time: Optional[str] = None
    offer_image_url: Optional[str] = None
    current_price: Optional[float] = None
    try:
        offer = await allegro_client.get_offer(offer_id)
        offer_title = offer.get("name") or offer.get("title")
        offer_end_time = offer.get("endingAt") or offer.get("endTime")
        # Image: try common Allegro response shapes
        images = offer.get("images") or []
        if images:
            offer_image_url = images[0].get("url")
        # Price: try common shapes
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
    except Exception:
        pass  # Non-fatal — scheduler will hydrate later

    db_snipe = await supabase_client.create_snipe(
        user_id=user["id"],
        allegro_offer_id=offer_id,
        allegro_offer_url=payload.allegro_offer_url,
        max_bid_amount=payload.max_bid_amount,
        offer_image_url=offer_image_url,
        current_price=current_price,
    )

    # Patch offer metadata into record if available
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
async def list_snipes(user_id: str = Query(...)):
    snipes = await supabase_client.get_snipes_for_user(user_id)
    return snipes


@router.get("/{snipe_id}", response_model=SnipeResponse)
async def get_snipe(snipe_id: str, user_id: str = Query(...)):
    snipe = await supabase_client.get_snipe_by_id(snipe_id)
    if not snipe or snipe.get("user_id") != user_id:
        raise HTTPException(status_code=404, detail="Snipe not found")
    return snipe


@router.delete("/{snipe_id}")
async def delete_snipe(snipe_id: str, user_id: str = Query(...)):
    # Prevent deletion of in-flight snipes
    snipe = await supabase_client.get_snipe_by_id(snipe_id)
    if not snipe:
        raise HTTPException(status_code=404, detail="Snipe not found")
    if snipe.get("status") == SnipeStatus.executing:
        raise HTTPException(status_code=409, detail="Cannot delete a snipe that is currently executing")

    deleted = await supabase_client.delete_snipe(snipe_id, user_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Snipe not found or access denied")
    return {"message": "Snipe deleted"}


@router.post("/{snipe_id}/cancel", response_model=SnipeResponse)
async def cancel_snipe(snipe_id: str, user_id: str = Query(...)):
    """Cancel a pending or active snipe."""
    snipe = await supabase_client.get_snipe_by_id(snipe_id)
    if not snipe or snipe.get("user_id") != user_id:
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
