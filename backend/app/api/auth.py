"""Allegro OAuth2 endpoints."""

from __future__ import annotations

import hashlib
import hmac
import secrets
import time
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import RedirectResponse

from app.config import settings
from app.models.schemas import TokenResponse
from app.services import allegro_client, supabase_client, token_manager

router = APIRouter(prefix="/auth", tags=["auth"])

ALLEGRO_SCOPES = " ".join([
    "allegro:api:bids",
    "allegro:api:sale:offers:read",
    "allegro:api:profile:read",
    "allegro:api:orders:read",
])

_STATE_TTL = 600  # 10 minut


def _sign_state(nonce: str) -> str:
    """Return 'nonce.timestamp.hmac' — verifiable without shared memory."""
    ts = str(int(time.time()))
    msg = f"{nonce}.{ts}".encode()
    sig = hmac.new(settings.encryption_key.encode(), msg, hashlib.sha256).hexdigest()[:16]
    return f"{nonce}.{ts}.{sig}"


def _verify_state(state: str) -> bool:
    parts = state.split(".")
    if len(parts) != 3:
        return False
    nonce, ts, sig = parts
    try:
        if time.time() - int(ts) > _STATE_TTL:
            return False
    except ValueError:
        return False
    msg = f"{nonce}.{ts}".encode()
    expected = hmac.new(settings.encryption_key.encode(), msg, hashlib.sha256).hexdigest()[:16]
    return hmac.compare_digest(sig, expected)


@router.get("/login")
async def login():
    """Redirect user to Allegro OAuth2 authorisation page."""
    state = _sign_state(secrets.token_urlsafe(16))

    params = {
        "response_type": "code",
        "client_id": settings.allegro_client_id,
        "redirect_uri": settings.allegro_redirect_uri,
        "scope": ALLEGRO_SCOPES,
        "state": state,
    }
    url = f"{settings.allegro_auth_url}/authorize?{urlencode(params)}"
    return RedirectResponse(url=url)


@router.get("/callback", response_model=TokenResponse)
async def callback(
    code: str = Query(...),
    state: str = Query(...),
):
    """Handle Allegro OAuth2 callback, exchange code for tokens, store encrypted."""
    if not _verify_state(state):
        raise HTTPException(status_code=400, detail="Invalid or expired OAuth state")

    try:
        token_data = await allegro_client.exchange_code(code)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Token exchange failed: {exc}") from exc

    access_token = token_data["access_token"]
    refresh_tok = token_data["refresh_token"]
    expires_in = int(token_data.get("expires_in", 3600))
    expires_at = (datetime.now(timezone.utc) + timedelta(seconds=expires_in)).isoformat()

    # Fetch user profile to get their Allegro ID
    try:
        profile = await allegro_client.get_user_profile(access_token)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Failed to fetch user profile: {exc}") from exc

    allegro_user_id = profile["id"]
    allegro_login = profile.get("login", "")

    encrypted_access = token_manager.encrypt_token(access_token)
    encrypted_refresh = token_manager.encrypt_token(refresh_tok)

    await supabase_client.upsert_user(
        allegro_user_id=allegro_user_id,
        allegro_login=allegro_login,
        encrypted_access_token=encrypted_access,
        encrypted_refresh_token=encrypted_refresh,
        token_expires_at=expires_at,
    )

    return TokenResponse(
        message="Authorisation successful",
        user_login=allegro_login,
    )


@router.post("/refresh")
async def refresh(allegro_user_id: str):
    """Manually refresh tokens for a user by their Allegro user ID."""
    user = await supabase_client.get_user_by_allegro_id(allegro_user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    try:
        refresh_tok = token_manager.decrypt_token(user["encrypted_refresh_token"])
        token_data = await allegro_client.refresh_token(refresh_tok)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Token refresh failed: {exc}") from exc

    new_access = token_data["access_token"]
    new_refresh = token_data["refresh_token"]
    expires_in = int(token_data.get("expires_in", 3600))
    expires_at = (datetime.now(timezone.utc) + timedelta(seconds=expires_in)).isoformat()

    await supabase_client.update_user_tokens(
        user_id=user["id"],
        encrypted_access_token=token_manager.encrypt_token(new_access),
        encrypted_refresh_token=token_manager.encrypt_token(new_refresh),
        token_expires_at=expires_at,
    )

    return {"message": "Tokens refreshed successfully"}
