"""Async Allegro REST API client with connection pooling, auto-refresh and retry."""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Optional

import aiohttp

from app.config import settings

logger = logging.getLogger(__name__)

_session: Optional[aiohttp.ClientSession] = None


def get_session() -> aiohttp.ClientSession:
    global _session
    if _session is None or _session.closed:
        connector = aiohttp.TCPConnector(limit=20, limit_per_host=10)
        timeout = aiohttp.ClientTimeout(total=10)
        _session = aiohttp.ClientSession(connector=connector, timeout=timeout)
    return _session


async def close_session() -> None:
    global _session
    if _session and not _session.closed:
        await _session.close()


# ---------- Internal helpers ----------

async def _request(
    method: str,
    url: str,
    access_token: Optional[str] = None,
    *,
    retries: int = 3,
    **kwargs,
) -> dict[str, Any]:
    session = get_session()
    headers = kwargs.pop("headers", {})
    headers["Accept"] = "application/vnd.allegro.public.v1+json"
    if access_token:
        headers["Authorization"] = f"Bearer {access_token}"

    last_exc: Exception = RuntimeError("No attempts made")
    for attempt in range(retries):
        try:
            async with session.request(method, url, headers=headers, **kwargs) as resp:
                if resp.status == 429:
                    retry_after = int(resp.headers.get("Retry-After", 2))
                    logger.warning("Rate limited by Allegro, waiting %ds", retry_after)
                    await asyncio.sleep(retry_after)
                    continue
                if resp.status == 401:
                    raise AllegroUnauthorizedError("Access token expired or invalid")
                if resp.status == 403:
                    body = await resp.text()
                    logger.warning("Allegro API %s %s → 403: %s", method, url, body[:500])
                    raise AllegroAccessDeniedError(f"Access denied: {body[:200]}")
                if resp.status == 404:
                    body = await resp.text()
                    logger.warning("Allegro API %s %s → 404: %s", method, url, body[:500])
                    raise AllegroNotFoundError(body[:500])
                if resp.status >= 400:
                    body = await resp.text()
                    logger.warning("Allegro API %s %s → %d: %s", method, url, resp.status, body[:500])
                resp.raise_for_status()
                return await resp.json()
        except (AllegroUnauthorizedError, AllegroAccessDeniedError, AllegroNotFoundError):
            raise
        except aiohttp.ClientError as exc:
            last_exc = exc
            if attempt < retries - 1:
                await asyncio.sleep(0.5 * (attempt + 1))
    raise last_exc


# ---------- Public API ----------


async def get_offer(offer_id: str, access_token: Optional[str] = None, offer_url: Optional[str] = None) -> dict[str, Any]:
    """Fetch offer details — try multiple endpoints."""
    # Try 1: GET /offers/{id}
    try:
        result = await _request("GET", f"{settings.allegro_api_url}/offers/{offer_id}", access_token=access_token)
        logger.info("GET /offers/%s keys: %s", offer_id, list(result.keys()))
        return result
    except AllegroAccessDeniedError as e1:
        logger.warning("GET /offers/%s access denied (marketplace approval needed), trying bidding endpoint: %s", offer_id, e1)
    except Exception as e1:
        logger.warning("GET /offers/%s failed: %s", offer_id, e1)

    # Try 2: GET /bidding/offers/{id}
    try:
        result = await _request("GET", f"{settings.allegro_api_url}/bidding/offers/{offer_id}", access_token=access_token)
        logger.info("GET /bidding/offers/%s keys: %s", offer_id, list(result.keys()))
        return result
    except AllegroNotFoundError as e2:
        body = str(e2).lower()
        if "unavailable" in body or "feature" in body:
            raise AllegroAccessDeniedError(
                f"GET /bidding/offers/{offer_id} feature unavailable (missing allegro:api:bidding scope?)"
            ) from e2
        raise AllegroNotFoundError(f"Offer {offer_id} not found") from e2
    except AllegroAccessDeniedError:
        raise
    except Exception as e2:
        logger.warning("GET /bidding/offers/%s failed: %s", offer_id, e2)
        raise AllegroAccessDeniedError(f"Could not fetch offer {offer_id} from any endpoint") from e2


async def place_bid(offer_id: str, amount: float, access_token: str) -> dict[str, Any]:
    """Place a bid on an auction offer."""
    url = f"{settings.allegro_api_url}/bidding/offers/{offer_id}/bids"
    payload = {"amount": {"amount": str(amount), "currency": "PLN"}}
    return await _request("POST", url, access_token=access_token, json=payload)


async def get_user_profile(access_token: str) -> dict[str, Any]:
    """Fetch the authenticated user's Allegro profile."""
    url = f"{settings.allegro_api_url}/me"
    return await _request("GET", url, access_token=access_token)


async def refresh_token(refresh_tok: str) -> dict[str, Any]:
    """Exchange a refresh token for a new access + refresh token pair."""
    url = f"{settings.allegro_auth_url}/token"
    session = get_session()
    data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_tok,
        "redirect_uri": settings.allegro_redirect_uri,
    }
    async with session.post(url, data=data, auth=_client_auth()) as resp:
        resp.raise_for_status()
        return await resp.json()


async def exchange_code(code: str) -> dict[str, Any]:
    """Exchange OAuth2 authorization code for tokens."""
    url = f"{settings.allegro_auth_url}/token"
    session = get_session()
    data = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": settings.allegro_redirect_uri,
        "client_id": settings.allegro_client_id,
    }
    logger.info("exchange_code: url=%s redirect_uri=%s client_id=%s code_len=%d",
                url, settings.allegro_redirect_uri, settings.allegro_client_id, len(code))
    async with session.post(url, data=data, auth=_client_auth()) as resp:
        if not resp.ok:
            body = await resp.text()
            raise Exception(f"{resp.status} {resp.reason} — {body}")
        return await resp.json()


# ---------- Auth helpers ----------

def _client_auth() -> aiohttp.BasicAuth:
    return aiohttp.BasicAuth(settings.allegro_client_id, settings.allegro_client_secret)


def _basic_auth() -> str:
    import base64
    creds = f"{settings.allegro_client_id}:{settings.allegro_client_secret}"
    return base64.b64encode(creds.encode()).decode()


# ---------- Exceptions ----------

class AllegroUnauthorizedError(Exception):
    pass


class AllegroNotFoundError(Exception):
    pass


class AllegroAccessDeniedError(Exception):
    pass
