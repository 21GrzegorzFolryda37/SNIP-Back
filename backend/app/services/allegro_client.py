"""Async Allegro REST API client with connection pooling, auto-refresh and retry."""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Optional

import aiohttp

from app.config import settings

logger = logging.getLogger(__name__)

_session: Optional[aiohttp.ClientSession] = None
_offers_listing_blocked: bool = False  # cached: True once we know /offers/listing returns 403
_scraping_blocked: bool = False  # cached: True once we know allegro.pl scraping returns 403
_app_token: Optional[str] = None
_app_token_expires: float = 0


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
    headers["User-Agent"] = "LastBid/1.0 (contact: kontakt@lastbid.pl)"
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


# ---------- App-level token (client_credentials) ----------

async def _get_app_token() -> str:
    """Return a cached app-level token via client_credentials flow."""
    import time as _time
    global _app_token, _app_token_expires
    if _app_token and _time.time() < _app_token_expires - 60:
        return _app_token
    url = f"{settings.allegro_auth_url}/token"
    session = get_session()
    async with session.post(url, data={"grant_type": "client_credentials"}, auth=_client_auth()) as resp:
        resp.raise_for_status()
        data = await resp.json()
    _app_token = data["access_token"]
    _app_token_expires = _time.time() + int(data.get("expires_in", 3600))
    logger.info("Obtained new app-level token (expires_in=%s)", data.get("expires_in"))
    return _app_token


# ---------- Public API ----------


async def get_offer(offer_id: str, access_token: Optional[str] = None, offer_url: Optional[str] = None) -> dict[str, Any]:
    """Fetch offer details — try API endpoints, then fall back to page scraping."""
    # Try 1: GET /offers/listing?offer.id={id}  (public marketplace search — use app token)
    global _offers_listing_blocked
    if not _offers_listing_blocked:
        try:
            app_token = await _get_app_token()
            result = await _request(
                "GET", f"{settings.allegro_api_url}/offers/listing",
                access_token=app_token,
                params={"offer.id": offer_id, "limit": 1},
            )
            items = result.get("items", {}).get("regular", [])
            if items:
                logger.info("GET /offers/listing offer.id=%s → found, keys: %s", offer_id, list(items[0].keys()))
                return items[0]
            logger.warning("GET /offers/listing offer.id=%s → 200 but no items", offer_id)
        except AllegroAccessDeniedError as e1:
            logger.warning("GET /offers/listing access denied — disabling for this session: %s", e1)
            _offers_listing_blocked = True
        except AllegroNotFoundError:
            raise
        except Exception as e1:
            logger.warning("GET /offers/listing failed: %s", e1)

    # Try 2: GET /bidding/offers/{id}
    try:
        result = await _request("GET", f"{settings.allegro_api_url}/bidding/offers/{offer_id}", access_token=access_token)
        logger.info("GET /bidding/offers/%s keys: %s", offer_id, list(result.keys()))
        return result
    except AllegroNotFoundError as e2:
        body = str(e2).lower()
        if "unavailable" in body or "feature" in body:
            logger.warning("GET /bidding/offers/%s feature unavailable, trying page scrape", offer_id)
        else:
            raise AllegroNotFoundError(f"Offer {offer_id} not found") from e2
    except AllegroAccessDeniedError:
        logger.warning("GET /bidding/offers/%s access denied, trying page scrape", offer_id)
    except Exception as e2:
        logger.warning("GET /bidding/offers/%s failed: %s — trying page scrape", offer_id, e2)

    # Try 3: scrape the offer page (no API approval needed)
    global _scraping_blocked
    if not _scraping_blocked:
        logger.info("Scraping offer page for %s", offer_id)
        scraped = await _scrape_offer_page(offer_id, offer_url)
        if scraped:
            return scraped

    raise AllegroAccessDeniedError(f"Could not fetch offer {offer_id} from any source")


async def _scrape_offer_page(offer_id: str, offer_url: Optional[str] = None) -> Optional[dict[str, Any]]:
    """Scrape auction end time and basic details from the Allegro offer page.

    Uses ScraperAPI (residential proxy) when SCRAPER_API_KEY is configured.
    Falls back to curl_cffi Chrome TLS impersonation otherwise.
    """
    import json as _json
    import re as _re
    from urllib.parse import urlencode

    # Always use the simple /oferta/{id} URL for scraping — less Cloudflare protection than /produkt/
    scrape_url = f"https://allegro.pl/oferta/{offer_id}"
    status = 0
    html = ""

    scraperapi_ok = False
    try:
        if settings.scraper_api_key:
            proxy_url = f"https://api.scraperapi.com?{urlencode({'api_key': settings.scraper_api_key, 'url': scrape_url, 'country_code': 'pl'})}"
            session = get_session()
            async with session.get(proxy_url, timeout=aiohttp.ClientTimeout(total=60)) as resp:
                status = resp.status
                body = await resp.text()
                if status == 200:
                    html = body
                    scraperapi_ok = True
                else:
                    logger.warning("_scrape_offer_page: ScraperAPI → %d: %s", status, body[:300])
    except Exception as exc:
        logger.warning("_scrape_offer_page: ScraperAPI exception: %s", exc)

    # Fallback: curl_cffi Chrome TLS impersonation (works if Cloudflare doesn't block DC IPs)
    if not scraperapi_ok:
        try:
            from curl_cffi.requests import AsyncSession
            async with AsyncSession(impersonate="chrome120") as s:
                resp = await s.get(scrape_url, timeout=15, allow_redirects=True)
            status = resp.status_code
            if status == 200:
                html = resp.text
                logger.info("_scrape_offer_page: curl_cffi succeeded for %s", scrape_url)
            else:
                logger.warning("_scrape_offer_page: curl_cffi → %d for %s", status, scrape_url)
        except Exception as exc:
            logger.warning("_scrape_offer_page: curl_cffi failed for %s: %s", scrape_url, exc)

    if status == 404:
        raise AllegroNotFoundError(f"Offer {offer_id} not found (scrape 404)")
    if status == 403 and not settings.scraper_api_key:
        global _scraping_blocked
        logger.warning("_scrape_offer_page: 403 (Cloudflare IP block) — disabling for this session. Set SCRAPER_API_KEY to fix.")
        _scraping_blocked = True
        return None
    if status != 200:
        return None

    ending_at: Optional[str] = None
    title: Optional[str] = None
    price: Optional[str] = None

    logger.info("_scrape_offer_page: html len=%d, has __NEXT_DATA__: %s, first_500=%r", len(html), '__NEXT_DATA__' in html, html[:500])

    # Strategy 1: __NEXT_DATA__ JSON block
    nd_match = _re.search(r'<script id="__NEXT_DATA__"[^>]*>([\s\S]+?)</script>', html)
    if nd_match:
        try:
            data = _json.loads(nd_match.group(1))
            ending_at = (
                _find_key(data, "endingAt")
                or _find_key(data, "endingTime")
                or _find_key(data, "endTime")
            )
            title = title or _find_key(data, "name")
            price = price or str(_find_key(data, "amount") or "")
            logger.info("_scrape_offer_page: __NEXT_DATA__ parsed, ending_at=%r", ending_at)
        except Exception as exc:
            logger.warning("_scrape_offer_page: __NEXT_DATA__ parse failed: %s", exc)

    # Strategy 2: JSON-LD structured data
    if not ending_at:
        for ld_match in _re.finditer(r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>([\s\S]+?)</script>', html):
            try:
                ld = _json.loads(ld_match.group(1))
                ending_at = _find_key(ld, "availabilityEnds") or _find_key(ld, "endDate") or _find_key(ld, "endTime")
                if ending_at:
                    logger.info("_scrape_offer_page: JSON-LD found ending_at=%r", ending_at)
                    break
            except Exception:
                pass

    # Strategy 3: raw regex over entire HTML
    if not ending_at:
        m = _re.search(r'"(?:endingAt|endingTime|endTime)"\s*:\s*"([^"]+)"', html)
        ending_at = m.group(1) if m else None

    # Strategy 4: ISO datetime in the future (auction end time in JS bundles)
    if not ending_at:
        import datetime as _dt
        now_iso = _dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
        for m in _re.finditer(r'"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2}))"', html):
            candidate = m.group(1)
            # Only accept dates in the future
            candidate_cmp = candidate[:19].replace("T", " ")
            if candidate_cmp > now_iso:
                ending_at = candidate
                logger.info("_scrape_offer_page: ISO date fallback=%r", ending_at)
                break

    # Strategy 5: Polish date format visible in page text
    # e.g. "(niedz., 8 mar 2026, 11:36:47)" → ISO UTC
    if not ending_at:
        import datetime as _dt
        from zoneinfo import ZoneInfo
        _PL_MONTHS = {
            'sty': 1, 'lut': 2, 'mar': 3, 'kwi': 4, 'maj': 5, 'cze': 6,
            'lip': 7, 'sie': 8, 'wrz': 9, 'paź': 10, 'lis': 11, 'gru': 12,
        }
        _pl_pat = _re.compile(
            r'\((?:pon\.|wt\.|śr\.|czw\.|pt\.|sob\.|niedz\.),?\s*'
            r'(\d{1,2})\s+(sty|lut|mar|kwi|maj|cze|lip|sie|wrz|pa[zź]|lis|gru)\s+'
            r'(\d{4}),\s*(\d{2}:\d{2}:\d{2})\)',
            _re.UNICODE,
        )
        pm = _pl_pat.search(html)
        if pm:
            try:
                day, mon_str, year, time_str = pm.group(1), pm.group(2), pm.group(3), pm.group(4)
                mon_str = mon_str.replace('pa', 'paź') if mon_str == 'paz' else mon_str
                month = _PL_MONTHS.get(mon_str)
                if month:
                    h, mi, s = map(int, time_str.split(':'))
                    warsaw = ZoneInfo("Europe/Warsaw")
                    dt_local = _dt.datetime(int(year), month, int(day), h, mi, s, tzinfo=warsaw)
                    ending_at = dt_local.astimezone(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                    logger.info("_scrape_offer_page: Polish date fallback=%r → %r", pm.group(0), ending_at)
            except Exception as exc:
                logger.warning("_scrape_offer_page: Polish date parse failed: %s", exc)

    if not title:
        m = _re.search(r'"name"\s*:\s*"([^"\\]{3,})"', html)
        title = m.group(1) if m else None

    logger.info("_scrape_offer_page: final ending_at=%r for offer %s", ending_at, offer_id)

    if not ending_at:
        return None

    logger.info("_scrape_offer_page: offer %s endingAt=%s title=%r", offer_id, ending_at, title)
    return {
        "endingAt": ending_at,
        "name": title,
        "sellingMode": {"price": {"amount": price}} if price else {},
    }


def _find_key(obj: Any, key: str) -> Any:
    """Recursively find the first occurrence of key in a nested dict/list."""
    if isinstance(obj, dict):
        if key in obj:
            return obj[key]
        for v in obj.values():
            result = _find_key(v, key)
            if result is not None:
                return result
    elif isinstance(obj, list):
        for item in obj:
            result = _find_key(item, key)
            if result is not None:
                return result
    return None


async def place_bid(offer_id: str, amount: float, access_token: str) -> dict[str, Any]:
    """Place a bid on an auction offer."""
    url = f"{settings.allegro_api_url}/bidding/offers/{offer_id}/bid"
    payload = {"amount": {"amount": str(amount), "currency": "PLN"}}
    return await _request("PUT", url, access_token=access_token, json=payload)


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
