"""Async Allegro REST API client with connection pooling, auto-refresh and retry."""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Optional

import aiohttp

from app.config import settings

logger = logging.getLogger(__name__)

_session: Optional[aiohttp.ClientSession] = None

# Next.js build ID cache for /_next/data/ endpoint
_next_build_id: Optional[str] = None
_next_build_id_expires: float = 0.0
_NEXT_BUILD_ID_TTL = 3600  # 1 hour


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


# ---------- Next.js build ID discovery ----------


async def _get_next_build_id() -> Optional[str]:
    """Fetch Allegro's Next.js build ID from their homepage (cached, TTL 1h).

    The build ID is needed to construct /_next/data/{buildId}/oferta/{slug}.json URLs.
    Homepage is lighter on CF protection than individual offer pages.
    """
    import re as _re
    import time as _time

    global _next_build_id, _next_build_id_expires

    if _next_build_id and _time.time() < _next_build_id_expires:
        return _next_build_id

    try:
        connector = aiohttp.TCPConnector(ssl=True)
        timeout = aiohttp.ClientTimeout(total=10)
        async with aiohttp.ClientSession(connector=connector, timeout=timeout, headers=_BROWSER_HEADERS) as s:
            async with s.get("https://allegro.pl/", allow_redirects=True) as resp:
                if resp.status == 200:
                    text = await resp.text()
                    # Primary: buildId field inside __NEXT_DATA__ script
                    m = _re.search(r'"buildId"\s*:\s*"([^"]+)"', text)
                    if not m:
                        # Fallback: buildId embedded in /_next/static/{id}/ script src
                        m = _re.search(r'/_next/static/([a-zA-Z0-9_-]+)/', text)
                    if m:
                        _next_build_id = m.group(1)
                        _next_build_id_expires = _time.time() + _NEXT_BUILD_ID_TTL
                        logger.info("_get_next_build_id: buildId=%s (TTL %ds)", _next_build_id, _NEXT_BUILD_ID_TTL)
                        return _next_build_id
                    logger.warning("_get_next_build_id: homepage 200 but buildId not found")
                else:
                    logger.warning("_get_next_build_id: homepage → %d", resp.status)
    except Exception as exc:
        logger.warning("_get_next_build_id: failed: %s", exc)

    return None


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
    headers["User-Agent"] = "LastBid/1.0.0 (+https://lastbid.pl/info)"
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
    """Fetch offer details — try beta API, then fall back to page scraping."""

    api_result: Optional[dict] = None

    def _has_ending(d: dict) -> bool:
        return bool(
            _find_key(d, "endingAt")
            or _find_key(d, "endingTime")
            or _find_key(d, "endTime")
        )

    # Try 1: GET /bidding/offers/{id} (beta endpoint — offer details incl. endingAt)
    try:
        result = await _request("GET", f"{settings.allegro_api_url}/bidding/offers/{offer_id}",
                                access_token=access_token,
                                headers={"Accept": "application/vnd.allegro.beta.v1+json"})
        logger.info("GET /bidding/offers/%s keys: %s", offer_id, list(result.keys()))
        if _has_ending(result):
            return result
        api_result = result
        logger.info("GET /bidding/offers/%s: no endingAt — trying page scrape", offer_id)
    except AllegroNotFoundError:
        logger.warning("GET /bidding/offers/%s → 404, falling through to page scrape", offer_id)
    except AllegroAccessDeniedError:
        logger.warning("GET /bidding/offers/%s access denied, trying page scrape", offer_id)
    except Exception as e:
        logger.warning("GET /bidding/offers/%s failed: %s — trying page scrape", offer_id, e)

    # Try 2: scrape the offer page
    logger.info("Scraping offer page for %s", offer_id)
    scraped = await _scrape_offer_page(offer_id, offer_url)
    if scraped and scraped.get("endingAt"):
        return {**scraped, **(api_result or {}), "endingAt": scraped["endingAt"]}

    if api_result:
        logger.warning("get_offer: no endingAt found from any source for %s, returning partial data", offer_id)
        return api_result

    raise AllegroAccessDeniedError(f"Could not fetch offer {offer_id} from any source")


_BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "pl-PL,pl;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
    "Cache-Control": "max-age=0",
}


async def _scrape_offer_page(offer_id: str, offer_url: Optional[str] = None) -> Optional[dict[str, Any]]:
    """Scrape auction end time and basic details from the Allegro offer page.

    Attempt order:
    1. /_next/data/{buildId}/oferta/{slug}.json — raw JSON pageProps, may bypass CF
    2. m.allegro.pl mobile page — separate CF config, may be more lenient
    3. Direct aiohttp GET with browser headers
    4. curl_cffi Chrome TLS impersonation (chrome131)
    5. cloudscraper
    6. Playwright with cached cf_clearance
    7. ScraperAPI (if SCRAPER_API_KEY configured)
    """
    import json as _json
    import re as _re
    from urllib.parse import urlencode, urlparse as _urlparse

    global _next_build_id, _next_build_id_expires

    scrape_url = f"https://allegro.pl/oferta/{offer_id}"
    status = 0
    html = ""

    # Attempt 1: /_next/data/ JSON endpoint — returns pageProps as pure JSON
    build_id = await _get_next_build_id()
    if build_id:
        try:
            if offer_url:
                page_path = _urlparse(offer_url).path.rstrip("/")
            else:
                page_path = f"/oferta/{offer_id}"
            next_data_url = f"https://allegro.pl/_next/data/{build_id}{page_path}.json"
            # XHR-like headers — different from HTML navigation headers
            nd_headers = {
                **_BROWSER_HEADERS,
                "Accept": "application/json, text/plain, */*",
                "Sec-Fetch-Dest": "empty",
                "Sec-Fetch-Mode": "cors",
                "Sec-Fetch-Site": "same-origin",
                "Referer": scrape_url,
            }
            nd_headers.pop("Sec-Fetch-User", None)
            nd_headers.pop("Upgrade-Insecure-Requests", None)
            connector = aiohttp.TCPConnector(ssl=True)
            timeout = aiohttp.ClientTimeout(total=15)
            async with aiohttp.ClientSession(connector=connector, timeout=timeout) as s:
                async with s.get(next_data_url, headers=nd_headers, allow_redirects=True) as resp:
                    nd_status = resp.status
                    if nd_status == 200:
                        data = await resp.json(content_type=None)
                        props = data.get("pageProps", {})
                        offer_node = props.get("offer") or props.get("item") or props.get("auction") or {}
                        ending_at = (
                            offer_node.get("endingAt") or offer_node.get("endingTime") or offer_node.get("endTime")
                            or _find_key(data, "endingAt") or _find_key(data, "endingTime") or _find_key(data, "endTime")
                        )
                        logger.info(
                            "_scrape_offer_page: _next_data → 200, endingAt=%r, props_keys=%s",
                            ending_at, list(props.keys())[:10],
                        )
                        if ending_at:
                            title = offer_node.get("name") or _find_key(data, "name")
                            price_raw = _find_key(data, "amount")
                            return {
                                "endingAt": ending_at,
                                "name": title,
                                "sellingMode": {"price": {"amount": str(price_raw)}} if price_raw else {},
                            }
                    elif nd_status == 404:
                        # Build ID is stale — invalidate cache so next call re-discovers it
                        _next_build_id = None
                        _next_build_id_expires = 0.0
                        logger.warning("_scrape_offer_page: _next_data → 404 (stale buildId), cache cleared")
                    else:
                        body = await resp.text()
                        logger.warning("_scrape_offer_page: _next_data → %d for %s, body=%r", nd_status, next_data_url, body[:300])
        except Exception as exc:
            logger.warning("_scrape_offer_page: _next_data exception: %s", exc)
    else:
        logger.info("_scrape_offer_page: buildId unavailable, skipping _next_data attempt")

    # Attempt 2: m.allegro.pl mobile — may have lighter CF rules
    if not html:
        mobile_url = f"https://m.allegro.pl/oferta/{offer_id}"
        mobile_headers = {
            **_BROWSER_HEADERS,
            "User-Agent": (
                "Mozilla/5.0 (Linux; Android 13; Pixel 7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/131.0.0.0 Mobile Safari/537.36"
            ),
        }
        try:
            connector = aiohttp.TCPConnector(ssl=True)
            timeout = aiohttp.ClientTimeout(total=20)
            async with aiohttp.ClientSession(connector=connector, timeout=timeout, headers=mobile_headers) as s:
                async with s.get(mobile_url, allow_redirects=True) as resp:
                    status = resp.status
                    body = await resp.text()
                    if status == 200:
                        html = body
                        logger.info("_scrape_offer_page: mobile → 200 for %s", mobile_url)
                    else:
                        logger.warning("_scrape_offer_page: mobile → %d for %s, body=%r", status, mobile_url, body[:500])
        except Exception as exc:
            logger.warning("_scrape_offer_page: mobile exception: %s", exc)

    # Attempt 3: direct aiohttp with browser headers
    if not html:
        try:
            connector = aiohttp.TCPConnector(ssl=True)
            timeout = aiohttp.ClientTimeout(total=20)
            async with aiohttp.ClientSession(connector=connector, timeout=timeout, headers=_BROWSER_HEADERS) as s:
                async with s.get(scrape_url, allow_redirects=True) as resp:
                    status = resp.status
                    body = await resp.text()
                    if status == 200:
                        html = body
                        logger.info("_scrape_offer_page: direct_get → 200 for %s", scrape_url)
                    else:
                        logger.warning("_scrape_offer_page: direct_get → %d for %s, body=%r", status, scrape_url, body[:500])
        except Exception as exc:
            logger.warning("_scrape_offer_page: direct_get exception: %s", exc)

    # Attempt 4: curl_cffi Chrome TLS impersonation
    if not html:
        try:
            from curl_cffi.requests import AsyncSession
            async with AsyncSession(impersonate="chrome131") as s:
                resp = await s.get(scrape_url, timeout=15, allow_redirects=True)
            status = resp.status_code
            if status == 200:
                html = resp.text
                logger.info("_scrape_offer_page: curl_cffi chrome131 → 200 for %s", scrape_url)
            else:
                logger.warning("_scrape_offer_page: curl_cffi chrome131 → %d for %s, body=%r", status, scrape_url, resp.text[:500])
        except Exception as exc:
            logger.warning("_scrape_offer_page: curl_cffi failed for %s: %s", scrape_url, exc)

    # Attempt 5: cloudscraper
    if not html:
        try:
            import cloudscraper
            scraper = cloudscraper.create_scraper(browser={"browser": "chrome", "platform": "windows", "mobile": False})
            resp = await asyncio.get_event_loop().run_in_executor(
                None, lambda: scraper.get(scrape_url, timeout=20, allow_redirects=True)
            )
            status = resp.status_code
            if status == 200:
                html = resp.text
                logger.info("_scrape_offer_page: cloudscraper → 200 for %s", scrape_url)
            else:
                logger.warning("_scrape_offer_page: cloudscraper → %d for %s, body=%r", status, scrape_url, resp.text[:500])
        except Exception as exc:
            logger.warning("_scrape_offer_page: cloudscraper failed for %s: %s", scrape_url, exc)

    # Attempt 6: Playwright with cached cf_clearance
    if not html:
        try:
            from app.services import playwright_scraper
            cf_cookies = await playwright_scraper.get_cached_cf_cookies()
            if cf_cookies:
                cookie_header = "; ".join(f"{k}={v}" for k, v in cf_cookies.items())
                pw_headers = {**_BROWSER_HEADERS, "Cookie": cookie_header}
                connector = aiohttp.TCPConnector(ssl=True)
                timeout = aiohttp.ClientTimeout(total=20)
                async with aiohttp.ClientSession(connector=connector, timeout=timeout) as s:
                    async with s.get(scrape_url, headers=pw_headers, allow_redirects=True) as resp:
                        status = resp.status
                        body = await resp.text()
                        if status == 200:
                            html = body
                            logger.info("_scrape_offer_page: playwright+cf_cookies → 200 for %s", scrape_url)
                        else:
                            logger.warning("_scrape_offer_page: playwright+cf_cookies → %d, body=%r", status, body[:500])
        except Exception as exc:
            logger.warning("_scrape_offer_page: playwright attempt failed: %s", exc)

    # Attempt 7: ScraperAPI with render=true (renders JS, bypasses Cloudflare)
    if not html:
        try:
            if settings.scraper_api_key:
                proxy_url = f"https://api.scraperapi.com?{urlencode({'api_key': settings.scraper_api_key, 'url': scrape_url, 'country_code': 'pl', 'render': 'true'})}"
                session = get_session()
                async with session.get(proxy_url, timeout=aiohttp.ClientTimeout(total=90)) as resp:
                    status = resp.status
                    body = await resp.text()
                    if status == 200:
                        html = body
                        logger.info("_scrape_offer_page: ScraperAPI → 200 for %s", scrape_url)
                    else:
                        logger.warning("_scrape_offer_page: ScraperAPI → %d: %s", status, body[:300])
        except Exception as exc:
            logger.warning("_scrape_offer_page: ScraperAPI exception: %s", exc)

    if status == 404:
        raise AllegroNotFoundError(f"Offer {offer_id} not found (scrape 404)")
    if not html:
        logger.warning("_scrape_offer_page: all attempts failed (last status=%d) for %s", status, offer_id)
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
            # Try specific auction paths first before broad _find_key
            # Allegro Next.js structure: props.pageProps.offer.endingAt (or similar)
            props = data.get("props", {}).get("pageProps", {})
            offer_node = props.get("offer") or props.get("item") or props.get("auction") or {}
            ending_at = (
                offer_node.get("endingAt")
                or offer_node.get("endingTime")
                or offer_node.get("endTime")
                # Fallback: broad search but log what we found to help debug
                or _find_key(data, "endingAt")
                or _find_key(data, "endingTime")
                or _find_key(data, "endTime")
            )
            title = title or offer_node.get("name") or _find_key(data, "name")
            price = price or str(_find_key(data, "amount") or "")
            logger.info(
                "_scrape_offer_page: __NEXT_DATA__ parsed, ending_at=%r, pageProps_keys=%s",
                ending_at, list(props.keys())[:10],
            )
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

    # Strategy 4: REMOVED — picking first future ISO date from HTML is unreliable
    # (Allegro pages contain many future dates: promos, shipping estimates, etc.)

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
                mon_str = 'paź' if mon_str == 'paz' else mon_str
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
    body = {"amount": str(amount), "currency": "PLN"}
    return await _request("PUT", url, access_token=access_token, json=body)


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


# ---------- Exceptions ----------

class AllegroUnauthorizedError(Exception):
    pass


class AllegroNotFoundError(Exception):
    pass


class AllegroAccessDeniedError(Exception):
    pass
