"""Playwright-based Cloudflare bypass with cf_clearance cookie caching."""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Optional

logger = logging.getLogger(__name__)

# Cached CF cookies: {"cf_clearance": "...", "__cf_bm": "...", "_expires": float}
_cf_cookie_cache: dict = {}
_CF_COOKIE_TTL = 25 * 60  # 25 min (CF clearance typically lasts ~30 min)
_playwright_lock = asyncio.Lock()


def _cache_valid() -> bool:
    return bool(_cf_cookie_cache) and time.time() < _cf_cookie_cache.get("_expires", 0)


async def _launch_and_get_cookies(target_url: str) -> Optional[dict]:
    """Launch headless Chromium, navigate to target_url, solve CF challenge, return cookies."""
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        logger.warning("playwright_scraper: playwright not installed — skipping")
        return None

    try:
        from playwright_stealth import stealth_async
        _use_stealth = True
    except ImportError:
        _use_stealth = False
        logger.warning("playwright_scraper: playwright-stealth not available, proceeding without it")

    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-setuid-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-blink-features=AutomationControlled",
                ],
            )
            ctx = await browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/131.0.0.0 Safari/537.36"
                ),
                locale="pl-PL",
                extra_http_headers={"Accept-Language": "pl-PL,pl;q=0.9,en-US;q=0.8,en;q=0.7"},
            )
            page = await ctx.new_page()
            if _use_stealth:
                await stealth_async(page)

            logger.info("playwright_scraper: navigating to %s", target_url)
            # Use "domcontentloaded" — "networkidle" times out on CF challenge pages
            # because the challenge JS keeps sending heartbeat requests indefinitely.
            await page.goto(target_url, wait_until="domcontentloaded", timeout=20_000)
            # Wait up to 12s for CF challenge JS to execute and set cf_clearance cookie
            for _ in range(12):
                cookies_now = await ctx.cookies()
                if any(c["name"] == "cf_clearance" for c in cookies_now):
                    logger.info("playwright_scraper: cf_clearance cookie appeared after JS challenge")
                    break
                await asyncio.sleep(1)

            cookies = await ctx.cookies()
            await browser.close()

            cookie_dict = {c["name"]: c["value"] for c in cookies}
            logger.info("playwright_scraper: obtained cookies: %s", list(cookie_dict.keys()))
            return cookie_dict

    except Exception as exc:
        logger.warning("playwright_scraper: browser launch failed: %s", exc)
        return None


async def get_cached_cf_cookies() -> Optional[dict]:
    """Return cached CF cookies, refreshing via Playwright when expired.

    Returns a dict of cookie name→value (without internal _expires key),
    or None if Playwright is unavailable or cf_clearance could not be obtained.
    """
    global _cf_cookie_cache

    if _cache_valid():
        ttl_left = _cf_cookie_cache["_expires"] - time.time()
        logger.info("playwright_scraper: using cached cf_clearance (%.0fs remaining)", ttl_left)
        return {k: v for k, v in _cf_cookie_cache.items() if k != "_expires"}

    async with _playwright_lock:
        # Re-check after acquiring lock (another coroutine may have refreshed)
        if _cache_valid():
            return {k: v for k, v in _cf_cookie_cache.items() if k != "_expires"}

        logger.info("playwright_scraper: cf_clearance expired/missing — launching browser")
        cookies = await _launch_and_get_cookies("https://allegro.pl/")

        if cookies and "cf_clearance" in cookies:
            _cf_cookie_cache = {**cookies, "_expires": time.time() + _CF_COOKIE_TTL}
            logger.info("playwright_scraper: cf_clearance cached for %ds", _CF_COOKIE_TTL)
            return {k: v for k, v in _cf_cookie_cache.items() if k != "_expires"}

        logger.warning(
            "playwright_scraper: no cf_clearance in response cookies=%s",
            list(cookies.keys()) if cookies else "none",
        )
        return None
