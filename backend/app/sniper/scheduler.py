"""APScheduler — polls Supabase every 30s and queues hot snipes."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from app.services import allegro_client, supabase_client, token_manager
from app.models.schemas import SnipeStatus
from app.sniper.engine import SniperEngine

logger = logging.getLogger(__name__)

# Snipes ending within this many seconds → move to hot queue
HOT_WINDOW_S = 600

_scheduler: AsyncIOScheduler | None = None
_in_progress: set[str] = set()  # snipe IDs currently being executed
_engine = SniperEngine()


def get_scheduler() -> AsyncIOScheduler:
    global _scheduler
    if _scheduler is None:
        _scheduler = AsyncIOScheduler(timezone="UTC")
    return _scheduler


def is_running() -> bool:
    s = _scheduler
    return s is not None and s.running


async def _poll_snipes() -> None:
    """Fetch active snipes from Supabase and schedule hot ones."""
    try:
        snipes = await supabase_client.get_active_snipes()
    except Exception as exc:
        logger.error("Failed to fetch active snipes: %s", exc)
        return

    now = datetime.now(timezone.utc).timestamp()

    for snipe in snipes:
        snipe_id = snipe["id"]

        if snipe_id in _in_progress:
            continue

        end_time_str = snipe.get("offer_end_time")
        if not end_time_str:
            # Try to hydrate offer_end_time from Allegro API
            try:
                user_data = snipe.get("users") or {}
                encrypted_token = user_data.get("encrypted_access_token")
                if encrypted_token:
                    access_token = token_manager.decrypt_token(encrypted_token)
                    offer = await allegro_client.get_offer(snipe["allegro_offer_id"], access_token=access_token)
                    end_time_str = offer.get("endingAt") or offer.get("endTime")
                    if end_time_str:
                        await supabase_client.update_snipe_status(
                            snipe_id, SnipeStatus.waiting,
                            offer_end_time=end_time_str,
                            offer_title=offer.get("name") or offer.get("title"),
                        )
                        logger.info("[snipe:%s] Hydrated offer_end_time=%s", snipe_id, end_time_str)
            except Exception as exc:
                logger.warning("[snipe:%s] Failed to hydrate offer_end_time: %s", snipe_id, exc)
            if not end_time_str:
                continue

        try:
            end_time = datetime.fromisoformat(
                end_time_str.replace("Z", "+00:00")
            ).timestamp()
        except ValueError:
            continue

        time_until_end = end_time - now

        if time_until_end <= 0:
            # Already ended — mark as lost
            await supabase_client.update_snipe_status(
                snipe_id,
                __import__("app.models.schemas", fromlist=["SnipeStatus"]).SnipeStatus.lost,
                "Offer ended before snipe could execute",
            )
            continue

        if time_until_end <= HOT_WINDOW_S:
            _in_progress.add(snipe_id)
            asyncio.create_task(_run_snipe(snipe))
            logger.info(
                "Queued snipe %s (%.0fs until end)", snipe_id, time_until_end
            )


async def _run_snipe(snipe: dict) -> None:
    snipe_id = snipe["id"]
    try:
        await _engine.execute_snipe(snipe)
    except Exception as exc:
        logger.exception("Unhandled error in snipe %s: %s", snipe_id, exc)
    finally:
        _in_progress.discard(snipe_id)


def start(loop: asyncio.AbstractEventLoop | None = None) -> None:
    s = get_scheduler()
    s.add_job(_poll_snipes, "interval", seconds=30, id="poll_snipes", replace_existing=True)
    s.start()
    logger.info("Sniper scheduler started")


def stop() -> None:
    if _scheduler and _scheduler.running:
        _scheduler.shutdown(wait=False)
        logger.info("Sniper scheduler stopped")


def active_snipe_count() -> int:
    return len(_in_progress)
