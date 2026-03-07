"""SniperEngine — executes a single snipe with multi-bid fallback strategy."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from app.config import settings
from app.models.schemas import SnipeStatus
from app.services import allegro_client, supabase_client, token_manager
from app.sniper import timing

logger = logging.getLogger(__name__)

# Bid schedule: seconds before auction end for each bid attempt.
# All values must be within the 0.1–1.0s window.
# The last offset is replaced at runtime by settings.snipe_offset_ms.
_BID_OFFSETS_S: list[float] = [0.9, 0.5, 0.15]

# How long after auction end to wait before checking the result.
_POST_END_WAIT_S: float = 8.0


class SniperEngine:
    """Executes a snipe for a given snipe record."""

    async def execute_snipe(self, snipe: dict[str, Any]) -> None:
        snipe_id = snipe["id"]
        offer_id = snipe["allegro_offer_id"]
        max_amount = float(snipe["max_bid_amount"])
        user = snipe.get("users") or {}
        user_login = user.get("allegro_login", "")

        logger.info("[snipe:%s] Starting execution for offer %s", snipe_id, offer_id)
        await supabase_client.log_action(snipe_id, "executing", f"offer_id={offer_id}")
        await supabase_client.update_snipe_status(snipe_id, SnipeStatus.executing)

        # 1. Decrypt access token
        try:
            access_token = token_manager.decrypt_token(user["encrypted_access_token"])
            refresh_tok = token_manager.decrypt_token(user["encrypted_refresh_token"])
        except Exception as exc:
            await self._fail(snipe_id, f"Token decryption failed: {exc}")
            return

        # 2. Proactive token refresh if expiring within 5 minutes
        expires_at_str = user.get("token_expires_at")
        if expires_at_str:
            try:
                expires_at = datetime.fromisoformat(expires_at_str.replace("Z", "+00:00"))
                if (expires_at - datetime.now(timezone.utc)).total_seconds() < 300:
                    access_token, refresh_tok = await self._refresh_and_save(
                        user["id"], refresh_tok
                    )
            except Exception as exc:
                logger.warning("[snipe:%s] Token refresh check failed: %s", snipe_id, exc)

        # 3. Fetch current offer state (fresh end time + current price)
        offer: dict = {}
        try:
            offer = await allegro_client.get_offer(offer_id, access_token=access_token, offer_url=snipe.get("allegro_offer_url"))
        except allegro_client.AllegroNotFoundError:
            await self._fail(snipe_id, "Offer not found on Allegro")
            return
        except allegro_client.AllegroAccessDeniedError as exc:
            logger.warning("[snipe:%s] get_offer access denied, falling back to stored end_time: %s", snipe_id, exc)
        except Exception as exc:
            logger.warning("[snipe:%s] get_offer failed, falling back to stored end_time: %s", snipe_id, exc)

        end_time_str = (
            offer.get("publication", {}).get("endingAt")
            or offer.get("endingAt")
            or offer.get("endingTime")
            or offer.get("endTime")
            or snipe.get("offer_end_time")
        )
        if not end_time_str:
            await self._fail(snipe_id, "Could not determine offer end time")
            return

        end_time = datetime.fromisoformat(end_time_str.replace("Z", "+00:00")).timestamp()
        now = timing.ntp_time()

        if end_time <= now:
            await self._fail(snipe_id, "Offer already ended before snipe execution")
            return

        # 4. Pre-bid price check: abort if current minimum bid exceeds our max
        current_min_bid = self._extract_current_min_bid(offer)
        if current_min_bid is not None and current_min_bid > max_amount:
            await self._fail(
                snipe_id,
                f"Biezaca cena ({current_min_bid:.2f} PLN) przekracza "
                f"maksymalna stawke ({max_amount:.2f} PLN) — snipe anulowany",
            )
            return
        if current_min_bid is not None:
            await supabase_client.log_action(
                snipe_id, "price_check",
                f"current_min_bid={current_min_bid:.2f} max_bid={max_amount:.2f} — OK",
            )

        # 5. Fire bids: all within 0.1–1.0s window before auction end.
        #    The final bid uses the configured SNIPE_OFFSET_MS setting.
        final_offset_s = settings.snipe_offset_ms / 1000.0
        bid_offsets = _BID_OFFSETS_S[:-1] + [final_offset_s]
        bid_times = sorted(end_time - off for off in bid_offsets)

        last_result: dict | None = None
        for i, bid_target in enumerate(bid_times):
            await supabase_client.log_action(
                snipe_id, "bid_scheduled",
                f"bid#{i+1} offset={end_time - bid_target:.3f}s before_end",
            )
            await timing.precise_sleep(bid_target)

            try:
                result = await allegro_client.place_bid(offer_id, max_amount, access_token)
                last_result = result
                await supabase_client.log_action(
                    snipe_id, "bid_placed", f"bid#{i+1} result={result}"
                )
                logger.info("[snipe:%s] Bid #%d placed: %s", snipe_id, i + 1, result)
            except allegro_client.AllegroUnauthorizedError:
                try:
                    access_token, refresh_tok = await self._refresh_and_save(
                        user["id"], refresh_tok
                    )
                    result = await allegro_client.place_bid(offer_id, max_amount, access_token)
                    last_result = result
                    await supabase_client.log_action(
                        snipe_id, "bid_placed_after_refresh", f"bid#{i+1} result={result}"
                    )
                except Exception as exc:
                    await supabase_client.log_action(
                        snipe_id, "bid_failed", f"bid#{i+1} error={exc}"
                    )
            except Exception as exc:
                await supabase_client.log_action(
                    snipe_id, "bid_failed", f"bid#{i+1} error={exc}"
                )
                logger.warning("[snipe:%s] Bid #%d failed: %s", snipe_id, i + 1, exc)

        # 6. All bids failed → error
        executed_at = datetime.now(timezone.utc).isoformat()
        if last_result is None:
            await supabase_client.update_snipe_status(
                snipe_id, SnipeStatus.error, "All bid attempts failed",
                executed_at=executed_at,
            )
            await supabase_client.log_action(snipe_id, "finished", "All bids failed")
            logger.info("[snipe:%s] Finished with status=error", snipe_id)
            return

        # 7. Wait for auction to fully end, then verify result
        await timing.precise_sleep(end_time + _POST_END_WAIT_S)

        won, msg, current_price = await self._verify_win(
            offer_id, access_token, user_login, last_result
        )
        final_status = SnipeStatus.won if won else SnipeStatus.lost

        await supabase_client.update_snipe_status(
            snipe_id, final_status, msg,
            current_price=current_price,
            executed_at=executed_at,
        )
        await supabase_client.log_action(snipe_id, "finished", msg)
        logger.info("[snipe:%s] Finished with status=%s", snipe_id, final_status)

    # ---------- Win verification ----------

    async def _verify_win(
        self,
        offer_id: str,
        access_token: str,
        user_login: str,
        last_bid_result: dict,
    ) -> tuple[bool, str, Optional[float]]:
        """
        Determine win/loss after the auction ends.
        Returns (won, message, current_price).

        Strategy:
        1. Re-fetch ended offer and check sellingMode.auction.winner.login
        2. Fall back to winningBid.winner in the last bid API response
        """
        current_price: Optional[float] = self._extract_price(last_bid_result)

        # --- Primary: post-auction offer state check ---
        try:
            offer = await allegro_client.get_offer(offer_id, access_token=access_token)
            auction_winner = (
                offer.get("sellingMode", {})
                    .get("auction", {})
                    .get("winner", {})
            )
            winner_login = auction_winner.get("login", "")
            if winner_login and user_login:
                won = winner_login.lower() == user_login.lower()
                msg = (
                    "Aukcja wygrana"
                    if won
                    else f"Aukcja przegrana (wygral: {winner_login})"
                )
                return won, msg, current_price
        except Exception as exc:
            logger.warning("[verify_win] Post-auction offer fetch failed: %s", exc)

        # --- Fallback: winningBid field from the last placed bid response ---
        winning_bid = last_bid_result.get("winningBid", {})
        if winning_bid.get("winner") is True:
            return True, "Aukcja wygrana (potwierdzone przez API)", current_price
        if winning_bid.get("winner") is False:
            return False, "Aukcja przegrana (potwierdzone przez API)", current_price

        # --- Last resort: bid was accepted but outcome unknown ---
        logger.warning(
            "[verify_win] Could not determine outcome definitively for offer %s", offer_id
        )
        return True, "Bid przyjety — wynik nieznany (brak danych z API)", current_price

    # ---------- Helpers ----------

    async def _refresh_and_save(self, user_id: str, refresh_tok: str) -> tuple[str, str]:
        token_data = await allegro_client.refresh_token(refresh_tok)
        new_access = token_data["access_token"]
        new_refresh = token_data["refresh_token"]
        expires_in = int(token_data.get("expires_in", 3600))
        expires_at = (
            datetime.now(timezone.utc) + timedelta(seconds=expires_in)
        ).isoformat()
        await supabase_client.update_user_tokens(
            user_id,
            token_manager.encrypt_token(new_access),
            token_manager.encrypt_token(new_refresh),
            expires_at,
        )
        return new_access, new_refresh

    async def _fail(self, snipe_id: str, reason: str) -> None:
        logger.error("[snipe:%s] Failed: %s", snipe_id, reason)
        await supabase_client.update_snipe_status(snipe_id, SnipeStatus.error, reason)
        await supabase_client.log_action(snipe_id, "error", reason)

    @staticmethod
    def _extract_current_min_bid(offer: dict) -> Optional[float]:
        """
        Extract the minimum bid required to become the highest bidder.
        For auctions: sellingMode.auction.minimalPrice.
        For buy-now: sellingMode.price.
        Returns None if price info is unavailable.
        """
        try:
            price_raw = (
                offer.get("sellingMode", {}).get("auction", {}).get("minimalPrice", {}).get("amount")
                or offer.get("sellingMode", {}).get("price", {}).get("amount")
                or offer.get("price", {}).get("amount")
            )
            return float(price_raw) if price_raw is not None else None
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _extract_price(result: dict) -> Optional[float]:
        """Extract current price from Allegro bid response."""
        try:
            price = (
                result.get("price", {}).get("amount")
                or result.get("currentPrice", {}).get("amount")
                or result.get("winningBid", {}).get("price", {}).get("amount")
            )
            return float(price) if price is not None else None
        except (TypeError, ValueError):
            return None
