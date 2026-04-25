import requests
import time
import logging

import database as db

TRADES_URL  = "https://data-api.polymarket.com/trades"
PROFILE_URL = "https://gamma-api.polymarket.com/public-profile"
HEADERS     = {"User-Agent": "PolyTracker/1.0"}
PAGE_SIZE   = 500

log = logging.getLogger(__name__)


def fetch_profile(wallet: str) -> dict:
    r = requests.get(
        PROFILE_URL,
        params={"address": wallet},
        headers=HEADERS,
        timeout=10,
    )
    r.raise_for_status()
    data = r.json()
    if isinstance(data, list):
        data = data[0] if data else {}
    return data or {}


def fetch_trades_page(wallet: str, offset: int = 0) -> list:
    r = requests.get(
        TRADES_URL,
        params={"user": wallet, "limit": PAGE_SIZE, "offset": offset},
        headers=HEADERS,
        timeout=15,
    )
    r.raise_for_status()
    return r.json() or []


def normalize_trade(wallet: str, raw: dict) -> dict:
    ts = raw.get("timestamp", 0)
    try:
        ts = int(float(ts))
    except (TypeError, ValueError):
        ts = 0
    return {
        "tx_hash":       raw.get("transactionHash", ""),
        "wallet":        wallet,
        "side":          (raw.get("side") or "").upper(),
        "market_title":  raw.get("title", ""),
        "market_slug":   raw.get("slug", ""),
        "market_icon":   raw.get("icon", ""),
        "event_slug":    raw.get("eventSlug", ""),
        "outcome":       raw.get("outcome", ""),
        "outcome_index": raw.get("outcomeIndex"),
        "size":          float(raw.get("size") or 0),
        "price":         float(raw.get("price") or 0),
        "timestamp":     ts,
        "fetched_at":    int(time.time()),
    }


def _profile_from_raw(raw: dict) -> dict:
    """Extract profile fields embedded in a trade row."""
    return {
        "name":         raw.get("name") or "",
        "pseudonym":    raw.get("pseudonym") or "",
        "bio":          raw.get("bio") or "",
        "profileImage": raw.get("profileImage") or raw.get("profileImageOptimized") or "",
    }


# ── Backfill (first add) ────────────────────────────────────────────────────

def backfill_trader(wallet: str):
    """Fetch profile + full trade history when a trader is first added."""
    profile = {}
    try:
        profile = fetch_profile(wallet)
        if profile:
            db.update_trader_profile(wallet, profile)
            log.info("Profile fetched for %s: %s", wallet,
                     profile.get("name") or profile.get("pseudonym") or "(no display name)")
    except Exception as e:
        log.warning("Profile fetch failed for %s: %s", wallet, e)

    try:
        all_rows = []
        first_raw = None
        offset = 0
        while True:
            page = fetch_trades_page(wallet, offset=offset)
            if not page:
                break
            if first_raw is None:
                first_raw = page[0]
            all_rows.extend(normalize_trade(wallet, t) for t in page)
            if len(page) < PAGE_SIZE:
                break
            offset += PAGE_SIZE

        if all_rows:
            db.upsert_trades(all_rows)
        db.set_last_fetched(wallet, int(time.time()))
        log.info("Backfill: stored %d trades for %s", len(all_rows), wallet)

        # Enrich profile with image/bio from trade data if API returned incomplete info
        if first_raw:
            db.enrich_trader_profile(wallet, _profile_from_raw(first_raw))

    except Exception as e:
        log.error("Backfill failed for %s: %s", wallet, e)


# ── Incremental fetch (scheduler) ──────────────────────────────────────────

def incremental_fetch(wallet: str) -> int:
    """Fetch only trades newer than what's in the DB. Returns count of new trades."""
    known_max = db.get_latest_trade_timestamp(wallet) or 0
    offset = 0
    total_new = 0
    first_new_raw = None

    while True:
        try:
            page = fetch_trades_page(wallet, offset=offset)
        except Exception as e:
            log.error("Fetch error for %s offset=%d: %s", wallet, offset, e)
            break

        if not page:
            break

        new_rows = []
        for t in page:
            if int(float(t.get("timestamp") or 0)) > known_max:
                new_rows.append(normalize_trade(wallet, t))
                if first_new_raw is None:
                    first_new_raw = t

        if new_rows:
            db.upsert_trades(new_rows)
            total_new += len(new_rows)

        # Reached already-seen trades — stop paging
        if len(new_rows) < len(page):
            break
        if len(page) < PAGE_SIZE:
            break

        offset += PAGE_SIZE

    db.set_last_fetched(wallet, int(time.time()))

    if total_new:
        log.info("Incremental: %d new trades for %s", total_new, wallet)
        # Keep profile image/bio fresh from latest trade data
        if first_new_raw:
            db.enrich_trader_profile(wallet, _profile_from_raw(first_new_raw))

    return total_new


def refresh_missing_profiles():
    """Fetch profiles for traders whose name or image is still empty."""
    traders = db.get_all_traders()
    for t in traders:
        if t.get("name") and t.get("profile_image"):
            continue
        wallet = t["wallet"]
        try:
            profile = fetch_profile(wallet)
            if profile:
                db.update_trader_profile(wallet, profile)
                log.info("Late profile fetch for %s: %s", wallet,
                         profile.get("name") or profile.get("pseudonym") or "(no name)")
            # Also try extracting from a trade row
            page = fetch_trades_page(wallet, offset=0)
            if page:
                db.enrich_trader_profile(wallet, _profile_from_raw(page[0]))
        except Exception as e:
            log.warning("Profile refresh failed for %s: %s", wallet, e)


def fetch_all_traders():
    traders = db.get_all_traders()
    for t in traders:
        # Skip traders whose backfill hasn't completed yet
        if t.get("last_fetched") is None:
            continue
        incremental_fetch(t["wallet"])


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    db.init_db()
    log.info("Running one-shot fetch for all tracked traders...")
    fetch_all_traders()
    log.info("Done.")
