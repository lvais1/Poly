"""
Co-trader recommendation engine.

Finds wallets worth following by analysing which other traders are active
in the same markets as the currently tracked wallets.

Scoring formula (higher = better):
  shared_markets * 4  (trades in same events as you)
+ log(volume) * 0.4   (high-volume traders)
+ recency * 6         (active in last 24 h gets max bonus)
+ 1 if has profile image
"""

import math
import logging
import threading
import time

import requests

import database as db

HEADERS   = {"User-Agent": "PolyTracker/1.0"}
CACHE_TTL = 300   # seconds between recomputes per session
log = logging.getLogger(__name__)

# Per-session cache: session_id -> (results, timestamp)
_cache: dict = {}
_lock = threading.Lock()


# ── Public API ──────────────────────────────────────────────────────────────

def get_recommendations(session_id: str, shuffle: bool = False) -> list:
    """Return cached recommendations for this session, recomputing if stale."""
    with _lock:
        if not shuffle and session_id in _cache:
            results, ts = _cache[session_id]
            if time.time() - ts < CACHE_TTL:
                return list(results)

    result = _compute(session_id, shuffle=shuffle)

    if not shuffle:
        with _lock:
            _cache[session_id] = (result, time.time())

    return result


def invalidate(session_id: str = None):
    """Force recompute on next call. Pass session_id=None to clear all."""
    with _lock:
        if session_id:
            _cache.pop(session_id, None)
        else:
            _cache.clear()


# ── Engine ──────────────────────────────────────────────────────────────────

def _compute(session_id: str, shuffle: bool = False) -> list:
    import random as _random

    tracked = {t["wallet"] for t in db.get_all_traders(session_id)}
    if not tracked:
        return []

    slugs = db.get_recent_event_slugs(session_id=session_id, limit=30)
    if not slugs:
        return []

    sample = _random.sample(slugs, min(12, len(slugs))) if shuffle and len(slugs) > 4 else slugs[:12]

    candidates: dict[str, dict] = {}

    for slug in sample:
        try:
            r = requests.get(
                "https://data-api.polymarket.com/trades",
                params={"eventSlug": slug, "limit": 100},
                headers=HEADERS,
                timeout=10,
            )
            if r.status_code != 200:
                continue

            for trade in r.json():
                w = trade.get("proxyWallet", "")
                if not w or w in tracked:
                    continue

                if w not in candidates:
                    candidates[w] = {
                        "wallet":         w,
                        "name":           trade.get("name")         or "",
                        "pseudonym":      trade.get("pseudonym")    or "",
                        "profileImage":   trade.get("profileImage") or "",
                        "bio":            trade.get("bio")          or "",
                        "volume":         0.0,
                        "trade_count":    0,
                        "last_ts":        0,
                        "shared_markets": set(),
                    }

                c = candidates[w]
                c["volume"]      += float(trade.get("size") or 0) * float(trade.get("price") or 0)
                c["trade_count"] += 1
                c["shared_markets"].add(slug)

                ts = int(float(trade.get("timestamp") or 0))
                if ts > c["last_ts"]:
                    c["last_ts"] = ts
                    if trade.get("name"):         c["name"]         = trade["name"]
                    if trade.get("pseudonym"):    c["pseudonym"]    = trade["pseudonym"]
                    if trade.get("profileImage"): c["profileImage"] = trade["profileImage"]
                    if trade.get("bio"):          c["bio"]          = trade["bio"]

        except Exception as e:
            log.warning("Rec fetch failed for slug %s: %s", slug, e)

    now = time.time()
    results = []

    for c in candidates.values():
        recency = max(0.0, 1.0 - (now - c["last_ts"]) / 86400)
        score = (
            len(c["shared_markets"]) * 4
            + math.log1p(c["volume"]) * 0.4
            + recency * 6
            + (1 if c["profileImage"] else 0)
        )
        results.append({
            "wallet":         c["wallet"],
            "name":           c["name"],
            "pseudonym":      c["pseudonym"],
            "profileImage":   c["profileImage"],
            "bio":            c["bio"],
            "score":          round(score, 2),
            "shared_markets": len(c["shared_markets"]),
            "volume":         round(c["volume"], 2),
            "trade_count":    c["trade_count"],
            "last_ts":        c["last_ts"],
        })

    results.sort(key=lambda x: x["score"], reverse=True)
    log.info("Recommendations [%s]: %d candidates → returning top %d",
             session_id[:8], len(results), min(30, len(results)))
    return results[:30]
