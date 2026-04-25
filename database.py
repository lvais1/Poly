import sqlite3
import time
import contextlib
from pathlib import Path

DB_PATH = Path(__file__).parent / "poly.db"

DDL = """
CREATE TABLE IF NOT EXISTS traders (
    wallet          TEXT PRIMARY KEY,
    name            TEXT,
    pseudonym       TEXT,
    bio             TEXT,
    profile_image   TEXT,
    x_username      TEXT,
    verified_badge  INTEGER DEFAULT 0,
    added_at        INTEGER NOT NULL,
    last_fetched    INTEGER
);

CREATE TABLE IF NOT EXISTS trades (
    tx_hash         TEXT PRIMARY KEY,
    wallet          TEXT NOT NULL,
    side            TEXT NOT NULL,
    market_title    TEXT,
    market_slug     TEXT,
    market_icon     TEXT,
    event_slug      TEXT,
    outcome         TEXT,
    outcome_index   INTEGER,
    size            REAL,
    price           REAL,
    timestamp       INTEGER NOT NULL,
    fetched_at      INTEGER NOT NULL,
    FOREIGN KEY (wallet) REFERENCES traders(wallet) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_trades_wallet_ts ON trades(wallet, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_trades_timestamp ON trades(timestamp DESC);
"""


def get_conn():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    return conn


@contextlib.contextmanager
def db():
    conn = get_conn()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db():
    with db() as conn:
        conn.executescript(DDL)


# ── Trader CRUD ────────────────────────────────────────────────────────────

def add_trader(wallet: str) -> bool:
    """Insert trader stub. Returns True if new, False if already exists."""
    with db() as conn:
        existing = conn.execute(
            "SELECT 1 FROM traders WHERE wallet = ?", (wallet,)
        ).fetchone()
        if existing:
            return False
        conn.execute(
            "INSERT INTO traders (wallet, added_at) VALUES (?, ?)",
            (wallet, int(time.time()))
        )
        return True


def update_trader_profile(wallet: str, profile: dict):
    with db() as conn:
        conn.execute("""
            UPDATE traders SET
                name           = ?,
                pseudonym      = ?,
                bio            = ?,
                profile_image  = ?,
                x_username     = ?,
                verified_badge = ?
            WHERE wallet = ?
        """, (
            profile.get("name"),
            profile.get("pseudonym"),
            profile.get("bio"),
            profile.get("profileImage"),
            profile.get("xUsername"),
            1 if profile.get("verifiedBadge") else 0,
            wallet,
        ))


def remove_trader(wallet: str):
    with db() as conn:
        conn.execute("DELETE FROM traders WHERE wallet = ?", (wallet,))


def get_all_traders():
    with db() as conn:
        rows = conn.execute(
            "SELECT * FROM traders ORDER BY added_at DESC"
        ).fetchall()
    return [dict(r) for r in rows]


def set_last_fetched(wallet: str, ts: int):
    with db() as conn:
        conn.execute(
            "UPDATE traders SET last_fetched = ? WHERE wallet = ?", (ts, wallet)
        )


# ── Trade CRUD ─────────────────────────────────────────────────────────────

def upsert_trades(rows: list):
    if not rows:
        return
    rows = [r for r in rows if r.get("tx_hash")]
    if not rows:
        return
    with db() as conn:
        conn.executemany("""
            INSERT OR IGNORE INTO trades
                (tx_hash, wallet, side, market_title, market_slug, market_icon,
                 event_slug, outcome, outcome_index, size, price, timestamp, fetched_at)
            VALUES
                (:tx_hash, :wallet, :side, :market_title, :market_slug, :market_icon,
                 :event_slug, :outcome, :outcome_index, :size, :price, :timestamp, :fetched_at)
        """, rows)


def get_trades(wallet: str = None, limit: int = 200):
    with db() as conn:
        if wallet:
            rows = conn.execute(
                "SELECT * FROM trades WHERE wallet = ? ORDER BY timestamp DESC LIMIT ?",
                (wallet, limit)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM trades ORDER BY timestamp DESC LIMIT ?",
                (limit,)
            ).fetchall()
    return [dict(r) for r in rows]


def get_latest_trade_timestamp(wallet: str):
    with db() as conn:
        row = conn.execute(
            "SELECT MAX(timestamp) as ts FROM trades WHERE wallet = ?", (wallet,)
        ).fetchone()
    return row["ts"] if row and row["ts"] is not None else None


def enrich_trader_profile(wallet: str, profile: dict):
    """Update profile fields only when the current stored value is NULL or empty."""
    with db() as conn:
        conn.execute("""
            UPDATE traders SET
                name          = COALESCE(NULLIF(TRIM(COALESCE(name,'')),          ''), NULLIF(?, '')),
                pseudonym     = COALESCE(NULLIF(TRIM(COALESCE(pseudonym,'')),     ''), NULLIF(?, '')),
                bio           = COALESCE(NULLIF(TRIM(COALESCE(bio,'')),           ''), NULLIF(?, '')),
                profile_image = COALESCE(NULLIF(TRIM(COALESCE(profile_image,'')), ''), NULLIF(?, ''))
            WHERE wallet = ?
        """, (
            profile.get("name"),
            profile.get("pseudonym"),
            profile.get("bio"),
            profile.get("profileImage"),
            wallet,
        ))


def get_recent_event_slugs(limit: int = 25) -> list:
    """Return the most recently traded event slugs across all tracked wallets."""
    with db() as conn:
        rows = conn.execute("""
            SELECT event_slug, MAX(timestamp) AS last_ts
            FROM trades
            WHERE event_slug != ''
            GROUP BY event_slug
            ORDER BY last_ts DESC
            LIMIT ?
        """, (limit,)).fetchall()
    return [r["event_slug"] for r in rows]


def get_all_stats() -> dict:
    """Return per-wallet trade statistics including activity data, keyed by wallet."""
    now = int(time.time())
    with db() as conn:
        rows = conn.execute("""
            SELECT
                wallet,
                COUNT(*)                                                              AS trade_count,
                SUM(CASE WHEN side='BUY'  THEN 1 ELSE 0 END)                        AS buy_count,
                SUM(CASE WHEN side='SELL' THEN 1 ELSE 0 END)                        AS sell_count,
                ROUND(SUM(CASE WHEN side='BUY'  THEN size*price ELSE 0 END), 2)     AS buy_vol,
                ROUND(SUM(CASE WHEN side='SELL' THEN size*price ELSE 0 END), 2)     AS sell_vol,
                ROUND(SUM(CASE WHEN side='SELL' THEN  size*price
                               WHEN side='BUY'  THEN -size*price
                               ELSE 0 END), 2)                                       AS net,
                MAX(timestamp)                                                        AS last_trade_ts,
                SUM(CASE WHEN timestamp > ? - 3600   THEN 1 ELSE 0 END)             AS trades_1h,
                SUM(CASE WHEN timestamp > ? - 86400  THEN 1 ELSE 0 END)             AS trades_24h,
                SUM(CASE WHEN timestamp > ? - 604800 THEN 1 ELSE 0 END)             AS trades_7d
            FROM trades
            GROUP BY wallet
        """, (now, now, now)).fetchall()
    return {r["wallet"]: dict(r) for r in rows}
