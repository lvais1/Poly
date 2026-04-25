import os
import sqlite3
import time
import logging
import contextlib
from pathlib import Path

DB_PATH = Path("/tmp/poly.db") if os.environ.get("VERCEL") else Path(__file__).parent / "poly.db"

log = logging.getLogger(__name__)

# traders: composite PK (wallet, session_id) so each user tracks wallets independently
# trades:  global/shared — same tx_hash is stored once regardless of who tracks that wallet
DDL = """
CREATE TABLE IF NOT EXISTS traders (
    wallet          TEXT NOT NULL,
    session_id      TEXT NOT NULL DEFAULT 'default',
    name            TEXT,
    pseudonym       TEXT,
    bio             TEXT,
    profile_image   TEXT,
    x_username      TEXT,
    verified_badge  INTEGER DEFAULT 0,
    added_at        INTEGER NOT NULL,
    last_fetched    INTEGER,
    PRIMARY KEY (wallet, session_id)
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
    fetched_at      INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_traders_session  ON traders(session_id);
CREATE INDEX IF NOT EXISTS idx_trades_wallet_ts ON trades(wallet, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_trades_timestamp ON trades(timestamp DESC);
"""


def get_conn():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
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


def _run_migrations():
    """Migrate single-user schema (wallet PK) → multi-user schema (wallet+session_id PK)."""
    conn = sqlite3.connect(str(DB_PATH))
    try:
        conn.execute("PRAGMA foreign_keys = OFF")
        cols = [r[1] for r in conn.execute("PRAGMA table_info(traders)").fetchall()]
        if "session_id" in cols:
            return

        log.info("DB migration: adding session_id to traders table")
        conn.execute("""
            CREATE TABLE traders_new (
                wallet          TEXT NOT NULL,
                session_id      TEXT NOT NULL DEFAULT 'default',
                name            TEXT,
                pseudonym       TEXT,
                bio             TEXT,
                profile_image   TEXT,
                x_username      TEXT,
                verified_badge  INTEGER DEFAULT 0,
                added_at        INTEGER NOT NULL,
                last_fetched    INTEGER,
                PRIMARY KEY (wallet, session_id)
            )
        """)
        conn.execute("""
            INSERT INTO traders_new
                (wallet, session_id, name, pseudonym, bio, profile_image,
                 x_username, verified_badge, added_at, last_fetched)
            SELECT wallet, 'default', name, pseudonym, bio, profile_image,
                   x_username, verified_badge, added_at, last_fetched
            FROM traders
        """)
        conn.execute("DROP TABLE traders")
        conn.execute("ALTER TABLE traders_new RENAME TO traders")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_traders_session ON traders(session_id)")

        # Recreate trades to drop the old FK constraint on wallet→traders(wallet)
        conn.execute("""
            CREATE TABLE trades_new (
                tx_hash      TEXT PRIMARY KEY,
                wallet       TEXT NOT NULL,
                side         TEXT NOT NULL,
                market_title TEXT, market_slug TEXT, market_icon TEXT,
                event_slug   TEXT, outcome TEXT, outcome_index INTEGER,
                size         REAL,  price REAL,
                timestamp    INTEGER NOT NULL,
                fetched_at   INTEGER NOT NULL
            )
        """)
        conn.execute("INSERT INTO trades_new SELECT * FROM trades")
        conn.execute("DROP TABLE trades")
        conn.execute("ALTER TABLE trades_new RENAME TO trades")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_trades_wallet_ts ON trades(wallet, timestamp DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_trades_timestamp ON trades(timestamp DESC)")

        conn.commit()
        log.info("DB migration complete")
    except Exception:
        conn.rollback()
        log.exception("DB migration failed")
        raise
    finally:
        conn.close()


def init_db():
    with db() as conn:
        conn.executescript(DDL)
    _run_migrations()


# ── Trader CRUD ────────────────────────────────────────────────────────────

def add_trader(wallet: str, session_id: str) -> bool:
    """Insert trader for this session. Returns True if new."""
    with db() as conn:
        if conn.execute(
            "SELECT 1 FROM traders WHERE wallet = ? AND session_id = ?",
            (wallet, session_id)
        ).fetchone():
            return False

        # If another session already tracks this wallet, copy the profile data
        existing = conn.execute(
            "SELECT * FROM traders WHERE wallet = ? LIMIT 1", (wallet,)
        ).fetchone()

        if existing:
            conn.execute("""
                INSERT INTO traders (wallet, session_id, name, pseudonym, bio,
                    profile_image, x_username, verified_badge, added_at, last_fetched)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (wallet, session_id,
                  existing["name"], existing["pseudonym"],
                  existing["bio"], existing["profile_image"],
                  existing["x_username"], existing["verified_badge"],
                  int(time.time()), existing["last_fetched"]))
        else:
            conn.execute(
                "INSERT INTO traders (wallet, session_id, added_at) VALUES (?, ?, ?)",
                (wallet, session_id, int(time.time()))
            )
        return True


def update_trader_profile(wallet: str, profile: dict):
    """Overwrite profile for all sessions tracking this wallet."""
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


def remove_trader(wallet: str, session_id: str):
    with db() as conn:
        conn.execute(
            "DELETE FROM traders WHERE wallet = ? AND session_id = ?",
            (wallet, session_id)
        )
        # Remove trade data only if no session tracks this wallet anymore
        if conn.execute(
            "SELECT COUNT(*) FROM traders WHERE wallet = ?", (wallet,)
        ).fetchone()[0] == 0:
            conn.execute("DELETE FROM trades WHERE wallet = ?", (wallet,))


def get_all_traders(session_id: str) -> list:
    with db() as conn:
        rows = conn.execute(
            "SELECT * FROM traders WHERE session_id = ? ORDER BY added_at DESC",
            (session_id,)
        ).fetchall()
    return [dict(r) for r in rows]


def get_all_unique_wallets() -> list:
    """All unique wallets across all sessions — used by the background scheduler."""
    with db() as conn:
        rows = conn.execute("""
            SELECT wallet,
                   MAX(name)          AS name,
                   MAX(profile_image) AS profile_image,
                   MIN(added_at)      AS added_at,
                   MAX(last_fetched)  AS last_fetched
            FROM traders
            GROUP BY wallet
        """).fetchall()
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


def get_trades(wallet: str = None, session_id: str = None, limit: int = 200):
    with db() as conn:
        if wallet and session_id:
            rows = conn.execute(
                "SELECT * FROM trades WHERE wallet = ? ORDER BY timestamp DESC LIMIT ?",
                (wallet, limit)
            ).fetchall()
        elif session_id:
            rows = conn.execute("""
                SELECT t.* FROM trades t
                WHERE t.wallet IN (SELECT wallet FROM traders WHERE session_id = ?)
                ORDER BY t.timestamp DESC LIMIT ?
            """, (session_id, limit)).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM trades ORDER BY timestamp DESC LIMIT ?", (limit,)
            ).fetchall()
    return [dict(r) for r in rows]


def get_latest_trade_timestamp(wallet: str):
    with db() as conn:
        row = conn.execute(
            "SELECT MAX(timestamp) as ts FROM trades WHERE wallet = ?", (wallet,)
        ).fetchone()
    return row["ts"] if row and row["ts"] is not None else None


def enrich_trader_profile(wallet: str, profile: dict):
    """Fill empty profile fields for all sessions tracking this wallet."""
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


def get_recent_event_slugs(session_id: str = None, limit: int = 25) -> list:
    with db() as conn:
        if session_id:
            rows = conn.execute("""
                SELECT t.event_slug, MAX(t.timestamp) AS last_ts
                FROM trades t
                WHERE t.event_slug != ''
                  AND t.wallet IN (SELECT wallet FROM traders WHERE session_id = ?)
                GROUP BY t.event_slug
                ORDER BY last_ts DESC
                LIMIT ?
            """, (session_id, limit)).fetchall()
        else:
            rows = conn.execute("""
                SELECT event_slug, MAX(timestamp) AS last_ts
                FROM trades
                WHERE event_slug != ''
                GROUP BY event_slug
                ORDER BY last_ts DESC
                LIMIT ?
            """, (limit,)).fetchall()
    return [r["event_slug"] for r in rows]


def get_all_stats(session_id: str) -> dict:
    now = int(time.time())
    with db() as conn:
        rows = conn.execute("""
            SELECT
                t.wallet,
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
            FROM trades t
            WHERE t.wallet IN (SELECT wallet FROM traders WHERE session_id = ?)
            GROUP BY t.wallet
        """, (now, now, now, session_id)).fetchall()
    return {r["wallet"]: dict(r) for r in rows}
