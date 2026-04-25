import secrets
import threading
import time
import logging
import uuid as _uuid
import webbrowser
from pathlib import Path

import requests as _requests
from flask import Flask, jsonify, request, render_template, session

import database as db
import fetcher
import recommendations as recs

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

app = Flask(__name__)

# Stable secret key — generated once, persisted to .secret_key so sessions
# survive app restarts.
_KEY_FILE = Path(__file__).parent / ".secret_key"
if _KEY_FILE.exists():
    app.secret_key = _KEY_FILE.read_text().strip()
else:
    app.secret_key = secrets.token_hex(32)
    _KEY_FILE.write_text(app.secret_key)

app.config["PERMANENT_SESSION_LIFETIME"] = 60 * 60 * 24 * 365  # 1 year


def get_session_id() -> str:
    """Return (and create if needed) the unique ID for this browser session."""
    if "uid" not in session:
        session["uid"] = str(_uuid.uuid4())
        session.permanent = True
    return session["uid"]


# ── Background scheduler ───────────────────────────────────────────────────

_scheduler_started = False
_scheduler_lock    = threading.Lock()


def _scheduler_loop():
    try:
        fetcher.refresh_missing_profiles()
    except Exception:
        log.exception("Profile refresh error")
    while True:
        try:
            fetcher.fetch_all_traders()
        except Exception:
            log.exception("Scheduler error")
        time.sleep(60)


def start_scheduler():
    global _scheduler_started
    with _scheduler_lock:
        if not _scheduler_started:
            t = threading.Thread(
                target=_scheduler_loop,
                daemon=True,
                name="fetcher-scheduler",
            )
            t.start()
            _scheduler_started = True
            log.info("Background fetcher started (60s interval)")


# ── Routes ─────────────────────────────────────────────────────────────────

@app.get("/")
def index():
    return render_template("index.html")


@app.get("/api/traders")
def api_list_traders():
    return jsonify(db.get_all_traders(get_session_id()))


@app.post("/api/traders")
def api_add_trader():
    body   = request.get_json(force=True, silent=True) or {}
    wallet = (body.get("wallet") or "").strip().lower()

    if not wallet.startswith("0x") or len(wallet) != 42:
        return jsonify({"error": "Invalid wallet address — must be 0x followed by 40 hex chars"}), 400

    sid     = get_session_id()
    created = db.add_trader(wallet, sid)
    if created:
        recs.invalidate(sid)
        threading.Thread(
            target=fetcher.backfill_trader,
            args=(wallet,),
            daemon=True,
            name=f"backfill-{wallet[:10]}",
        ).start()
        return jsonify({"status": "added", "wallet": wallet}), 201
    else:
        return jsonify({"status": "already_tracked", "wallet": wallet}), 200


@app.delete("/api/traders/<wallet>")
def api_remove_trader(wallet):
    wallet = wallet.strip().lower()
    db.remove_trader(wallet, get_session_id())
    return jsonify({"status": "removed", "wallet": wallet})


@app.get("/api/trades")
def api_get_trades():
    sid    = get_session_id()
    wallet = (request.args.get("wallet") or "").strip().lower() or None
    limit  = min(int(request.args.get("limit", 500)), 2000)
    return jsonify(db.get_trades(wallet=wallet, session_id=sid, limit=limit))


@app.get("/api/stats")
def api_get_stats():
    return jsonify(db.get_all_stats(get_session_id()))


@app.get("/api/recommendations")
def api_recommendations():
    shuffle = request.args.get("shuffle") == "1"
    return jsonify(recs.get_recommendations(get_session_id(), shuffle=shuffle))


@app.get("/api/positions/<wallet>")
def api_positions(wallet):
    wallet = wallet.strip().lower()
    try:
        r = _requests.get(
            "https://data-api.polymarket.com/positions",
            params={"user": wallet, "limit": 50, "sizeThreshold": "0.01"},
            headers={"User-Agent": "PolyTracker/1.0"},
            timeout=10,
        )
        r.raise_for_status()
        return jsonify(r.json())
    except Exception:
        return jsonify([])


@app.post("/api/fetch")
def api_manual_fetch():
    threading.Thread(
        target=fetcher.fetch_all_traders,
        daemon=True,
        name="manual-fetch",
    ).start()
    return jsonify({"status": "fetch_triggered"})


# ── pywebview JS API ────────────────────────────────────────────────────────

class PyWebViewAPI:
    def open_url(self, url: str):
        if url.startswith("http"):
            webbrowser.open(url)


# ── Boot ───────────────────────────────────────────────────────────────────

def _run_flask():
    import logging as _log
    _log.getLogger("werkzeug").setLevel(logging.ERROR)
    app.run(host="127.0.0.1", port=5000, debug=False, use_reloader=False)


if __name__ == "__main__":
    db.init_db()
    start_scheduler()
    try:
        import webview  # type: ignore
    except ImportError as exc:
        raise RuntimeError(
            "Desktop mode requires pywebview. "
            "Install it in a Python 3.12/3.13 virtual environment and run again."
        ) from exc

    flask_thread = threading.Thread(target=_run_flask, daemon=True, name="flask")
    flask_thread.start()
    time.sleep(0.8)

    log.info("Opening Poly Tracker window")
    webview.create_window(
        title="Poly Tracker",
        url="http://127.0.0.1:5000",
        width=1280,
        height=800,
        min_size=(900, 600),
        background_color="#0a0a0a",
        js_api=PyWebViewAPI(),
    )
    webview.start()
