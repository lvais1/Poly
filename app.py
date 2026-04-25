import threading
import time
import logging
import webbrowser

import requests as _requests
from flask import Flask, jsonify, request, render_template

import database as db
import fetcher
import recommendations as recs

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

app = Flask(__name__)

# ── Background scheduler ───────────────────────────────────────────────────

_scheduler_started = False
_scheduler_lock    = threading.Lock()


def _scheduler_loop():
    # On first run, fix any traders with missing profiles
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
    return jsonify(db.get_all_traders())


@app.post("/api/traders")
def api_add_trader():
    body   = request.get_json(force=True, silent=True) or {}
    wallet = (body.get("wallet") or "").strip().lower()

    if not wallet.startswith("0x") or len(wallet) != 42:
        return jsonify({"error": "Invalid wallet address — must be 0x followed by 40 hex chars"}), 400

    created = db.add_trader(wallet)
    if created:
        recs.invalidate()
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
    db.remove_trader(wallet)
    return jsonify({"status": "removed", "wallet": wallet})


@app.get("/api/trades")
def api_get_trades():
    wallet = (request.args.get("wallet") or "").strip().lower() or None
    limit  = min(int(request.args.get("limit", 500)), 2000)
    return jsonify(db.get_trades(wallet=wallet, limit=limit))


@app.get("/api/stats")
def api_get_stats():
    return jsonify(db.get_all_stats())


@app.get("/api/recommendations")
def api_recommendations():
    return jsonify(recs.get_recommendations())


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
