"""API monitor: checks config.json endpoints on a schedule and stores results in logs.db."""
import asyncio, json, sqlite3, statistics, time
from contextlib import asynccontextmanager, closing
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
from fastapi import FastAPI
from fastapi.responses import FileResponse

CONFIG = json.loads(Path("config.json").read_text())
INTERVAL = CONFIG.get("interval_seconds", 60)
RETENTION_DAYS = CONFIG.get("retention_days", 30)
WEBHOOK_URL = CONFIG.get("alerts", {}).get("webhook_url", "")
DB_FILE = Path("logs.db")
_lock = asyncio.Lock()

def _iso_now(dt: datetime | None = None) -> str:
    return (dt or datetime.now(timezone.utc)).isoformat(timespec="milliseconds").replace("+00:00", "Z")

def _init_db() -> None:
    with closing(sqlite3.connect(DB_FILE)) as con:
        con.execute("CREATE TABLE IF NOT EXISTS logs (id INTEGER PRIMARY KEY AUTOINCREMENT, url TEXT NOT NULL, status TEXT NOT NULL, success INTEGER NOT NULL, slow INTEGER, duration INTEGER, error TEXT, timestamp TEXT NOT NULL)")

_init_db()

def _row_to_record(row: tuple) -> dict:
    url, status, success, slow, duration, error, timestamp = row
    rec = {"url": url, "status": "ERROR" if status == "ERROR" else int(status),
           "success": bool(success), "timestamp": timestamp}
    for key, value in (("slow", None if slow is None else bool(slow)), ("duration", duration), ("error", error)):
        if value is not None:
            rec[key] = value
    return rec

def _save_logs(records: list[dict]) -> None:
    # ponytail: per-call connections + per-endpoint stats queries; fine at this scale, tune when history grows.
    rows = [(r["url"], str(r["status"]), int(r["success"]), r.get("slow"), r.get("duration"), r.get("error"), r["timestamp"]) for r in records]
    with closing(sqlite3.connect(DB_FILE)) as con:
        con.executemany("INSERT INTO logs (url, status, success, slow, duration, error, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?)", rows)
        con.commit()

def _read_logs(limit: int) -> list[dict]:
    with closing(sqlite3.connect(DB_FILE)) as con:
        rows = con.execute("SELECT url, status, success, slow, duration, error, timestamp FROM logs ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
    return [_row_to_record(row) for row in rows]

def _prev_success() -> dict:
    prev = {}
    with closing(sqlite3.connect(DB_FILE)) as con:
        for endpoint in CONFIG["endpoints"]:
            row = con.execute("SELECT success FROM logs WHERE url = ? ORDER BY id DESC LIMIT 1", (endpoint["url"],)).fetchone()
            if row:
                prev[endpoint["url"]] = bool(row[0])
    return prev

def _purge(days: int) -> None:
    cutoff = _iso_now(datetime.now(timezone.utc) - timedelta(days=days))
    with closing(sqlite3.connect(DB_FILE)) as con:
        con.execute("DELETE FROM logs WHERE timestamp < ?", (cutoff,))
        con.commit()

def _alerts(prev: dict[str, bool], records: list[dict]) -> list[str]:
    messages = []
    for rec in records:
        was_up, now_up = prev.get(rec["url"], True), rec["success"]
        if was_up and not now_up:
            messages.append(f"[api-monitor] DOWN: {rec['url']} - {rec.get('error') or 'HTTP ' + str(rec['status'])}")
        elif not was_up and now_up:
            messages.append(f"[api-monitor] RECOVERED: {rec['url']} - HTTP {rec['status']} in {rec['duration']}ms")
    return messages

async def check_endpoint(client: httpx.AsyncClient, endpoint: dict) -> dict:
    start = time.perf_counter()
    try:
        response = await client.get(endpoint["url"], timeout=endpoint.get("timeout", 10))
        duration = round((time.perf_counter() - start) * 1000)
        return {
            "url": endpoint["url"], "status": response.status_code, "duration": duration,
            "success": 200 <= response.status_code < 300, "slow": duration > endpoint["threshold"],
            "timestamp": _iso_now(),
        }
    except Exception as e:
        return {
            "url": endpoint["url"], "status": "ERROR", "success": False, "error": str(e), "timestamp": _iso_now(),
        }

async def run_checks() -> list[dict]:
    async with _lock:
        prev = _prev_success()
        records = []
        async with httpx.AsyncClient(timeout=10) as client:
            for endpoint in CONFIG["endpoints"]:
                rec = await check_endpoint(client, endpoint)
                records.append(rec)
                if rec["status"] == "ERROR":
                    print(f"[{rec['timestamp']}] {rec['url']} - DOWN - {rec['error']}", flush=True)
                else:
                    print(f"[{rec['timestamp']}] {rec['url']} - {rec['status']} - {rec['duration']}ms" + (" SLOW" if rec["slow"] else ""), flush=True)
        _save_logs(records)
        _purge(RETENTION_DAYS)
        messages = _alerts(prev, records)
        if messages:
            async with httpx.AsyncClient(timeout=5) as client:
                for msg in messages:
                    print(msg, flush=True)
                    if WEBHOOK_URL:
                        try:
                            await client.post(WEBHOOK_URL, json={"text": msg, "content": msg}, timeout=5)
                        except Exception as e:
                            print(f"alert webhook failed: {e}", flush=True)
        return records

async def _scheduler() -> None:
    # ponytail: fixed sleep interval, no cron parsing; APScheduler only if cron expressions become a real need.
    while True:
        await run_checks()
        await asyncio.sleep(INTERVAL)

@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(_scheduler())
    yield
    task.cancel()

app = FastAPI(lifespan=lifespan)

@app.get("/")
async def index():
    return FileResponse("index.html")

@app.get("/logs")
async def get_logs(limit: int = 100) -> dict:
    """Most recent `limit` log entries, newest first."""
    return {"logs": _read_logs(limit)}

@app.get("/endpoints")
async def get_endpoints() -> list:
    return CONFIG["endpoints"]

@app.post("/check")
async def check_now() -> list:
    return await run_checks()

@app.get("/stats")
async def get_stats() -> dict:
    stats = []
    with closing(sqlite3.connect(DB_FILE)) as con:
        for endpoint in CONFIG["endpoints"]:
            url = endpoint["url"]
            checks, successes = con.execute("SELECT COUNT(*), COALESCE(SUM(success), 0) FROM logs WHERE url = ?", (url,)).fetchone()
            durations = [d for (d,) in con.execute("SELECT duration FROM logs WHERE url = ? AND duration IS NOT NULL", (url,))]
            last = con.execute("SELECT status, timestamp FROM logs WHERE url = ? ORDER BY id DESC LIMIT 1", (url,)).fetchone()
            if len(durations) == 1:
                p50 = p95 = durations[0]
            elif durations:
                q = statistics.quantiles(durations, n=100, method="inclusive")
                p50, p95 = round(q[49]), round(q[94])
            else:
                p50 = p95 = None
            last_status = last_check = None
            if last:
                last_status = "ERROR" if last[0] == "ERROR" else int(last[0])
                last_check = last[1]
            stats.append({
                "url": url, "checks": checks, "uptime_pct": round(100 * successes / checks, 1) if checks else None,
                "p50_ms": p50, "p95_ms": p95, "last_status": last_status, "last_check": last_check,
            })
    return {"stats": stats}
