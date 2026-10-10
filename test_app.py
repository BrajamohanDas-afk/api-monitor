import asyncio
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx

import app


async def main() -> None:
    async def slow_ok(request):
        await asyncio.sleep(0.01)
        return httpx.Response(200)

    async with httpx.AsyncClient(transport=httpx.MockTransport(slow_ok), timeout=10) as client:
        rec = await app.check_endpoint(client, {"url": "http://x", "threshold": 1})
    assert rec["status"] == 200 and rec["success"] is True and rec["slow"] is True, rec

    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(500)), timeout=10) as client:
        rec500 = await app.check_endpoint(client, {"url": "http://x", "threshold": 1000})
    assert rec500["status"] == 500 and rec500["success"] is False, rec500

    def boom(request):
        raise httpx.ConnectError("boom")

    async with httpx.AsyncClient(transport=httpx.MockTransport(boom), timeout=10) as client:
        err = await app.check_endpoint(client, {"url": "http://x", "threshold": 1000})
    assert err["status"] == "ERROR" and err["success"] is False and "boom" in err["error"], err

    with tempfile.TemporaryDirectory() as tmp:
        app.DB_FILE = Path(tmp) / "logs.db"
        app._init_db()

        app._save_logs([rec500, err])
        http_rec = next(r for r in (await app.get_logs())["logs"] if r["status"] == 500)
        err_rec = next(r for r in (await app.get_logs())["logs"] if r["status"] == "ERROR")
        assert set(http_rec) == {"url", "status", "success", "slow", "duration", "timestamp"}, http_rec
        assert isinstance(http_rec["status"], int) and http_rec["status"] == 500
        assert set(err_rec) == {"url", "status", "success", "error", "timestamp"}, err_rec

        down = {"url": "http://a", "status": "ERROR", "success": False, "error": "boom"}
        assert app._alerts({}, [down]) == ["[api-monitor] DOWN: http://a - boom"]
        assert app._alerts({"http://a": False}, [down]) == []
        up = {"url": "http://a", "status": 200, "success": True, "duration": 12}
        assert app._alerts({"http://a": False}, [up]) == ["[api-monitor] RECOVERED: http://a - HTTP 200 in 12ms"]
        assert app._alerts({"http://a": True}, [up]) == []

        old, fresh = app._iso_now(datetime.now(timezone.utc) - timedelta(days=40)), app._iso_now()
        app._save_logs([
            {"url": "http://p", "status": 200, "success": True, "slow": False, "duration": 5, "timestamp": old},
            {"url": "http://p", "status": 200, "success": True, "slow": False, "duration": 5, "timestamp": fresh},
        ])
        app._purge(30)
        remaining = [r for r in app._read_logs(100) if r["url"] == "http://p"]
        assert len(remaining) == 1 and remaining[0]["timestamp"] == fresh, remaining

        app.CONFIG = {"endpoints": [{"url": "http://s", "threshold": 1000}]}
        app._save_logs([
            {"url": "http://s", "status": 200 if ok else 500, "success": ok, "slow": False,
             "duration": d, "timestamp": app._iso_now()}
            for d, ok in zip([10, 20, 30, 40, 100], [True, True, True, True, False])
        ])
        s = (await app.get_stats())["stats"][0]
        assert s["checks"] == 5 and s["uptime_pct"] == 80.0, s
        assert 25 <= s["p50_ms"] <= 35, s
        assert 80 <= s["p95_ms"] <= 100, s
        assert s["last_status"] == 500 and s["last_check"], s

        app.CONFIG = {"endpoints": [{"url": "http://empty", "threshold": 1000}]}
        assert (await app.get_stats())["stats"] == [
            {"url": "http://empty", "checks": 0, "uptime_pct": None, "p50_ms": None, "p95_ms": None,
             "last_status": None, "last_check": None}
        ]

        app.CONFIG = {"endpoints": [{"url": "http://one", "threshold": 1000}]}
        app._save_logs([{"url": "http://one", "status": 200, "success": True, "slow": False, "duration": 7,
                         "timestamp": app._iso_now()}])
        s = (await app.get_stats())["stats"][0]
        assert s["p50_ms"] == 7 and s["p95_ms"] == 7, s


asyncio.run(main())
print("ok")
