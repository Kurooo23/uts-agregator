# test/test_stats.py

# --- ensure project root on sys.path when running this file directly ---
import os, sys, asyncio, pytest
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
# ----------------------------------------------------------------------

from httpx import AsyncClient
from src.main import app


@pytest.mark.asyncio
async def test_stats_endpoint():
    await app.router.startup()
    try:
        async with AsyncClient(app=app, base_url="http://test") as ac:
            # kirim beberapa duplikat melalui endpoint demo
            r = await ac.post("/_demo/publish_dup?topic=stats.v1&base_id=S-1&copies=4")
            assert r.status_code in (200, 202)

            # polling sampai topik tercatat dan metrik tersedia
            topics = []
            s = {}
            for _ in range(10):      # total maks ~1.0s
                await asyncio.sleep(0.1)
                s = (await ac.get("/stats")).json()
                topics = s.get("topics", [])
                if "stats.v1" in topics:
                    break

            # kunci metrik harus tersedia
            assert {"received","unique_processed","duplicate_dropped","topics","uptime_seconds","queue_depth"} <= set(s.keys())
            assert "stats.v1" in topics
    finally:
        await app.router.shutdown()
