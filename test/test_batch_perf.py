# test/test_batch_perf.py

# --- ensure project root on sys.path when running this file directly ---
import os, sys, asyncio, pytest
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
# ----------------------------------------------------------------------

from httpx import AsyncClient
from src.main import app


@pytest.mark.asyncio
async def test_small_stress_batch():
    await app.router.startup()
    try:
        async with AsyncClient(app=app, base_url="http://test") as ac:
            topic = "batch.perf.v1"

            # buat batch berisi beberapa event unik + beberapa duplikat ID sama
            events = []
            for i in range(10):
                events.append({
                    "topic": topic,
                    "event_id": f"B-{i}",
                    "timestamp": "2025-10-18T00:00:00+00:00",
                    "source": "unit-test",
                    "payload": {"i": i}
                })
            # tambahkan 5 duplikat dari event id ke-3
            for _ in range(5):
                events.append({
                    "topic": topic,
                    "event_id": "B-3",
                    "timestamp": "2025-10-18T00:00:00+00:00",
                    "source": "unit-test",
                    "payload": {"dup": True}
                })

            r = await ac.post("/publish", json={"events": events})
            assert r.status_code in (200, 202)
            accepted = r.json().get("accepted", 0)
            assert accepted == len(events)

            # polling sampai minimal 10 event unik tampak
            got = []
            for _ in range(20):          # total maks ~2.0s
                await asyncio.sleep(0.1)
                got = (await ac.get("/events", params={"topic": topic, "limit": 100})).json()
                if len(got) >= 10:
                    break

            # hanya 10 unik yang seharusnya tersimpan karena dedup PK(topic,event_id)
            assert len([e for e in got if e["topic"] == topic]) == 10
    finally:
        await app.router.shutdown()
