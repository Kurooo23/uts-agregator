# test/test_api_basic.py

# --- ensure project root on sys.path when running this file directly ---
import os, sys, asyncio, pytest
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
# ----------------------------------------------------------------------

from httpx import AsyncClient
from src.main import app, STORE, Store


@pytest.mark.asyncio
async def test_publish_and_dedup():
    # Pastikan lifecycle FastAPI terpanggil (startup/shutdown) pada httpx lama
    await app.router.startup()
    try:
        async with AsyncClient(app=app, base_url="http://test") as ac:
            e = {
                "topic": "app.billing.payments.v1",
                "event_id": "E-123",
                "timestamp": "2025-10-16T15:00:00+00:00",
                "source": "unit-test",
                "payload": {"amount": 100}
            }

            # kirim event yang sama 3x (harus terdeduplikasi)
            for _ in range(3):
                r = await ac.post("/publish", json=e)
                assert r.status_code == 202

            # polling singkat agar konsumer sempat memproses antrean
            ev = []
            for _ in range(10):     # total maks ~1.0s
                await asyncio.sleep(0.1)
                ev = (await ac.get("/events", params={"topic": e["topic"]})).json()
                if len(ev) >= 1:
                    break

            assert len(ev) == 1, f"Expected 1 event after dedup, got {len(ev)}"
    finally:
        await app.router.shutdown()


@pytest.mark.asyncio
async def test_store_persist_across_instances(tmp_path):
    # Verifikasi persistensi DB & dedup level storage (tanpa layer HTTP)
    db = tmp_path / "agg.db"
    s1 = Store(str(db))

    # objek tiruan yang cukup untuk Store.upsert_event(e)
    e = type(
        "Obj",
        (),
        dict(
            topic="t",
            event_id="id-1",
            timestamp="2025-10-17T00:00:00+00:00",
            source="u",
            payload={"x": 1},
            iso=lambda self="": "2025-10-17T00:00:00+00:00",
        ),
    )

    ok = await s1.upsert_event(e)
    assert ok is True  # insert baru

    # instance baru pada DB yang sama → harus terdeteksi duplikat
    s2 = Store(str(db))
    ok2 = await s2.upsert_event(e)
    assert ok2 is False
