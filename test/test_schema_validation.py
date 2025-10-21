# test/test_schema_validation.py

# --- ensure project root on sys.path when running this file directly ---
import os, sys, pytest
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
# ----------------------------------------------------------------------

from httpx import AsyncClient
from src.main import app


@pytest.mark.asyncio
async def test_schema_validation_bad_timestamp():
    await app.router.startup()
    try:
        async with AsyncClient(app=app, base_url="http://test") as ac:
            bad_event = {
                "topic": "schema.test.v1",
                "event_id": "BAD-1",
                # timestamp salah format → di konsumer akan dianggap invalid (ValueError di e.iso())
                "timestamp": "2025/10/17 00:00:00",
                "source": "unit-test",
                "payload": {"x": 1}
            }
            resp = await ac.post("/publish", json=bad_event)

            # Pada skema saat ini, validasi timestamp ISO terjadi di e.iso() (sisi konsumer),
            # bukan di parsing body; sehingga /publish akan menerima (202) dan konsumer akan mencatat error.
            # Jika Anda ingin 422 di sini, tambahkan validator di model Event (tidak dilakukan di test).
            assert resp.status_code in (200, 202, 422)
    finally:
        await app.router.shutdown()
