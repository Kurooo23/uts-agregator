import asyncio, tempfile
import pytest
from src.main import Store

@pytest.mark.asyncio
async def test_persistence_across_restart(tmp_path):
    db = tmp_path / "dedup.db"
    store = Store(str(db))
    class E:
        topic="order.v1"; event_id="OID-77"; source="t"; payload={"ok":True}
        timestamp="2025-10-17T01:00:00+00:00"
        def iso(self): return self.timestamp
    assert await store.upsert_event(E()) is True
    # "restart": instance baru, DB sama
    store2 = Store(str(db))
    assert await store2.upsert_event(E()) is False
