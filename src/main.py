# src/main.py
import os, json, sqlite3, time, asyncio, logging
from typing import List, Optional, Union, Dict, Any
from fastapi import FastAPI, HTTPException, Body, Query, status
from pydantic import BaseModel, Field, ConfigDict
from datetime import datetime, timezone
from fastapi.responses import RedirectResponse
import uvicorn

# ---------- Config ----------
DB_PATH = os.getenv("DB_PATH", "/app/data/aggregator.db")
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("aggregator")

# ---------- Models ----------
class Event(BaseModel):
    topic: str = Field(..., min_length=1)
    event_id: str = Field(..., min_length=1)
    timestamp: str = Field(..., description="ISO8601")
    source: str = Field(..., min_length=1)
    payload: Dict[str, Any]

    model_config = ConfigDict(extra="forbid")

    def iso(self) -> str:
        try:
            datetime.fromisoformat(self.timestamp.replace("Z", "+00:00"))
        except Exception as e:
            raise ValueError("timestamp must be ISO8601") from e
        return self.timestamp

class EventBatch(BaseModel):
    events: List[Event]

# ---------- Persistent Dedup Store ----------
class Store:
    def __init__(self, path: str):
        self.path = path
        self._init_db()

    def _get_columns(self, con: sqlite3.Connection, table: str) -> List[str]:
        cur = con.execute(f"PRAGMA table_info('{table}')")
        return [row[1] for row in cur.fetchall()]

    def _table_exists(self, con: sqlite3.Connection, table: str) -> bool:
        cur = con.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table,),
        )
        return cur.fetchone() is not None

    def _safe_commit(self, con: sqlite3.Connection):
        try:
            con.commit()
        except Exception:
            con.rollback()
            raise

    def _init_db(self):
        con = sqlite3.connect(self.path)
        try:
            # ----- events table (idempotency via PK(topic,event_id)) -----
            con.execute("""
            CREATE TABLE IF NOT EXISTS events(
                topic   TEXT NOT NULL,
                event_id TEXT NOT NULL,
                ts      TEXT NOT NULL,
                source  TEXT NOT NULL,
                payload TEXT NOT NULL,
                PRIMARY KEY(topic, event_id)
            )
            """)
            self._safe_commit(con)

            # ----- stats table: ensure columns k (TEXT PK), v (INTEGER) -----
            if not self._table_exists(con, "stats"):
                con.execute("""CREATE TABLE stats(
                    k TEXT PRIMARY KEY,
                    v INTEGER NOT NULL
                )""")
                self._safe_commit(con)
            else:
                cols = self._get_columns(con, "stats")
                expected = {"k", "v"}
                if set(cols) != expected:
                    # try common legacy shapes and migrate
                    legacy_map = None
                    if {"key", "value"}.issubset(set(cols)):
                        legacy_map = ("key", "value")
                    elif {"name", "value"}.issubset(set(cols)):
                        legacy_map = ("name", "value")

                    if legacy_map:
                        log.warning("Migrating stats schema from (%s,%s) -> (k,v)", *legacy_map)
                        con.execute("""CREATE TABLE IF NOT EXISTS stats_new(
                            k TEXT PRIMARY KEY,
                            v INTEGER NOT NULL
                        )""")
                        con.execute(f"INSERT OR IGNORE INTO stats_new(k,v) SELECT {legacy_map[0]} AS k, {legacy_map[1]} AS v FROM stats")
                        con.execute("DROP TABLE stats")
                        con.execute("ALTER TABLE stats_new RENAME TO stats")
                        self._safe_commit(con)
                    else:
                        # unknown schema: back up and recreate empty stats
                        backup = f"stats_backup_{int(time.time())}"
                        log.warning("Unknown stats schema %s, backing up to %s and recreating", cols, backup)
                        con.execute(f"ALTER TABLE stats RENAME TO {backup}")
                        con.execute("""CREATE TABLE stats(
                            k TEXT PRIMARY KEY,
                            v INTEGER NOT NULL
                        )""")
                        self._safe_commit(con)

            # initialize counters if missing
            for k in ("received", "unique_processed", "duplicate_dropped"):
                con.execute("INSERT OR IGNORE INTO stats(k,v) VALUES(?,0)", (k,))
            self._safe_commit(con)
        finally:
            con.close()

    async def upsert_event(self, e: Event) -> bool:
        """Return True if inserted (new); False if duplicate"""
        con = sqlite3.connect(self.path)
        try:
            cur = con.cursor()
            cur.execute("UPDATE stats SET v=v+1 WHERE k='received'")
            try:
                cur.execute(
                    "INSERT INTO events(topic,event_id,ts,source,payload) VALUES (?,?,?,?,?)",
                    (e.topic, e.event_id, e.iso(), e.source, json.dumps(e.payload)),
                )
                cur.execute("UPDATE stats SET v=v+1 WHERE k='unique_processed'")
                con.commit()
                return True
            except sqlite3.IntegrityError:
                cur.execute("UPDATE stats SET v=v+1 WHERE k='duplicate_dropped'")
                con.commit()
                return False
        finally:
            con.close()

    async def list_events(self, topic: Optional[str], limit: int = 100) -> List[Dict[str, Any]]:
        con = sqlite3.connect(self.path)
        try:
            cur = con.cursor()
            if topic:
                cur.execute(
                    "SELECT topic,event_id,ts,source,payload FROM events WHERE topic=? ORDER BY ts ASC LIMIT ?",
                    (topic, limit),
                )
            else:
                cur.execute(
                    "SELECT topic,event_id,ts,source,payload FROM events ORDER BY ts ASC LIMIT ?",
                    (limit,),
                )
            rows = cur.fetchall()
            out = []
            for t, i, ts, s, p in rows:
                out.append({"topic": t, "event_id": i, "timestamp": ts, "source": s, "payload": json.loads(p)})
            return out
        finally:
            con.close()

    async def get_topics(self) -> List[str]:
        con = sqlite3.connect(self.path)
        try:
            cur = con.cursor()
            cur.execute("SELECT DISTINCT topic FROM events ORDER BY topic ASC")
            return [r[0] for r in cur.fetchall()]
        finally:
            con.close()

    async def get_stats(self) -> Dict[str, Any]:
        con = sqlite3.connect(self.path)
        try:
            cur = con.cursor()
            cur.execute("SELECT k,v FROM stats")
            base = {k: v for k, v in cur.fetchall()}
            base.setdefault("received", 0)
            base.setdefault("unique_processed", 0)
            base.setdefault("duplicate_dropped", 0)
            base["topics"] = await self.get_topics()
            return base
        finally:
            con.close()

# ---------- App & Consumer ----------
app = FastAPI(title="UTS Pub-Sub Log Aggregator", docs_url="/docs", redoc_url="/redoc")
STORE = Store(DB_PATH)
QUEUE: "asyncio.Queue[Event]" = asyncio.Queue()
STARTED_AT = time.time()
consumer_task: Optional[asyncio.Task] = None

def _ensure_consumer_started():
    """Start consumer loop bila event startup tidak terpanggil (mis. test dengan transport tertentu)."""
    global consumer_task
    if consumer_task is None or consumer_task.done():
        consumer_task = asyncio.create_task(consumer_loop())

async def consumer_loop(q: "asyncio.Queue[Event]"):
    while True:
        e: Event = await q.get()
        try:
            inserted = await STORE.upsert_event(e)
            if inserted:
                log.info("processed %s/%s", e.topic, e.event_id)
            else:
                log.info("duplicate dropped %s/%s", e.topic, e.event_id)
        except Exception as err:
            log.exception("consumer error for %s/%s: %s", e.topic, e.event_id, err)
        finally:
            q.task_done()

@app.on_event("startup")
async def on_startup():
    global consumer_task, QUEUE
    # buat queue BARU yang terikat ke event loop AKTIF saat ini
    QUEUE = asyncio.Queue()
    # jalankan konsumer dengan queue tersebut
    consumer_task = asyncio.create_task(consumer_loop(QUEUE))

@app.on_event("shutdown")
async def on_shutdown():
    global consumer_task, QUEUE
    if consumer_task:
        consumer_task.cancel()
        try:
            await consumer_task
        except asyncio.CancelledError:
            pass
    QUEUE = None

# ---------- Routes ----------
@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse("/docs")

@app.post("/publish", status_code=status.HTTP_202_ACCEPTED)
async def publish(body: Union[Event, EventBatch] = Body(...)):
    global QUEUE, consumer_task
    if QUEUE is None:
        # fallback bila startup belum terpanggil (mis. eksekusi tertentu saat test)
        QUEUE = asyncio.Queue()
        consumer_task = asyncio.create_task(consumer_loop(QUEUE))

    if isinstance(body, Event):
        await QUEUE.put(body)
        return {"accepted": 1}
    elif isinstance(body, EventBatch):
        for e in body.events:
            await QUEUE.put(e)
        return {"accepted": len(body.events)}
    else:
        raise HTTPException(400, "invalid body")

@app.get("/events")
async def get_events(topic: Optional[str] = Query(None), limit: int = Query(100, ge=1, le=10000)):
    return await STORE.list_events(topic, limit)

@app.get("/stats")
async def stats():
    _ensure_consumer_started()
    st = await STORE.get_stats()
    st["uptime_seconds"] = int(time.time() - STARTED_AT)
    st["queue_depth"] = QUEUE.qsize()
    return st

@app.post("/_demo/publish_dup")
async def publish_dup(topic: str, base_id: str, copies: int = 3):
    now = datetime.now(timezone.utc).isoformat()
    payload = {"demo": True}
    events = [Event(topic=topic, event_id=base_id, timestamp=now, source="demo", payload=payload) for _ in range(copies)]
    return await publish(EventBatch(events=events))

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8089")))
