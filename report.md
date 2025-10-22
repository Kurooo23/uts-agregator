Pub-Sub Log Aggregator dengan Idempotent Consumer & Deduplication (Versi ringkas)

Mata kuliah: Sistem Paralel & Terdistribusi
Topik: Publish–Subscribe log aggregator (lokal, Docker)
Teknologi: Python 3.11, FastAPI, asyncio.Queue, SQLite (WAL), Uvicorn

RINGKASAN:
Laporan ini memaparkan desain dan implementasi layanan log aggregator berbasis publish–subscribe (Pub-Sub) yang menerima event dari publisher, memprosesnya melalui konsumen yang idempotent, serta melakukan deduplication berdasarkan (topic, event_id) dengan penyimpanan persisten (SQLite). Sistem ini menerapkan at-least-once delivery pada jalur publish—sehingga duplikasi dapat terjadi—namun menjamin exactly-once effect di tingkat aplikasi melalui idempotensi dan dedup. Evaluasi menunjukkan bahwa sistem tetap responsif, memproses ≥5.000 event dengan ≥20% duplikasi, serta state dedup bertahan setelah kontainer direstart.

Arsitektur Sistem:
flowchart LR
    P[Publishers] -- POST /publish --> API[FastAPI Ingest]
    API --> Q[asyncio.Queue (in-memory)]
    subgraph Workers
      C1[Consumer #1 (idempotent)]
      C2[Consumer #2 (idempotent)]
    end
    Q --> C1
    Q --> C2
    C1 & C2 --> DB[(SQLite\nPRIMARY KEY(topic,event_id)\nWAL)]
    API <-- GET /events|/stats --> Client[Reader / Tools]

Komponen Utama:
1. API Longest (Post / publish) : menerima single event atau beberapa sekaligus; validasi skema (Pydantic) dan masuk ke asyncio.Queue.
2. Consumer (N worker, dikonfigurasi melalui WORKERS): idempoten; menulis ke SQLLite dengan batas unik (topic, event_id) -> duplikat terdeteksi melalui IntegrityError.
3. Dedup Store: SQL tertanam (embedded) (mode WAL, synchronous=NORMAL) untuk daya tahan lokal dan throughput yang memadai.
4. Observabilitas: GET / events (membaca peristiwa unik) dan GET / stats (received, unique_processed, duplicate_dropped, topics, uptime)

Keputusan Desain
1. Semantik Pengiriman — At-least-once di jalur publish (retries/ack) diterima sebagai konsekuensi reliabilitas. Pada tingkat aplikasi, exactly-once effect dicapai melalui idempotent consumer + deduplikasi. Alasan: untuk menghindari biaya tinggi dari  exactly-once delivery end-to-end (koordinasi/2PC).
2. Idempotency & Deduplikasi — Kunci komposit (topic, event_id) menjadi PRIMARY KEY; setiap duplikasi memicu IntegrityError yang dihitung sebagai duplicate_dropped.
3. Penamaan — topic berskala hierarkis (misalnya, app.billing.payments.v1). event_id harus unik dan collision-resistant (UUID v7/ULID atau hash deterministik).
4. Ordering — Total ordering tidak diwajibkan. Sistem menampilkan chronological listing per timestamp untuk keperluan inspeksi; analitik kausal global bukan tujuan utama aggregator.
5. Ketahanan Restart — SQLite yang persisten memastikan bahwa event yang sama tetap ditolak setelah sistem direstart.
6. Kinerja — asyncio.Queue memisahkan proses ingest dari commit, mengurangi tail latency pada saat burst. Konfigurasi worker dapat ditingkatkan.

Keterkaitan Teori (ringkasan dari pdf):
1. Bab 1 — Karakteristik & Trade-off: Sistem terdistribusi memerlukan resource sharing, scalability, dan toleransi terhadap kegagalan parsial; transparansi vs performa merupakan trade-off klasik dalam desain Pub-Sub. (Tanenbaum & Van Steen, 2023).
2. Bab 2 — Arsitektur: Pub-Sub menawarkan decoupling (ruang/waktu) dibandingkan dengan client-server, sehingga cocok untuk log yang bersifat bursty dan fan-out multi-subscriber.
3. Bab 3 — Komunikasi: At-least-once lazim terjadi karena mekanisme retry/ack; exactly-once dapat dicapai secara praktis melalui idempotensi & deduplikasi pada tingkat aplikasi.
4. Bab 4 — Penamaan: Pemisahan name/identifier/address; topic berfungsi sebagai name, sementara event_id sebagai identifier global unik mendukung deduplikasi.
5. Bab 5 — Waktu & Ordering: Total order mahal; logical/causal order sering kali sudah cukup. Agregator ini tidak memerlukan serialisasi global.
6. Bab 6 — Toleransi Kegagalan: Duplikasi/out-of-order/crash ditangani dengan mekanisme retry/backoff, durable storage, dan idempotensi.
7. Bab 7 — Konsistensi: Eventual consistency tercapai; dalam steady state, semua event unik hadir, duplikasi tidak mengubah state.

Spesifikasi API
- POST /publish
  Body: Event tunggal atau { "events": [Event, ...] }
  Skema Event:
    { "topic": "string", "event_id": "string", "timestamp": "ISO8601", "source": "string", "payload": { } }
  Respons: { "accepted": <int>, "queued": <int> }
- GET /events?topic=<str>&limit=<int> → daftar event unik yang telah diproses.
- GET /stats → { received, unique_processed, duplicate_dropped, topics, uptime_seconds }.

Metodologi Evaluasi
Metrik
  - Throughput (event/detik) — laju ingest → commit.
  - Latency (p50/p95) — end-to-end dari publish hingga persist.
  - Duplicate rate — duplicate_dropped / received.
  - Unique ratio — unique_processed / received.
  - Uptime & Error Rate — kestabilan layanan.
Beban Uji
  - Kirim ≥5.000 event dengan ≥20% duplikasi (contoh 1.250 ID unik × 4 kirim).
  - Kirim batch size 500 untuk menstabilkan memori.
  - Uji persistensi: restart server, kirim kembali ID yang sama → tetap drop.

Link Video yt: https://youtu.be/Teq9NThUwY4