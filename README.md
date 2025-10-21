# UTS Pub-Sub Log Aggregator

## Build & Run
```bash
docker build -t uts-aggregator .
docker run -p 8080:8080 -v aggdata:/app/data uts-aggregator

docker compose up --build
