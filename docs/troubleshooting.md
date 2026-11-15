# Troubleshooting

Start with these two commands:

```bash
docker compose ps                 # every service except bootstrap should be "running"
docker compose logs bootstrap     # one line per completed step, or the failing step + a hint
```

The header of the UI shows the same health checks (each is a real API call) and the free Flink
task slots.

| Symptom | Likely cause | What to do |
| --- | --- | --- |
| Port already allocated | Something else uses 8090 or 8081 | Set `DEMO_UI_PORT` / `FLINK_UI_PORT` in `.env`. |
| First build fails while downloading | No internet, or a proxy blocks Docker Hub, Maven Central or PyPI | The first build needs all three. Configure Docker's proxy and mirrors. |
| A container restarts or is OOM-killed | Docker Desktop memory too low | Allow 8 GB of available RAM for the stack; stop unrelated workloads if resources are tight. |
| Bootstrap: "columns differ" / "older table layout" | Volumes from an earlier version of the demo | `docker compose down --volumes` (deletes only this demo's volumes), then `docker compose up -d`. |
| Stage 2: "No Flink TaskManager is registered" | TaskManager stopped | `docker compose start flink-taskmanager`, then press the button again. |
| Stage 2 or 3: "All 4 Flink task slots are busy" | Long-running streaming SQL statements | Cancel them in the SQL or Query Workspace. |
| Stage 2 or 3: "did not finish within 120 s … cancelled" | The job could not get resources or hung | Open the job link (Flink UI → Exceptions). |
| Stage 3: "Fluss has no TabletServer registered" | TabletServer stopped | `docker compose start fluss-tablet`; stage 3 can be retried for the same run. |
| Stage 3: records rejected | A SQL INSERT wrote records that do not match `computed-result/v1` | The reasons are listed per record. Fix the INSERT; rejected records are never written. |
| SQL: `ClassCastException … Integer cannot be cast to … Double` | Flink 1.20's `JSON_VALUE(... RETURNING DOUBLE)` on an integer JSON value | Use `CAST(JSON_VALUE(x, '$.f') AS DOUBLE)` (the examples do). `RETURNING BIGINT` is not supported in 1.20 either. |
| SQL over Fluss: `FileNotFoundException … /fluss/remote-data/kv/…` | The TaskManager cannot read Fluss KV snapshots | The TaskManager must mount `fluss-remote-data` at `/fluss/remote-data` (see compose.yaml). |
| SQL over Fluss misses rows written seconds ago | Full-table reads use the latest KV snapshot (every 30 s here) | Use a full-primary-key filter (a point lookup), or wait for the next snapshot. |
| Python: "The Python runner is not available" | `python-runner` stopped or restarting | `docker compose logs python-runner`. |
| "Connection to the demo app lost" | `demo-app` restarted | The page reconnects. In-memory run state is reset when the app restarts; data in Kafka and Fluss stays. |
| Apple Silicon / ARM | Not tested | Only `linux/amd64` was tested. |

## Useful commands

```bash
# Read a stored row through the same native lookup the UI uses
docker compose exec demo-app curl -s "localhost:8090/api/lookup/supplier_risk?run_id=<RUN_ID>&supplier_id=SUP-NORTHSTAR"

# Raw records of the intermediate topic
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 \
  --topic computed-results --from-beginning --property print.offset=true --timeout-ms 5000

# What the processing job did with each event
docker compose logs flink-taskmanager | grep -E "APPLIED|BUFFERED|REVIEWED|DUPLICATE|REJECTED"

# Unit tests, or the full end-to-end acceptance test
docker build --target test -f docker/app/Dockerfile .
bash scripts/smoke.sh

# Stop (keep data) / delete only this demo's data
docker compose down
docker compose down --volumes
```

`docker compose down --volumes` removes only volumes created by this Compose project
(`payment-risk-streamhouse-demo_*`). Do not use `docker system prune` for this demo.
