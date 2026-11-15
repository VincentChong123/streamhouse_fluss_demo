# Configuration

Every setting is optional. Compose reads overrides from a `.env` file in the repository root, or
from your shell environment. Copy this block into `.env` and change what you need:

```dotenv
# Host ports (always bound to 127.0.0.1 only).
DEMO_UI_PORT=8090
FLINK_UI_PORT=8081

# Which web/vN.html the app serves at "/". Older versions stay in web/ at /web/vN.html.
DEMO_UI_VERSION=v2

# Large-payment threshold in minor units (5000000 = USD 50,000.00). Passed to every
# processing job, so a change applies to the next "Run Flink processing".
RULE_LARGE_PAYMENT_THRESHOLD_MINOR=5000000

# A processing or ingestion job that has not finished after this long is cancelled.
STAGE_JOB_TIMEOUT_MS=120000

# Required before exposing the UI beyond localhost: SQL and Python execution then need
# "Authorization: Bearer <token>" (the UI asks for it). Empty = no token (local use).
PLAYGROUND_ACCESS_TOKEN=

# false disables the Python tab's execution entirely.
PLAYGROUND_PYTHON_ENABLED=true
```

> The repository has no `.env.example`, because that file could not be written in the
> environment where the repository was generated. Copy the block above instead.

## Internal settings (set in compose.yaml or the app defaults)

| Variable | Default | Meaning |
| --- | --- | --- |
| `KAFKA_TOPIC` | `business-events` | Stage 1 input topic (validated business events) |
| `KAFKA_COMPUTED_TOPIC` | `computed-results` | Between processing and ingestion |
| `KAFKA_PLAYGROUND_TOPIC` | `playground-events` | Free-form JSON for the SQL Workspace |
| `FLUSS_DATABASE` | `payment_risk` | Fluss database of the three tables |
| `SQL_GATEWAY_URL` | `http://flink-sql-gateway:8083` | SQL Gateway REST endpoint |
| `PYTHON_RUNNER_URL` | `http://python-runner:8000` | Python execution service |
| `DEMO_EMIT_PACE_MS` | `150` | Pause between scenario events, so the UI can show them arriving one by one; each event still waits for its own ack. `0` sends them back to back |
| `STAGE_JOB_POLL_MS` | `100` | How often a running stage job's state is read from the Flink REST API |
| `DEMO_SEND_TIMEOUT_MS` | `15000` | Producer acknowledgment timeout |
| `SQL_BATCH_TIMEOUT_MS` | `90000` | Batch statements are cancelled after this |
| `SQL_STREAMING_MAX_MS` | `300000` | Streaming statements are stopped after this |
| `SQL_MAX_ROWS` | `500` | Rows kept per SELECT; the job is stopped once reached |
| `SQL_MAX_CONCURRENT` | `2` | Concurrent SQL statements (one per workspace tab) |
| `PYTHON_TIMEOUT_SECONDS` | `20` | Wall-clock limit per Python execution |

## Resources

Explicit sizes in `compose.yaml`: Kafka heap 512 MB, ZooKeeper heap 256 MB, JobManager 1024 MB
process, TaskManager 1728 MB process with 4 slots, SQL Gateway heap 384 MB, Python runner 768 MB
container limit. The app uses at most 60 % of its container memory. See the README for measured
usage.
