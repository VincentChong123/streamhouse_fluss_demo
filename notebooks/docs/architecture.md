# Docker Image Interaction Architecture

Based on [docs/architecture.md](../../docs/architecture.md) and [compose.yaml](../../compose.yaml), this document illustrates the runtime interactions between the Docker images and Compose services in the **Payment Risk Streamhouse Demo**.

## Docker Images & Services

| Docker Image | Service(s) | Network(s) | Role |
| --- | --- | --- | --- |
| `payment-risk-streamhouse-demo/app:local`<br/>(`eclipse-temurin:17-jre`) | `bootstrap`, `demo-app`, `e2e` (`--profile test`) | `default` (`demo-app` also on `python-exec`) | One-shot topic/table setup (`bootstrap`), UI/API orchestrator on `127.0.0.1:8090` (`serve`), and acceptance tests (`verify`). |
| `apache/kafka:3.9.1` | `kafka` | `default` | Single-node KRaft broker (`kafka:9092`, auto-create off, 1 partition per topic). |
| `payment-risk-streamhouse-demo/flink:1.20-1.0.0-local`<br/>(`apache/fluss-quickstart-flink:1.20-1.0.0` + Kafka SQL connector) | `flink-sql-gateway`, `flink-jobmanager`, `flink-taskmanager` | `default` | SQL Gateway REST v2 (`:8083`), JobManager REST/UI (`127.0.0.1:8081`), TaskManager (4 slots, mounts `fluss-remote-data` read-only). |
| `apache/fluss:1.0.0` | `fluss-coordinator`, `fluss-tablet` | `default`, `python-exec` | Metadata/placement coordinator and TabletServer (`:9123`); persists KV snapshots every 30 s to shared `fluss-remote-data`. |
| `zookeeper:3.9.2` | `zookeeper` | `default` | Coordination service (`:2181`), used exclusively by Apache Fluss. |
| `payment-risk-streamhouse-demo/python-runner:local`<br/>(`python:3.12-slim` + `pyfluss 1.0.0`) | `python-runner` | `python-exec` (internal only) | Hardened, isolated Python execution sandbox (`:8000`) reachable only by `demo-app` and talking only to Fluss. |

## Sequence Diagram of Docker Image Interactions

```mermaid
sequenceDiagram
    autonumber

    actor Client as Browser (web/v2.html)<br/>or Marimo Notebooks

    box rgba(59, 130, 246, 0.08) app:local (eclipse-temurin:17-jre)
        participant Bootstrap as bootstrap
        participant DemoApp as demo-app (:8090)
        participant E2E as e2e (--profile test)
    end

    box rgba(245, 158, 11, 0.08) apache/kafka:3.9.1
        participant Kafka as kafka (:9092)
    end

    box rgba(236, 72, 153, 0.08) flink:1.20-1.0.0-local (quickstart + Kafka SQL)
        participant SqlGw as flink-sql-gateway (:8083)
        participant FlinkJM as flink-jobmanager (:8081)
        participant FlinkTM as flink-taskmanager
    end

    box rgba(16, 185, 129, 0.08) apache/fluss:1.0.0
        participant FlussCoord as fluss-coordinator (:9123)
        participant FlussTab as fluss-tablet (:9123)
    end

    box rgba(107, 114, 128, 0.08) zookeeper:3.9.2
        participant ZK as zookeeper (:2181)
    end

    box rgba(139, 92, 246, 0.08) python-runner:local (python:3.12-slim + pyfluss)
        participant PyRunner as python-runner (:8000)
    end

    %% -------------------------------------------------------------------------
    %% 0. Startup & One-Shot Bootstrap
    %% -------------------------------------------------------------------------
    rect rgba(107, 114, 128, 0.06)
        Note over Bootstrap,ZK: 0. Cluster Startup & One-Shot Bootstrap (no events emitted, no jobs submitted)
        FlussCoord->>ZK: Register coordinator & cluster metadata (:2181)
        FlussTab->>ZK: Register TabletServer & watch metadata (:2181)
        FlussTab->>FlussCoord: Heartbeat & bucket assignment
        FlinkTM->>FlinkJM: Register 4 task slots (RPC)

        Bootstrap->>Kafka: Create topics (business-events, computed-results, playground-events)
        Kafka-->>Bootstrap: Topics ready (1 partition each)
        Bootstrap->>FlussCoord: Create payment_risk DB & PK tables (supplier_risk, payment_reviews, event_receipts)
        FlussCoord->>FlussTab: Assign table buckets
        FlussCoord-->>Bootstrap: Tables ready
        Bootstrap-->>DemoApp: Write /status/bootstrap.json (shared volume, read-only in demo-app)
    end

    %% -------------------------------------------------------------------------
    %% 1. Stage 1: Emit Events
    %% -------------------------------------------------------------------------
    rect rgba(245, 158, 11, 0.06)
        Note over Client,Kafka: 1. Stage 1 - Emit Business Events (Events Workspace)
        Client->>DemoApp: Create run & trigger Stage 1 (HTTP)
        DemoApp->>Kafka: Capture initial log-end offsets (business-events, computed-results)
        Kafka-->>DemoApp: Start offsets
        DemoApp->>Kafka: Produce business-events (acks=all, idempotent, key=run_id|supplier_id)
        Kafka-->>DemoApp: Durable ack (partition 0, offset)
        DemoApp-->>Client: SSE state snapshot (Stage 1 SUCCEEDED)
    end

    %% -------------------------------------------------------------------------
    %% 2. Stage 2: Bounded Flink Processing Job
    %% -------------------------------------------------------------------------
    rect rgba(236, 72, 153, 0.06)
        Note over Client,FlinkTM: 2. Stage 2 - Bounded Flink Processing Job (Kafka -> Kafka)
        Client->>DemoApp: Trigger Stage 2 (HTTP)
        DemoApp->>Kafka: Read current log-end offset [start, end) of business-events
        Kafka-->>DemoApp: Bounded offset range [start, end)
        DemoApp->>FlinkJM: POST /jars/upload (flink-job.jar, first time only)
        FlinkJM-->>DemoApp: jar_id
        DemoApp->>FlinkJM: POST /jars/:id/run (processing mode, parallelism=1, restart=none)
        FlinkJM->>FlinkTM: Deploy bounded processing task
        FlinkTM->>Kafka: Read business-events [start, end) (Kafka SQL connector)
        Note over FlinkTM: Filter run_id, keyBy(run_id|supplier_id),<br/>CorrelationEngine + PaymentReviewRule
        FlinkTM->>Kafka: Write computed-results (computed-result/v1)
        FlinkTM-->>FlinkJM: Task FINISHED + accumulators
        DemoApp->>FlinkJM: Poll job status until FINISHED (120 s timeout) & fetch accumulators
        FlinkJM-->>DemoApp: FINISHED + accumulator counts
        DemoApp->>Kafka: Read back computed-results range & filter by execution ID
        Kafka-->>DemoApp: Computed records
        DemoApp-->>Client: SSE state snapshot (Stage 2 SUCCEEDED)
    end

    %% -------------------------------------------------------------------------
    %% 3. Stage 3: Bounded Flink Ingestion + Fluss Read-Back Verification
    %% -------------------------------------------------------------------------
    rect rgba(16, 185, 129, 0.06)
        Note over Client,FlussTab: 3. Stage 3 - Bounded Flink Ingestion & Native Fluss Read-Back Proof
        Client->>DemoApp: Trigger Stage 3 (HTTP)
        DemoApp->>Kafka: Capture computed-results [start, end) & pre-validate contract
        Kafka-->>DemoApp: Validated computed-result/v1 records
        DemoApp->>FlinkJM: POST /jars/:id/run (ingestion mode, parallelism=1, restart=none)
        FlinkJM->>FlinkTM: Deploy bounded ingestion task
        FlinkTM->>Kafka: Read computed-results [start, end)
        Note over FlinkTM: Filter run_id, validate computed-result/v1,<br/>route via side outputs to 3 PK tables
        FlinkTM->>FlussCoord: Resolve fluss_catalog.payment_risk metadata & bucket leader
        FlussCoord-->>FlinkTM: TabletServer routing (fluss-tablet:9123)
        FlinkTM->>FlussTab: 3 x INSERT INTO (supplier_risk, payment_reviews, event_receipts)
        FlussTab-->>FlinkTM: Write acks (official Fluss connector)
        Note over FlussTab: Periodically writes KV snapshots (every 30 s)<br/>to shared fluss-remote-data volume
        FlinkTM-->>FlinkJM: Task FINISHED
        DemoApp->>FlinkJM: Poll until FINISHED
        FlinkJM-->>DemoApp: FINISHED

        DemoApp->>FlussCoord: Resolve table metadata for read-back proof
        FlussCoord-->>DemoApp: TabletServer routing
        DemoApp->>FlussTab: Native Lookuper.lookup(pk) for every expected row (up to 2 retries)
        FlussTab-->>DemoApp: Stored rows (verified values, computed_by, ingested_by)
        DemoApp-->>Client: SSE state snapshot (Stage 3 SUCCEEDED only on full match)
    end

    %% -------------------------------------------------------------------------
    %% 4. Stage 4: Native Fluss Primary-Key Lookups
    %% -------------------------------------------------------------------------
    rect rgba(59, 130, 246, 0.06)
        Note over Client,FlussTab: 4. Stage 4 - Low-Latency Primary-Key Lookups
        Client->>DemoApp: Trigger Stage 4 or GET /api/lookup/:table
        DemoApp->>FlussCoord: Lookup metadata (cached)
        DemoApp->>FlussTab: Lookuper.lookup(pk) on supplier_risk & payment_reviews
        FlussTab-->>DemoApp: Rows + lookup timings
        DemoApp-->>Client: SSE state snapshot / JSON rows & timings
    end

    %% -------------------------------------------------------------------------
    %% 5. Interactive SQL Workspace (On-Demand)
    %% -------------------------------------------------------------------------
    opt SQL Workspace Execution (Flink SQL Gateway)
        Client->>DemoApp: POST /api/sql/executions (SQL statement, batch or streaming)
        DemoApp->>SqlGw: Open SQL Gateway session (REST v2 :8083) & register Kafka + fluss_catalog
        DemoApp->>SqlGw: Execute SQL statement
        SqlGw->>FlinkJM: Submit SQL job (execution.target=remote)
        FlinkJM->>FlinkTM: Execute SQL operators
        FlinkTM->>Kafka: Read bounded/unbounded Kafka tables (or INSERT INTO computed_results)
        FlinkTM->>FlussCoord: Resolve fluss_catalog metadata
        FlinkTM->>FlussTab: Point lookups or scan KV snapshots (fluss-remote-data mounted :ro)
        SqlGw-->>DemoApp: Fetch result pages
        DemoApp->>SqlGw: Close operation (cancels active job) & close session
        Note over DemoApp,Kafka: For INSERT INTO computed_results, demo-app tracks the job<br/>on flink-jobmanager and reads back/validates Kafka records
        DemoApp-->>Client: SQL result rows & execution status
    end

    %% -------------------------------------------------------------------------
    %% 6. Interactive Python Workspace (On-Demand, python-exec internal network)
    %% -------------------------------------------------------------------------
    opt Python Query Workspace (isolated python-exec internal network)
        Client->>DemoApp: POST /api/python/executions (Python snippet)
        DemoApp->>PyRunner: HTTP execute request over python-exec network (:8000)
        PyRunner->>FlussCoord: Connect via pyfluss 1.0.0 (fluss-coordinator:9123)
        FlussCoord-->>PyRunner: Table & bucket metadata
        PyRunner->>FlussTab: Primary-key lookups / table reads (fluss-tablet:9123)
        FlussTab-->>PyRunner: Query results
        PyRunner-->>DemoApp: stdout / stderr / exit status
        DemoApp-->>Client: Python execution output
    end

    %% -------------------------------------------------------------------------
    %% 7. Containerised Acceptance Checks (--profile test)
    %% -------------------------------------------------------------------------
    opt Acceptance Verification (docker compose --profile test run e2e)
        E2E->>DemoApp: Drive stages 1-4 & API checks (http://demo-app:8090)
        DemoApp-->>E2E: Stage statuses, read-back proof & lookup responses
        E2E->>Kafka: Independent topic/record verification
        E2E->>FlussTab: Independent Fluss state verification
    end
```

## Network Boundaries

- **`default` network**: Connects `kafka`, `zookeeper`, `fluss-coordinator`, `fluss-tablet`, `flink-jobmanager`, `flink-taskmanager`, `flink-sql-gateway`, `bootstrap`, `demo-app`, and `e2e`.
- **`python-exec` network (`internal: true`)**: Connects only `demo-app`, `python-runner`, `fluss-coordinator`, and `fluss-tablet`. `python-runner` cannot reach Kafka, Flink, ZooKeeper, or external networks.
- **Shared volumes used for inter-container data exchange**:
  - `fluss-remote-data`: Written by `fluss-coordinator` and `fluss-tablet` (KV snapshots every 30 s) and mounted read-only (`:ro`) by `flink-taskmanager` for Flink SQL scans of primary-key tables.
  - `bootstrap-status`: Written by `bootstrap` (`/status/bootstrap.json`) and mounted read-only (`:ro`) by `demo-app`.
  - `flink-checkpoints`: Shared by `flink-jobmanager` and `flink-taskmanager`.
