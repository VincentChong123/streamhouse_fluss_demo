# Contributing

Changes should keep the demo truthful and easy to run.

## Ground rules

1. **Never fake a pipeline step.** Every status and result on screen must come from Kafka, the
   Flink REST API, the SQL Gateway, the Python runner or a Fluss read. On a timeout or failure the
   stage shows the error. Nothing advances on a timer, nothing starts on page load, and no row is
   synthesized or served from memory as if it came from Fluss.
2. **Stages are separate operations.** Processing (Kafka → Kafka) and ingestion (Kafka → Fluss)
   are distinct bounded Flink jobs started by their own buttons. The demo app never writes to
   `supplier_risk`, `payment_reviews` or `event_receipts`; only the ingestion job does.
3. **Persistence is proven by read-back.** Stage 3 succeeds only when native lookups return every
   expected row.
4. **One scenario in the UI.** Negative cases belong in `scenarios/` fixtures and tests, not in the
   UI.
5. **Version HTML, don't overwrite it.** UI revisions go into a new `web/vN.html`. Keep older
   versions and switch with `DEMO_UI_VERSION`. Record the default in the README.

## Build and test

The host needs only Docker with Compose v2 and bash.

```bash
# Unit tests (rule, correlation, fixtures, contracts, SQL guard rails): no services needed
docker build --target test -f docker/app/Dockerfile .

# End-to-end acceptance test against real services (deletes only this demo's volumes first)
bash scripts/smoke.sh
```

If you have Java 17 and Maven locally, `mvn verify` runs the same unit tests.

## Changing a contract

- Fluss tables: `common/.../TableSchemas.java`, shared by the bootstrap (DDL), the ingestion job
  (sink SQL) and the app (read-back). The bootstrap refuses tables with a different layout; run
  `docker compose down --volumes` (this demo's data only) after a change.
- Computed results: `common/.../ComputedResults.java` (`computed-result/v1`). Change the version
  string for an incompatible change.
- Business events: a new `schema_version` and a new `schemas/business-event-vN.schema.json`.
