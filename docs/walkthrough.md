# Walkthrough

Start the stack with `docker compose up --build -d` and open http://localhost:8090. The header
shows the health of Kafka, Flink, Fluss, the SQL Gateway and the Python runner, and how many Flink
task slots are free. Nothing has run yet: the current run has an ID, but no events, jobs or rows.

## The topology

```text
stage 1   app producer ──► Kafka  business-events      (input, validated contract)
stage 2                    Flink  processing job       (bounded: this run's input offsets)
                           ──► Kafka  computed-results (intermediate topic, computed-result/v1)
stage 3                    Flink  ingestion job        (bounded: this run's computed results)
                           ──► Fluss  supplier_risk, payment_reviews, event_receipts
stage 4   app ◄── native primary-key lookups (Fluss Java client)
```

The intermediate topic `computed-results` is the boundary between computing and storing. After
stage 2 the results exist as Kafka records only; after stage 3 they exist in Fluss. Each record
is a `computed-result/v1` envelope:

```json
{
  "contract": "computed-result/v1",
  "run_id": "run-20261113-013319-99c2",
  "target_table": "payment_reviews",
  "execution_id": "proc-20261113-013319-99c2-1",
  "producer": "flink-processing-job",
  "row_json": "{\"run_id\":\"run-…\",\"payment_id\":\"PAY-NS-0001\",\"outcome\":\"MANUAL_REVIEW\",…}"
}
```

`row_json` holds one row of `target_table`. The same contract is validated by the app before an
ingestion job is submitted and again inside the ingestion job; incompatible records are listed
with their reasons and never written.

## Stage 1 — Events (Kafka)

Press **Emit scenario events**. The producer sends the four events one after another; each row
moves from *Not sent* to *Sending* to *Acknowledged* only when Kafka returns the record's
partition and offset (acks=all). Expand a row to see the exact JSON and key that were sent.

At this point Kafka holds the events and nothing else has happened: no Flink job ran, and Fluss
has no rows for this run.

## Stage 2 — Job Execution (Flink)

The left side shows the rule. Press **Run Flink processing**. The app:

1. captures the input range: from the offset where this run started to the topic's current end;
2. submits the processing job through the Flink REST API, with that range as arguments;
3. waits until Flink reports FINISHED (or FAILED, or a timeout, after which the job is cancelled);
4. reads what the job wrote to `computed-results` and shows it.

The right side shows the job ID (a link to the job in the Flink UI), its state, the input and
output offset ranges, the job's accumulators (records read, records of other runs skipped,
results emitted), the supplier context as it evolved (v1 NORMAL → v2 ELEVATED → v3 HIGH, changed
fields highlighted), and the payment review: MANUAL_REVIEW, with each condition and the event IDs
it used as evidence.

The Fluss tables still have no rows for this run.

## Stage 3 — Ingestion (Fluss)

Press **Ingest results into Fluss**. The app reads this run's records from `computed-results`,
validates them, submits the bounded ingestion job and then looks up **every** row it expects in
Fluss. The stage succeeds only if every lookup returns the expected values, including
`computed_by` (which execution computed the row) and `ingested_by` (this ingestion job).

The three supplier versions are written in order to a primary-key table, so `supplier_risk`
keeps the latest, v3.

## Stage 4 — Query Results (Fluss)

Press **Query Fluss**. Two native primary-key lookups read `supplier_risk` and `payment_reviews`
for the run. The result shows the context version, the source event references, the bank-change
time, the unfamiliar-device evidence, the risk status, the review outcome, the lookup durations
and the raw response. This is a key lookup, not SQL; SQL over Fluss is in the Query Workspace.

## Repeat and refresh

- Refreshing the browser shows the run's actual state. It never re-runs anything.
- **Start new demo** creates a new run ID. The stages start empty; the previous run's rows stay in
  Fluss under the old run ID.
- Stages 2 and 3 can be run again. A new execution captures a new offset range.

## Exercises

### 1. A late event and bounded ranges

1. Complete stages 1 and 2.
2. In **Events**, load the *Payment request* template (it targets the current run, with a new
   payment ID) and press **Send event**. Note its offset: it is after the processing range.
3. The stage 2 results do not contain it. Press **Run Flink processing** again: the new execution
   reads five records of this run and reviews the new payment too.

### 2. Two different queries over your own events

1. In **Events**, choose `playground-events`, enable batch mode and send:
   ```text
   {"category":"invoice","amount":100}
   {"category":"invoice","amount":250}
   {"category":"refund","amount":4000}
   ```
2. In **SQL Workspace**, run *Aggregate: custom events by category*, then *Filter: custom events
   above an amount*. The first returns totals per category; the second returns only the 4000
   record. Change the filter and run it again.

### 3. Feed the ingestion stage from SQL

1. Complete stages 1 and 2 for a run.
2. In **SQL Workspace**, run *Write computed results: amount-only review (INSERT)*. The result
   shows how many records were written to `computed-results` and whether they satisfy the
   contract.
3. Press **Ingest results into Fluss**. The read-back shows a `payment_reviews` row with
   `computed_by = sql-amount-review`: your SQL output, now stored in Fluss.
4. Try an incompatible record, for example `amount_minor` as a string:
   ```sql
   INSERT INTO computed_results VALUES ('computed-result/v1', '<run id>', 'payment_reviews',
     'sql-bad', 'flink-sql', '{"run_id":"<run id>","payment_id":"PAY-BAD","amount_minor":"oops"}')
   ```
   Stage 3 then lists the record as rejected, with the reason, and does not write it.

### 4. Query Fluss with SQL and Python

- **Query Workspace → SQL** runs Flink SQL over the Fluss catalog: point queries, `COUNT(*)`,
  `GROUP BY` over a whole table, or a streaming changelog that runs until you cancel it.
- **Query Workspace → Python** runs the example with pyfluss, the Fluss Python client. Change the
  key, for example to an earlier run ID from the Execution History, and run it again.

### 5. Stop a service

```bash
docker compose stop flink-taskmanager   # stage 2: fails with "No Flink TaskManager is registered"
docker compose start flink-taskmanager  # run stage 2 again

docker compose stop fluss-tablet        # stage 3: fails with a Fluss message; stage 4 stays locked
docker compose start fluss-tablet       # run stage 3 again: it succeeds for the same run
```

A failed stage shows the error. It never shows simulated success.
