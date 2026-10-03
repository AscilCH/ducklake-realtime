# Problem 1: Capture

## The problem

DuckLake has no write-ahead log. Something has to turn its commits into an ordered stream of row changes. Nothing may be lost if the reader crashes, and the database should be read once, not once per client.

## The solution

```
DuckLake commit --> ducklake-cdc consumer --> capture worker --> NATS JetStream (disk log)
```

1. Every DuckLake commit creates a snapshot.
2. A `ducklake-cdc` consumer (`orders_sink`) is a durable cursor over those snapshots. Its position is stored in Postgres, in the schema `__ducklake_cdc`.
3. The worker (`realtime/capture/worker.py`) loops: read the next batch, publish each change to JetStream, and only then call `cdc_commit`.
4. Each event goes to the subject `<prefix>.<table_id>`. The `table_id` stays the same when the table is renamed.

## Why it does not lose data

The cursor moves only after the publish succeeded. If the worker dies between the two, the batch is read again after restart. This is at-least-once delivery: duplicates are possible, losses are not. Clients de-duplicate with the log sequence number.

## What an event looks like

One message per row change: `insert`, `update_preimage`, `update_postimage` or `delete`. It carries the snapshot id, the row id, the table name and the row values.

## Verified

| Test | Result |
|---|---|
| Insert, update, delete | Arrive in snapshot order |
| Update | Pre and post image both arrive. In one snapshot the post-image can come first, so pair them by `(snapshot_id, rowid)` |
| Worker killed after read, before `cdc_commit` | The batch is delivered again |
| Killed worker | Keeps its 60 s lease on the consumer until it expires. Release it on a clean shutdown |
| Worker start with no consumer | It creates one for `CDC_TABLE` (starts from now, so older rows are not captured) |

## Not tested yet

- Commit-to-event latency (median and p99).
- A 10k-row commit in one go.
- NATS down while the worker is running.

## Limits to know

- `ducklake-cdc` is pre-alpha. The API may change.
- The worker follows one table. Several tables need several consumers.
- Small inserts are kept by DuckLake inside Postgres (inlined data), not in Parquet files. Capture works the same either way.
- Rows inserted before the consumer exists are not in the stream.
