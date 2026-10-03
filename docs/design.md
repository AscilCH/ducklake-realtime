# Design and test plan

## Goal

Deliver DuckLake changes to clients in near real time, using `ducklake-cdc` as the durable change source and a disk-backed log as the buffer between the database and clients.

```
DuckLake --> ducklake-cdc worker --> disk log (NATS JetStream) --> client
```

Out of scope for now: WebSocket serving, authentication, filters, ephemeral messaging, SDKs.

## 1. Capture

**Problem:** can `ducklake-cdc` turn DuckLake commits into an ordered stream of row changes, with no loss if the reader crashes?

DuckLake has no write-ahead log. Capture here means reading snapshots through a durable cursor.

**Approach**
1. Run DuckDB with DuckLake and the `ducklake-cdc` extension. Use a Postgres catalog and local disk for data.
2. Create a table and a DML consumer (`cdc_dml_consumer_create`).
3. A worker loops: read changes, publish to the log, then `cdc_commit`. It commits only after the publish succeeds.
4. Run inserts, updates and deletes.

**Tests**

| Test | Expected |
|---|---|
| Insert, update, delete | Events arrive in order, with pre and post images |
| Kill the worker after reading, before `cdc_commit` | The batch is delivered again after restart (duplicates allowed, loss not) |
| Commit-to-event latency, 1000 samples | Record median and p99 |
| 10k rows in one commit | Arrive in full and in order |

## 2. Schema boundary

**Problem:** when a table's shape changes, the stream must stop at the right place so clients never apply new-shape rows to an old schema, and the client must be able to recover.

**Approach**
1. With the capture worker running, apply DDL changes to the table.
2. Observe the consumer: the terminal signal from `cdc_window`, `terminal_at_snapshot` in `cdc_list_consumers`, and any error emitted.
3. Start a successor consumer at the boundary snapshot.
4. The worker writes a `schema_changed` marker to the log (old shape, new shape, cursor).
5. A test client stops applying old-shape rows on the marker, reloads, and continues.

**Tests**

| DDL | Question |
|---|---|
| Add column | Does it end the consumer? |
| Drop column | Same |
| Rename column, change type | Same |
| Rename table | Does not end the consumer (verified) |
| Drop and recreate table | New identity (verified): needs a new consumer |
| Rows on both sides of the DDL | None missing, none mixed |

## 3. Resume

**Problem:** a client that disconnects and reconnects must catch up from the log, not by querying DuckLake, and many clients reconnecting at once must not load the database.

**Approach**
1. The log is NATS JetStream with file storage. The worker publishes every change with a sequence number, under a retention limit.
2. A test client remembers the last sequence it processed.
3. The client disconnects, the producer keeps writing, the client reconnects and replays from its last sequence.
4. Delivery is at-least-once, so the client removes duplicates using a unique event key.
5. Count DuckLake queries during replay. Expected: none beyond the worker's normal reads.

**Tests**

| Test | Expected |
|---|---|
| Disconnect, write 1000 rows, reconnect | All arrive, in order, without gaps or duplicates |
| 100 clients reconnect at once | No extra DuckLake queries |
| Reconnect after the retention period | Explicit "too old, reload" signal |
| Reconnect across a schema change marker | Client reloads instead of applying old-shape rows |
| Worker restart during replay | No loss |

## Build order

1. Confirm the extension installs and runs.
2. Capture.
3. Schema boundary.
4. Resume.
5. Write up results.

## Open points

- Log retention window for the tests.
- Whether the extension installs as a community extension or needs a local build.

## Results

### Capture (verified)

- Insert, update and delete arrive in snapshot order. Within one snapshot the post-image can come before the pre-image, so pair them by `(snapshot_id, rowid)`.
- A batch read but not committed is delivered again after the lease is released: at-least-once, no loss.
- `cdc_dml_changes_listen` takes a 60 s lease on the consumer. Release it on shutdown; a killed worker keeps it until it expires.

### Schema boundary (verified)

| DDL | Ends the consumer | Worker action |
|---|---|---|
| Add column | yes | marker, successor consumer at the boundary snapshot |
| Drop column | yes | same |
| Rename column | yes | same |
| Change column type | yes | same |
| Rename table | no | none, the subscription follows the table by id |
| Drop table | yes | `table_dropped` marker, wait for the name to reappear |
| Drop and recreate | yes, new identity | new consumer at the snapshot that created the table |

Behaviours the worker has to handle:

- **The boundary is silent.** Listen and read return zero rows. The worker detects it through `cdc_window` (`terminal`, `terminal_at_snapshot`). `CDC_SCHEMA_TERMINATED` is only raised by `cdc_commit` past the boundary.
- **Successor consumers are created with `table_id`**, which survives renames.
- **A dropped table has no successor.** Its id is no longer live, so `cdc_dml_consumer_create` fails. A table recreated under the same name has a new id and only exists from the snapshot that created it, so the consumer must start there (`start_at` earlier is rejected as "not live"). The worker finds that snapshot in `snapshots()`.
- **Table names in events are resolved at read time**, not at commit time. Subjects therefore use the stable `table_id` (`<prefix>.<table_id>`), and the name is only a payload field.
- **After a drop the consumer list no longer shows the table name.** The subscription row keeps it.

Markers on the log:

- `schema_changed`: the shape changed at `boundary_snapshot`. Rows after it have the new shape.
- `table_dropped`: the stream for this `table_id` has ended. A recreated table appears under a new `table_id`.