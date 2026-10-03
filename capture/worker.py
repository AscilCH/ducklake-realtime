import asyncio
import json

import duckdb
import nats

CATALOG = "ducklake:postgres:dbname=ducklake host=127.0.0.1 port=5433 user=ducklake password=ducklake"
CONSUMER = "orders_sink"
NATS_URL = "nats://127.0.0.1:4222"


async def main():
    c = duckdb.connect()
    c.sql("LOAD ducklake")
    c.sql("LOAD ducklake_cdc")
    c.sql(f"ATTACH '{CATALOG}' AS lake (DATA_PATH 'data/lake/')")

    nc = await nats.connect(NATS_URL)
    js = nc.jetstream()
    await js.add_stream(name="CHANGES", subjects=["changes.>"])

    try:
        while True:
            rel = c.sql(
                f"SELECT * FROM cdc_dml_changes_listen('lake', '{CONSUMER}', timeout_ms := 1000)"
            )
            cols = rel.columns
            rows = rel.fetchall()
            if not rows:
                await asyncio.sleep(0)
                continue

            first = cols.index("table_name") + 1  # table columns sit between these two
            last = cols.index("snapshot_time")
            i_snap, i_rowid = cols.index("snapshot_id"), cols.index("rowid")
            i_type, i_table = cols.index("change_type"), cols.index("table_name")

            for r in rows:
                event = {
                    "snapshot_id": r[i_snap],
                    "rowid": r[i_rowid],
                    "change_type": r[i_type],
                    "table": r[i_table],
                    "row": dict(zip(cols[first:last], r[first:last])),
                    "snapshot_time": r[last].isoformat(),
                }
                msg_id = f"{r[i_snap]}-{r[i_rowid]}-{r[i_type]}"
                await js.publish(
                    f"changes.{r[i_table]}",
                    json.dumps(event, default=str).encode(),
                    headers={"Nats-Msg-Id": msg_id},
                )

            end_snapshot = max(r[cols.index("end_snapshot")] for r in rows)
            c.sql(f"SELECT * FROM cdc_commit('lake', '{CONSUMER}', {end_snapshot})").fetchall()
            print(f"published {len(rows)} events, committed up to snapshot {end_snapshot}")
    finally:
        c.sql(f"SELECT * FROM cdc_consumer_release('lake', '{CONSUMER}')").fetchall()
        await nc.close()


try:
    asyncio.run(main())
except KeyboardInterrupt:
    pass