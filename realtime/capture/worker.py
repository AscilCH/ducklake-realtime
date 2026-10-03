"""Capture worker: reads DuckLake changes through ducklake-cdc and publishes them to JetStream."""

import asyncio
import json
import logging
import signal
import sys
from concurrent.futures import ThreadPoolExecutor

import nats

from realtime import lake
from realtime.config import ConfigError, Settings

log = logging.getLogger("capture")



def build_event(row: dict) -> dict:
    """Turn one CDC row into a log event. Table columns sit between table_name and snapshot_time."""
    keys = list(row)
    first = keys.index("table_name") + 1
    last = keys.index("snapshot_time")
    return {
        "type": "change",
        "snapshot_id": row["snapshot_id"],
        "rowid": row["rowid"],
        "change_type": row["change_type"],
        "table_id": row["table_id"],
        "table": row["table_name"],
        "row": {key: row[key] for key in keys[first:last]},
        "snapshot_time": row["snapshot_time"].isoformat(),
    }


class CaptureWorker:
    def __init__(self, settings: Settings, con, js):
        self._settings = settings
        self._con = con
        self._js = js
        # One dedicated thread for every DuckDB call: DuckDB blocks, and the event loop must not.
        self._db = ThreadPoolExecutor(max_workers=1, thread_name_prefix="duckdb")
        self.consumer: str | None = None
        self._waiting_for: tuple[str, int] | None = None  # (table name, drop snapshot)

    async def _call(self, fn, *args):
        return await asyncio.get_running_loop().run_in_executor(self._db, fn, *args)

    def _subject(self, table_id: int) -> str:
        return f"{self._settings.subject_prefix}.{table_id}"

    # --- database operations (run on the DuckDB thread) ---

    def _latest_consumer(self) -> str | None:
        """After a schema change a successor exists; the newest table consumer is the live one."""
        tables = [
            row
            for row in lake.cdc(self._con, "cdc_list_consumers")
            if row["consumer_kind"] == "dml" and row["table_id"] is not None
        ]
        return max(tables, key=lambda row: row["consumer_id"])["consumer_name"] if tables else None

    def _listen(self, consumer: str) -> list[dict]:
        return lake.cdc(
            self._con,
            "cdc_dml_changes_listen",
            consumer,
            timeout_ms=self._settings.listen_timeout_ms,
        )

    def _window(self, consumer: str) -> dict:
        return lake.cdc(self._con, "cdc_window", consumer)[0]

    def _table_of(self, consumer: str) -> tuple[int, str]:
        row = next(
            r for r in lake.cdc(self._con, "cdc_list_consumers") if r["consumer_name"] == consumer
        )
        return row["table_id"], row["table_name"]

    def _commit(self, consumer: str, snapshot: int) -> None:
        lake.cdc(self._con, "cdc_commit", consumer, snapshot)

    def _switch(self, old: str, successor: str, table_id: int, boundary: int) -> None:
        """Create the successor if it does not exist yet (safe to retry), then release the old one."""
        existing = {row["consumer_name"] for row in lake.cdc(self._con, "cdc_list_consumers")}
        if successor not in existing:
            lake.cdc(
                self._con,
                "cdc_dml_consumer_create",
                successor,
                table_id=table_id,
                start_at=boundary,
            )
        lake.cdc(self._con, "cdc_consumer_release", old)

    def _release(self, consumer: str) -> None:
        lake.cdc(self._con, "cdc_consumer_release", consumer)

    def _dropped_table_name(self, consumer: str) -> str | None:
        """Qualified name of the consumer's table if it was dropped, else None."""
        for row in lake.cdc(self._con, "cdc_list_subscriptions"):
            if row["consumer_name"] == consumer and row["status"] == "dropped":
                return row["current_qualified_name"] or row["original_qualified_name"]
        return None

    def _created_snapshot(self, table_name: str, after: int) -> int | None:
        """First snapshot after `after` that created a table with this qualified name."""
        rows = self._con.execute(
            "SELECT snapshot_id, changes FROM lake.snapshots() WHERE snapshot_id > ? ORDER BY 1",
            [after],
        ).fetchall()
        for snapshot_id, changes in rows:
            if table_name in (changes.get("tables_created") or []):
                return snapshot_id
        return None

    def _attach_by_name(self, table_name: str, boundary: int) -> str | None:
        """Create a consumer on a recreated table; None while it has not been recreated yet."""
        created = self._created_snapshot(table_name, boundary)
        if created is None:
            return None
        consumer = f"{self._settings.base_consumer}_{created}"
        existing = {row["consumer_name"] for row in lake.cdc(self._con, "cdc_list_consumers")}
        if consumer not in existing:
            lake.cdc(
                self._con,
                "cdc_dml_consumer_create",
                consumer,
                table_name=table_name,
                start_at=created,
            )
        return consumer

    # --- main flow ---

    async def run(self, stop: asyncio.Event) -> None:
        self.consumer = await self._call(self._latest_consumer) or self._settings.base_consumer
        log.info("using consumer %s", self.consumer)
        try:
            while not stop.is_set():
                if self.consumer is None:
                    await self._wait_for_table(stop)
                    continue
                rows = await self._call(self._listen, self.consumer)
                if rows:
                    await self._publish_batch(rows)
                    continue
                # Silence can mean a schema boundary: check the window.
                window = await self._call(self._window, self.consumer)
                if window["terminal"]:
                    await self._handle_boundary(window["terminal_at_snapshot"])
        finally:
            if self.consumer is not None:
                try:
                    await self._call(self._release, self.consumer)
                except Exception:
                    log.exception("could not release consumer %s", self.consumer)
            self._db.shutdown()

    async def _publish_batch(self, rows: list[dict]) -> None:
        for row in rows:
            event = build_event(row)
            await self._js.publish(
                self._subject(event["table_id"]),
                json.dumps(event, default=str).encode(),
                headers={"Nats-Msg-Id": f"{row['snapshot_id']}-{row['rowid']}-{row['change_type']}"},
            )
            log.info(
                "  %-6s snapshot %d  %s  %s",
                event["change_type"],
                event["snapshot_id"],
                event["table"],
                event["row"],
            )
        end_snapshot = max(row["end_snapshot"] for row in rows)
        # Commit only after every event is in the log: a crash replays the batch, never loses it.
        await self._call(self._commit, self.consumer, end_snapshot)
        log.info("published %d events, committed up to snapshot %d", len(rows), end_snapshot)

    async def _handle_boundary(self, boundary: int) -> None:
        table_id, table_name = await self._call(self._table_of, self.consumer)
        dropped_name = await self._call(self._dropped_table_name, self.consumer)
        dropped = dropped_name is not None
        table_name = table_name or dropped_name
        marker = {
            "type": "table_dropped" if dropped else "schema_changed",
            "table_id": table_id,
            "table": table_name,
            "boundary_snapshot": boundary,
        }
        await self._js.publish(
            self._subject(table_id),
            json.dumps(marker).encode(),
            headers={"Nats-Msg-Id": f"{marker['type']}-{table_id}-{boundary}"},  # deduped on retry
        )
        if dropped:
            # The table identity is gone for good. A table recreated under the same name is a
            # new identity (new table_id), so wait for it and attach a fresh consumer.
            await self._call(self._release, self.consumer)
            log.info("table %s dropped at snapshot %d: waiting for it to be recreated", table_name, boundary)
            self._waiting_for = (table_name, boundary)
            self.consumer = None
            return
        successor = f"{self._settings.base_consumer}_{boundary}"
        await self._call(self._switch, self.consumer, successor, table_id, boundary)
        log.info("schema boundary at snapshot %d: %s -> %s", boundary, self.consumer, successor)
        self.consumer = successor

    async def _wait_for_table(self, stop: asyncio.Event) -> None:
        table_name, boundary = self._waiting_for
        consumer = await self._call(self._attach_by_name, table_name, boundary)
        if consumer:
            log.info("table %s exists again: using consumer %s", table_name, consumer)
            self.consumer, self._waiting_for = consumer, None
            return
        try:
            await asyncio.wait_for(stop.wait(), self._settings.listen_timeout_ms / 1000)
        except asyncio.TimeoutError:
            pass


async def run(settings: Settings) -> None:
    con = lake.connect(settings)
    nc = await nats.connect(settings.nats_url)
    try:
        js = nc.jetstream()
        await js.add_stream(
            name=settings.stream, subjects=[f"{settings.subject_prefix}.>"]
        )

        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, lambda *_: loop.call_soon_threadsafe(stop.set))

        await CaptureWorker(settings, con, js).run(stop)
    finally:
        await nc.close()
        con.close()


def main() -> None:
    try:
        settings = Settings.from_env()
    except ConfigError as error:
        sys.exit(str(error))
    logging.basicConfig(
        level=settings.log_level,
        stream=sys.stdout,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    asyncio.run(run(settings))


if __name__ == "__main__":
    main()
