"""ChangeSource on DuckLake with the ducklake-cdc extension. All DuckDB and cdc_* calls live here."""

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor

import duckdb

from realtime import events, lake
from realtime.capture.ports import Batch, TableInfo
from realtime.config import Settings

log = logging.getLogger("capture")


def build_event(row: dict) -> dict:
    """Turn one CDC row into a log event. Table columns sit between table_name and snapshot_time."""
    keys = list(row)
    first = keys.index("table_name") + 1
    last = keys.index("snapshot_time")
    return events.change_event(
        snapshot_id=row["snapshot_id"],
        rowid=row["rowid"],
        change_type=row["change_type"],
        table_id=row["table_id"],
        table=row["table_name"],
        row={key: row[key] for key in keys[first:last]},
        snapshot_time=row["snapshot_time"].isoformat(),
    )


class DuckLakeCdcSource:
    def __init__(self, settings: Settings, con):
        self._settings = settings
        self._con = con
        # One dedicated thread for every DuckDB call: DuckDB blocks, and the event loop must not.
        self._db = ThreadPoolExecutor(max_workers=1, thread_name_prefix="duckdb")
        self.consumer: str | None = None

    async def _call(self, fn, *args):
        return await asyncio.get_running_loop().run_in_executor(self._db, fn, *args)

    # --- ChangeSource ---

    async def open(self) -> None:
        self.consumer = await self._call(self._latest_consumer)
        if self.consumer is None:
            self.consumer = await self._call(self._bootstrap)
        log.info("using consumer %s", self.consumer)

    async def read(self) -> Batch | None:
        rows = await self._call(self._listen, self.consumer)
        if not rows:
            return None
        return Batch([build_event(row) for row in rows], max(row["end_snapshot"] for row in rows))

    async def commit(self, snapshot: int) -> None:
        await self._call(self._commit, self.consumer, snapshot)

    async def boundary(self) -> int | None:
        # Silence can mean a schema boundary: the window says whether the consumer has ended.
        window = await self._call(self._window, self.consumer)
        return window["terminal_at_snapshot"] if window["terminal"] else None

    async def table(self) -> TableInfo:
        table_id, name = await self._call(self._table_of, self.consumer)
        dropped_name = await self._call(self._dropped_table_name, self.consumer)
        return TableInfo(table_id, name or dropped_name, dropped_name is not None)

    async def columns_at(self, table: str, snapshot: int) -> list[str] | None:
        return await self._call(self._columns_at, table, snapshot)

    async def succeed(self, boundary: int) -> None:
        table_id, _ = await self._call(self._table_of, self.consumer)
        successor = f"{self._settings.base_consumer}_{boundary}"
        await self._call(self._switch, self.consumer, successor, table_id, boundary)
        log.info("schema boundary at snapshot %d: %s -> %s", boundary, self.consumer, successor)
        self.consumer = successor

    async def abandon(self) -> None:
        await self._call(self._release, self.consumer)
        self.consumer = None

    async def attach_recreated(self, table: str, boundary: int) -> bool:
        consumer = await self._call(self._attach_by_name, table, boundary)
        if consumer is None:
            return False
        log.info("table %s exists again: using consumer %s", table, consumer)
        self.consumer = consumer
        return True

    async def close(self) -> None:
        try:
            if self.consumer is not None:
                try:
                    await self._call(self._release, self.consumer)
                except Exception:
                    log.exception("could not release consumer %s", self.consumer)
        finally:
            self._db.shutdown()

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

    def _columns_at(self, table_name: str, snapshot: int) -> list[str] | None:
        """Column names of the table as of `snapshot` (DuckLake time travel); None if unavailable."""
        qualified = ".".join('"' + part.replace('"', '""') + '"' for part in table_name.split("."))
        try:
            cursor = self._con.execute(
                f"SELECT * FROM {lake.NAME}.{qualified} AT (VERSION => {int(snapshot)}) LIMIT 0"
            )
        except duckdb.Error as error:
            log.warning("could not read columns of %s at snapshot %d: %s", table_name, snapshot, error)
            return None
        return [column[0] for column in cursor.description]

    def _bootstrap(self) -> str:
        """First start on a fresh catalog: there is no consumer yet, so create one on CDC_TABLE."""
        table = self._settings.cdc_table
        if not table:
            raise SystemExit("no consumer exists yet: set CDC_TABLE to the table to follow")
        name = self._settings.base_consumer
        lake.cdc(self._con, "cdc_dml_consumer_create", name, table_name=table)
        log.info("created consumer %s on table %s", name, table)
        return name
