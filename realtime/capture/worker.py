"""Capture worker: moves changes from a ChangeSource to an EventLog, in order, losing none.

It depends on the two interfaces in `ports.py`. `run` at the bottom wires the real ones:
DuckLake with ducklake-cdc, and NATS JetStream.
"""

import asyncio
import logging
import signal
import sys

import nats

from realtime import events, lake
from realtime.capture.cdc_source import DuckLakeCdcSource
from realtime.capture.nats_log import NatsLog
from realtime.capture.ports import Batch, ChangeSource, EventLog
from realtime.config import ConfigError, Settings

log = logging.getLogger("capture")


class CaptureWorker:
    def __init__(self, source: ChangeSource, event_log: EventLog, prefix: str, retry_seconds: float = 1.0):
        self._source = source
        self._log = event_log
        self._prefix = prefix
        self._retry = retry_seconds
        self._waiting_for: tuple[str, int] | None = None  # (table name, drop snapshot)

    async def run(self, stop: asyncio.Event) -> None:
        await self._source.open()
        try:
            while not stop.is_set():
                if self._waiting_for:
                    await self._wait_for_table(stop)
                    continue
                batch = await self._source.read()
                if batch:
                    await self._publish_batch(batch)
                    continue
                boundary = await self._source.boundary()
                if boundary is not None:
                    await self._handle_boundary(boundary)
        finally:
            await self._source.close()

    async def _publish(self, event: dict) -> None:
        await self._log.publish(
            events.subject(self._prefix, event["table_id"]), events.encode(event), events.message_id(event)
        )

    async def _publish_batch(self, batch: Batch) -> None:
        for event in batch.events:
            await self._publish(event)
            log.info(
                "  %-6s snapshot %d  %s  %s",
                event["change_type"],
                event["snapshot_id"],
                event["table"],
                event["row"],
            )
        # Commit only after every event is in the log: a crash replays the batch, never loses it.
        await self._source.commit(batch.end_snapshot)
        log.info("published %d events, committed up to snapshot %d", len(batch.events), batch.end_snapshot)

    async def _handle_boundary(self, boundary: int) -> None:
        table = await self._source.table()
        if table.dropped:
            marker = events.marker_event(
                events.TABLE_DROPPED, table_id=table.table_id, table=table.name, boundary=boundary
            )
        else:
            # The new shape, read at the boundary snapshot. None if it could not be read.
            columns = await self._source.columns_at(table.name, boundary)
            marker = events.marker_event(
                events.SCHEMA_CHANGED,
                table_id=table.table_id,
                table=table.name,
                boundary=boundary,
                columns=columns,
            )
        await self._publish(marker)  # deduplicated by the log if a retry publishes it again
        if table.dropped:
            # The table identity is gone for good. A table recreated under the same name is a
            # new identity (new table_id), so wait for it and attach a fresh reader.
            await self._source.abandon()
            log.info("table %s dropped at snapshot %d: waiting for it to be recreated", table.name, boundary)
            self._waiting_for = (table.name, boundary)
            return
        await self._source.succeed(boundary)

    async def _wait_for_table(self, stop: asyncio.Event) -> None:
        name, boundary = self._waiting_for
        if await self._source.attach_recreated(name, boundary):
            self._waiting_for = None
            return
        try:
            await asyncio.wait_for(stop.wait(), self._retry)
        except asyncio.TimeoutError:
            pass


async def run(settings: Settings) -> None:
    con = lake.connect(settings)
    nc = await nats.connect(settings.nats_url)
    try:
        js = nc.jetstream()
        await js.add_stream(name=settings.stream, subjects=[f"{settings.subject_prefix}.>"])

        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, lambda *_: loop.call_soon_threadsafe(stop.set))

        worker = CaptureWorker(
            DuckLakeCdcSource(settings, con),
            NatsLog(js),
            settings.subject_prefix,
            retry_seconds=settings.listen_timeout_ms / 1000,
        )
        await worker.run(stop)
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
