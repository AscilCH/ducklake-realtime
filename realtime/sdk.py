"""Small client library: subscribe to one table and react to changes.

    sub = Subscription(nats_url, stream, "changes", table_id=1)

    @sub.on_change
    def changed(change): ...        # change.type, change.row, change.before, change.snapshot

    @sub.on_schema_change
    def reshaped(columns): ...      # the table changed shape: the stream stopped there and went on

    @sub.on_table_dropped
    def dropped(): ...

    await sub.run()

The library never talks to DuckLake. It reads the log only. `sub.position` is the last log
sequence handled: save it and pass it back as `start_after` to resume without missing anything.
"""

import asyncio
from dataclasses import dataclass
from typing import Callable

import nats
from nats.errors import TimeoutError as NatsTimeout
from nats.js.api import ConsumerConfig, DeliverPolicy

from realtime import events


@dataclass
class Change:
    type: str  # insert | update | delete
    snapshot: int
    rowid: int
    row: dict | None  # the row after the change (None for delete)
    before: dict | None  # the row before the change (update and delete)


class Subscription:
    def __init__(
        self,
        nats_url: str,
        stream: str,
        prefix: str,
        table_id: int,
        start_after: int | None = None,
    ):
        self._url, self._stream = nats_url, stream
        self._subject = events.subject(prefix, table_id)
        self.position = start_after  # None: from now on
        self._on_change: Callable[[Change], None] = lambda change: None
        self._on_schema: Callable[[list[str] | None], None] = lambda columns: None
        self._on_dropped: Callable[[], None] = lambda: None
        self._halves: dict[tuple, dict] = {}  # (snapshot, rowid) -> update waiting for its other half
        self._stop = asyncio.Event()

    def on_change(self, fn):
        self._on_change = fn
        return fn

    def on_schema_change(self, fn):
        self._on_schema = fn
        return fn

    def on_table_dropped(self, fn):
        self._on_dropped = fn
        return fn

    def stop(self) -> None:
        self._stop.set()

    async def run(self) -> None:
        nc = await nats.connect(self._url)
        try:
            js = nc.jetstream()
            if self.position is None:
                self.position = (await js.stream_info(self._stream)).state.last_seq
            sub = await js.subscribe(
                self._subject,
                stream=self._stream,
                ordered_consumer=True,
                config=ConsumerConfig(
                    deliver_policy=DeliverPolicy.BY_START_SEQUENCE,
                    opt_start_seq=self.position + 1,
                ),
            )
            try:
                while not self._stop.is_set():
                    try:
                        msg = await sub.next_msg(timeout=1)
                    except NatsTimeout:
                        continue
                    ended = self._handle(events.decode(msg.data))
                    self.position = msg.metadata.sequence.stream
                    if ended:
                        return
            finally:
                await sub.unsubscribe()
        finally:
            await nc.close()

    def _handle(self, event: dict) -> bool:
        """Turn one log event into a callback. Returns True when the stream is over."""
        kind = event["type"]
        if kind == events.SCHEMA_CHANGED:
            self._on_schema(event.get("columns"))
        elif kind == events.TABLE_DROPPED:
            self._on_dropped()
            return True
        else:
            self._change(event)
        return False

    def _change(self, event: dict) -> None:
        change_type, snapshot, rowid = event["change_type"], event["snapshot_id"], event["rowid"]
        if change_type == "insert":
            self._on_change(Change("insert", snapshot, rowid, event["row"], None))
        elif change_type == "delete":
            self._on_change(Change("delete", snapshot, rowid, None, event["row"]))
        else:  # the two halves of an update can arrive in either order
            half = "before" if change_type == "update_preimage" else "after"
            pair = self._halves.setdefault((snapshot, rowid), {})
            pair[half] = event["row"]
            if len(pair) == 2:
                del self._halves[(snapshot, rowid)]
                self._on_change(Change("update", snapshot, rowid, pair["after"], pair["before"]))
