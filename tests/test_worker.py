"""The capture worker against in-memory fakes: no DuckLake, no NATS."""

import asyncio

import pytest

from realtime import events
from realtime.capture.ports import Batch, TableInfo
from realtime.capture.worker import CaptureWorker


def change(snapshot, rowid=1, kind="insert", row=None):
    return events.change_event(
        snapshot_id=snapshot, rowid=rowid, change_type=kind, table_id=1, table="orders",
        row=row or {"id": rowid}, snapshot_time="2026-01-01T00:00:00",
    )


class FakeLog:
    def __init__(self, fail_times=0):
        self.messages: list[tuple[str, dict, str]] = []
        self.fail_times = fail_times

    async def publish(self, subject, payload, message_id):
        if self.fail_times:
            self.fail_times -= 1
            raise ConnectionError("log down")
        self.messages.append((subject, events.decode(payload), message_id))


class FakeSource:
    """Plays a script: each read() returns the next item (a Batch or None)."""

    def __init__(self, script, stop, boundaries=(), table=None, recreated_after=0):
        self.script, self.stop = list(script), stop
        self.boundaries = list(boundaries)
        self._table = table or TableInfo(1, "orders", False)
        self.recreated_after = recreated_after
        self.calls: list[str] = []

    async def open(self):
        self.calls.append("open")

    async def read(self):
        if not self.script:
            if not self.boundaries:
                self.stop.set()
            return None
        item = self.script.pop(0)
        return item

    async def commit(self, snapshot):
        self.calls.append(f"commit {snapshot}")

    async def boundary(self):
        return self.boundaries.pop(0) if self.boundaries else None

    async def table(self):
        return self._table

    async def columns_at(self, table, snapshot):
        return ["id", "priority"]

    async def succeed(self, boundary):
        self.calls.append(f"succeed {boundary}")
        self.boundaries or self.stop.set()

    async def abandon(self):
        self.calls.append("abandon")

    async def attach_recreated(self, table, boundary):
        self.calls.append("attach?")
        if self.recreated_after > 0:
            self.recreated_after -= 1
            return False
        self.calls.append(f"attached {table}")
        self.stop.set()
        return True

    async def close(self):
        self.calls.append("close")


def test_commits_only_after_every_event_is_published():
    async def scenario():
        stop = asyncio.Event()
        log = FakeLog()
        source = FakeSource([Batch([change(5), change(5, rowid=2)], 5)], stop)
        await CaptureWorker(source, log, "changes").run(stop)
        return source, log

    source, log = asyncio.run(scenario())
    assert [m[1]["rowid"] for m in log.messages] == [1, 2]
    assert log.messages[0][0] == "changes.1"
    assert "commit 5" in source.calls
    assert source.calls[-1] == "close"


def test_failed_publish_does_not_move_the_cursor():
    async def scenario():
        stop = asyncio.Event()
        log = FakeLog(fail_times=1)
        source = FakeSource([Batch([change(5)], 5)], stop)
        with pytest.raises(ConnectionError):
            await CaptureWorker(source, log, "changes").run(stop)
        return source

    source = asyncio.run(scenario())
    assert not any(call.startswith("commit") for call in source.calls)
    assert source.calls[-1] == "close"  # the reader is released even on failure


def test_schema_change_publishes_marker_with_columns_then_succeeds():
    async def scenario():
        stop = asyncio.Event()
        log = FakeLog()
        source = FakeSource([], stop, boundaries=[9])
        await CaptureWorker(source, log, "changes").run(stop)
        return source, log

    source, log = asyncio.run(scenario())
    subject, marker, message_id = log.messages[0]
    assert subject == "changes.1"
    assert marker["type"] == events.SCHEMA_CHANGED
    assert marker["columns"] == ["id", "priority"]
    assert marker["boundary_snapshot"] == 9
    assert message_id == "schema_changed-1-9"
    assert "succeed 9" in source.calls


def test_drop_publishes_marker_then_waits_for_the_table():
    async def scenario():
        stop = asyncio.Event()
        log = FakeLog()
        source = FakeSource(
            [], stop, boundaries=[12], table=TableInfo(1, "orders", True), recreated_after=2
        )
        await CaptureWorker(source, log, "changes", retry_seconds=0.01).run(stop)
        return source, log

    source, log = asyncio.run(scenario())
    assert log.messages[0][1]["type"] == events.TABLE_DROPPED
    assert "columns" not in log.messages[0][1]
    assert source.calls.count("attach?") == 3  # two misses, then it exists again
    assert "attached orders" in source.calls
    assert "abandon" in source.calls
