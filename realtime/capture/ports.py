"""The two things the capture worker depends on. It knows neither DuckLake nor NATS, only these."""

from dataclasses import dataclass
from typing import Protocol


@dataclass
class Batch:
    events: list[dict]  # change events, built with realtime.events.change_event
    end_snapshot: int  # confirm up to here once every event is in the log


@dataclass
class TableInfo:
    table_id: int
    name: str
    dropped: bool


class ChangeSource(Protocol):
    """Ordered changes of one table, with a cursor that only moves when told to."""

    async def open(self) -> None: ...

    async def read(self) -> Batch | None:
        """The next changes after the cursor, or None when there is nothing (yet)."""

    async def commit(self, snapshot: int) -> None:
        """Move the cursor past `snapshot`. Called only after the events are in the log."""

    async def boundary(self) -> int | None:
        """The snapshot where the table changed shape or was dropped, if the stream ended there."""

    async def table(self) -> TableInfo: ...

    async def columns_at(self, table: str, snapshot: int) -> list[str] | None: ...

    async def succeed(self, boundary: int) -> None:
        """Continue after a schema change with a new reader that starts at the boundary."""

    async def abandon(self) -> None:
        """The table is gone: give up the reader."""

    async def attach_recreated(self, table: str, boundary: int) -> bool:
        """After a drop: start reading the table again once it exists. False while it does not."""

    async def close(self) -> None: ...


class EventLog(Protocol):
    """Durable, ordered log. Publishing the same message id twice stores it once."""

    async def publish(self, subject: str, payload: bytes, message_id: str) -> None: ...
