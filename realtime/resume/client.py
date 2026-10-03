"""Resume client: follows one table's changes from the log and catches up after a disconnect.

It never talks to DuckLake. Its position (last log sequence) and its local copy of the rows live
in a state file, so a restart continues exactly where the previous run stopped.
"""

import asyncio
import json
import logging
import os
import signal
import sys
from dataclasses import asdict, dataclass, field

import nats
from nats.errors import TimeoutError as NatsTimeout
from nats.js.api import ConsumerConfig, DeliverPolicy

from realtime import events
from realtime.config import ClientSettings, ConfigError

log = logging.getLogger("client")


class TooOld(Exception):
    """The saved position has been removed from the log: the client must reload."""


@dataclass
class State:
    last_seq: int = 0
    rows: dict[str, dict] = field(default_factory=dict)  # rowid -> current row
    columns: list[str] = field(default_factory=list)  # current shape of the table
    needs_reload: bool = False  # a schema change this client cannot apply on its own
    ended: bool = False  # the table was dropped: this stream is over


def load_state(path: str) -> State | None:
    try:
        with open(path, encoding="utf-8") as handle:
            return State(**json.load(handle))
    except FileNotFoundError:
        return None


def save_state(path: str, state: State) -> None:
    """Write atomically, so a crash never leaves a half-written state file."""
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    temporary = f"{path}.tmp"
    with open(temporary, "w", encoding="utf-8") as handle:
        json.dump(asdict(state), handle)
    os.replace(temporary, path)


def _reshape(state: State, columns: list[str] | None) -> None:
    """Apply a schema change to the local rows: forget dropped columns, add new ones as empty.

    A drop plus an add in one change looks like a rename: the values cannot be mapped from the
    log alone, so the client asks for a reload instead of guessing.
    """
    if columns is None:
        state.needs_reload = True  # the worker could not tell us the new shape
        return
    known, new = set(state.columns), set(columns)
    removed, added = known - new, new - known
    if known and removed and added:
        state.needs_reload = True
    else:
        for row in state.rows.values():
            for name in removed:
                row.pop(name, None)
            for name in added:
                row.setdefault(name, None)
    state.columns = list(columns)


def apply(state: State, event: dict) -> None:
    """Apply one log event. Idempotent: applying the same event twice gives the same view."""
    kind = event["type"]
    if kind == events.CHANGE:
        rowid = str(event["rowid"])
        change = event["change_type"]
        if not state.columns:
            state.columns = list(event["row"])
        if change in ("insert", "update_postimage"):
            state.rows[rowid] = event["row"]
        elif change == "delete":
            state.rows.pop(rowid, None)
        # update_preimage carries the old values and changes nothing in the view.
    elif kind == events.SCHEMA_CHANGED:
        _reshape(state, event.get("columns"))
    elif kind == events.TABLE_DROPPED:
        state.ended = True


async def start_position(js, settings: ClientSettings, state: State | None) -> State:
    """Decide where to read from, and refuse a position that retention has already removed."""
    head = (await js.stream_info(settings.stream)).state
    if state is None:
        # New client: changes from now on only (history for new users is out of scope).
        return State(last_seq=head.last_seq)
    if head.first_seq and state.last_seq + 1 < head.first_seq:
        raise TooOld(
            f"saved position {state.last_seq} is older than the log start {head.first_seq}"
        )
    return state


def render(event: dict) -> str:
    if event["type"] == "change":
        return f"{event['change_type']:<16} snapshot {event['snapshot_id']}  {event['row']}"
    return f"{event['type'].upper()} at snapshot {event['boundary_snapshot']}"


async def follow(settings: ClientSettings, stop: asyncio.Event) -> None:
    nc = await nats.connect(settings.nats_url)
    try:
        js = nc.jetstream()
        state = await start_position(js, settings, load_state(settings.state_file))
        head = (await js.stream_info(settings.stream)).state.last_seq
        log.info(
            "resuming after log sequence %d (log head %d, %d to replay)",
            state.last_seq,
            head,
            max(head - state.last_seq, 0),
        )

        subject = events.subject(settings.subject_prefix, settings.table_id)
        sub = await js.subscribe(
            subject,
            stream=settings.stream,
            ordered_consumer=True,
            config=ConsumerConfig(
                deliver_policy=DeliverPolicy.BY_START_SEQUENCE,
                opt_start_seq=state.last_seq + 1,
            ),
        )
        try:
            while not stop.is_set() and not state.ended:
                try:
                    msg = await sub.next_msg(timeout=1)
                except NatsTimeout:
                    continue
                event = events.decode(msg.data)
                apply(state, event)
                state.last_seq = msg.metadata.sequence.stream
                save_state(settings.state_file, state)  # position and view move together
                log.info("seq %-4d %s", state.last_seq, render(event))
        finally:
            await sub.unsubscribe()

        if state.ended:
            log.info("table %d was dropped: this stream is over", settings.table_id)
        if state.needs_reload:
            log.warning("a schema change was seen: reload the table before trusting old rows")
        log.info("view: %d rows, position %d", len(state.rows), state.last_seq)
        for rowid, row in sorted(state.rows.items(), key=lambda item: int(item[0])):
            log.info("  rowid %-4s %s", rowid, row)
    finally:
        await nc.close()


async def run(settings: ClientSettings) -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: loop.call_soon_threadsafe(stop.set))
    await follow(settings, stop)


def main() -> None:
    try:
        settings = ClientSettings.from_env()
    except ConfigError as error:
        sys.exit(str(error))
    logging.basicConfig(
        level=settings.log_level,
        stream=sys.stdout,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    try:
        asyncio.run(run(settings))
    except TooOld as error:
        sys.exit(f"too old, reload required: {error}")


if __name__ == "__main__":
    main()
