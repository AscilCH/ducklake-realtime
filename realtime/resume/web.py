"""Web view of a log client: a table that updates live, with buttons to disconnect and reconnect
so you can watch the client catch up from the log.

The server plays the role of the client. It reads the log only (never DuckLake), keeps its
position and rows in the same state file as `realtime.resume.client`, and pushes snapshots to the
browser over server-sent events. Do not run it at the same time as the command-line client.
"""

import asyncio
import json
import logging
import sys
from collections import deque
from datetime import datetime
from pathlib import Path

import nats
from aiohttp import web
from nats.errors import TimeoutError as NatsTimeout
from nats.js.api import ConsumerConfig, DeliverPolicy

from realtime import events as events_log
from realtime.config import ClientSettings, ConfigError
from realtime.resume.client import (
    State,
    TooOld,
    apply,
    load_state,
    save_state,
    start_position,
)

log = logging.getLogger("web")
PAGE = Path(__file__).with_name("index.html")


class LiveView:
    """One client: follows the log while connected, keeps its position while disconnected."""

    def __init__(self, settings: ClientSettings):
        self._settings = settings
        self.state = State()
        self.feed: deque[dict] = deque(maxlen=60)
        self.received = 0  # events received since this process started
        self.table_name: str | None = None
        self._open_updates: dict[tuple, dict] = {}  # (snapshot, rowid) -> entry missing a half
        self.head = 0
        self.error: str | None = None
        self._nc = None
        self._js = None
        self._follow_task: asyncio.Task | None = None
        self._tick_task: asyncio.Task | None = None
        self._waiters: set[asyncio.Event] = set()

    # --- lifecycle ---

    async def start(self) -> None:
        self._nc = await nats.connect(self._settings.nats_url)
        self._js = self._nc.jetstream()
        await self._refresh_head()
        self.state = load_state(self._settings.state_file) or State(last_seq=self.head)
        self._tick_task = asyncio.create_task(self._tick())
        await self.connect()

    async def stop(self) -> None:
        await self.disconnect()
        if self._tick_task:
            self._tick_task.cancel()
        await self._nc.close()

    # --- actions ---

    @property
    def online(self) -> bool:
        return self._follow_task is not None and not self._follow_task.done()

    async def connect(self) -> None:
        if self.online or self.state.ended:
            return
        try:
            self.state = await start_position(self._js, self._settings, self.state)
        except TooOld as error:
            self.error = f"Too old, reload required: {error}"
            self._notify()
            return
        self.error = None
        self._follow_task = asyncio.create_task(self._follow())
        self._notify()

    async def disconnect(self) -> None:
        task, self._follow_task = self._follow_task, None
        if task:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        self._notify()

    async def reset(self) -> None:
        """Start again from the end of the log with an empty view (a real reload would read DuckLake)."""
        await self.disconnect()
        await self._refresh_head()
        self.state = State(last_seq=self.head)
        save_state(self._settings.state_file, self.state)
        self.feed.clear()
        self.error = None
        await self.connect()

    # --- background work ---

    async def _follow(self) -> None:
        settings = self._settings
        sub = await self._js.subscribe(
            events_log.subject(settings.subject_prefix, settings.table_id),
            stream=settings.stream,
            ordered_consumer=True,
            config=ConsumerConfig(
                deliver_policy=DeliverPolicy.BY_START_SEQUENCE,
                opt_start_seq=self.state.last_seq + 1,
            ),
        )
        try:
            while not self.state.ended:
                try:
                    msg = await sub.next_msg(timeout=1)
                except NatsTimeout:
                    continue
                event = events_log.decode(msg.data)
                # No await between these three lines: a disconnect can never split them.
                apply(self.state, event)
                self.state.last_seq = msg.metadata.sequence.stream
                save_state(settings.state_file, self.state)
                self._record(event, self.state.last_seq)
                self._notify()
        except asyncio.CancelledError:
            raise
        except Exception as error:  # surface it in the page instead of dying silently
            log.exception("follow failed")
            self.error = f"{type(error).__name__}: {error}"
        finally:
            try:
                await sub.unsubscribe()
            except Exception:
                pass
            self._notify()

    async def _tick(self) -> None:
        """Keep the log head fresh, so the page can show how many events are waiting."""
        while True:
            await asyncio.sleep(1)
            try:
                await self._refresh_head()
            except Exception:
                continue
            self._notify()

    async def _refresh_head(self) -> None:
        info = await self._js.stream_info(self._settings.stream)
        self.head = info.state.last_seq

    def _record(self, event: dict, seq: int) -> None:
        """Add an event to the feed. The two halves of an update are merged into one entry."""
        self.received += 1
        stamp = datetime.now().strftime("%H:%M:%S")
        if event.get("table"):
            self.table_name = event["table"]
        if event["type"] != "change":
            self.feed.appendleft(
                {"seq": seq, "time": stamp, "kind": event["type"], "snapshot": event["boundary_snapshot"]}
            )
            return
        change = event["change_type"]
        if change in ("insert", "delete"):
            key = "after" if change == "insert" else "before"
            self.feed.appendleft(
                {
                    "seq": seq,
                    "time": stamp,
                    "kind": change,
                    "snapshot": event["snapshot_id"],
                    "rowid": event["rowid"],
                    key: event["row"],
                }
            )
            return
        # update_preimage / update_postimage arrive in either order for the same snapshot and row
        half = "before" if change == "update_preimage" else "after"
        pair = (event["snapshot_id"], event["rowid"])
        entry = self._open_updates.pop(pair, None)
        if entry is None:
            entry = {
                "seq": seq,
                "time": stamp,
                "kind": "update",
                "snapshot": event["snapshot_id"],
                "rowid": event["rowid"],
            }
            self._open_updates[pair] = entry
            self.feed.appendleft(entry)
        entry[half] = event["row"]

    # --- what the page sees ---

    def subscribe(self, wake: asyncio.Event) -> None:
        self._waiters.add(wake)

    def unsubscribe(self, wake: asyncio.Event) -> None:
        self._waiters.discard(wake)

    def _notify(self) -> None:
        for wake in self._waiters:
            wake.set()

    def snapshot(self) -> dict:
        if self.error:
            status = "error"
        elif self.state.ended:
            status = "ended"
        elif self.online:
            status = "online"
        else:
            status = "offline"
        rows = sorted(self.state.rows.items(), key=lambda item: int(item[0]))
        settings = self._settings
        return {
            "status": status,
            "error": self.error,
            "table_id": settings.table_id,
            "table": self.table_name,
            "channel": events_log.subject(settings.subject_prefix, settings.table_id),
            "received": self.received,
            "last_seq": self.state.last_seq,
            "head": self.head,
            "waiting": max(self.head - self.state.last_seq, 0),
            "needs_reload": self.state.needs_reload,
            "ended": self.state.ended,
            "columns": self.state.columns,
            "rows": [{"rowid": rowid, "row": row} for rowid, row in rows],
            "feed": list(self.feed),
        }


# --- HTTP ---


async def index(request: web.Request) -> web.StreamResponse:
    return web.FileResponse(PAGE)


async def state(request: web.Request) -> web.Response:
    return web.json_response(request.app["view"].snapshot())


async def events(request: web.Request) -> web.StreamResponse:
    view: LiveView = request.app["view"]
    response = web.StreamResponse(
        headers={"Content-Type": "text/event-stream", "Cache-Control": "no-cache"}
    )
    await response.prepare(request)
    wake = asyncio.Event()
    view.subscribe(wake)
    try:
        while True:
            wake.clear()
            await response.write(f"data: {json.dumps(view.snapshot())}\n\n".encode())
            try:
                await asyncio.wait_for(wake.wait(), 15)
            except asyncio.TimeoutError:
                pass  # send the snapshot again as a keep-alive
    except ConnectionResetError:
        pass
    finally:
        view.unsubscribe(wake)
    return response


def action(name: str):
    async def handler(request: web.Request) -> web.Response:
        view: LiveView = request.app["view"]
        await getattr(view, name)()
        return web.json_response(view.snapshot())

    return handler


def make_app(settings: ClientSettings) -> web.Application:
    app = web.Application()

    async def lifecycle(app: web.Application):
        view = LiveView(settings)
        app["view"] = view
        await view.start()
        yield
        await view.stop()

    app.cleanup_ctx.append(lifecycle)
    app.add_routes(
        [
            web.get("/", index),
            web.get("/events", events),
            web.get("/api/state", state),
            web.post("/api/connect", action("connect")),
            web.post("/api/disconnect", action("disconnect")),
            web.post("/api/reset", action("reset")),
        ]
    )
    return app


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
    log.info("open http://%s:%d", settings.web_host, settings.web_port)
    web.run_app(make_app(settings), host=settings.web_host, port=settings.web_port, print=None)


if __name__ == "__main__":
    main()
