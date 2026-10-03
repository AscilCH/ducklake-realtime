"""Dev helper: print the messages stored in the JetStream stream as a readable table."""

import asyncio
import json
import sys

import nats

from realtime.config import ConfigError, Settings


def describe(payload: dict) -> tuple[str, str, str, str]:
    """Return (kind, snapshot, table, detail) for one message."""
    table = f"{payload.get('table', '?')}"
    if payload.get("type") == "schema_changed":
        return "SCHEMA CHANGE", str(payload["boundary_snapshot"]), table, "stream restarts here"
    if payload.get("type") == "table_dropped":
        return "TABLE DROPPED", str(payload["boundary_snapshot"]), table, "stream ended"
    row = ", ".join(f"{k}={v!r}" for k, v in payload["row"].items())
    return payload["change_type"], str(payload["snapshot_id"]), table, row


async def dump(settings: Settings) -> None:
    nc = await nats.connect(settings.nats_url)
    try:
        js = nc.jetstream()
        info = await js.stream_info(settings.stream)
        print(f"{'seq':>3}  {'subject':<22} {'kind':<14} {'snap':>4}  {'table':<14} detail")
        print("-" * 90)
        for seq in range(info.state.first_seq, info.state.last_seq + 1):
            msg = await js.get_msg(settings.stream, seq)
            kind, snap, table, detail = describe(json.loads(msg.data))
            print(f"{seq:>3}  {msg.subject:<22} {kind:<14} {snap:>4}  {table:<14} {detail}")
    finally:
        await nc.close()


def main() -> None:
    try:
        settings = Settings.from_env()
    except ConfigError as error:
        sys.exit(str(error))
    asyncio.run(dump(settings))


if __name__ == "__main__":
    main()