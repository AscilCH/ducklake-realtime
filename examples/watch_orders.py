"""Demo of the client library: a few lines to follow a table.

Run with: uv run --env-file .env python -m examples.watch_orders
"""

import asyncio
import os

from realtime.sdk import Subscription


async def main() -> None:
    sub = Subscription(
        os.environ["NATS_URL"],
        os.environ["JETSTREAM_NAME"],
        os.environ["JETSTREAM_SUBJECT_PREFIX"],
        table_id=int(os.environ["CLIENT_TABLE_ID"]),
    )

    @sub.on_change
    def changed(change):
        print(f"{change.type.upper():7} snapshot {change.snapshot}  {change.before or ''} -> {change.row or ''}")

    @sub.on_schema_change
    def reshaped(columns):
        print(f"SCHEMA  the table now has columns {columns}. The stream stopped at the change and went on.")

    @sub.on_table_dropped
    def dropped():
        print("DROPPED the table is gone, this subscription is over")

    print("subscribed, waiting for changes (Ctrl+C to stop)")
    await sub.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
