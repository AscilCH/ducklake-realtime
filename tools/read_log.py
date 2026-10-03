"""Dev helper: print every message currently stored in the JetStream stream."""

import asyncio
import json
import sys

import nats

from realtime.config import ConfigError, Settings


async def dump(settings: Settings) -> None:
    nc = await nats.connect(settings.nats_url)
    try:
        js = nc.jetstream()
        info = await js.stream_info(settings.stream)
        for seq in range(info.state.first_seq, info.state.last_seq + 1):
            msg = await js.get_msg(settings.stream, seq)
            print(seq, msg.subject, json.loads(msg.data))
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