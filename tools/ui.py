"""Dev helper: open the DuckDB web UI on the lake, so tables can be browsed and SQL run in a browser."""

import sys
import time

from realtime import lake
from realtime.config import ConfigError, Settings


def main() -> None:
    try:
        settings = Settings.from_env()
    except ConfigError as error:
        sys.exit(str(error))
    con = lake.connect(settings)
    con.execute("INSTALL ui")
    con.execute("CALL start_ui()")  # serves on http://localhost:4213 and opens the browser
    print(f"DuckDB UI running on http://localhost:4213 (catalog '{lake.NAME}'). Ctrl+C to stop.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        con.close()


if __name__ == "__main__":
    main()
