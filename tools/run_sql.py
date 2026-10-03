"""Dev helper: run SQL statements against the lake using the same environment settings."""

import sys

from realtime import lake
from realtime.config import ConfigError, Settings


def main() -> None:
    if len(sys.argv) < 2:
        sys.exit('usage: python -m tools.run_sql "<SQL>" ["<SQL>" ...]')
    try:
        settings = Settings.from_env()
    except ConfigError as error:
        sys.exit(str(error))
    con = lake.connect(settings)
    try:
        for statement in sys.argv[1:]:
            print(con.execute(statement).fetchall())
    finally:
        con.close()


if __name__ == "__main__":
    main()