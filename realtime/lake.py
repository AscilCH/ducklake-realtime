import duckdb

from realtime.config import Settings

NAME = "lake"  # catalog alias used in all SQL


def _literal(value) -> str:
    """Render a Python value as a safely escaped SQL literal."""
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, int):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


def _conninfo_value(value: str) -> str:
    """Quote a libpq connection value only when it needs quoting."""
    if value and not any(ch.isspace() or ch in "'\\" for ch in value):
        return value
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _conninfo(settings: Settings) -> str:
    parts = {
        "dbname": settings.postgres_db,
        "host": settings.postgres_host,
        "port": str(settings.postgres_port),
        "user": settings.postgres_user,
        "password": settings.postgres_password,
    }
    return " ".join(f"{key}={_conninfo_value(value)}" for key, value in parts.items())


def connect(settings: Settings) -> duckdb.DuckDBPyConnection:
    """Open a DuckDB connection with the lake attached through the Postgres catalog."""
    con = duckdb.connect()
    con.execute("LOAD ducklake")
    con.execute("LOAD ducklake_cdc")
    catalog = f"ducklake:postgres:{_conninfo(settings)}"
    con.execute(
        f"ATTACH {_literal(catalog)} AS {NAME} (DATA_PATH {_literal(settings.data_path)})"
    )
    return con


def cdc(con: duckdb.DuckDBPyConnection, function: str, *args, **named) -> list[dict]:
    """Call a ducklake-cdc table function. The catalog is always the first argument."""
    parts = [_literal(NAME)]
    parts += [_literal(arg) for arg in args]
    parts += [f"{key} := {_literal(value)}" for key, value in named.items()]
    cursor = con.execute(f"SELECT * FROM {function}({', '.join(parts)})")
    columns = [column[0] for column in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]