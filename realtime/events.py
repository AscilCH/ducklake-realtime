"""What travels on the log. The one place that knows the event shape and the subject format.

Producers (the capture worker) build events with these functions. Consumers (clients) read them
with `decode` and compare against the constants. Nothing else spells out field names or subjects.
"""

import json

CHANGE = "change"
SCHEMA_CHANGED = "schema_changed"
TABLE_DROPPED = "table_dropped"


def subject(prefix: str, table_id: int) -> str:
    """Channel of one table. The id survives a rename, the name does not."""
    return f"{prefix}.{table_id}"


def change_event(
    *, snapshot_id: int, rowid: int, change_type: str, table_id: int, table: str, row: dict, snapshot_time: str
) -> dict:
    return {
        "type": CHANGE,
        "snapshot_id": snapshot_id,
        "rowid": rowid,
        "change_type": change_type,
        "table_id": table_id,
        "table": table,
        "row": row,
        "snapshot_time": snapshot_time,
    }


def marker_event(kind: str, *, table_id: int, table: str, boundary: int, columns: list[str] | None = None) -> dict:
    """A schema_changed or table_dropped marker. Only schema_changed carries the new columns."""
    marker = {"type": kind, "table_id": table_id, "table": table, "boundary_snapshot": boundary}
    if kind == SCHEMA_CHANGED:
        marker["columns"] = columns
    return marker


def message_id(event: dict) -> str:
    """Unique per event, so the log drops a duplicate published after a retry."""
    if event["type"] == CHANGE:
        return f"{event['snapshot_id']}-{event['rowid']}-{event['change_type']}"
    return f"{event['type']}-{event['table_id']}-{event['boundary_snapshot']}"


def encode(event: dict) -> bytes:
    return json.dumps(event, default=str).encode()


def decode(data: bytes) -> dict:
    return json.loads(data)
