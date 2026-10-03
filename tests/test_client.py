from realtime.resume.client import State, apply, load_state, save_state


def change(change_type, rowid, row, snapshot=1):
    return {
        "type": "change",
        "change_type": change_type,
        "rowid": rowid,
        "snapshot_id": snapshot,
        "row": row,
    }


def test_insert_update_delete():
    state = State()
    apply(state, change("insert", 1, {"id": 1, "v": "a"}))
    apply(state, change("update_postimage", 1, {"id": 1, "v": "b"}))
    assert state.rows == {"1": {"id": 1, "v": "b"}}
    apply(state, change("delete", 1, {"id": 1, "v": "b"}))
    assert state.rows == {}


def test_preimage_is_ignored_whatever_the_order():
    state = State()
    apply(state, change("insert", 1, {"id": 1, "v": "a"}))
    apply(state, change("update_postimage", 1, {"id": 1, "v": "b"}))
    apply(state, change("update_preimage", 1, {"id": 1, "v": "a"}))
    assert state.rows["1"]["v"] == "b"


def test_applying_twice_gives_the_same_view():
    once, twice = State(), State()
    events = [change("insert", 1, {"id": 1}), change("insert", 2, {"id": 2})]
    for event in events:
        apply(once, event)
    for event in events + events:
        apply(twice, event)
    assert once.rows == twice.rows


def test_markers():
    state = State()
    apply(state, {"type": "schema_changed", "boundary_snapshot": 9})
    assert state.needs_reload and not state.ended
    apply(state, {"type": "table_dropped", "boundary_snapshot": 20})
    assert state.ended


def test_state_roundtrip(tmp_path):
    path = str(tmp_path / "nested" / "state.json")
    assert load_state(path) is None
    state = State(last_seq=7, rows={"1": {"id": 1}})
    save_state(path, state)
    assert load_state(path) == state


def test_reshape_drop_add_rename():
    from realtime.resume.client import State, apply

    s = State()
    apply(s, {"type": "change", "change_type": "insert", "snapshot_id": 2, "rowid": 1, "row": {"id": 1, "amount": 5}, "table": "orders"})
    apply(s, {"type": "schema_changed", "boundary_snapshot": 3, "columns": ["id"], "table": "orders"})
    assert s.rows["1"] == {"id": 1} and not s.needs_reload
    apply(s, {"type": "schema_changed", "boundary_snapshot": 4, "columns": ["id", "priority"], "table": "orders"})
    assert s.rows["1"] == {"id": 1, "priority": None} and not s.needs_reload
    apply(s, {"type": "schema_changed", "boundary_snapshot": 5, "columns": ["id", "p2"], "table": "orders"})
    assert s.needs_reload
