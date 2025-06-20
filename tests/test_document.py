import pytest
from ChimeraDB import chimera


def test_document_crud(tmp_path):
    wal_path  = str(tmp_path / "doc.wal")
    snap_path = str(tmp_path / "doc.snap")
    engine = chimera.DocumentEngine(wal_path, snap_path)

    # bring engine up (load snapshot + replay WAL)
    engine.startup()

    # nothing there yet
    assert engine.get("users", "alice") is None
    assert engine.query("users", {}) == []

    # PUT = upsert
    alice = {"email": "alice@example.com", "age": 30}
    engine.put("users", "alice", alice)
    out = engine.get("users", "alice")
    assert out["_id"] == "alice"
    assert out["email"] == "alice@example.com"
    assert out["age"] == 30

    # overwrite existing
    alice2 = {"email": "alice@new.com", "age": 31}
    engine.put("users", "alice", alice2)
    out2 = engine.get("users", "alice")
    assert out2["email"] == "alice@new.com"
    assert out2["age"] == 31

    # query all matching (here only one)
    matches = engine.query("users", {"age": 31})
    assert isinstance(matches, list) and len(matches) == 1
    assert matches[0]["_id"] == "alice"

    # DELETE
    assert engine.delete("users", "alice") is True
    assert engine.get("users", "alice") is None

    # deleting again returns False
    assert engine.delete("users", "alice") is False

    engine.shutdown()


def test_persistence_across_restarts(tmp_path):
    wal_path  = str(tmp_path / "doc.wal")
    snap_path = str(tmp_path / "doc.snap")

    # 1st instance: put and persist
    eng1 = chimera.DocumentEngine(wal_path, snap_path)
    eng1.startup()
    bob = {"role": "admin"}
    eng1.put("users", "bob", bob)
    eng1.shutdown()

    # 2nd instance: recover state
    eng2 = chimera.DocumentEngine(wal_path, snap_path)
    eng2.startup()
    got = eng2.get("users", "bob")
    assert got["_id"] == "bob" and got["role"] == "admin"

    # delete and persist
    assert eng2.delete("users", "bob") is True
    eng2.shutdown()

    # 3rd instance: confirm deletion
    eng3 = chimera.DocumentEngine(wal_path, snap_path)
    eng3.startup()
    assert eng3.get("users", "bob") is None
    eng3.shutdown()
