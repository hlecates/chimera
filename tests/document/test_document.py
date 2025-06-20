import pytest
from ChimeraDB import chimera

def test_document_crud_and_index_equality(tmp_path):
    wal_path  = str(tmp_path / "doc.wal")
    snap_path = str(tmp_path / "doc.snap")
    engine = chimera.DocumentEngine(wal_path, snap_path)
    engine.startup()

    # Basic CRUD (unchanged)
    assert engine.get("users", "alice") is None
    assert engine.query("users", {}) == []

    alice = {"email": "alice@example.com", "age": 30}
    engine.put("users", "alice", alice)
    out = engine.get("users", "alice")
    assert out["_id"] == "alice"
    assert out["email"] == "alice@example.com"
    assert out["age"] == 30

    # Equality query should hit the index under the hood
    # (we can’t see the index directly, but behavior must be correct)
    matches = engine.query("users", {"age": 30})
    assert len(matches) == 1 and matches[0]["_id"] == "alice"

    # Overwrite existing and re-query
    alice2 = {"email": "alice@new.com", "age": 31}
    engine.put("users", "alice", alice2)
    matches = engine.query("users", {"age": 30})
    assert matches == []                  # old index entry dropped
    matches = engine.query("users", {"age": 31})
    assert len(matches) == 1 and matches[0]["_id"] == "alice"

    # Test update() helper and index maintenance
    count = engine.update("users", {"_id": "alice"}, {"$set": {"age": 42}})
    assert count == 1
    # Should no longer find age=31 but find age=42
    assert engine.query("users", {"age": 31}) == []
    assert engine.query("users", {"age": 42})[0]["_id"] == "alice"

    # Delete should remove from index too
    assert engine.delete("users", "alice") is True
    assert engine.get("users", "alice") is None
    assert engine.query("users", {"age": 42}) == []

    engine.shutdown()


def test_range_queries_and_fallback(tmp_path):
    wal_path  = str(tmp_path / "doc2.wal")
    snap_path = str(tmp_path / "doc2.snap")
    engine = chimera.DocumentEngine(wal_path, snap_path)
    engine.startup()

    # Insert multiple users with different ages
    data = [
        ("bob",   {"age": 20}),
        ("carol", {"age": 35}),
        ("dave",  {"age": 50}),
    ]
    for key, doc in data:
        engine.put("people", key, doc)

    # $gt operator must work (no direct index since it's a range)
    over_30 = engine.query("people", {"age": {"$gt": 30}})
    ids = sorted([d["_id"] for d in over_30])
    assert ids == ["carol", "dave"]

    # $lte operator
    under_or_eq_35 = engine.query("people", {"age": {"$lte": 35}})
    ids = sorted([d["_id"] for d in under_or_eq_35])
    assert ids == ["bob", "carol"]

    # Mixed filter: equality + range
    result = engine.query("people", {"age": {"$gte": 20, "$lt": 50}})
    ids = sorted([d["_id"] for d in result])
    assert ids == ["bob", "carol"]

    engine.shutdown()


def test_persistence_and_index_rebuild(tmp_path):
    wal_path  = str(tmp_path / "doc3.wal")
    snap_path = str(tmp_path / "doc3.snap")

    # 1) write two docs, shutdown
    eng1 = chimera.DocumentEngine(wal_path, snap_path)
    eng1.startup()
    eng1.put("x", "a", {"v": 1})
    eng1.put("x", "b", {"v": 2})
    eng1.shutdown()

    # 2) restart, index should be rebuilt; range query still works
    eng2 = chimera.DocumentEngine(wal_path, snap_path)
    eng2.startup()
    # verify data is back
    assert eng2.get("x", "a")["_id"] == "a"
    assert eng2.get("x", "b")["_id"] == "b"
    # verify index rebuild: equality lookup
    assert eng2.query("x", {"v": 2})[0]["_id"] == "b"
    eng2.shutdown()

    # 3) Delete one, persist, then verify on next restart
    eng3 = chimera.DocumentEngine(wal_path, snap_path)
    eng3.startup()
    assert eng3.delete("x", {"_id": "a"}) is True
    eng3.shutdown()

    eng4 = chimera.DocumentEngine(wal_path, snap_path)
    eng4.startup()
    # a should be gone, b still present
    assert eng4.get("x", "a") is None
    assert eng4.get("x", "b")["_id"] == "b"
    eng4.shutdown()
