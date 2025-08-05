import pytest
import json
from ChimeraDB import chimera

def test_column_crud_and_index_equality(tmp_path):
    wal_path = str(tmp_path / "col.wal")
    snap_path = str(tmp_path / "col.snap")
    engine = chimera.ColumnEngine(wal_path, snap_path)
    engine.startup()

    # Basic CRUD operations
    assert engine.get("users", "alice") is None
    assert engine.query("users", {}) == []

    # Insert a row
    alice_row = {"name": "Alice", "age": 30, "email": "alice@example.com"}
    engine.put("users", "alice", alice_row)
    
    # Retrieve the row
    result = engine.get("users", "alice")
    assert result["name"] == "Alice"
    assert result["age"] == 30
    assert result["email"] == "alice@example.com"

    # Query by equality (should use index)
    matches = engine.query("users", {"age": 30})
    assert len(matches) == 1
    assert matches[0]["name"] == "Alice"

    # Update the row
    engine.update("users", {"_id": "alice"}, {"age": 31})
    
    # Verify update
    updated = engine.get("users", "alice")
    assert updated["age"] == 31
    
    # Old query should return empty
    assert engine.query("users", {"age": 30}) == []
    # New query should return the row
    assert len(engine.query("users", {"age": 31})) == 1

    # Delete the row
    assert engine.delete("users", "alice") is True
    assert engine.get("users", "alice") is None
    assert engine.query("users", {"age": 31}) == []

    engine.shutdown()

def test_column_range_queries(tmp_path):
    wal_path = str(tmp_path / "col2.wal")
    snap_path = str(tmp_path / "col2.snap")
    engine = chimera.ColumnEngine(wal_path, snap_path)
    engine.startup()

    # Insert multiple rows
    data = [
        ("bob", {"name": "Bob", "age": 20, "salary": 50000}),
        ("carol", {"name": "Carol", "age": 35, "salary": 75000}),
        ("dave", {"name": "Dave", "age": 50, "salary": 100000}),
    ]
    for key, row in data:
        engine.put("employees", key, row)

    # Range queries
    over_30 = engine.query("employees", {"age": {"$gt": 30}})
    assert len(over_30) == 2
    
    under_40 = engine.query("employees", {"age": {"$lt": 40}})
    assert len(under_40) == 2
    
    salary_range = engine.query("employees", {"salary": {"$gte": 60000, "$lt": 90000}})
    assert len(salary_range) == 1
    assert salary_range[0]["name"] == "Carol"

    engine.shutdown()

def test_column_persistence(tmp_path):
    wal_path = str(tmp_path / "col3.wal")
    snap_path = str(tmp_path / "col3.snap")

    # First instance
    eng1 = chimera.ColumnEngine(wal_path, snap_path)
    eng1.startup()
    eng1.put("test", "row1", {"value": 42})
    eng1.shutdown()

    # Second instance
    eng2 = chimera.ColumnEngine(wal_path, snap_path)
    eng2.startup()
    result = eng2.get("test", "row1")
    assert result["value"] == 42
    eng2.shutdown()

    # Third instance with update
    eng3 = chimera.ColumnEngine(wal_path, snap_path)
    eng3.startup()
    eng3.update("test", {"_id": "row1"}, {"value": 84})
    eng3.shutdown()

    # Fourth instance
    eng4 = chimera.ColumnEngine(wal_path, snap_path)
    eng4.startup()
    result = eng4.get("test", "row1")
    assert result["value"] == 84
    eng4.shutdown()

def test_column_validation(tmp_path):
    wal_path = str(tmp_path / "col4.wal")
    snap_path = str(tmp_path / "col4.snap")
    engine = chimera.ColumnEngine(wal_path, snap_path)
    engine.startup()

    # Test invalid table name
    with pytest.raises(ValueError, match="Table name must be a non-empty string"):
        engine.put("", "key", {"value": 1})

    # Test invalid row ID
    with pytest.raises(ValueError, match="Row must have a non-empty primary key"):
        engine.put("table", "", {"value": 1})

    engine.shutdown()