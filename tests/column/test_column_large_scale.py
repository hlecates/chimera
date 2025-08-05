import random
import string
import threading
import pytest
from ChimeraDB import chimera

def random_row():
    return {
        "name": "".join(random.choices(string.ascii_lowercase, k=8)),
        "age": random.randint(18, 80),
        "salary": random.randint(30000, 150000),
        "department": random.choice(["engineering", "sales", "marketing", "hr"]),
    }

@pytest.mark.parametrize("n_rows", [10000, 50000])
def test_large_scale_column_operations(tmp_path, n_rows):
    wal_path = str(tmp_path / "big_col.wal")
    snap_path = str(tmp_path / "big_col.snap")
    engine = chimera.ColumnEngine(wal_path, snap_path)
    engine.startup()

    # Bulk insert
    rows = {}
    for i in range(n_rows):
        key = f"employee_{i}"
        row = random_row()
        rows[key] = row
        engine.put("employees", key, row)

    # Verify total count
    all_rows = engine.query("employees", {})
    assert len(all_rows) == n_rows

    # Sample point lookups
    sample_keys = random.sample(list(rows.keys()), 100)
    for key in sample_keys:
        result = engine.get("employees", key)
        assert result is not None
        assert result["name"] == rows[key]["name"]

    # Equality queries
    engineering_dept = engine.query("employees", {"department": "engineering"})
    assert all(row["department"] == "engineering" for row in engineering_dept)

    # Range queries
    high_salary = engine.query("employees", {"salary": {"$gt": 100000}})
    assert all(row["salary"] > 100000 for row in high_salary)

    engine.shutdown()

def test_concurrent_column_operations(tmp_path):
    wal_path = str(tmp_path / "concurrent_col.wal")
    snap_path = str(tmp_path / "concurrent_col.snap")
    engine = chimera.ColumnEngine(wal_path, snap_path)
    engine.startup()

    def writer(start, count):
        for i in range(start, start + count):
            key = f"c_emp_{i}"
            row = {"id": i, "value": i * 10}
            engine.put("concurrent", key, row)

    def reader(keys):
        for key in keys:
            _ = engine.get("concurrent", key)

    # Launch writer threads
    write_threads = [threading.Thread(target=writer, args=(i * 1000, 1000)) for i in range(5)]
    for t in write_threads:
        t.start()
    for t in write_threads:
        t.join()

    # Launch reader threads
    all_keys = [f"c_emp_{i}" for i in range(5000)]
    reader_threads = [threading.Thread(target=reader, args=(all_keys[i*1000:(i+1)*1000],)) for i in range(5)]
    for t in reader_threads:
        t.start()
    for t in reader_threads:
        t.join()

    # Final verification
    assert len(engine.query("concurrent", {})) == 5000
    engine.shutdown()