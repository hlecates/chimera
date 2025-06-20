import random
import string
import threading
import pytest
from ChimeraDB import chimera


def random_doc():
    return {
        "name": "".join(random.choices(string.ascii_lowercase, k=8)),
        "age": random.randint(0, 120),
        "score": random.random(),
    }

@pytest.mark.parametrize("n_docs", [10000, 50000])
def test_large_scale_inserts_and_queries(tmp_path, n_docs):
    wal_path = str(tmp_path / "big.wal")
    snap_path = str(tmp_path / "big.snap")
    engine = chimera.DocumentEngine(wal_path, snap_path)
    engine.startup()

    # Bulk insert
    docs = {}
    for i in range(n_docs):
        key = f"user_{i}"
        doc = random_doc()
        docs[key] = doc
        engine.put("people", key, doc)

    # Verify total count
    all_docs = engine.query("people", {})
    assert len(all_docs) == n_docs

    # Sample point-lookups
    sample_keys = random.sample(list(docs.keys()), 100)
    for key in sample_keys:
        got = engine.get("people", key)
        assert got is not None
        assert got["_id"] == key
        assert got["age"] == docs[key]["age"]
        assert got["score"] == pytest.approx(docs[key]["score"], rel=1e-9)

    # Equality query test: pick an existing age
    sample_age = docs[random.choice(sample_keys)]["age"]
    eq_results = engine.query("people", {"age": sample_age})
    assert all(d["age"] == sample_age for d in eq_results)

    # Range query test: ages > 50
    range_results = engine.query("people", {"age": {"$gt": 50}})
    assert all(d["age"] > 50 for d in range_results)

    engine.shutdown()


def test_concurrent_writes_and_reads(tmp_path):
    wal_path  = str(tmp_path / "concurrent.wal")
    snap_path = str(tmp_path / "concurrent.snap")
    engine = chimera.DocumentEngine(wal_path, snap_path)
    engine.startup()

    def writer(start, count):
        for i in range(start, start + count):
            key = f"cuser_{i}"
            engine.put("concurrent", key, {"age": i})

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
    all_keys = [f"cuser_{i}" for i in range(5000)]
    reader_threads = [threading.Thread(target=reader, args=(all_keys[i*1000:(i+1)*1000],)) for i in range(5)]
    for t in reader_threads:
        t.start()
    for t in reader_threads:
        t.join()

    # Final sanity check: full-scan count
    assert len(engine.query("concurrent", {})) == 5000
    engine.shutdown()