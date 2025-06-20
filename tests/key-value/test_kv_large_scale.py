import random
import string
import json
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
    wal_path  = str(tmp_path / "big.wal")
    snap_path = str(tmp_path / "big.snap")
    engine = chimera.KVEngine(wal_path, snap_path)
    engine.startup()

    # Bulk insert of JSON‐serialized documents as bytes
    docs = {}
    for i in range(n_docs):
        key = f"user_{i}"
        doc = random_doc()
        data = json.dumps(doc).encode('utf-8')
        docs[key] = data    
        engine.put("people", key, data)

    with pytest.raises(NotImplementedError):
        engine.query("people", {})

    # Sample point-lookups
    sample_keys = random.sample(list(docs.keys()), 100)
    for key in sample_keys:
        got = engine.get("people", key)
        assert got == docs[key]

    engine.shutdown()


def test_concurrent_writes_and_reads(tmp_path):
    wal_path  = str(tmp_path / "concurrent.wal")
    snap_path = str(tmp_path / "concurrent.snap")
    engine = chimera.KVEngine(wal_path, snap_path)
    engine.startup()

    def writer(start, count):
        for i in range(start, start + count):
            key = f"cuser_{i}"
            data = json.dumps({"age": i}).encode('utf-8')
            engine.put("concurrent", key, data)

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

    with pytest.raises(NotImplementedError):
        engine.query("concurrent", {})

    engine.shutdown()