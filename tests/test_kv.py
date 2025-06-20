from ChimeraDB import chimera

def test_put_get_delete(tmp_path):
    # create clean WAL and snapshot files in a temporary directory
    wal_path = str(tmp_path / "kv.wal")
    snap_path = str(tmp_path / "kv.snap")
    engine = chimera.KVEngine(wal_path, snap_path)

    # initial put/get
    engine.put("users", "alice", b"data1")
    assert engine.get("users", "alice") == b"data1"

    # overwrite existing value
    engine.put("users", "alice", b"data2")
    assert engine.get("users", "alice") == b"data2"

    # delete the key and verify removal
    assert engine.delete("users", "alice") is True
    assert engine.get("users", "alice") is None

    # deleting a non-existent key should return False
    assert engine.delete("users", "alice") is False

    # clean shutdown should flush any pending writes
    engine.shutdown()



    