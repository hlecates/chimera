import pytest
from ChimeraDB import chimera
from ChimeraDB.chimera.storage.snapshot import SnapshotManager


def test_kv_put_get_delete(tmp_path):
    # create clean WAL and snapshot files
    wal_path = str(tmp_path / "kv.wal")
    snap_path = str(tmp_path / "kv.snap")
    engine = chimera.KVEngine(wal_path, snap_path)

    # bring engine up (load snapshot + replay WAL)
    engine.startup()

    # initial PUT and GET
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

    engine.shutdown()


def test_kv_persistence_across_restarts(tmp_path):
    wal_path = str(tmp_path / "kv.wal")
    snap_path = str(tmp_path / "kv.snap")

    # First engine instance: insert and persist
    eng1 = chimera.KVEngine(wal_path, snap_path)
    eng1.startup()
    eng1.put("config", "version", b"v1")
    eng1.shutdown()

    # Second engine: recover state
    eng2 = chimera.KVEngine(wal_path, snap_path)
    eng2.startup()
    assert eng2.get("config", "version") == b"v1"

    # Delete and persist
    assert eng2.delete("config", "version") is True
    eng2.shutdown()

    # Third engine: ensure deletion persisted
    eng3 = chimera.KVEngine(wal_path, snap_path)
    eng3.startup()
    assert eng3.get("config", "version") is None
    eng3.shutdown()



    