import os
import threading
import base64
import pickle
import json
import pytest

from chimera.storage.wal import WriteAheadLog
from chimera.storage.snapshot import SnapShotManager
from chimera.engines.kv_engine import KeyValueEngine

def test_put_get_delete(tmp_path):
    wal_path = str(tmp_path / "kv.wal")
    snap_path = str(tmp_path / "kv.snap")
    engine = KeyValueEngine(wal_path, snap_path)

    engine.put("users", "alice", b"data1")
    assert engine.get("users", "alice") == b"data1"

    engine.put("users", "alice", b"data2")
    assert engine.get("users", "alice") == b"data2"

    assert engine.delete("users", "alice")
    assert engine.get("users", "alice")
    assert not engine.delete("users", "alice")

    engine.shutdown()

if __name__ == "__main__":
    import sys
    sys.exit(pytest.main([__file__]))



    