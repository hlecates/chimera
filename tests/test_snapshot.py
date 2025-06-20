import pytest
from ChimeraDB.chimera.storage.snapshot import SnapshotManager


def test_snapshot_manager_direct(tmp_path):
    # direct SnapshotManager test (KV-compatible)
    snap_path = str(tmp_path / "kv_direct.snap")
    mgr = SnapshotManager(snap_path)

    original = {"coll": {"key": b"value"}}
    mgr.create("latest", original)
    loaded = mgr.load("latest")
    assert loaded == original

    # modifying loaded should not mutate the stored snapshot
    loaded["coll"]["key"] = b"other"
    reloaded = mgr.load("latest")
    assert reloaded["coll"]["key"] == b"value"