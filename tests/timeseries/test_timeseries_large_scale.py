import random
import threading
import pytest
import time
from ChimeraDB import chimera

@pytest.mark.parametrize("n_points", [10000, 50000])
def test_large_scale_timeseries_operations(tmp_path, n_points):
    wal_path = str(tmp_path / "big_ts.wal")
    snap_path = str(tmp_path / "big_ts.snap")
    engine = chimera.TimeSeriesEngine(wal_path, snap_path)
    engine.startup()

    # Add data points over time
    base_time = int(time.time())
    points = {}
    
    for i in range(n_points):
        timestamp = base_time + i * 60  # 1-minute intervals
        value = random.uniform(0, 100)
        tags = {"sensor": f"sensor_{i % 10}", "location": f"loc_{i % 5}"}
        points[timestamp] = {"value": value, "tags": tags}
        engine.add_point("temperature", timestamp, value, tags)

    # Verify total count
    all_points = engine.query("temperature", {"time_range": {"start": base_time, "end": base_time + n_points * 60}})
    assert len(all_points) == n_points

    # Time range query
    mid_time = base_time + (n_points // 2) * 60
    half_points = engine.query("temperature", {"time_range": {"start": base_time, "end": mid_time}})
    assert len(half_points) == n_points // 2

    # Tag-based query
    sensor_0_points = engine.query("temperature", {"tags": {"sensor": "sensor_0"}})
    assert len(sensor_0_points) == n_points // 10

    # Aggregation query
    aggregations = engine.query("temperature", {
        "aggregation": {
            "interval": "1h",
            "start": base_time,
            "end": base_time + n_points * 60,
            "function": "avg",
            "field": "value"
        }
    })
    assert len(aggregations) > 0

    engine.shutdown()

def test_concurrent_timeseries_operations(tmp_path):
    wal_path = str(tmp_path / "concurrent_ts.wal")
    snap_path = str(tmp_path / "concurrent_ts.snap")
    engine = chimera.TimeSeriesEngine(wal_path, snap_path)
    engine.startup()

    def point_writer(start, count):
        base_time = int(time.time())
        for i in range(start, start + count):
            timestamp = base_time + i
            value = random.uniform(0, 100)
            engine.add_point("concurrent", timestamp, value, {"thread": f"t_{start}"})

    # Launch writer threads
    write_threads = [threading.Thread(target=point_writer, args=(i * 1000, 1000)) for i in range(5)]
    for t in write_threads:
        t.start()
    for t in write_threads:
        t.join()

    # Final verification
    all_points = engine.query("concurrent", {})
    assert len(all_points) == 5000

    engine.shutdown()

def test_timeseries_retention_policy(tmp_path):
    wal_path = str(tmp_path / "retention_ts.wal")
    snap_path = str(tmp_path / "retention_ts.snap")
    # Create engine with 1-day retention
    engine = chimera.TimeSeriesEngine(wal_path, snap_path, retention_days=1)
    engine.startup()

    # Add old and new data points
    current_time = int(time.time())
    old_time = current_time - (2 * 24 * 3600)  # 2 days ago
    new_time = current_time - (12 * 3600)       # 12 hours ago

    engine.add_point("test", old_time, 100, {"old": True})
    engine.add_point("test", new_time, 200, {"new": True})

    # After startup, old data should be cleaned up
    points = engine.query("test", {})
    # Only new data should remain
    assert len(points) == 1
    assert points[0]["value"] == 200

    engine.shutdown() 