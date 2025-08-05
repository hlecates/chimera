import pytest
import json
import time
from ChimeraDB import chimera

def test_timeseries_basic_operations(tmp_path):
    wal_path = str(tmp_path / "ts.wal")
    snap_path = str(tmp_path / "ts.snap")
    engine = chimera.TimeSeriesEngine(wal_path, snap_path)
    engine.startup()

    # Add data points
    current_time = int(time.time())
    engine.add_point("cpu_usage", current_time, 75.5, {"host": "server1", "cpu": "cpu0"})
    engine.add_point("cpu_usage", current_time + 60, 80.2, {"host": "server1", "cpu": "cpu0"})
    engine.add_point("cpu_usage", current_time + 120, 65.8, {"host": "server1", "cpu": "cpu0"})

    # Query time range
    points = engine.query("cpu_usage", {"time_range": {"start": current_time, "end": current_time + 180}})
    assert len(points) == 3

    # Query by tags
    server1_points = engine.query("cpu_usage", {"tags": {"host": "server1"}})
    assert len(server1_points) == 3

    # Get latest point
    latest = engine.get_latest_point("cpu_usage")
    assert latest["value"] == 65.8

    engine.shutdown()

def test_timeseries_aggregation(tmp_path):
    wal_path = str(tmp_path / "ts2.wal")
    snap_path = str(tmp_path / "ts2.snap")
    engine = chimera.TimeSeriesEngine(wal_path, snap_path)
    engine.startup()

    # Add data points over time
    base_time = int(time.time())
    for i in range(10):
        engine.add_point("temperature", base_time + i * 60, 20 + i, {"sensor": "temp1"})

    # Aggregate by 5-minute intervals
    aggregations = engine.query("temperature", {
        "aggregation": {
            "interval": "5m",
            "start": base_time,
            "end": base_time + 600,
            "function": "avg",
            "field": "value"
        }
    })
    
    assert len(aggregations) > 0
    for agg in aggregations:
        assert "timestamp" in agg
        assert "value" in agg
        assert "count" in agg

    engine.shutdown()

def test_timeseries_value_range_queries(tmp_path):
    wal_path = str(tmp_path / "ts3.wal")
    snap_path = str(tmp_path / "ts3.snap")
    engine = chimera.TimeSeriesEngine(wal_path, snap_path)
    engine.startup()

    # Add data points with different values
    current_time = int(time.time())
    for i in range(10):
        engine.add_point("pressure", current_time + i * 60, 100 + i * 10, {"unit": "psi"})

    # Query by value range
    high_pressure = engine.query("pressure", {"value_range": {"field": "value", "min": 150, "max": 200}})
    assert len(high_pressure) == 5  # values 150, 160, 170, 180, 190

    engine.shutdown()

def test_timeseries_metadata_operations(tmp_path):
    wal_path = str(tmp_path / "ts4.wal")
    snap_path = str(tmp_path / "ts4.snap")
    engine = chimera.TimeSeriesEngine(wal_path, snap_path)
    engine.startup()

    # Update series metadata
    engine.update_series_metadata("cpu_usage", {
        "description": "CPU usage metrics",
        "unit": "percentage",
        "tags": ["monitoring", "performance"]
    })

    # Get metadata
    metadata = engine.get_series_metadata("cpu_usage")
    assert metadata["description"] == "CPU usage metrics"
    assert metadata["unit"] == "percentage"

    # Delete series
    assert engine.delete_series("cpu_usage") is True
    assert engine.get_series_metadata("cpu_usage") == {}

    engine.shutdown()