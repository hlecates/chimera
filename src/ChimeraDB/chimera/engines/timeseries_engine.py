import threading
import json
import time
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timedelta

from ChimeraDB.chimera.storage.wal import WriteAheadLog
from ChimeraDB.chimera.storage.snapshot import SnapshotManager
from ChimeraDB.chimera.engines.engine_interface import EngineInterface

class TimeSeriesEngine(EngineInterface):
    def __init__(
        self,
        wal_path: str,
        snap_path: str,
        max_series_name_len: int = 128,
        max_point_id_len: int = 256,
        retention_days: int = 365,
        aggregation_intervals: List[str] = None
    ):
        # config limits
        self.max_series_name_len = max_series_name_len
        self.max_point_id_len = max_point_id_len
        self.retention_days = retention_days
        
        # Default aggregation intervals: 1m, 5m, 1h, 1d
        self.aggregation_intervals = aggregation_intervals or ['1m', '5m', '1h', '1d']

        # store: series -> { points: {timestamp -> point_data}, metadata: {...} }
        self.store: Dict[str, Dict[str, Any]] = {}
        
        # indexes for fast time-based queries
        # time index: series -> timestamp -> point_id
        self.time_indexes: Dict[str, Dict[int, set]] = {}
        # tag indexes: series -> tag_name -> tag_value -> set(timestamps)
        self.tag_indexes: Dict[str, Dict[str, Dict[str, set]]] = {}
        # value indexes for range queries: series -> field -> sorted_timestamps
        self.value_indexes: Dict[str, Dict[str, List[int]]] = {}

        # persistence
        self.lock = threading.RLock()
        self.wal = WriteAheadLog(wal_path)
        self.snapshot_mgr = SnapshotManager(snap_path)

    def startup(self) -> None:
        # Load snapshot and WAL
        snapshot = self.snapshot_mgr.load('latest')
        self.store = snapshot if snapshot else {}

        # Replay WAL
        for op in self.wal.replay():
            self._apply(op)

        # Build indexes from current store
        self.time_indexes = {}
        self.tag_indexes = {}
        self.value_indexes = {}
        
        for series_name, series_data in self.store.items():
            self.time_indexes.setdefault(series_name, {})
            self.tag_indexes.setdefault(series_name, {})
            self.value_indexes.setdefault(series_name, {})
            
            for timestamp, point_data in series_data.get('points', {}).items():
                self._add_to_indexes(series_name, timestamp, point_data)

        # Clean up old data based on retention
        self._cleanup_old_data()

        self.wal.rotate()

    def shutdown(self) -> None:
        with self.lock:
            self.snapshot_mgr.create("latest", self.store)
            self.wal.close()

    def recover(self) -> None:
        with self.lock:
            self.startup()

    def _validate_series_point(self, series: str, point_id: Any) -> None:
        if not isinstance(series, str) or not series:
            raise ValueError("Series name must be a non-empty string")
        if len(series) > self.max_series_name_len:
            raise ValueError(f"Series name exceeds {self.max_series_name_len} chars")
        if not point_id:
            raise ValueError("Point must have a non-empty ID")
        if isinstance(point_id, str) and len(point_id) > self.max_point_id_len:
            raise ValueError(f"Point ID exceeds {self.max_point_id_len} chars")

    def _apply(self, op: Dict) -> None:
        series = op['series']
        self.store.setdefault(series, {'points': {}, 'metadata': {}})
        self.time_indexes.setdefault(series, {})
        self.tag_indexes.setdefault(series, {})
        self.value_indexes.setdefault(series, {})

        if op['operation'] == 'INSERT_POINT':
            timestamp = op['timestamp']
            point_data = op['point_data']
            
            # Remove stale indexes if overwriting existing point
            if timestamp in self.store[series]['points']:
                old_point = self.store[series]['points'][timestamp]
                self._remove_from_indexes(series, timestamp, old_point)
            
            self.store[series]['points'][timestamp] = point_data
            self._add_to_indexes(series, timestamp, point_data)

        elif op['operation'] == 'UPDATE_METADATA':
            metadata = op['metadata']
            self.store[series]['metadata'].update(metadata)

        elif op['operation'] == 'DELETE_POINT':
            timestamp = op['timestamp']
            
            if timestamp in self.store[series]['points']:
                point_data = self.store[series]['points'][timestamp]
                self._remove_from_indexes(series, timestamp, point_data)
                del self.store[series]['points'][timestamp]

        elif op['operation'] == 'DELETE_SERIES':
            # Remove all indexes for this series
            if series in self.time_indexes:
                del self.time_indexes[series]
            if series in self.tag_indexes:
                del self.tag_indexes[series]
            if series in self.value_indexes:
                del self.value_indexes[series]
            
            # Remove from store
            if series in self.store:
                del self.store[series]

    def put(self, series: str, key: Any, value: bytes) -> None:
        # For time-series engine, we'll treat this as adding a data point
        # key should be timestamp, value should be JSON-serialized point data
        self._validate_series_point(series, key)
        
        try:
            point_data = json.loads(value.decode('utf-8'))
        except (json.JSONDecodeError, UnicodeDecodeError):
            raise ValueError("Value must be valid JSON-encoded point data")
        
        # Ensure timestamp is in point_data
        point_data['timestamp'] = key
        
        op = {'operation': 'INSERT_POINT', 'series': series, 'timestamp': key, 'point_data': point_data}
        with self.lock:
            self._apply(op)
            self.wal.append(op)

    def get(self, series: str, key: Any) -> bytes:
        self._validate_series_point(series, key)
        
        with self.lock:
            point_data = self.store.get(series, {}).get('points', {}).get(key)
            if point_data is None:
                return None
            return json.dumps(point_data).encode('utf-8')

    def delete(self, series: str, key: Any) -> bool:
        self._validate_series_point(series, key)
        
        with self.lock:
            if key not in self.store.get(series, {}).get('points', {}):
                return False
            
            op = {'operation': 'DELETE_POINT', 'series': series, 'timestamp': key}
            self._apply(op)
            self.wal.append(op)
            return True

    def query(self, series: str, filter: dict) -> list:
        with self.lock:
            if not filter:
                # Return all points in the series
                points = []
                for timestamp, point_data in self.store.get(series, {}).get('points', {}).items():
                    points.append(point_data)
                return sorted(points, key=lambda x: x.get('timestamp', 0))
            
            # Handle different query types
            if 'time_range' in filter:
                return self._query_time_range(series, filter['time_range'])
            elif 'aggregation' in filter:
                return self._query_aggregation(series, filter['aggregation'])
            elif 'tags' in filter:
                return self._query_by_tags(series, filter['tags'])
            elif 'value_range' in filter:
                return self._query_value_range(series, filter['value_range'])
            else:
                # Default to time range query
                return self._query_time_range(series, filter)

    def _add_to_indexes(self, series: str, timestamp: int, point_data: Dict[str, Any]) -> None:
        # Add to time index
        self.time_indexes[series].setdefault(timestamp, set()).add(timestamp)
        
        # Add to tag indexes
        for tag_name, tag_value in point_data.get('tags', {}).items():
            tag_idx = self.tag_indexes[series].setdefault(tag_name, {})
            tag_idx.setdefault(str(tag_value), set()).add(timestamp)
        
        # Add to value indexes for numeric fields
        for field, value in point_data.items():
            if isinstance(value, (int, float)) and field not in ['timestamp', 'tags']:
                if field not in self.value_indexes[series]:
                    self.value_indexes[series][field] = []
                self.value_indexes[series][field].append(timestamp)
                # Keep sorted for efficient range queries
                self.value_indexes[series][field].sort()

    def _remove_from_indexes(self, series: str, timestamp: int, point_data: Dict[str, Any]) -> None:
        # Remove from time index
        if timestamp in self.time_indexes[series]:
            self.time_indexes[series][timestamp].discard(timestamp)
            if not self.time_indexes[series][timestamp]:
                del self.time_indexes[series][timestamp]
        
        # Remove from tag indexes
        for tag_name, tag_value in point_data.get('tags', {}).items():
            tag_idx = self.tag_indexes[series].get(tag_name, {})
            tag_timestamps = tag_idx.get(str(tag_value), set())
            tag_timestamps.discard(timestamp)
            if not tag_timestamps:
                tag_idx.pop(str(tag_value), None)
        
        # Remove from value indexes
        for field, value in point_data.items():
            if isinstance(value, (int, float)) and field not in ['timestamp', 'tags']:
                if field in self.value_indexes[series]:
                    try:
                        self.value_indexes[series][field].remove(timestamp)
                    except ValueError:
                        pass

    def _query_time_range(self, series: str, time_range: Dict[str, Any]) -> List[Dict[str, Any]]:
        start_time = time_range.get('start')
        end_time = time_range.get('end')
        limit = time_range.get('limit', 100000)  # Increased default limit
        
        points = []
        series_points = self.store.get(series, {}).get('points', {})
        
        # Use time index if available
        if series in self.time_indexes:
            timestamps = sorted(self.time_indexes[series].keys())
        else:
            timestamps = sorted(series_points.keys())
        
        for timestamp in timestamps:
            if start_time and timestamp < start_time:
                continue
            if end_time and timestamp > end_time:
                continue
            
            point_data = series_points.get(timestamp)
            if point_data:
                points.append(point_data)
                if len(points) >= limit:
                    break
        
        return points

    def _query_aggregation(self, series: str, aggregation: Dict[str, Any]) -> List[Dict[str, Any]]:
        interval = aggregation.get('interval', '1h')
        start_time = aggregation.get('start')
        end_time = aggregation.get('end')
        function = aggregation.get('function', 'avg')
        field = aggregation.get('field', 'value')
        
        # Convert interval to seconds
        interval_seconds = self._parse_interval(interval)
        
        # Get points in time range
        time_range = {'start': start_time, 'end': end_time}
        points = self._query_time_range(series, time_range)
        
        # Group by interval
        buckets = {}
        for point in points:
            timestamp = point.get('timestamp', 0)
            bucket_start = (timestamp // interval_seconds) * interval_seconds
            
            if bucket_start not in buckets:
                buckets[bucket_start] = []
            buckets[bucket_start].append(point.get(field, 0))
        
        # Apply aggregation function
        results = []
        for bucket_start, values in sorted(buckets.items()):
            if function == 'avg':
                result_value = sum(values) / len(values)
            elif function == 'sum':
                result_value = sum(values)
            elif function == 'min':
                result_value = min(values)
            elif function == 'max':
                result_value = max(values)
            elif function == 'count':
                result_value = len(values)
            else:
                result_value = sum(values) / len(values)  # default to avg
            
            results.append({
                'timestamp': bucket_start,
                'value': result_value,
                'count': len(values)
            })
        
        return results

    def _query_by_tags(self, series: str, tags: Dict[str, Any]) -> List[Dict[str, Any]]:
        # Find timestamps that match all tag conditions
        matching_timestamps = None
        
        for tag_name, tag_value in tags.items():
            tag_timestamps = self.tag_indexes.get(series, {}).get(tag_name, {}).get(str(tag_value), set())
            
            if matching_timestamps is None:
                matching_timestamps = tag_timestamps
            else:
                matching_timestamps &= tag_timestamps
        
        if not matching_timestamps:
            return []
        
        # Get points for matching timestamps
        points = []
        series_points = self.store.get(series, {}).get('points', {})
        
        for timestamp in sorted(matching_timestamps):
            point_data = series_points.get(timestamp)
            if point_data:
                points.append(point_data)
        
        return points

    def _query_value_range(self, series: str, value_range: Dict[str, Any]) -> List[Dict[str, Any]]:
        field = value_range.get('field', 'value')
        min_value = value_range.get('min')
        max_value = value_range.get('max')
        
        points = []
        series_points = self.store.get(series, {}).get('points', {})
        
        for timestamp, point_data in series_points.items():
            value = point_data.get(field)
            if value is not None:
                if min_value is not None and value < min_value:
                    continue
                if max_value is not None and value > max_value:
                    continue
                points.append(point_data)
        
        return sorted(points, key=lambda x: x.get('timestamp', 0))

    def _parse_interval(self, interval: str) -> int:
        """Convert interval string to seconds"""
        if interval.endswith('s'):
            return int(interval[:-1])
        elif interval.endswith('m'):
            return int(interval[:-1]) * 60
        elif interval.endswith('h'):
            return int(interval[:-1]) * 3600
        elif interval.endswith('d'):
            return int(interval[:-1]) * 86400
        else:
            return 3600  # default to 1 hour

    def _cleanup_old_data(self) -> None:
        """Remove data older than retention period"""
        cutoff_time = int(time.time()) - (self.retention_days * 86400)
        
        for series_name in list(self.store.keys()):
            points_to_remove = []
            series_points = self.store[series_name].get('points', {})
            
            for timestamp in series_points:
                if timestamp < cutoff_time:
                    points_to_remove.append(timestamp)
            
            for timestamp in points_to_remove:
                point_data = series_points[timestamp]
                self._remove_from_indexes(series_name, timestamp, point_data)
                del series_points[timestamp]

    def add_point(self, series: str, timestamp: int, value: float, tags: Dict[str, Any] = None) -> None:
        """Add a data point to a time series"""
        point_data = {
            'timestamp': timestamp,
            'value': value,
            'tags': tags or {}
        }
        
        op = {'operation': 'INSERT_POINT', 'series': series, 'timestamp': timestamp, 'point_data': point_data}
        with self.lock:
            self._apply(op)
            self.wal.append(op)
            
            # Clean up old data after adding new data
            self._cleanup_old_data()

    # Additional time-series specific methods

    def get_series_metadata(self, series: str) -> Dict[str, Any]:
        with self.lock:
            return self.store.get(series, {}).get('metadata', {})

    def update_series_metadata(self, series: str, metadata: Dict[str, Any]) -> None:
        op = {'operation': 'UPDATE_METADATA', 'series': series, 'metadata': metadata}
        with self.lock:
            self._apply(op)
            self.wal.append(op)

    def delete_series(self, series: str) -> bool:
        with self.lock:
            if series not in self.store:
                return False
            
            op = {'operation': 'DELETE_SERIES', 'series': series}
            self._apply(op)
            self.wal.append(op)
            return True

    def get_latest_point(self, series: str) -> Optional[Dict[str, Any]]:
        with self.lock:
            series_points = self.store.get(series, {}).get('points', {})
            if not series_points:
                return None
            
            latest_timestamp = max(series_points.keys())
            return series_points[latest_timestamp]
