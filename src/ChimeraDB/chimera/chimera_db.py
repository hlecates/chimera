"""
ChimeraDB - Polyglot NoSQL Database

Unified API for intelligent engine selection and data management.
"""

import os
import time
from typing import Any, Dict, List, Optional, Union
from pathlib import Path

from .engines.kv_engine import KVEngine
from .engines.document_engine import DocumentEngine
from .engines.column_engine import ColumnEngine
from .engines.graph_engine import GraphEngine
from .engines.timeseries_engine import TimeSeriesEngine
from .profiler import DataProfiler, EngineSelector, PerformanceMetrics


class ChimeraDB:
    """Main ChimeraDB class providing unified access to all engines."""
    
    def __init__(self, data_dir: str = "./chimera_data"):
        """
        Initialize ChimeraDB.
        
        Args:
            data_dir: Directory to store engine data
        """
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(exist_ok=True)
        
        # Initialize engines
        self.engines = {}
        self._initialize_engines()
        
        # Initialize profiler components
        self.profiler = DataProfiler()
        self.engine_selector = EngineSelector()
        self.metrics = PerformanceMetrics()
        
        # Engine mapping
        self.engine_map = {
            'kv': KVEngine,
            'document': DocumentEngine,
            'column': ColumnEngine,
            'graph': GraphEngine,
            'timeseries': TimeSeriesEngine
        }
    
    def _initialize_engines(self) -> None:
        """Initialize all storage engines."""
        engine_configs = {
            'kv': {
                'wal_path': str(self.data_dir / "kv" / "wal"),
                'snap_path': str(self.data_dir / "kv" / "snapshots")
            },
            'document': {
                'wal_path': str(self.data_dir / "document" / "wal"),
                'snap_path': str(self.data_dir / "document" / "snapshots")
            },
            'column': {
                'wal_path': str(self.data_dir / "column" / "wal"),
                'snap_path': str(self.data_dir / "column" / "snapshots")
            },
            'graph': {
                'wal_path': str(self.data_dir / "graph" / "wal"),
                'snap_path': str(self.data_dir / "graph" / "snapshots")
            },
            'timeseries': {
                'wal_path': str(self.data_dir / "timeseries" / "wal"),
                'snap_path': str(self.data_dir / "timeseries" / "snapshots")
            }
        }
        
        # Create directories
        for engine_name, config in engine_configs.items():
            Path(config['wal_path']).parent.mkdir(parents=True, exist_ok=True)
            Path(config['snap_path']).parent.mkdir(parents=True, exist_ok=True)
        
        # Initialize engines
        self.engines['kv'] = KVEngine(**engine_configs['kv'])
        self.engines['document'] = DocumentEngine(**engine_configs['document'])
        self.engines['column'] = ColumnEngine(**engine_configs['column'])
        self.engines['graph'] = GraphEngine(**engine_configs['graph'])
        self.engines['timeseries'] = TimeSeriesEngine(**engine_configs['timeseries'])
    
    def startup(self) -> None:
        """Start up all engines."""
        for engine_name, engine in self.engines.items():
            try:
                engine.startup()
                print(f"✓ {engine_name} engine started successfully")
            except Exception as e:
                print(f"✗ Failed to start {engine_name} engine: {e}")
    
    def shutdown(self) -> None:
        """Shut down all engines."""
        for engine_name, engine in self.engines.items():
            try:
                engine.shutdown()
                print(f"✓ {engine_name} engine shut down successfully")
            except Exception as e:
                print(f"✗ Failed to shut down {engine_name} engine: {e}")
    
    def recommend_engine(self, data: List[Dict], use_case: str = "general") -> Dict[str, Any]:
        """
        Get engine recommendation for the given data.
        
        Args:
            data: Dataset to analyze
            use_case: Specific use case (analytics, transactional, real-time, graph_analysis)
            
        Returns:
            Dictionary with recommendation details
        """
        recommendation = self.engine_selector.select_engine(data, use_case)
        
        return {
            'recommended_engine': recommendation.engine.value,
            'confidence': recommendation.confidence,
            'reasoning': recommendation.reasoning,
            'expected_performance': recommendation.expected_performance,
            'alternative_engines': self._get_alternative_engines(data, use_case)
        }
    
    def _get_alternative_engines(self, data: List[Dict], use_case: str) -> List[str]:
        """Get alternative engine recommendations."""
        profile = self.profiler.profile_data(data)
        recommendations = profile['engine_recommendations']
        
        # Remove the primary recommendation
        if recommendations:
            recommendations = recommendations[1:]
        
        return recommendations
    
    def auto_store(self, collection: str, data: List[Dict], use_case: str = "general") -> Dict[str, Any]:
        """
        Automatically select the best engine and store data.
        
        Args:
            collection: Collection/table name
            data: Data to store
            use_case: Use case for engine selection
            
        Returns:
            Dictionary with storage details and performance metrics
        """
        # Get engine recommendation
        recommendation = self.recommend_engine(data, use_case)
        selected_engine = recommendation['recommended_engine']
        
        # Store data with performance tracking
        start_time = time.time()
        storage_result = self._store_with_engine(selected_engine, collection, data)
        end_time = time.time()
        
        # Record performance metrics
        actual_performance = {
            'write_latency_ms': (end_time - start_time) * 1000,
            'throughput_ops_per_sec': len(data) / (end_time - start_time) if (end_time - start_time) > 0 else 0
        }
        
        self.metrics.record_performance(selected_engine, actual_performance)
        
        # Record feedback for learning
        self.engine_selector.record_performance_feedback(
            selected_engine, 
            actual_performance, 
            recommendation['expected_performance']
        )
        
        return {
            'engine_used': selected_engine,
            'confidence': recommendation['confidence'],
            'items_stored': len(data),
            'actual_performance': actual_performance,
            'expected_performance': recommendation['expected_performance'],
            'reasoning': recommendation['reasoning']
        }
    
    def _store_with_engine(self, engine_name: str, collection: str, data: List[Dict]) -> Dict[str, Any]:
        """Store data using the specified engine."""
        engine = self.engines[engine_name]
        
        if engine_name == 'kv':
            # KV engine stores bytes
            for i, item in enumerate(data):
                key = str(i)
                value = str(item).encode('utf-8')
                engine.put(collection, key, value)
        
        elif engine_name == 'document':
            # Document engine stores JSON documents
            for i, item in enumerate(data):
                key = str(i)
                engine.put(collection, key, item)
        
        elif engine_name == 'column':
            # Column engine stores structured data
            for i, item in enumerate(data):
                key = str(i)
                engine.put(collection, key, item)
        
        elif engine_name == 'graph':
            # Graph engine stores nodes
            for i, item in enumerate(data):
                key = str(i)
                value = str(item).encode('utf-8')
                engine.put(collection, key, value)
        
        elif engine_name == 'timeseries':
            # Time-series engine stores temporal data
            current_time = int(time.time())
            for i, item in enumerate(data):
                timestamp = current_time + i
                value = str(item).encode('utf-8')
                engine.put(collection, timestamp, value)
        
        return {'status': 'success', 'engine': engine_name}
    
    def query(self, collection: str, engine: str, filter: Dict = None) -> List[Dict]:
        """
        Query data from the specified engine.
        
        Args:
            collection: Collection/table name
            engine: Engine to query
            filter: Query filter
            
        Returns:
            List of matching documents
        """
        if engine not in self.engines:
            raise ValueError(f"Unknown engine: {engine}")
        
        start_time = time.time()
        results = self.engines[engine].query(collection, filter or {})
        end_time = time.time()
        
        # Record query performance
        self.metrics.record_metric(
            engine, 
            'query', 
            (end_time - start_time) * 1000,
            {'collection': collection, 'filter_size': len(str(filter or {}))}
        )
        
        return results
    
    def get_performance_stats(self, engine: str = None, time_window: float = None) -> Dict[str, Any]:
        """
        Get performance statistics for engines.
        
        Args:
            engine: Specific engine to get stats for (None for all)
            time_window: Time window in seconds (None for all time)
            
        Returns:
            Performance statistics
        """
        if engine:
            return self.metrics.get_engine_stats(engine, time_window)
        else:
            return self.metrics.get_comparative_stats()
    
    def get_learning_insights(self) -> Dict[str, Any]:
        """Get insights from performance learning."""
        return self.engine_selector.get_learning_insights()
    
    def export_metrics(self, format: str = "json") -> str:
        """Export performance metrics."""
        return self.metrics.export_metrics(format)
    
    def get_engine_info(self) -> Dict[str, Any]:
        """Get information about available engines."""
        return {
            'available_engines': list(self.engines.keys()),
            'engine_descriptions': {
                'kv': 'Key-Value storage for simple data',
                'document': 'Document storage for flexible schemas',
                'column': 'Column storage for analytical queries',
                'graph': 'Graph storage for relationship data',
                'timeseries': 'Time-series storage for temporal data'
            },
            'data_dir': str(self.data_dir)
        }
    
    def health_check(self) -> Dict[str, Any]:
        """Perform health check on all engines."""
        health_status = {}
        
        for engine_name, engine in self.engines.items():
            try:
                # Simple health check - try to access engine attributes
                engine_healthy = hasattr(engine, 'store') and hasattr(engine, 'lock')
                health_status[engine_name] = {
                    'status': 'healthy' if engine_healthy else 'unhealthy',
                    'store_size': len(engine.store) if hasattr(engine, 'store') else 0
                }
            except Exception as e:
                health_status[engine_name] = {
                    'status': 'error',
                    'error': str(e)
                }
        
        return health_status 