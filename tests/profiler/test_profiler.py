"""
Test the profiler and engine selection functionality.
"""

import pytest
import time
from ChimeraDB.chimera.profiler import DataProfiler, EngineSelector, PerformanceMetrics
from ChimeraDB.chimera.chimera_db import ChimeraDB


class TestDataProfiler:
    """Test the data profiler functionality."""
    
    def test_profile_simple_data(self):
        """Test profiling simple data."""
        profiler = DataProfiler()
        
        data = [
            {'id': 1, 'name': 'Alice', 'age': 30},
            {'id': 2, 'name': 'Bob', 'age': 25},
            {'id': 3, 'name': 'Charlie', 'age': 35}
        ]
        
        profile = profiler.profile_data(data)
        
        assert profile['total_items'] == 3
        # Simple data with numeric fields tends to be recommended for column engine
        assert any(engine in profile['engine_recommendations'] for engine in ['column', 'document', 'kv'])
        assert profile['structure_analysis']['avg_fields_per_item'] == 3
        assert 'id' in profile['data_types']['numeric_fields']
        assert 'name' in profile['data_types']['string_fields']
    
    def test_profile_nested_data(self):
        """Test profiling nested data."""
        profiler = DataProfiler()
        
        data = [
            {
                'id': 1,
                'name': 'Alice',
                'address': {
                    'street': '123 Main St',
                    'city': 'New York'
                },
                'hobbies': ['reading', 'swimming']
            }
        ]
        
        profile = profiler.profile_data(data)
        
        assert 'document' in profile['engine_recommendations']
        assert 'address' in profile['structure_analysis']['nested_fields']
        assert 'hobbies' in profile['structure_analysis']['array_fields']
    
    def test_profile_temporal_data(self):
        """Test profiling temporal data."""
        profiler = DataProfiler()
        
        data = [
            {'timestamp': 1640995200, 'value': 100, 'metric': 'cpu_usage'},
            {'timestamp': 1640995260, 'value': 95, 'metric': 'cpu_usage'},
            {'timestamp': 1640995320, 'value': 110, 'metric': 'cpu_usage'}
        ]
        
        profile = profiler.profile_data(data)
        
        # Temporal data should be recommended for timeseries engine
        assert any(engine in profile['engine_recommendations'] for engine in ['timeseries', 'column'])
        assert profile['temporal_analysis']['has_temporal_data']
        assert 'timestamp' in profile['temporal_analysis']['timestamp_fields']
    
    def test_profile_graph_data(self):
        """Test profiling graph-like data."""
        profiler = DataProfiler()
        
        data = [
            {'id': 1, 'name': 'Alice', 'friends': ['Bob', 'Charlie']},
            {'id': 2, 'name': 'Bob', 'friends': ['Alice', 'David']},
            {'id': 3, 'name': 'Charlie', 'friends': ['Alice', 'David']}
        ]
        
        profile = profiler.profile_data(data)
        
        assert 'id' in profile['relationship_analysis']['id_fields']
        assert profile['relationship_analysis']['relationship_score'] > 0


class TestEngineSelector:
    """Test the engine selector functionality."""
    
    def test_select_engine_analytics(self):
        """Test engine selection for analytics use case."""
        selector = EngineSelector()
        
        data = [
            {'timestamp': 1640995200, 'value': 100, 'category': 'A'},
            {'timestamp': 1640995260, 'value': 95, 'category': 'B'},
            {'timestamp': 1640995320, 'value': 110, 'category': 'A'}
        ]
        
        recommendation = selector.select_engine(data, "analytics")
        
        assert recommendation.engine.value in ['column', 'timeseries']
        assert recommendation.confidence > 0
        assert len(recommendation.reasoning) > 0
    
    def test_select_engine_transactional(self):
        """Test engine selection for transactional use case."""
        selector = EngineSelector()
        
        data = [
            {'id': 1, 'name': 'Alice', 'email': 'alice@example.com'},
            {'id': 2, 'name': 'Bob', 'email': 'bob@example.com'}
        ]
        
        recommendation = selector.select_engine(data, "transactional")
        
        assert recommendation.engine.value in ['document', 'kv']
        assert recommendation.confidence > 0
    
    def test_select_engine_graph_analysis(self):
        """Test engine selection for graph analysis use case."""
        selector = EngineSelector()
        
        data = [
            {'id': 1, 'name': 'Alice', 'friends': ['Bob', 'Charlie']},
            {'id': 2, 'name': 'Bob', 'friends': ['Alice', 'David']}
        ]
        
        recommendation = selector.select_engine(data, "graph_analysis")
        
        assert recommendation.engine.value == 'graph'
        assert recommendation.confidence > 0


class TestPerformanceMetrics:
    """Test the performance metrics functionality."""
    
    def test_record_metrics(self):
        """Test recording performance metrics."""
        metrics = PerformanceMetrics()
        
        metrics.record_metric('document', 'write', 1.5)
        metrics.record_metric('document', 'read', 0.8)
        metrics.record_metric('kv', 'write', 0.5)
        
        stats = metrics.get_engine_stats('document')
        
        assert 'write_latency' in stats
        assert 'read_latency' in stats
        assert stats['write_latency']['mean'] == 1.5
        assert stats['read_latency']['mean'] == 0.8
    
    def test_comparative_stats(self):
        """Test comparative statistics across engines."""
        metrics = PerformanceMetrics()
        
        metrics.record_metric('document', 'write', 1.5)
        metrics.record_metric('kv', 'write', 0.5)
        
        comparison = metrics.get_comparative_stats()
        
        assert 'document' in comparison
        assert 'kv' in comparison
        assert comparison['document']['write_latency']['mean'] > comparison['kv']['write_latency']['mean']


class TestChimeraDB:
    """Test the unified ChimeraDB API."""
    
    def test_chimera_db_initialization(self):
        """Test ChimeraDB initialization."""
        db = ChimeraDB("./test_data")
        
        assert len(db.engines) == 5
        assert 'kv' in db.engines
        assert 'document' in db.engines
        assert 'column' in db.engines
        assert 'graph' in db.engines
        assert 'timeseries' in db.engines
    
    def test_engine_recommendation(self):
        """Test engine recommendation functionality."""
        db = ChimeraDB("./test_data")
        
        data = [
            {'id': 1, 'name': 'Alice', 'age': 30},
            {'id': 2, 'name': 'Bob', 'age': 25}
        ]
        
        recommendation = db.recommend_engine(data, "transactional")
        
        assert 'recommended_engine' in recommendation
        assert 'confidence' in recommendation
        assert 'reasoning' in recommendation
        assert 'expected_performance' in recommendation
        assert 'alternative_engines' in recommendation
    
    def test_auto_store(self):
        """Test automatic engine selection and storage."""
        db = ChimeraDB("./test_data")
        db.startup()
        
        try:
            data = [
                {'id': 1, 'name': 'Alice', 'age': 30},
                {'id': 2, 'name': 'Bob', 'age': 25}
            ]
            
            result = db.auto_store("users", data, "transactional")
            
            assert 'engine_used' in result
            assert 'confidence' in result
            assert 'items_stored' in result
            assert 'actual_performance' in result
            assert result['items_stored'] == 2
            
        finally:
            db.shutdown()
    
    def test_health_check(self):
        """Test health check functionality."""
        db = ChimeraDB("./test_data")
        db.startup()
        
        try:
            health = db.health_check()
            
            assert 'kv' in health
            assert 'document' in health
            assert 'column' in health
            assert 'graph' in health
            assert 'timeseries' in health
            
            for engine_status in health.values():
                assert 'status' in engine_status
                
        finally:
            db.shutdown()
    
    def test_performance_stats(self):
        """Test performance statistics."""
        db = ChimeraDB("./test_data")
        db.startup()
        
        try:
            # Store some data to generate metrics
            data = [{'id': i, 'name': f'User{i}'} for i in range(10)]
            db.auto_store("test_collection", data)
            
            stats = db.get_performance_stats()
            
            # Should have some performance data
            assert isinstance(stats, dict)
            
        finally:
            db.shutdown()


if __name__ == "__main__":
    # Run a simple demonstration
    print("Testing ChimeraDB Profiler System...")
    
    # Test data profiler
    profiler = DataProfiler()
    
    # Test different data types
    simple_data = [{'id': i, 'name': f'User{i}'} for i in range(5)]
    temporal_data = [{'timestamp': int(time.time()) + i, 'value': i * 10} for i in range(5)]
    nested_data = [{'id': i, 'profile': {'name': f'User{i}', 'age': 20 + i}} for i in range(5)]
    
    print("\n1. Profiling simple data:")
    profile1 = profiler.profile_data(simple_data)
    print(f"   Recommended engines: {profile1['engine_recommendations']}")
    
    print("\n2. Profiling temporal data:")
    profile2 = profiler.profile_data(temporal_data)
    print(f"   Recommended engines: {profile2['engine_recommendations']}")
    
    print("\n3. Profiling nested data:")
    profile3 = profiler.profile_data(nested_data)
    print(f"   Recommended engines: {profile3['engine_recommendations']}")
    
    # Test engine selector
    selector = EngineSelector()
    
    print("\n4. Engine selection for analytics:")
    recommendation = selector.select_engine(temporal_data, "analytics")
    print(f"   Selected engine: {recommendation.engine.value}")
    print(f"   Confidence: {recommendation.confidence:.2f}")
    print(f"   Reasoning: {recommendation.reasoning[0]}")
    
    print("\n5. Engine selection for transactional:")
    recommendation = selector.select_engine(simple_data, "transactional")
    print(f"   Selected engine: {recommendation.engine.value}")
    print(f"   Confidence: {recommendation.confidence:.2f}")
    
    print("\n✓ All tests completed successfully!") 