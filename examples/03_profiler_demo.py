import time
import json
from ChimeraDB.chimera.profiler import DataProfiler, EngineSelector, PerformanceMetrics
from ChimeraDB.chimera.chimera_db import ChimeraDB


def profiler_demo():
    profiler = DataProfiler()
    selector = EngineSelector()
    metrics = PerformanceMetrics()
    
    print("1. Simple Data Profiling")
    print("   Analyzing basic user data...")
    
    simple_data = [
        {'id': 1, 'name': 'Alice', 'age': 30, 'email': 'alice@example.com'},
        {'id': 2, 'name': 'Bob', 'age': 25, 'email': 'bob@example.com'},
        {'id': 3, 'name': 'Charlie', 'age': 35, 'email': 'charlie@example.com'}
    ]
    
    profile = profiler.profile_data(simple_data)
    
    print(f"   Total items: {profile['total_items']}")
    print(f"   Average fields per item: {profile['structure_analysis']['avg_fields_per_item']:.1f}")
    print(f"   Schema consistency: {profile['structure_analysis']['schema_consistency']:.2f}")
    print(f"   Numeric fields: {profile['data_types']['numeric_fields']}")
    print(f"   String fields: {profile['data_types']['string_fields']}")
    print(f"   Recommended engines: {profile['engine_recommendations']}")
    
    print("\n2. Complex Data Profiling")
    print("   Analyzing nested and mixed data...")
    
    complex_data = [
        {
            'id': 1,
            'name': 'Alice',
            'profile': {
                'age': 30,
                'location': {'city': 'NYC', 'country': 'USA'},
                'preferences': ['dark_theme', 'notifications']
            },
            'orders': [
                {'order_id': 'A001', 'amount': 99.99, 'status': 'completed'},
                {'order_id': 'A002', 'amount': 149.99, 'status': 'pending'}
            ],
            'metadata': {
                'created_at': '2024-01-01',
                'last_login': '2024-01-15',
                'tags': ['premium', 'verified']
            }
        },
        {
            'id': 2,
            'name': 'Bob',
            'profile': {
                'age': 25,
                'location': {'city': 'SF', 'country': 'USA'},
                'preferences': ['light_theme']
            },
            'orders': [
                {'order_id': 'B001', 'amount': 79.99, 'status': 'completed'}
            ],
            'metadata': {
                'created_at': '2024-01-05',
                'last_login': '2024-01-14',
                'tags': ['new_user']
            }
        }
    ]
    
    complex_profile = profiler.profile_data(complex_data)
    
    print(f"   Nested fields: {complex_profile['structure_analysis']['nested_fields']}")
    print(f"   Array fields: {complex_profile['structure_analysis']['array_fields']}")
    print(f"   Mixed type fields: {complex_profile['data_types']['mixed_type_fields']}")
    print(f"   Recommended engines: {complex_profile['engine_recommendations']}")
    
    print("\n3. Temporal Data Profiling")
    print("   Analyzing time-series data...")
    
    temporal_data = [
        {'timestamp': 1640995200, 'sensor_id': 'temp_001', 'value': 22.5, 'location': 'room_1'},
        {'timestamp': 1640995260, 'sensor_id': 'temp_001', 'value': 22.8, 'location': 'room_1'},
        {'timestamp': 1640995320, 'sensor_id': 'temp_001', 'value': 23.1, 'location': 'room_1'},
        {'timestamp': 1640995380, 'sensor_id': 'temp_001', 'value': 22.9, 'location': 'room_1'},
        {'timestamp': 1640995440, 'sensor_id': 'temp_001', 'value': 22.7, 'location': 'room_1'}
    ]
    
    temporal_profile = profiler.profile_data(temporal_data)
    
    print(f"   Has temporal data: {temporal_profile['temporal_analysis']['has_temporal_data']}")
    print(f"   Timestamp fields: {temporal_profile['temporal_analysis']['timestamp_fields']}")
    print(f"   Recommended engines: {temporal_profile['engine_recommendations']}")
    
    print("\n4. Graph Data Profiling")
    print("   Analyzing relationship data...")
    
    graph_data = [
        {'id': 1, 'name': 'Alice', 'friends': [2, 3], 'groups': ['tech', 'sports']},
        {'id': 2, 'name': 'Bob', 'friends': [1, 3, 4], 'groups': ['tech']},
        {'id': 3, 'name': 'Charlie', 'friends': [1, 2, 4], 'groups': ['sports']},
        {'id': 4, 'name': 'Diana', 'friends': [2, 3], 'groups': ['tech', 'music']}
    ]
    
    graph_profile = profiler.profile_data(graph_data)
    
    print(f"   ID fields: {graph_profile['relationship_analysis']['id_fields']}")
    print(f"   Potential foreign keys: {graph_profile['relationship_analysis']['potential_foreign_keys']}")
    print(f"   Relationship score: {graph_profile['relationship_analysis']['relationship_score']:.2f}")
    print(f"   Recommended engines: {graph_profile['engine_recommendations']}")
    
    print("\n5. Engine Selection by Use Case")
    
    use_cases = [
        ("transactional", simple_data, "Transactional workloads"),
        ("analytics", temporal_data, "Analytical queries"),
        ("real-time", simple_data, "Real-time applications"),
        ("graph_analysis", graph_data, "Graph analysis")
    ]
    
    for use_case, data, description in use_cases:
        print(f"\n   {description}:")
        recommendation = selector.select_engine(data, use_case)
        print(f"     Selected engine: {recommendation.engine.value}")
        print(f"     Confidence: {recommendation.confidence:.2f}")
        print(f"     Expected write latency: {recommendation.expected_performance['write_latency_ms']:.2f}ms")
        print(f"     Expected read latency: {recommendation.expected_performance['read_latency_ms']:.2f}ms")
        print(f"     Reasoning: {recommendation.reasoning[0]}")
    
    print("\n6. Performance Metrics and Learning")
    
    metrics.record_metric('document', 'write', 1.5)
    metrics.record_metric('document', 'read', 0.8)
    metrics.record_metric('kv', 'write', 0.5)
    metrics.record_metric('kv', 'read', 0.3)
    metrics.record_metric('column', 'write', 2.1)
    metrics.record_metric('column', 'query', 5.2)
    
    doc_stats = metrics.get_engine_stats('document')
    kv_stats = metrics.get_engine_stats('kv')
    
    print(f"   Document engine - Write latency: {doc_stats['write_latency']['mean']:.2f}ms")
    print(f"   Document engine - Read latency: {doc_stats['read_latency']['mean']:.2f}ms")
    print(f"   KV engine - Write latency: {kv_stats['write_latency']['mean']:.2f}ms")
    print(f"   KV engine - Read latency: {kv_stats['read_latency']['mean']:.2f}ms")
    
    print("\n7. Learning from Performance Feedback")
    
    selector.record_performance_feedback(
        'document',
        {'write_latency_ms': 1.8, 'read_latency_ms': 0.9},
        {'write_latency_ms': 1.5, 'read_latency_ms': 0.8}
    )
    
    selector.record_performance_feedback(
        'kv',
        {'write_latency_ms': 0.6, 'read_latency_ms': 0.4},
        {'write_latency_ms': 0.5, 'read_latency_ms': 0.3}
    )
    
    insights = selector.get_learning_insights()
    print(f"   Total feedback records: {insights.get('total_feedback_records', 0)}")
    
    if 'engine_performance' in insights:
        for engine, perf in insights['engine_performance'].items():
            print(f"   {engine} - Prediction accuracy: {perf['prediction_accuracy']:.2f}")
    
    print("\n8. Detailed Profile Analysis")
    
    mixed_data = [
        {'id': 1, 'name': 'Product A', 'price': 99.99, 'category': 'Electronics', 'tags': ['new', 'popular']},
        {'id': 2, 'name': 'Product B', 'price': 149.99, 'category': 'Electronics', 'tags': ['premium']},
        {'id': 3, 'name': 'Product C', 'price': 29.99, 'category': 'Books', 'tags': ['bestseller']},
        {'id': 4, 'name': 'Product D', 'price': 79.99, 'category': 'Clothing', 'tags': ['trending']}
    ]
    
    detailed_profile = profiler.profile_data(mixed_data)
    
    print("   Data Characteristics:")
    print(f"     - Size: {detailed_profile['size_characteristics']['avg_item_size']:.0f} bytes avg")
    print(f"     - Structure: {detailed_profile['structure_analysis']['avg_fields_per_item']:.1f} fields avg")
    print(f"     - Cardinality: {len(detailed_profile['cardinality_analysis']['high_cardinality_fields'])} high-cardinality fields")
    print(f"     - Query complexity: {detailed_profile['query_patterns']['estimated_query_complexity']}")
    
    print("   Query Patterns:")
    print(f"     - Equality fields: {len(detailed_profile['query_patterns']['equality_query_fields'])}")
    print(f"     - Range fields: {len(detailed_profile['query_patterns']['range_query_fields'])}")
    print(f"     - Text fields: {len(detailed_profile['query_patterns']['text_query_fields'])}")
    
    print("   Engine Recommendations:")
    for i, engine in enumerate(detailed_profile['engine_recommendations'], 1):
        print(f"     {i}. {engine}")
    



if __name__ == "__main__":
    profiler_demo() 