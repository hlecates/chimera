import time
from ChimeraDB.chimera.chimera_db import ChimeraDB


def engine_comparison_example():
    db = ChimeraDB("./example_data")
    db.startup()
    
    try:
        print("1. Key-Value Engine - Simple Data Storage")
        print("   Use case: Caching, session storage, simple lookups")
        
        kv_data = [
            {'session_id': 'sess_001', 'user_id': 'user_123', 'last_activity': '2024-01-01T10:00:00'},
            {'session_id': 'sess_002', 'user_id': 'user_456', 'last_activity': '2024-01-01T11:00:00'},
            {'session_id': 'sess_003', 'user_id': 'user_789', 'last_activity': '2024-01-01T12:00:00'}
        ]
        
        kv_recommendation = db.recommend_engine(kv_data, "real-time")
        print(f"   Recommendation: {kv_recommendation['recommended_engine']}")
        print(f"   Confidence: {kv_recommendation['confidence']:.2f}")
        
        kv_result = db.auto_store("sessions", kv_data, "real-time")
        print(f"   Performance: {kv_result['actual_performance']['write_latency_ms']:.2f}ms\n")
        
        print("2. Document Engine - Complex, Nested Data")
        print("   Use case: User profiles, product catalogs, flexible schemas")
        
        document_data = [
            {
                'id': 1,
                'name': 'Alice Johnson',
                'email': 'alice@example.com',
                'profile': {
                    'age': 30,
                    'location': {
                        'city': 'New York',
                        'country': 'USA',
                        'coordinates': {'lat': 40.7128, 'lng': -74.0060}
                    },
                    'preferences': {
                        'theme': 'dark',
                        'language': 'en',
                        'notifications': ['email', 'push']
                    }
                },
                'orders': [
                    {'order_id': 'ord_001', 'amount': 99.99, 'status': 'completed'},
                    {'order_id': 'ord_002', 'amount': 149.99, 'status': 'pending'}
                ]
            },
            {
                'id': 2,
                'name': 'Bob Smith',
                'email': 'bob@example.com',
                'profile': {
                    'age': 25,
                    'location': {
                        'city': 'San Francisco',
                        'country': 'USA',
                        'coordinates': {'lat': 37.7749, 'lng': -122.4194}
                    },
                    'preferences': {
                        'theme': 'light',
                        'language': 'en',
                        'notifications': ['email']
                    }
                },
                'orders': [
                    {'order_id': 'ord_003', 'amount': 79.99, 'status': 'completed'}
                ]
            }
        ]
        
        doc_recommendation = db.recommend_engine(document_data, "transactional")
        print(f"   Recommendation: {doc_recommendation['recommended_engine']}")
        print(f"   Confidence: {doc_recommendation['confidence']:.2f}")
        
        doc_result = db.auto_store("users", document_data, "transactional")
        print(f"   Performance: {doc_result['actual_performance']['write_latency_ms']:.2f}ms\n")
        
        print("3. Column Engine - Analytical Data")
        print("   Use case: Business intelligence, data warehousing, aggregations")
        
        column_data = [
            {'product_id': 1, 'category': 'Electronics', 'price': 299.99, 'sales': 150, 'region': 'North'},
            {'product_id': 2, 'category': 'Electronics', 'price': 199.99, 'sales': 200, 'region': 'South'},
            {'product_id': 3, 'category': 'Clothing', 'price': 49.99, 'sales': 300, 'region': 'North'},
            {'product_id': 4, 'category': 'Clothing', 'price': 79.99, 'sales': 100, 'region': 'South'},
            {'product_id': 5, 'category': 'Books', 'price': 19.99, 'sales': 500, 'region': 'North'},
            {'product_id': 6, 'category': 'Books', 'price': 29.99, 'sales': 250, 'region': 'South'},
            {'product_id': 7, 'category': 'Electronics', 'price': 399.99, 'sales': 75, 'region': 'North'},
            {'product_id': 8, 'category': 'Clothing', 'price': 89.99, 'sales': 180, 'region': 'South'}
        ]
        
        col_recommendation = db.recommend_engine(column_data, "analytics")
        print(f"   Recommendation: {col_recommendation['recommended_engine']}")
        print(f"   Confidence: {col_recommendation['confidence']:.2f}")
        
        col_result = db.auto_store("products", column_data, "analytics")
        print(f"   Performance: {col_result['actual_performance']['write_latency_ms']:.2f}ms\n")
        
        print("4. Graph Engine - Relationship Data")
        print("   Use case: Social networks, recommendation systems, network analysis")
        
        graph_data = [
            {'id': 1, 'name': 'Alice', 'type': 'user', 'connections': [2, 3, 4]},
            {'id': 2, 'name': 'Bob', 'type': 'user', 'connections': [1, 3, 5]},
            {'id': 3, 'name': 'Charlie', 'type': 'user', 'connections': [1, 2, 4, 5]},
            {'id': 4, 'name': 'Diana', 'type': 'user', 'connections': [1, 3, 6]},
            {'id': 5, 'name': 'Eve', 'type': 'user', 'connections': [2, 3, 6]},
            {'id': 6, 'name': 'Frank', 'type': 'user', 'connections': [4, 5]}
        ]
        
        graph_recommendation = db.recommend_engine(graph_data, "graph_analysis")
        print(f"   Recommendation: {graph_recommendation['recommended_engine']}")
        print(f"   Confidence: {graph_recommendation['confidence']:.2f}")
        
        graph_result = db.auto_store("social_network", graph_data, "graph_analysis")
        print(f"   Performance: {graph_result['actual_performance']['write_latency_ms']:.2f}ms\n")
        
        print("5. Time-Series Engine - Temporal Data")
        print("   Use case: IoT sensors, monitoring, time-based analytics")
        
        base_time = int(time.time())
        timeseries_data = []
        
        for i in range(24):
            timestamp = base_time + (i * 3600)
            timeseries_data.append({
                'timestamp': timestamp,
                'sensor_id': 'temp_sensor_001',
                'temperature': 20 + (i % 10),
                'humidity': 50 + (i % 20),
                'location': 'server_room_1'
            })
        
        ts_recommendation = db.recommend_engine(timeseries_data, "analytics")
        print(f"   Recommendation: {ts_recommendation['recommended_engine']}")
        print(f"   Confidence: {ts_recommendation['confidence']:.2f}")
        
        ts_result = db.auto_store("sensor_data", timeseries_data, "analytics")
        print(f"   Performance: {ts_result['actual_performance']['write_latency_ms']:.2f}ms\n")
        
        print("6. Performance Comparison Summary")
        print("   Engine recommendations and performance metrics:")
        
        results = [
            ("Key-Value", kv_result),
            ("Document", doc_result),
            ("Column", col_result),
            ("Graph", graph_result),
            ("Time-Series", ts_result)
        ]
        
        for engine_name, result in results:
            print(f"   {engine_name}: {result['actual_performance']['write_latency_ms']:.2f}ms "
                  f"(confidence: {result['confidence']:.2f})")
        
    finally:
        db.shutdown()


if __name__ == "__main__":
    engine_comparison_example() 