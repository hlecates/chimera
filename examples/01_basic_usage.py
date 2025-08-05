import time
from ChimeraDB.chimera.chimera_db import ChimeraDB


def basic_usage_example():
    print("1. Initializing ChimeraDB...")
    db = ChimeraDB("./example_data")
    db.startup()
    
    try:
        print("\n2. Available Engines:")
        engine_info = db.get_engine_info()
        for engine, description in engine_info['engine_descriptions'].items():
            print(f"   - {engine}: {description}")
        
        print("\n3. Creating sample data...")
        user_data = [
            {'id': 1, 'name': 'Alice', 'email': 'alice@example.com', 'age': 30, 'city': 'New York'},
            {'id': 2, 'name': 'Bob', 'email': 'bob@example.com', 'age': 25, 'city': 'San Francisco'},
            {'id': 3, 'name': 'Charlie', 'email': 'charlie@example.com', 'age': 35, 'city': 'Chicago'},
            {'id': 4, 'name': 'Diana', 'email': 'diana@example.com', 'age': 28, 'city': 'Boston'},
            {'id': 5, 'name': 'Eve', 'email': 'eve@example.com', 'age': 32, 'city': 'Seattle'}
        ]
        
        print("\n4. Getting engine recommendation...")
        recommendation = db.recommend_engine(user_data, "transactional")
        print(f"   Recommended engine: {recommendation['recommended_engine']}")
        print(f"   Confidence: {recommendation['confidence']:.2f}")
        print(f"   Alternative engines: {recommendation['alternative_engines']}")
        print(f"   Reasoning: {recommendation['reasoning'][0]}")
        
        print("\n5. Automatically storing data...")
        result = db.auto_store("users", user_data, "transactional")
        print(f"   Engine used: {result['engine_used']}")
        print(f"   Items stored: {result['items_stored']}")
        print(f"   Actual performance: {result['actual_performance']['write_latency_ms']:.2f}ms")
        print(f"   Expected performance: {result['expected_performance']['write_latency_ms']:.2f}ms")
        
        print("\n6. Querying data...")
        if result['engine_used'] != 'kv':
            results = db.query("users", result['engine_used'], {'age': {'$gt': 30}})
            print(f"   Found {len(results)} users over 30 years old:")
            for user in results:
                print(f"     - {user['name']} ({user['age']} years old)")
        else:
            print("   KV engine doesn't support complex queries - using direct get operations")
            for i in range(1, 4):
                user_data = db.engines['kv'].get("users", str(i))
                if user_data:
                    print(f"     - Retrieved user {i} from KV store")
        
        print("\n7. Health check...")
        health = db.health_check()
        for engine, status in health.items():
            print(f"   {engine}: {status['status']} (store size: {status['store_size']})")
        
        print("\n8. Performance statistics...")
        stats = db.get_performance_stats()
        for engine, engine_stats in stats.items():
            if engine_stats:
                print(f"   {engine}: {len(engine_stats)} metrics recorded")
        
    finally:
        db.shutdown()


if __name__ == "__main__":
    basic_usage_example() 