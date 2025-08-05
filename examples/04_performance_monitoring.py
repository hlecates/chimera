import time
import json
import random
from ChimeraDB.chimera.chimera_db import ChimeraDB
from ChimeraDB.chimera.profiler import PerformanceMetrics


def performance_monitoring_example():
    db = ChimeraDB("./example_data")
    db.startup()
    metrics = PerformanceMetrics()
    
    try:
        print("1. Initial Health Check")
        health = db.health_check()
        
        for engine, status in health.items():
            print(f"   {engine}: {status['status']} (store size: {status['store_size']})")
        
        print("\n2. Simulating Performance Data")
        print("   Recording metrics for different engines...")
        
        scenarios = [
            # (engine, operation, base_latency, variance)
            ('kv', 'write', 0.5, 0.2),
            ('kv', 'read', 0.3, 0.1),
            ('document', 'write', 1.5, 0.5),
            ('document', 'read', 0.8, 0.3),
            ('document', 'query', 5.0, 2.0),
            ('column', 'write', 2.0, 0.8),
            ('column', 'read', 1.0, 0.4),
            ('column', 'query', 3.0, 1.5),
            ('graph', 'write', 1.8, 0.6),
            ('graph', 'read', 1.2, 0.4),
            ('graph', 'query', 8.0, 3.0),
            ('timeseries', 'write', 1.0, 0.3),
            ('timeseries', 'read', 0.6, 0.2),
            ('timeseries', 'query', 2.5, 1.0)
        ]
        
        for i in range(50):
            for engine, operation, base_latency, variance in scenarios:
                latency = base_latency + random.uniform(-variance, variance)
                latency = max(0.1, latency)
                
                metrics.record_metric(
                    engine, 
                    operation, 
                    latency,
                    {
                        'iteration': i,
                        'data_size': random.randint(100, 1000),
                        'concurrent_ops': random.randint(1, 10)
                    }
                )
            
            time.sleep(0.01)
        
        print("\n3. Performance Statistics")
        
        for engine in ['kv', 'document', 'column', 'graph', 'timeseries']:
            stats = metrics.get_engine_stats(engine)
            if stats:
                print(f"\n   {engine.upper()} Engine:")
                for metric, stat in stats.items():
                    print(f"     {metric}: {stat['mean']:.2f}ms avg "
                          f"({stat['min']:.2f}ms - {stat['max']:.2f}ms)")
        
        print("\n4. Comparative Analysis")
        comparison = metrics.get_comparative_stats()
        
        print("   Engine Performance Comparison:")
        for engine, stats in comparison.items():
            if 'write_latency' in stats and 'read_latency' in stats:
                write_avg = stats['write_latency']['mean']
                read_avg = stats['read_latency']['mean']
                print(f"     {engine}: Write={write_avg:.2f}ms, Read={read_avg:.2f}ms")
        
        print("\n5. Performance Trends")
        
        trends = metrics.get_performance_trends('document', 'write', hours=1)
        
        if trends:
            print(f"   Document engine write trends (last hour):")
            print(f"     Data points: {len(trends)}")
            print(f"     Average latency: {sum(t['mean'] for t in trends) / len(trends):.2f}ms")
            print(f"     Trend direction: {'increasing' if trends[-1]['mean'] > trends[0]['mean'] else 'decreasing'}")
        
        print("\n6. Anomaly Detection")
        
        metrics.record_metric('document', 'write', 15.0)
        metrics.record_metric('kv', 'read', 5.0)
        
        for engine in ['document', 'kv']:
            anomalies = metrics.get_anomalies(engine, 'write' if engine == 'document' else 'read')
            if anomalies:
                print(f"   {engine} engine anomalies detected: {len(anomalies)}")
                for anomaly in anomalies[:3]:  # Show first 3
                    print(f"     - Value: {anomaly['value']:.2f}ms (z-score: {anomaly['z_score']:.2f})")
        
        print("\n7. Performance-Based Recommendations")
        
        use_cases = ["general", "analytics", "transactional", "real-time"]
        
        for use_case in use_cases:
            recommendations = metrics.get_engine_recommendations(use_case)
            if recommendations:
                print(f"   {use_case.title()} use case:")
                sorted_recs = sorted(recommendations.items(), key=lambda x: x[1], reverse=True)
                for engine, score in sorted_recs[:3]:  # Top 3
                    print(f"     - {engine}: {score:.2f} points")
        
        print("\n8. Metrics Export")
        
        exported_metrics = metrics.export_metrics("json")
        
        metrics_data = json.loads(exported_metrics)
        total_metrics = len(metrics_data['metrics'])
        total_engines = len(metrics_data['engine_stats'])
        
        print(f"   Total metrics recorded: {total_metrics}")
        print(f"   Engines with data: {total_engines}")
        
        if metrics_data['metrics']:
            sample_metric = metrics_data['metrics'][0]
            print(f"   Sample metric: {sample_metric['engine']} {sample_metric['operation']} "
                  f"{sample_metric['value']:.2f}ms")
        
        print("\n9. Real-time Monitoring Simulation")
        
        print("   Simulating real-time performance monitoring...")
        
        for i in range(10):
            if i < 3:
                latency = 1.0 + random.uniform(-0.2, 0.2)
            elif i < 6:
                latency = 2.5 + random.uniform(-0.5, 0.5)
            else:
                latency = 5.0 + random.uniform(-1.0, 1.0)
            
            metrics.record_metric('document', 'write', latency, {'load_level': i})
            
            recent_anomalies = metrics.get_anomalies('document', 'write', threshold=2.5)
            if recent_anomalies:
                print(f"     ⚠️  Anomaly detected at iteration {i}: {latency:.2f}ms")
            
            time.sleep(0.1)
        
        print("\n10. Performance Insights")
        
        all_stats = metrics.get_comparative_stats()
        
        fastest_writes = min(all_stats.items(), key=lambda x: x[1].get('write_latency', {}).get('mean', float('inf')))
        fastest_reads = min(all_stats.items(), key=lambda x: x[1].get('read_latency', {}).get('mean', float('inf')))
        most_stable = min(all_stats.items(), key=lambda x: x[1].get('write_latency', {}).get('std', float('inf')))
        
        print(f"   Fastest writes: {fastest_writes[0]} ({fastest_writes[1]['write_latency']['mean']:.2f}ms)")
        print(f"   Fastest reads: {fastest_reads[0]} ({fastest_reads[1]['read_latency']['mean']:.2f}ms)")
        print(f"   Most stable: {most_stable[0]} (std: {most_stable[1]['write_latency']['std']:.2f}ms)")
        
        print("\n   Performance Recommendations:")
        for engine, stats in all_stats.items():
            if 'write_latency' in stats and 'read_latency' in stats:
                write_avg = stats['write_latency']['mean']
                read_avg = stats['read_latency']['mean']
                
                if write_avg > 3.0:
                    print(f"     - {engine}: Consider write optimization (avg: {write_avg:.2f}ms)")
                if read_avg > 2.0:
                    print(f"     - {engine}: Consider read optimization (avg: {read_avg:.2f}ms)")
        
    finally:
        db.shutdown()


def stress_test_example():
    db = ChimeraDB("./stress_test_data")
    db.startup()
    
    try:
        print("1. Stress Testing Different Engines")
        
        test_sizes = [100, 500, 1000]
        
        for size in test_sizes:
            print(f"\n   Testing with {size} records...")
            
            test_data = [
                {
                    'id': i,
                    'name': f'User{i}',
                    'email': f'user{i}@example.com',
                    'age': random.randint(18, 65),
                    'score': random.uniform(0, 100),
                    'tags': random.sample(['premium', 'verified', 'active', 'new'], random.randint(1, 3))
                }
                for i in range(size)
            ]
            
            engines = ['kv', 'document', 'column']
            
            for engine in engines:
                start_time = time.time()
                
                result = db.auto_store(f"stress_test_{engine}_{size}", test_data, "transactional")
                
                end_time = time.time()
                total_time = end_time - start_time
                throughput = size / total_time if total_time > 0 else 0
                
                print(f"     {engine}: {total_time:.2f}s ({throughput:.0f} ops/sec)")
        
        print("\n2. Concurrent Access Simulation")
        
        print("   Simulating concurrent read/write patterns...")
        
        for i in range(20):
            if i % 3 == 0:
                data = [{'id': i, 'value': f'data_{i}'}]
                db.auto_store("concurrent_test", data, "transactional")
            else:
                db.query("concurrent_test", "document", {})
            
            time.sleep(0.05)
        
        print("   Concurrent access simulation completed.")
        
    finally:
        db.shutdown()


if __name__ == "__main__":
    performance_monitoring_example()
    stress_test_example() 