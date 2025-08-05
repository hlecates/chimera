import time
import statistics
from typing import Any, Dict, List, Optional
from collections import defaultdict, deque
from dataclasses import dataclass


@dataclass
class PerformanceMetric:
    value: float
    timestamp: float
    operation: str
    engine: str
    metadata: Dict[str, Any]


class PerformanceMetrics:
    
    def __init__(self, max_history: int = 10000):
        self.max_history = max_history
        self.metrics: List[PerformanceMetric] = []
        self.engine_stats = defaultdict(lambda: {
            'write_latency': deque(maxlen=1000),
            'read_latency': deque(maxlen=1000),
            'query_latency': deque(maxlen=1000),
            'throughput': deque(maxlen=1000),
            'error_rate': deque(maxlen=1000),
            'memory_usage': deque(maxlen=1000)
        })
        
    def record_metric(
        self, 
        engine: str, 
        operation: str, 
        value: float, 
        metadata: Dict[str, Any] = None
    ) -> None:

        metric = PerformanceMetric(
            value=value,
            timestamp=time.time(),
            operation=operation,
            engine=engine,
            metadata=metadata or {}
        )
        
        self.metrics.append(metric)
        
        # Keep history within limits
        if len(self.metrics) > self.max_history:
            self.metrics.pop(0)
        
        # Update engine-specific stats
        if operation in ['write', 'read', 'query']:
            self.engine_stats[engine][f'{operation}_latency'].append(value)
        elif operation == 'throughput':
            self.engine_stats[engine]['throughput'].append(value)
        elif operation == 'error':
            self.engine_stats[engine]['error_rate'].append(value)
        elif operation == 'memory':
            self.engine_stats[engine]['memory_usage'].append(value)
    
    def record_performance(self, engine: str, performance: Dict[str, float]) -> None:
        for metric_name, value in performance.items():
            if metric_name.endswith('_latency_ms'):
                operation = metric_name.replace('_latency_ms', '')
                self.record_metric(engine, operation, value)
            elif metric_name == 'throughput_ops_per_sec':
                self.record_metric(engine, 'throughput', value)
            elif metric_name == 'error_rate':
                self.record_metric(engine, 'error', value)
            elif metric_name == 'memory_usage_mb':
                self.record_metric(engine, 'memory', value)
    
    def get_engine_stats(self, engine: str, time_window: Optional[float] = None) -> Dict[str, Any]:
        if engine not in self.engine_stats:
            return {}
        
        stats = {}
        current_time = time.time()
        
        for metric_name, values in self.engine_stats[engine].items():
            if not values:
                continue
            
            # Filter by time window if specified
            if time_window:
                filtered_values = [
                    v for v in values 
                    if current_time - v < time_window
                ]
            else:
                filtered_values = list(values)
            
            if filtered_values:
                stats[metric_name] = {
                    'count': len(filtered_values),
                    'mean': statistics.mean(filtered_values),
                    'median': statistics.median(filtered_values),
                    'min': min(filtered_values),
                    'max': max(filtered_values),
                    'std': statistics.stdev(filtered_values) if len(filtered_values) > 1 else 0
                }
        
        return stats
    
    def get_comparative_stats(self, engines: List[str] = None) -> Dict[str, Any]:
        if engines is None:
            engines = list(self.engine_stats.keys())
        
        comparison = {}
        
        for engine in engines:
            stats = self.get_engine_stats(engine)
            if stats:
                comparison[engine] = stats
        
        return comparison
    
    def get_performance_trends(self, engine: str, metric: str, hours: int = 24) -> List[Dict[str, Any]]:
        if engine not in self.engine_stats or metric not in self.engine_stats[engine]:
            return []
        
        current_time = time.time()
        cutoff_time = current_time - (hours * 3600)
        
        # Get metrics within time window
        recent_metrics = [
            m for m in self.metrics 
            if m.engine == engine and m.operation == metric and m.timestamp >= cutoff_time
        ]
        
        # Group by hour for trend analysis
        hourly_stats = defaultdict(list)
        for metric_obj in recent_metrics:
            hour = int(metric_obj.timestamp // 3600) * 3600
            hourly_stats[hour].append(metric_obj.value)
        
        trends = []
        for hour, values in sorted(hourly_stats.items()):
            trends.append({
                'timestamp': hour,
                'count': len(values),
                'mean': statistics.mean(values),
                'median': statistics.median(values),
                'min': min(values),
                'max': max(values)
            })
        
        return trends
    
    def get_anomalies(self, engine: str, metric: str, threshold: float = 2.0) -> List[Dict[str, Any]]:
        if engine not in self.engine_stats or metric not in self.engine_stats[engine]:
            return []
        
        values = list(self.engine_stats[engine][metric])
        if len(values) < 10:
            return []
        
        mean = statistics.mean(values)
        std = statistics.stdev(values)
        
        anomalies = []
        for i, value in enumerate(values):
            z_score = abs(value - mean) / std if std > 0 else 0
            if z_score > threshold:
                anomalies.append({
                    'index': i,
                    'value': value,
                    'z_score': z_score,
                    'timestamp': time.time() - (len(values) - i) * 0.1  # Approximate timestamp
                })
        
        return anomalies
    
    def get_engine_recommendations(self, use_case: str = "general") -> Dict[str, float]:
        recommendations = {}
        
        for engine in self.engine_stats:
            stats = self.get_engine_stats(engine)
            if not stats:
                continue
            
            score = 0.0
            
            # Latency scoring (lower is better)
            if 'read_latency' in stats:
                avg_read_latency = stats['read_latency']['mean']
                if avg_read_latency < 1.0:
                    score += 3
                elif avg_read_latency < 5.0:
                    score += 2
                elif avg_read_latency < 10.0:
                    score += 1
            
            if 'write_latency' in stats:
                avg_write_latency = stats['write_latency']['mean']
                if avg_write_latency < 1.0:
                    score += 3
                elif avg_write_latency < 5.0:
                    score += 2
                elif avg_write_latency < 10.0:
                    score += 1
            
            # Throughput scoring (higher is better)
            if 'throughput' in stats:
                avg_throughput = stats['throughput']['mean']
                if avg_throughput > 10000:
                    score += 3
                elif avg_throughput > 1000:
                    score += 2
                elif avg_throughput > 100:
                    score += 1
            
            # Error rate scoring (lower is better)
            if 'error_rate' in stats:
                avg_error_rate = stats['error_rate']['mean']
                if avg_error_rate < 0.001:
                    score += 3
                elif avg_error_rate < 0.01:
                    score += 2
                elif avg_error_rate < 0.1:
                    score += 1
            
            # Use case specific scoring
            if use_case == "analytics":
                if 'query_latency' in stats:
                    avg_query_latency = stats['query_latency']['mean']
                    if avg_query_latency < 10.0:
                        score += 2
                    elif avg_query_latency < 50.0:
                        score += 1
            elif use_case == "transactional":
                if 'write_latency' in stats and 'read_latency' in stats:
                    total_latency = stats['write_latency']['mean'] + stats['read_latency']['mean']
                    if total_latency < 2.0:
                        score += 2
                    elif total_latency < 5.0:
                        score += 1
            elif use_case == "real-time":
                if 'read_latency' in stats:
                    if stats['read_latency']['mean'] < 0.5:
                        score += 3
                    elif stats['read_latency']['mean'] < 1.0:
                        score += 2
                    elif stats['read_latency']['mean'] < 2.0:
                        score += 1
            
            recommendations[engine] = score
        
        return recommendations
    
    def export_metrics(self, format: str = "json") -> str:
        if format == "json":
            import json
            return json.dumps({
                'metrics': [
                    {
                        'value': m.value,
                        'timestamp': m.timestamp,
                        'operation': m.operation,
                        'engine': m.engine,
                        'metadata': m.metadata
                    }
                    for m in self.metrics
                ],
                'engine_stats': dict(self.engine_stats)
            }, indent=2)
        else:
            raise ValueError(f"Unsupported format: {format}")
    
    def clear_metrics(self, engine: str = None) -> None:
        if engine:
            if engine in self.engine_stats:
                for metric_list in self.engine_stats[engine].values():
                    metric_list.clear()
        else:
            self.metrics.clear()
            self.engine_stats.clear() 