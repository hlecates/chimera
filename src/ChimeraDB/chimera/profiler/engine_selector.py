import time
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum

from .data_profiler import DataProfiler
from .metrics import PerformanceMetrics


class EngineType(Enum):
    KV = "kv"
    DOCUMENT = "document"
    COLUMN = "column"
    GRAPH = "graph"
    TIMESERIES = "timeseries"


@dataclass
class EngineRecommendation:
    engine: EngineType
    confidence: float
    reasoning: List[str]
    expected_performance: Dict[str, float]


class EngineSelector:

    def __init__(self):
        self.profiler = DataProfiler()
        self.metrics = PerformanceMetrics()
        self.learning_history = []
        
    def select_engine(self, data: List[Dict], use_case: str = "general") -> EngineRecommendation:
        # Profile the data
        profile = self.profiler.profile_data(data)
        
        # Get initial recommendations
        recommended_engines = profile['engine_recommendations']
        
        # Apply use case specific adjustments
        adjusted_recommendations = self._apply_use_case_adjustments(
            recommended_engines, use_case, profile
        )
        
        # Select the best engine
        best_engine = adjusted_recommendations[0] if adjusted_recommendations else EngineType.DOCUMENT
        
        # Generate reasoning
        reasoning = self._generate_reasoning(profile, best_engine, use_case)
        
        # Estimate performance
        expected_performance = self._estimate_performance(profile, best_engine, use_case)
        
        # Calculate confidence
        confidence = self._calculate_confidence(profile, best_engine, use_case)
        
        return EngineRecommendation(
            engine=EngineType(best_engine),
            confidence=confidence,
            reasoning=reasoning,
            expected_performance=expected_performance
        )
    
    def _apply_use_case_adjustments(
        self, 
        recommendations: List[str], 
        use_case: str, 
        profile: Dict[str, Any]
    ) -> List[str]:

        adjusted = recommendations.copy()
        
        if use_case == "analytics":
            # Prefer column and timeseries for analytics
            if "column" in adjusted:
                adjusted.insert(0, adjusted.pop(adjusted.index("column")))
            if "timeseries" in adjusted:
                adjusted.insert(0, adjusted.pop(adjusted.index("timeseries")))
                
        elif use_case == "transactional":
            # Prefer document and kv for transactional workloads
            if "document" in adjusted:
                adjusted.insert(0, adjusted.pop(adjusted.index("document")))
            if "kv" in adjusted:
                adjusted.insert(0, adjusted.pop(adjusted.index("kv")))
                
        elif use_case == "real-time":
            # Prefer kv and timeseries for real-time
            if "kv" in adjusted:
                adjusted.insert(0, adjusted.pop(adjusted.index("kv")))
            if "timeseries" in adjusted:
                adjusted.insert(0, adjusted.pop(adjusted.index("timeseries")))
                
        elif use_case == "graph_analysis":
            # Prefer graph engine for graph analysis
            if "graph" in adjusted:
                adjusted.insert(0, adjusted.pop(adjusted.index("graph")))
            else:
                adjusted.insert(0, "graph")
        
        return adjusted
    
    def _generate_reasoning(
        self, 
        profile: Dict[str, Any], 
        selected_engine: str, 
        use_case: str
    ) -> List[str]:

        reasoning = []
        
        # Data size reasoning
        total_items = profile['total_items']
        if total_items < 1000:
            reasoning.append(f"Small dataset ({total_items} items) - suitable for all engines")
        elif total_items < 100000:
            reasoning.append(f"Medium dataset ({total_items} items) - consider performance implications")
        else:
            reasoning.append(f"Large dataset ({total_items} items) - prioritize efficient engines")
        
        # Structure reasoning
        structure = profile['structure_analysis']
        if structure.get('nested_fields'):
            reasoning.append("Contains nested fields - document engine handles this well")
        if structure.get('array_fields'):
            reasoning.append("Contains array fields - document engine supports this")
        
        # Data type reasoning
        data_types = profile['data_types']
        if data_types.get('numeric_fields'):
            reasoning.append("Contains numeric fields - good for column and timeseries engines")
        if data_types.get('mixed_type_fields'):
            reasoning.append("Contains mixed data types - document engine handles this well")
        
        # Query pattern reasoning
        query_patterns = profile['query_patterns']
        if query_patterns.get('range_query_fields'):
            reasoning.append("Contains range query fields - column engine optimizes for this")
        if query_patterns.get('estimated_query_complexity') == 'complex':
            reasoning.append("Complex query patterns - document engine provides flexibility")
        
        # Relationship reasoning
        relationships = profile['relationship_analysis']
        if relationships.get('id_fields') or relationships.get('potential_foreign_keys'):
            reasoning.append("Contains relationship patterns - graph engine can model this")
        
        # Temporal reasoning
        temporal = profile['temporal_analysis']
        if temporal.get('has_temporal_data'):
            reasoning.append("Contains temporal data - timeseries engine specializes in this")
        
        # Use case specific reasoning
        if use_case == "analytics":
            reasoning.append("Analytics use case - column and timeseries engines provide aggregation capabilities")
        elif use_case == "transactional":
            reasoning.append("Transactional use case - document and kv engines provide fast CRUD operations")
        elif use_case == "real-time":
            reasoning.append("Real-time use case - kv and timeseries engines provide low-latency access")
        elif use_case == "graph_analysis":
            reasoning.append("Graph analysis use case - graph engine provides specialized graph operations")
        
        # Engine specific reasoning
        if selected_engine == "kv":
            reasoning.append("Selected KV engine for simple key-value storage")
        elif selected_engine == "document":
            reasoning.append("Selected document engine for flexible schema and complex queries")
        elif selected_engine == "column":
            reasoning.append("Selected column engine for analytical queries and aggregations")
        elif selected_engine == "graph":
            reasoning.append("Selected graph engine for relationship analysis and path queries")
        elif selected_engine == "timeseries":
            reasoning.append("Selected timeseries engine for temporal data and time-based queries")
        
        return reasoning
    
    def _estimate_performance(
        self, 
        profile: Dict[str, Any], 
        engine: str, 
        use_case: str
    ) -> Dict[str, float]:

        estimates = {
            'write_latency_ms': 1.0,
            'read_latency_ms': 1.0,
            'query_latency_ms': 5.0,
            'storage_efficiency': 0.8,
            'memory_usage_mb': 100.0
        }
        
        # Adjust based on data characteristics
        total_items = profile['total_items']
        avg_item_size = profile['size_characteristics']['avg_item_size']
        
        # Size-based adjustments
        if total_items > 100000:
            estimates['write_latency_ms'] *= 2
            estimates['read_latency_ms'] *= 1.5
            estimates['query_latency_ms'] *= 3
            estimates['memory_usage_mb'] *= 2
        
        if avg_item_size > 1024:
            estimates['write_latency_ms'] *= 1.5
            estimates['read_latency_ms'] *= 1.2
            estimates['storage_efficiency'] *= 0.9
        
        # Engine-specific adjustments
        if engine == "kv":
            estimates['write_latency_ms'] *= 0.8
            estimates['read_latency_ms'] *= 0.7
            estimates['query_latency_ms'] = float('inf')  # No complex queries
        elif engine == "document":
            estimates['write_latency_ms'] *= 1.0
            estimates['read_latency_ms'] *= 1.0
            estimates['query_latency_ms'] *= 1.5
        elif engine == "column":
            estimates['write_latency_ms'] *= 1.5
            estimates['read_latency_ms'] *= 0.8
            estimates['query_latency_ms'] *= 0.7
            estimates['storage_efficiency'] *= 1.2
        elif engine == "graph":
            estimates['write_latency_ms'] *= 1.3
            estimates['read_latency_ms'] *= 1.2
            estimates['query_latency_ms'] *= 2.0
            estimates['memory_usage_mb'] *= 1.5
        elif engine == "timeseries":
            estimates['write_latency_ms'] *= 0.9
            estimates['read_latency_ms'] *= 0.8
            estimates['query_latency_ms'] *= 0.6
            estimates['storage_efficiency'] *= 1.1
        
        return estimates
    
    def _calculate_confidence(
        self, 
        profile: Dict[str, Any], 
        engine: str, 
        use_case: str
    ) -> float:

        confidence = 0.5  # Base confidence
        
        # Data size confidence
        total_items = profile['total_items']
        if 100 <= total_items <= 10000:
            confidence += 0.1  # Optimal size range
        elif total_items > 100000:
            confidence -= 0.1  # May need optimization
        
        # Schema consistency
        schema_consistency = profile['structure_analysis'].get('schema_consistency', 0)
        if schema_consistency > 0.8:
            confidence += 0.1
        elif schema_consistency < 0.3:
            confidence -= 0.1
        
        # Engine-specific confidence
        if engine == "kv" and not profile['structure_analysis'].get('nested_fields'):
            confidence += 0.2
        elif engine == "document" and profile['structure_analysis'].get('nested_fields'):
            confidence += 0.2
        elif engine == "column" and profile['data_types'].get('numeric_fields'):
            confidence += 0.2
        elif engine == "graph" and profile['relationship_analysis'].get('id_fields'):
            confidence += 0.2
        elif engine == "timeseries" and profile['temporal_analysis'].get('has_temporal_data'):
            confidence += 0.2
        
        # Use case alignment
        if use_case == "analytics" and engine in ["column", "timeseries"]:
            confidence += 0.1
        elif use_case == "transactional" and engine in ["document", "kv"]:
            confidence += 0.1
        elif use_case == "real-time" and engine in ["kv", "timeseries"]:
            confidence += 0.1
        elif use_case == "graph_analysis" and engine == "graph":
            confidence += 0.2
        
        return max(0.0, min(1.0, confidence))
    
    def record_performance_feedback(
        self, 
        engine: str, 
        actual_performance: Dict[str, float], 
        expected_performance: Dict[str, float]
    ) -> None:

        feedback = {
            'engine': engine,
            'actual': actual_performance,
            'expected': expected_performance,
            'timestamp': time.time()
        }
        self.learning_history.append(feedback)
        
        # Update metrics
        self.metrics.record_performance(engine, actual_performance)
    
    def get_learning_insights(self) -> Dict[str, Any]:

        if not self.learning_history:
            return {}
        
        insights = {
            'total_feedback_records': len(self.learning_history),
            'engine_performance': {},
            'accuracy_improvements': {}
        }
        
        # Analyze performance by engine
        engine_feedback = {}
        for feedback in self.learning_history:
            engine = feedback['engine']
            if engine not in engine_feedback:
                engine_feedback[engine] = []
            engine_feedback[engine].append(feedback)
        
        for engine, feedbacks in engine_feedback.items():
            avg_actual_latency = sum(f['actual'].get('read_latency_ms', 0) for f in feedbacks) / len(feedbacks)
            avg_expected_latency = sum(f['expected'].get('read_latency_ms', 0) for f in feedbacks) / len(feedbacks)
            
            insights['engine_performance'][engine] = {
                'avg_actual_latency_ms': avg_actual_latency,
                'avg_expected_latency_ms': avg_expected_latency,
                'prediction_accuracy': 1 - abs(avg_actual_latency - avg_expected_latency) / max(avg_expected_latency, 1)
            }
        
        return insights 