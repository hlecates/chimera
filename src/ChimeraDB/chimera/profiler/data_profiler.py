import json
import time
from typing import Any, Dict, List, Optional, Union
from collections import defaultdict, Counter
import statistics


class DataProfiler:
   
    
    def __init__(self):
        self.profiles = {}
        
    def profile_data(self, data: Union[List[Dict], Dict], sample_size: int = 1000) -> Dict[str, Any]:
        
        if isinstance(data, dict):
            data = [data]
        
        if not data:
            return self._empty_profile()
        
        # Sample data if too large
        if len(data) > sample_size:
            import random
            data = random.sample(data, sample_size)
        
        profile = {
            'total_items': len(data),
            'structure_analysis': self._analyze_structure(data),
            'data_types': self._analyze_data_types(data),
            'size_characteristics': self._analyze_sizes(data),
            'query_patterns': self._analyze_query_patterns(data),
            'relationship_analysis': self._analyze_relationships(data),
            'temporal_analysis': self._analyze_temporal_patterns(data),
            'cardinality_analysis': self._analyze_cardinality(data),
            'engine_recommendations': []
        }
        
        # Generate engine recommendations
        profile['engine_recommendations'] = self._generate_recommendations(profile)
        
        return profile
    
    def _empty_profile(self) -> Dict[str, Any]:
        return {
            'total_items': 0,
            'structure_analysis': {},
            'data_types': {},
            'size_characteristics': {},
            'query_patterns': {},
            'relationship_analysis': {},
            'temporal_analysis': {},
            'cardinality_analysis': {},
            'engine_recommendations': ['kv']  # Default for empty data
        }
    
    def _analyze_structure(self, data: List[Dict]) -> Dict[str, Any]:
        """Analyze the structure of the data."""
        if not data:
            return {}
        
        # Field presence analysis
        field_presence = defaultdict(int)
        nested_fields = defaultdict(int)
        array_fields = defaultdict(int)
        
        for item in data:
            for field, value in item.items():
                field_presence[field] += 1
                
                if isinstance(value, dict):
                    nested_fields[field] += 1
                elif isinstance(value, list):
                    array_fields[field] += 1
        
        total_items = len(data)
        field_coverage = {field: count / total_items for field, count in field_presence.items()}
        
        return {
            'field_presence': dict(field_presence),
            'field_coverage': field_coverage,
            'nested_fields': list(nested_fields.keys()),
            'array_fields': list(array_fields.keys()),
            'avg_fields_per_item': sum(len(item) for item in data) / len(data),
            'schema_consistency': self._calculate_schema_consistency(data)
        }
    
    def _analyze_data_types(self, data: List[Dict]) -> Dict[str, Any]:
        type_counts = defaultdict(lambda: defaultdict(int))
        numeric_fields = []
        string_fields = []
        boolean_fields = []
        
        for item in data:
            for field, value in item.items():
                value_type = type(value).__name__
                type_counts[field][value_type] += 1
                
                if isinstance(value, (int, float)):
                    numeric_fields.append(field)
                elif isinstance(value, str):
                    string_fields.append(field)
                elif isinstance(value, bool):
                    boolean_fields.append(field)
        
        return {
            'type_distribution': dict(type_counts),
            'numeric_fields': list(set(numeric_fields)),
            'string_fields': list(set(string_fields)),
            'boolean_fields': list(set(boolean_fields)),
            'mixed_type_fields': [field for field, types in type_counts.items() if len(types) > 1]
        }
    
    def _analyze_sizes(self, data: List[Dict]) -> Dict[str, Any]:
        item_sizes = []
        field_sizes = defaultdict(list)
        
        for item in data:
            item_json = json.dumps(item)
            item_sizes.append(len(item_json))
            
            for field, value in item.items():
                field_json = json.dumps(value)
                field_sizes[field].append(len(field_json))
        
        return {
            'avg_item_size': statistics.mean(item_sizes) if item_sizes else 0,
            'max_item_size': max(item_sizes) if item_sizes else 0,
            'min_item_size': min(item_sizes) if item_sizes else 0,
            'size_variance': statistics.variance(item_sizes) if len(item_sizes) > 1 else 0,
            'field_sizes': {field: {
                'avg': statistics.mean(sizes),
                'max': max(sizes),
                'min': min(sizes)
            } for field, sizes in field_sizes.items()}
        }
    
    def _analyze_query_patterns(self, data: List[Dict]) -> Dict[str, Any]:
     # This is a simplified analysis - in practice, track actual queries
        equality_fields = []
        range_fields = []
        text_fields = []
        
        for item in data:
            for field, value in item.items():
                if isinstance(value, (int, float)):
                    if field not in range_fields:
                        range_fields.append(field)
                elif isinstance(value, str):
                    if len(value) > 50:  # Long text
                        text_fields.append(field)
                    else:
                        equality_fields.append(field)
                else:
                    equality_fields.append(field)
        
        return {
            'equality_query_fields': list(set(equality_fields)),
            'range_query_fields': list(set(range_fields)),
            'text_query_fields': list(set(text_fields)),
            'estimated_query_complexity': self._estimate_query_complexity(data)
        }
    
    def _analyze_relationships(self, data: List[Dict]) -> Dict[str, Any]:
       # Look for foreign key patterns
        potential_fks = []
        id_fields = []
        
        for item in data:
            for field, value in item.items():
                field_lower = field.lower()
                if any(keyword in field_lower for keyword in ['id', '_id', 'ref', 'key']):
                    id_fields.append(field)
                
                # Check if field contains IDs (simplified)
                if isinstance(value, str) and len(value) < 50:
                    if any(keyword in value.lower() for keyword in ['id', 'ref', 'key']):
                        potential_fks.append(field)
        
        return {
            'id_fields': list(set(id_fields)),
            'potential_foreign_keys': list(set(potential_fks)),
            'relationship_score': len(set(id_fields)) / max(len(data), 1)
        }
    
    def _analyze_temporal_patterns(self, data: List[Dict]) -> Dict[str, Any]:
        timestamp_fields = []
        date_fields = []
        
        for item in data:
            for field, value in item.items():
                field_lower = field.lower()
                if any(keyword in field_lower for keyword in ['time', 'date', 'created', 'updated']):
                    if isinstance(value, (int, float)):
                        timestamp_fields.append(field)
                    elif isinstance(value, str):
                        date_fields.append(field)
        
        return {
            'timestamp_fields': list(set(timestamp_fields)),
            'date_fields': list(set(date_fields)),
            'has_temporal_data': len(timestamp_fields) > 0 or len(date_fields) > 0
        }
    
    def _analyze_cardinality(self, data: List[Dict]) -> Dict[str, Any]:
        """Analyze cardinality of fields."""
        field_cardinality = {}
        
        for field in set().union(*[set(item.keys()) for item in data]):
            values = [item.get(field) for item in data if field in item]
            
            # Handle unhashable types (dicts, lists) by converting to strings
            hashable_values = []
            for value in values:
                if isinstance(value, (dict, list)):
                    hashable_values.append(str(value))
                else:
                    hashable_values.append(value)
            
            unique_values = set(hashable_values)
            cardinality = len(unique_values) / len(hashable_values) if hashable_values else 0
            field_cardinality[field] = cardinality
        
        return {
            'field_cardinality': field_cardinality,
            'high_cardinality_fields': [field for field, card in field_cardinality.items() if card > 0.8],
            'low_cardinality_fields': [field for field, card in field_cardinality.items() if card < 0.1]
        }
    
    def _calculate_schema_consistency(self, data: List[Dict]) -> float:
        if not data:
            return 0.0
        
        all_fields = set().union(*[set(item.keys()) for item in data])
        field_presence = defaultdict(int)
        
        for item in data:
            for field in all_fields:
                if field in item:
                    field_presence[field] += 1
        
        consistency_scores = [count / len(data) for count in field_presence.values()]
        return statistics.mean(consistency_scores) if consistency_scores else 0.0
    
    def _estimate_query_complexity(self, data: List[Dict]) -> str:
        if not data:
            return 'simple'
        
        avg_fields = sum(len(item) for item in data) / len(data)
        nested_count = sum(1 for item in data for value in item.values() if isinstance(value, dict))
        
        if avg_fields > 10 or nested_count > len(data) * 0.5:
            return 'complex'
        elif avg_fields > 5:
            return 'moderate'
        else:
            return 'simple'
    
    def _generate_recommendations(self, profile: Dict[str, Any]) -> List[str]:
        recommendations = []
        scores = {
            'kv': 0,
            'document': 0,
            'column': 0,
            'graph': 0,
            'timeseries': 0
        }
        
        # Key-Value engine scoring
        if profile['total_items'] < 1000:
            scores['kv'] += 3
        if profile['size_characteristics']['avg_item_size'] < 1024:
            scores['kv'] += 2
        if not profile['structure_analysis']['nested_fields']:
            scores['kv'] += 2
        
        # Document engine scoring
        if profile['structure_analysis']['nested_fields']:
            scores['document'] += 3
        if profile['structure_analysis']['array_fields']:
            scores['document'] += 2
        if profile['data_types']['mixed_type_fields']:
            scores['document'] += 2
        if profile['query_patterns']['estimated_query_complexity'] == 'complex':
            scores['document'] += 2
        # Give document engine a base score for general use
        scores['document'] += 1
        
        # Column engine scoring
        if profile['data_types']['numeric_fields']:
            scores['column'] += 2
        if profile['query_patterns']['range_query_fields']:
            scores['column'] += 3
        if profile['cardinality_analysis']['low_cardinality_fields']:
            scores['column'] += 2
        if profile['structure_analysis']['schema_consistency'] > 0.8:
            scores['column'] += 2
        
        # Graph engine scoring
        if profile['relationship_analysis']['id_fields']:
            scores['graph'] += 2
        if profile['relationship_analysis']['potential_foreign_keys']:
            scores['graph'] += 3
        if profile['relationship_analysis']['relationship_score'] > 0.3:
            scores['graph'] += 2
        
        # Time-series engine scoring
        if profile['temporal_analysis']['has_temporal_data']:
            scores['timeseries'] += 4  # Higher weight for temporal data
        if profile['temporal_analysis']['timestamp_fields']:
            scores['timeseries'] += 3
        if profile['data_types']['numeric_fields']:
            scores['timeseries'] += 1
        
        # Select top 2 engines
        sorted_engines = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        recommendations = [engine for engine, score in sorted_engines[:2] if score > 0]
        
        # Always include at least one recommendation
        if not recommendations:
            recommendations = ['document']  # Default fallback
        
        return recommendations 