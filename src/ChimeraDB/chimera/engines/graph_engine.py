import threading
import json
from typing import Any, Dict, List, Optional, Set, Tuple

from ChimeraDB.chimera.storage.wal import WriteAheadLog
from ChimeraDB.chimera.storage.snapshot import SnapshotManager
from ChimeraDB.chimera.engines.engine_interface import EngineInterface

class GraphEngine(EngineInterface):
    def __init__(
        self,
        wal_path: str,
        snap_path: str,
        max_graph_name_len: int = 128,
        max_node_id_len: int = 256
    ):
        # config limits
        self.max_graph_name_len = max_graph_name_len
        self.max_node_id_len = max_node_id_len

        # store: graph -> { nodes: {node_id -> node_data}, edges: {edge_id -> edge_data} }
        self.store: Dict[str, Dict[str, Dict[Any, Any]]] = {}
        
        # indexes for fast lookups
        # node indexes: graph -> field -> value -> set(node_ids)
        self.node_indexes: Dict[str, Dict[str, Dict[Any, set]]] = {}
        # edge indexes: graph -> field -> value -> set(edge_ids)
        self.edge_indexes: Dict[str, Dict[str, Dict[Any, set]]] = {}
        # adjacency lists: graph -> node_id -> {outgoing: set(edge_ids), incoming: set(edge_ids)}
        self.adjacency: Dict[str, Dict[Any, Dict[str, set]]] = {}

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

        # Build indexes and adjacency lists from current store
        self.node_indexes = {}
        self.edge_indexes = {}
        self.adjacency = {}
        
        for graph_name, graph_data in self.store.items():
            self.node_indexes.setdefault(graph_name, {})
            self.edge_indexes.setdefault(graph_name, {})
            self.adjacency.setdefault(graph_name, {})
            
            # Build node indexes
            for node_id, node_data in graph_data.get('nodes', {}).items():
                for field, value in node_data.items():
                    self._add_to_node_index(graph_name, field, node_id, value)
                self.adjacency[graph_name].setdefault(node_id, {'outgoing': set(), 'incoming': set()})
            
            # Build edge indexes and adjacency
            for edge_id, edge_data in graph_data.get('edges', {}).items():
                for field, value in edge_data.items():
                    if field not in ['from', 'to']:  # Don't index connection fields
                        self._add_to_edge_index(graph_name, field, edge_id, value)
                
                # Build adjacency lists
                from_node = edge_data.get('from')
                to_node = edge_data.get('to')
                if from_node and to_node:
                    self.adjacency[graph_name].setdefault(from_node, {'outgoing': set(), 'incoming': set()})
                    self.adjacency[graph_name].setdefault(to_node, {'outgoing': set(), 'incoming': set()})
                    self.adjacency[graph_name][from_node]['outgoing'].add(edge_id)
                    self.adjacency[graph_name][to_node]['incoming'].add(edge_id)

        self.wal.rotate()

    def shutdown(self) -> None:
        with self.lock:
            self.snapshot_mgr.create("latest", self.store)
            self.wal.close()

    def recover(self) -> None:
        with self.lock:
            self.startup()

    def _validate_graph_node(self, graph: str, node_id: Any) -> None:
        if not isinstance(graph, str) or not graph:
            raise ValueError("Graph name must be a non-empty string")
        if len(graph) > self.max_graph_name_len:
            raise ValueError(f"Graph name exceeds {self.max_graph_name_len} chars")
        if not node_id:
            raise ValueError("Node must have a non-empty ID")
        if isinstance(node_id, str) and len(node_id) > self.max_node_id_len:
            raise ValueError(f"Node ID exceeds {self.max_node_id_len} chars")

    def _apply(self, op: Dict) -> None:
        graph = op['graph']
        self.store.setdefault(graph, {'nodes': {}, 'edges': {}})
        self.node_indexes.setdefault(graph, {})
        self.edge_indexes.setdefault(graph, {})
        self.adjacency.setdefault(graph, {})

        if op['operation'] == 'ADD_NODE':
            node_id = op['node_id']
            node_data = op['node_data']
            
            # Remove stale index if overwriting existing node
            if node_id in self.store[graph]['nodes']:
                old_node = self.store[graph]['nodes'][node_id]
                for field, value in old_node.items():
                    self._remove_from_node_index(graph, field, node_id, value)
            
            self.store[graph]['nodes'][node_id] = node_data
            for field, value in node_data.items():
                self._add_to_node_index(graph, field, node_id, value)
            
            self.adjacency[graph].setdefault(node_id, {'outgoing': set(), 'incoming': set()})

        elif op['operation'] == 'ADD_EDGE':
            edge_id = op['edge_id']
            edge_data = op['edge_data']
            
            # Remove stale index if overwriting existing edge
            if edge_id in self.store[graph]['edges']:
                old_edge = self.store[graph]['edges'][edge_id]
                for field, value in old_edge.items():
                    if field not in ['from', 'to']:
                        self._remove_from_edge_index(graph, field, edge_id, value)
                
                # Remove from old adjacency
                old_from = old_edge.get('from')
                old_to = old_edge.get('to')
                if old_from and old_to:
                    self.adjacency[graph][old_from]['outgoing'].discard(edge_id)
                    self.adjacency[graph][old_to]['incoming'].discard(edge_id)
            
            self.store[graph]['edges'][edge_id] = edge_data
            
            # Add to indexes (excluding connection fields)
            for field, value in edge_data.items():
                if field not in ['from', 'to']:
                    self._add_to_edge_index(graph, field, edge_id, value)
            
            # Update adjacency
            from_node = edge_data.get('from')
            to_node = edge_data.get('to')
            if from_node and to_node:
                self.adjacency[graph].setdefault(from_node, {'outgoing': set(), 'incoming': set()})
                self.adjacency[graph].setdefault(to_node, {'outgoing': set(), 'incoming': set()})
                self.adjacency[graph][from_node]['outgoing'].add(edge_id)
                self.adjacency[graph][to_node]['incoming'].add(edge_id)

        elif op['operation'] == 'DELETE_NODE':
            node_id = op['node_id']
            
            # Remove all edges connected to this node
            connected_edges = set()
            if node_id in self.adjacency[graph]:
                connected_edges.update(self.adjacency[graph][node_id]['outgoing'])
                connected_edges.update(self.adjacency[graph][node_id]['incoming'])
            
            for edge_id in connected_edges:
                if edge_id in self.store[graph]['edges']:
                    edge_data = self.store[graph]['edges'][edge_id]
                    for field, value in edge_data.items():
                        if field not in ['from', 'to']:
                            self._remove_from_edge_index(graph, field, edge_id, value)
                    del self.store[graph]['edges'][edge_id]
            
            # Remove node from indexes
            if node_id in self.store[graph]['nodes']:
                node_data = self.store[graph]['nodes'][node_id]
                for field, value in node_data.items():
                    self._remove_from_node_index(graph, field, node_id, value)
                del self.store[graph]['nodes'][node_id]
            
            # Remove from adjacency
            if node_id in self.adjacency[graph]:
                del self.adjacency[graph][node_id]

        elif op['operation'] == 'DELETE_EDGE':
            edge_id = op['edge_id']
            
            if edge_id in self.store[graph]['edges']:
                edge_data = self.store[graph]['edges'][edge_id]
                
                # Remove from indexes
                for field, value in edge_data.items():
                    if field not in ['from', 'to']:
                        self._remove_from_edge_index(graph, field, edge_id, value)
                
                # Remove from adjacency
                from_node = edge_data.get('from')
                to_node = edge_data.get('to')
                if from_node and from_node in self.adjacency[graph]:
                    self.adjacency[graph][from_node]['outgoing'].discard(edge_id)
                if to_node and to_node in self.adjacency[graph]:
                    self.adjacency[graph][to_node]['incoming'].discard(edge_id)
                
                del self.store[graph]['edges'][edge_id]

    def put(self, graph: str, key: Any, value: bytes) -> None:
        # For graph engine, we'll treat this as adding a node with the key as node_id
        # and the value as JSON-serialized node data
        self._validate_graph_node(graph, key)
        
        try:
            node_data = json.loads(value.decode('utf-8'))
        except (json.JSONDecodeError, UnicodeDecodeError):
            raise ValueError("Value must be valid JSON-encoded node data")
        
        op = {'operation': 'ADD_NODE', 'graph': graph, 'node_id': key, 'node_data': node_data}
        with self.lock:
            self._apply(op)
            self.wal.append(op)

    def get(self, graph: str, key: Any) -> bytes:
        self._validate_graph_node(graph, key)
        
        with self.lock:
            node_data = self.store.get(graph, {}).get('nodes', {}).get(key)
            if node_data is None:
                return None
            return json.dumps(node_data).encode('utf-8')

    def delete(self, graph: str, key: Any) -> bool:
        self._validate_graph_node(graph, key)
        
        with self.lock:
            if key not in self.store.get(graph, {}).get('nodes', {}):
                return False
            
            op = {'operation': 'DELETE_NODE', 'graph': graph, 'node_id': key}
            self._apply(op)
            self.wal.append(op)
            return True

    def query(self, graph: str, filter: dict) -> list:
        with self.lock:
            if not filter:
                # Return all nodes
                nodes = []
                for node_id, node_data in self.store.get(graph, {}).get('nodes', {}).items():
                    nodes.append({'_id': node_id, **node_data})
                return nodes
            
            # Handle different query types
            if 'node_filter' in filter:
                return self._query_nodes(graph, filter['node_filter'])
            elif 'edge_filter' in filter:
                return self._query_edges(graph, filter['edge_filter'])
            elif 'path' in filter:
                return self._query_path(graph, filter['path'])
            else:
                # Default to node query
                return self._query_nodes(graph, filter)

    def _add_to_node_index(self, graph: str, field: str, node_id: Any, value: Any) -> None:
        field_idx = self.node_indexes.setdefault(graph, {}).setdefault(field, {})
        field_idx.setdefault(value, set()).add(node_id)

    def _remove_from_node_index(self, graph: str, field: str, node_id: Any, value: Any) -> None:
        field_idx = self.node_indexes.get(graph, {}).get(field, {})
        ids = field_idx.get(value)
        if ids:
            ids.discard(node_id)
            if not ids:
                field_idx.pop(value, None)

    def _add_to_edge_index(self, graph: str, field: str, edge_id: Any, value: Any) -> None:
        field_idx = self.edge_indexes.setdefault(graph, {}).setdefault(field, {})
        field_idx.setdefault(value, set()).add(edge_id)

    def _remove_from_edge_index(self, graph: str, field: str, edge_id: Any, value: Any) -> None:
        field_idx = self.edge_indexes.get(graph, {}).get(field, {})
        ids = field_idx.get(value)
        if ids:
            ids.discard(edge_id)
            if not ids:
                field_idx.pop(value, None)

    def _query_nodes(self, graph: str, filter: Dict[str, Any]) -> List[Dict[str, Any]]:
        # Use index for simple equality queries
        if len(filter) == 1 and not any(isinstance(v, dict) for v in filter.values()):
            field, value = next(iter(filter.items()))
            node_ids = self.node_indexes.get(graph, {}).get(field, {}).get(value, set())
            return [{'_id': node_id, **self.store[graph]['nodes'][node_id]} 
                   for node_id in node_ids if node_id in self.store[graph]['nodes']]
        
        # Fallback to full scan
        results = []
        for node_id, node_data in self.store.get(graph, {}).get('nodes', {}).items():
            if self._match_node(node_data, filter):
                results.append({'_id': node_id, **node_data})
        return results

    def _query_edges(self, graph: str, filter: Dict[str, Any]) -> List[Dict[str, Any]]:
        # Use index for simple equality queries
        if len(filter) == 1 and not any(isinstance(v, dict) for v in filter.values()):
            field, value = next(iter(filter.items()))
            edge_ids = self.edge_indexes.get(graph, {}).get(field, {}).get(value, set())
            return [{'_id': edge_id, **self.store[graph]['edges'][edge_id]} 
                   for edge_id in edge_ids if edge_id in self.store[graph]['edges']]
        
        # Fallback to full scan
        results = []
        for edge_id, edge_data in self.store.get(graph, {}).get('edges', {}).items():
            if self._match_edge(edge_data, filter):
                results.append({'_id': edge_id, **edge_data})
        return results

    def _query_path(self, graph: str, path_filter: Dict[str, Any]) -> List[Dict[str, Any]]:
        # Simple path query implementation
        start_node = path_filter.get('start')
        end_node = path_filter.get('end')
        max_depth = path_filter.get('max_depth', 3)
        
        if not start_node or not end_node:
            return []
        
        # Simple BFS path finding
        queue = [(start_node, [start_node])]
        paths = []
        
        while queue and len(paths) < 10:  # Limit results
            current, path = queue.pop(0)
            if current == end_node:
                paths.append(path)
                continue
            
            if len(path) >= max_depth:
                continue
            
            if current in self.adjacency.get(graph, {}):
                for edge_id in self.adjacency[graph][current]['outgoing']:
                    edge_data = self.store[graph]['edges'].get(edge_id, {})
                    next_node = edge_data.get('to')
                    if next_node and next_node not in path:
                        queue.append((next_node, path + [next_node]))
            else:
                # Debug: current node not in adjacency
                pass
        
        return [{'path': path} for path in paths]

    def _match_node(self, node_data: Dict[str, Any], filter: Dict[str, Any]) -> bool:
        for field, cond in filter.items():
            val = node_data.get(field)
            if isinstance(cond, dict):
                for op, threshold in cond.items():
                    if op == "$gt" and not (val > threshold): return False
                    if op == "$gte" and not (val >= threshold): return False
                    if op == "$lt" and not (val < threshold): return False
                    if op == "$lte" and not (val <= threshold): return False
                    if op == "$ne" and not (val != threshold): return False
            else:
                if val != cond:
                    return False
        return True

    def _match_edge(self, edge_data: Dict[str, Any], filter: Dict[str, Any]) -> bool:
        for field, cond in filter.items():
            val = edge_data.get(field)
            if isinstance(cond, dict):
                for op, threshold in cond.items():
                    if op == "$gt" and not (val > threshold): return False
                    if op == "$gte" and not (val >= threshold): return False
                    if op == "$lt" and not (val < threshold): return False
                    if op == "$lte" and not (val <= threshold): return False
                    if op == "$ne" and not (val != threshold): return False
            else:
                if val != cond:
                    return False
        return True

    # Additional graph-specific methods
    def add_edge(self, graph: str, edge_id: Any, from_node: Any, to_node: Any, edge_data: Dict[str, Any] = None) -> None:
        if edge_data is None:
            edge_data = {}
        edge_data['from'] = from_node
        edge_data['to'] = to_node
        
        op = {'operation': 'ADD_EDGE', 'graph': graph, 'edge_id': edge_id, 'edge_data': edge_data}
        with self.lock:
            self._apply(op)
            self.wal.append(op)

    def delete_edge(self, graph: str, edge_id: Any) -> bool:
        with self.lock:
            if edge_id not in self.store.get(graph, {}).get('edges', {}):
                return False
            
            op = {'operation': 'DELETE_EDGE', 'graph': graph, 'edge_id': edge_id}
            self._apply(op)
            self.wal.append(op)
            return True

    def get_neighbors(self, graph: str, node_id: Any, direction: str = 'both') -> List[Dict[str, Any]]:
        with self.lock:
            if node_id not in self.adjacency.get(graph, {}):
                return []
            
            neighbors = []
            node_adj = self.adjacency[graph][node_id]
            
            if direction in ['out', 'both']:
                for edge_id in node_adj['outgoing']:
                    edge_data = self.store[graph]['edges'].get(edge_id, {})
                    to_node = edge_data.get('to')
                    if to_node:
                        neighbors.append({
                            'node_id': to_node,
                            'edge_id': edge_id,
                            'direction': 'outgoing',
                            'edge_data': edge_data
                        })
            
            if direction in ['in', 'both']:
                for edge_id in node_adj['incoming']:
                    edge_data = self.store[graph]['edges'].get(edge_id, {})
                    from_node = edge_data.get('from')
                    if from_node:
                        neighbors.append({
                            'node_id': from_node,
                            'edge_id': edge_id,
                            'direction': 'incoming',
                            'edge_data': edge_data
                        })
            
            return neighbors 