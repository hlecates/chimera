import pytest
import json
from ChimeraDB import chimera

def test_graph_node_operations(tmp_path):
    wal_path = str(tmp_path / "graph.wal")
    snap_path = str(tmp_path / "graph.snap")
    engine = chimera.GraphEngine(wal_path, snap_path)
    engine.startup()

    # Add nodes
    alice_data = {"name": "Alice", "age": 30}
    bob_data = {"name": "Bob", "age": 25}
    
    engine.put("social", "alice", json.dumps(alice_data).encode('utf-8'))
    engine.put("social", "bob", json.dumps(bob_data).encode('utf-8'))

    # Retrieve nodes
    alice = json.loads(engine.get("social", "alice").decode('utf-8'))
    assert alice["name"] == "Alice"
    
    bob = json.loads(engine.get("social", "bob").decode('utf-8'))
    assert bob["name"] == "Bob"

    # Query nodes
    nodes = engine.query("social", {"node_filter": {"age": {"$gt": 25}}})
    assert len(nodes) == 1
    assert nodes[0]["name"] == "Alice"

    # Delete node
    assert engine.delete("social", "alice") is True
    assert engine.get("social", "alice") is None

    engine.shutdown()

def test_graph_edge_operations(tmp_path):
    wal_path = str(tmp_path / "graph2.wal")
    snap_path = str(tmp_path / "graph2.snap")
    engine = chimera.GraphEngine(wal_path, snap_path)
    engine.startup()

    # Add nodes first
    engine.put("social", "alice", json.dumps({"name": "Alice"}).encode('utf-8'))
    engine.put("social", "bob", json.dumps({"name": "Bob"}).encode('utf-8'))
    engine.put("social", "carol", json.dumps({"name": "Carol"}).encode('utf-8'))

    # Add edges
    engine.add_edge("social", "edge1", "alice", "bob", {"type": "friend", "since": 2020})
    engine.add_edge("social", "edge2", "bob", "carol", {"type": "friend", "since": 2021})
    engine.add_edge("social", "edge3", "alice", "carol", {"type": "colleague", "since": 2019})

    # Query edges
    friend_edges = engine.query("social", {"edge_filter": {"type": "friend"}})
    assert len(friend_edges) == 2

    # Get neighbors
    alice_neighbors = engine.get_neighbors("social", "alice")
    assert len(alice_neighbors) == 2  # bob and carol

    # Delete edge
    assert engine.delete_edge("social", "edge1") is True
    alice_neighbors_after = engine.get_neighbors("social", "alice")
    assert len(alice_neighbors_after) == 1  # only carol

    engine.shutdown()

def test_graph_path_queries(tmp_path):
    wal_path = str(tmp_path / "graph3.wal")
    snap_path = str(tmp_path / "graph3.snap")
    engine = chimera.GraphEngine(wal_path, snap_path)
    engine.startup()

    # Create a simple path: A -> B -> C -> D
    for node in ["A", "B", "C", "D"]:
        engine.put("path", node, json.dumps({"name": node}).encode('utf-8'))
    
    engine.add_edge("path", "ab", "A", "B", {"weight": 1})
    engine.add_edge("path", "bc", "B", "C", {"weight": 2})
    engine.add_edge("path", "cd", "C", "D", {"weight": 1})

    # Find path from A to D
    paths = engine.query("path", {"path": {"start": "A", "end": "D", "max_depth": 4}})
    assert len(paths) > 0
    assert "path" in paths[0]

    engine.shutdown()

