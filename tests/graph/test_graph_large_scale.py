import random
import string
import threading
import json
import pytest
from ChimeraDB import chimera

def random_node():
    return {
        "name": "".join(random.choices(string.ascii_lowercase, k=8)),
        "age": random.randint(18, 80),
        "city": random.choice(["NYC", "LA", "Chicago", "Houston"]),
    }

@pytest.mark.parametrize("n_nodes", [10000, 50000])
def test_large_scale_graph_operations(tmp_path, n_nodes):
    wal_path = str(tmp_path / "big_graph.wal")
    snap_path = str(tmp_path / "big_graph.snap")
    engine = chimera.GraphEngine(wal_path, snap_path)
    engine.startup()

    # Add nodes
    nodes = {}
    for i in range(n_nodes):
        key = f"user_{i}"
        node_data = random_node()
        nodes[key] = node_data
        engine.put("social", key, json.dumps(node_data).encode('utf-8'))

    # Add some edges
    edges_added = 0
    for i in range(0, n_nodes, 10):  # Connect every 10th node
        if i + 1 < n_nodes:
            engine.add_edge("social", f"edge_{i}", f"user_{i}", f"user_{i+1}", {"type": "friend"})
            edges_added += 1

    # Verify nodes
    all_nodes = engine.query("social", {"node_filter": {}})
    assert len(all_nodes) == n_nodes

    # Query by city
    nyc_users = engine.query("social", {"node_filter": {"city": "NYC"}})
    assert all(node["city"] == "NYC" for node in nyc_users)

    # Query edges
    friend_edges = engine.query("social", {"edge_filter": {"type": "friend"}})
    assert len(friend_edges) == edges_added

    engine.shutdown()

def test_concurrent_graph_operations(tmp_path):
    wal_path = str(tmp_path / "concurrent_graph.wal")
    snap_path = str(tmp_path / "concurrent_graph.snap")
    engine = chimera.GraphEngine(wal_path, snap_path)
    engine.startup()

    def node_writer(start, count):
        for i in range(start, start + count):
            key = f"g_user_{i}"
            node_data = {"id": i, "value": i}
            engine.put("concurrent", key, json.dumps(node_data).encode('utf-8'))

    def edge_writer(start, count):
        for i in range(start, start + count):
            if i + 1 < 5000:
                engine.add_edge("concurrent", f"g_edge_{i}", f"g_user_{i}", f"g_user_{i+1}", {"type": "link"})

    # Launch node writer threads
    node_threads = [threading.Thread(target=node_writer, args=(i * 1000, 1000)) for i in range(5)]
    for t in node_threads:
        t.start()
    for t in node_threads:
        t.join()

    # Launch edge writer threads
    edge_threads = [threading.Thread(target=edge_writer, args=(i * 1000, 1000)) for i in range(5)]
    for t in edge_threads:
        t.start()
    for t in edge_threads:
        t.join()

    # Final verification
    assert len(engine.query("concurrent", {"node_filter": {}})) == 5000
    engine.shutdown()