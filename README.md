# <img src="docs/Chimera-Logo.png" alt="Chimera Logo" width="24" height="24"> ChimeraDB: Polyglot NoSQL Database

*ChimeraDB* is a polyglot NoSQL database that implements the five standard  NoSQL databases with intelligent engine selection and performance monitoring.

## Inspiration

ChimeraDB was inspired by research into using Reinforcement Learning (RL) for hyperparameter tuning machine learning. Rather than treating machine learning as a purely decision-driven tool, I wanted to explore how ML could be used in the "process" of data science. The goal was to create a system that helps users discover the right database engine for their data through modeling and intelligent hueristics rather than requiring deep knowledge of NoSQL database characteristics.

This approach makes database technology more accessible to developers who may not have extensive knowledge of different NoSQL database types, while also providing an easy-to-use tool for smaller projects that need data storage.

## Features

### **Core Capabilities**
- **Five Engine Types**: Key-Value, Document, Column-Family, Graph, and Time-Series storage engines
- **Intelligent Engine Selection**: Automatic recommendation based on data characteristics and use case
- **Performance Monitoring**: Real-time metrics, trend analysis, and anomaly detection
- **ACID Compliance**: Write-Ahead Log (WAL) and snapshot-based durability
- **Unified API**: Single interface for all engines with automatic engine selection

### **Data Profiling & Intelligence**
- **Data Structure Analysis**: Automatic detection of data patterns and relationships
- **Engine Recommendation**: ML-driven engine selection with confidence scoring
- **Performance Prediction**: Expected vs. actual performance tracking
- **Learning System**: Continuous improvement from usage feedback

### **Storage & Durability**
- **Write-Ahead Logging**: Ensures data durability and crash recovery
- **Snapshot Management**: Point-in-time recovery capabilities
- **Multi-Engine Persistence**: Each engine maintains its own WAL and snapshots

## Architecture

```
ChimeraDB/
├── src/ChimeraDB/chimera/
│   ├── engines/           # Storage engines
│   │   ├── kv_engine.py
│   │   ├── document_engine.py
│   │   ├── column_engine.py
│   │   ├── graph_engine.py
│   │   └── timeseries_engine.py
│   ├── profiler/          # Intelligence layer
│   │   ├── data_profiler.py
│   │   ├── engine_selector.py
│   │   └── metrics.py
│   ├── storage/           # Durability layer
│   │   ├── wal.py
│   │   └── snapshot.py
│   └── chimera_db.py      # Main API
├── tests/                 # Test suite
├── examples/              # Usage examples
└── docs/                  # Documentation (e.g. logo)
```

## Quick Start

### Installation

```bash
git clone https://github.com/hlecates/chimera
cd chimera

# Install in development mode
pip install -e .
```

### Basic Usage

```python
from ChimeraDB.chimera.chimera_db import ChimeraDB

# Initialize database
db = ChimeraDB("./data_directory")
db.startup()

# Store data with automatic engine selection
user_data = [
    {'id': 1, 'name': 'Alice', 'email': 'alice@example.com', 'age': 30},
    {'id': 2, 'name': 'Bob', 'email': 'bob@example.com', 'age': 25}
]

# Get engine recommendation
recommendation = db.recommend_engine(user_data, "transactional")
print(f"Recommended: {recommendation['recommended_engine']}")

# Auto-store with intelligent engine selection
result = db.auto_store("users", user_data, "transactional")
print(f"Engine used: {result['engine_used']}")

# Query data
results = db.query("users", result['engine_used'], {'age': {'$gt': 25}})

# Performance monitoring
stats = db.get_performance_stats()
health = db.health_check()

db.shutdown()
```

## Engine Types

### Key-Value Engine
- **Best for**: Simple key-value pairs
- **Features**: O(1) read/write operations, minimal overhead
- **Use cases**: User sessions, simple lookups

### Document Engine
- **Best for**: Complex, nested data structures
- **Features**: JSON-like documents, flexible schema
- **Use cases**: User profiles, product catalogs

### Column Engine
- **Best for**: Analytical workloads, time-series data
- **Features**: Column-oriented storage, efficient aggregations
- **Use cases**: Financial data

### Graph Engine
- **Best for**: Relationship-heavy data
- **Features**: Node-edge relationships, graph traversal
- **Use cases**: Social networks, recommendation systems

### Time-Series Engine
- **Best for**: Temporal data
- **Features**: Time-based indexing, efficient range queries
- **Use cases**: Sensor data, financial time series

## API Reference

#### `ChimeraDB(data_dir: str)`
Initialize the database with specified data directory.

#### `startup()`
Start all storage engines and initialize WAL/snapshot systems.

#### `shutdown()`
Gracefully shut down all engines and persist final state.

#### `recommend_engine(data: List[Dict], use_case: str) -> Dict`
Get engine recommendation based on data characteristics and use case.

#### `auto_store(collection: str, data: List[Dict], use_case: str) -> Dict`
Automatically store data using the best engine for the use case.

## Status

- [ ] Enhanced ML-based engine selection
- [ ] Web-based administration
- [ ] Distributed clustering support
- [ ] Advanced query optimization



