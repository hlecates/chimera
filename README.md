# Chimera: Polyglot NoSQL Prototype

![Chimera Logo](docs/Chimera-Logo.png)

*Chimera* is a polyglot NoSQL database prototype that:

* **Implements** the five canonical NoSQL data models (Key–Value, Document, Column-Family, Graph, Time-Series).
* **Allows** users to manually select or ingest data into any engine.
* **Profiles** ingested data and recommends (or auto-selects) the most suitable engine via heuristics or a trained model.

## Project Goals

* **Modularity**: Each engine should be self-contained and adhere to the common interface, and follow ACID to best of ability.
* **Durability**: Engines persist data reliably via WAL and snapshots.
* **Usability**: Simple API for selecting, ingesting, querying, and recovering data.
* **Intelligence**: Basic heuristics for engine selection, with a path toward ML-driven recommendations.

## Next Steps

* Implement and test the KV engine with WAL + snapshots.
* Implement and test the document based storage with WAL + snapshots.
* Implement and test the column based storage with WAL + snapshots.
* Implement and test the graph based storage with WAL + snapshots.
* Implement and test the time series based storage with WAL + snapshots.
* Build the data profiler and basic heuristics.
* Construct a frontend. 

---

*This README is a temporary scaffold. It will evolve as the project develops.*
