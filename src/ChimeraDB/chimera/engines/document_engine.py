import threading
import json
from typing import Any, Dict, Optional

from ChimeraDB.chimera.storage.wal import WriteAheadLog
from ChimeraDB.chimera.storage.snapshot import SnapshotManager
from ChimeraDB.chimera.engines.engine_interface import EngineInterface

class DocumentEngine(EngineInterface):
    def __init__(
        self,
        wal_path: str,
        snap_path: str,
        max_collection_name_len: int = 128,
        max_id_name_len: int = 256,
        max_document_size: int = 10 * 1024 * 1024
    ):
        # config limits
        self.max_collection_name_len = max_collection_name_len
        self.max_id_name_len = max_id_name_len
        self.max_document_size = max_document_size

        # in-memory store: collection -> { _id : document }
        self.store: Dict[str, Dict[Any, Dict]] = {}
        self.lock = threading.RLock()
        # indexes: collection -> field -> value -> set of ids
        # speeds up equality queries from linear to about constant
        self.indexes: Dict[str, Dict[str, Dict[Any, set]]] = {}

        # persistence
        self.wal = WriteAheadLog(wal_path)
        self.snapshot_mgr = SnapshotManager(snap_path)

    def startup(self) -> None:
        # Load snapshot and WAL
        snapshot = self.snapshot_mgr.load('latest')
        self.store = snapshot if snapshot else {}

        # Replay WAL
        for op in self.wal.replay():
            self._apply(op)

        # Build indexes from current store
        self.indexes = {}
        for coll, docs in self.store.items():
            self.indexes.setdefault(coll, {})
            for _id, doc in docs.items():
                self._add_to_index(coll, _id, doc)

        self.wal.rotate()

    def shutdown(self) -> None:
        with self.lock:
            self.snapshot_mgr.create('latest', self.store)
            self.wal.close()

    def recover(self) -> None:
        with self.lock:
            self.startup()

    def _validate_collection_id(self, collection: str, doc_id: Any) -> None:
        if not isinstance(collection, str) or not collection:
            raise ValueError("Collection name must be a non-empty string")
        if len(collection) > self.max_collection_name_len:
            raise ValueError(f"Collection name exceeds {self.max_collection_name_len} characters")
        if not doc_id:
            raise ValueError("Document must have a non-empty '_id'")
        if isinstance(doc_id, str) and len(doc_id) > self.max_id_name_len:
            raise ValueError(f"Document '_id' exceeds {self.max_id_name_len} characters")

    def _apply(self, op: Dict) -> None:
        coll = op['collection']
        self.store.setdefault(coll, {})
        self.indexes.setdefault(coll, {})

        if op['operation'] == 'INSERT':
            doc = op['document']
            key = doc.get('_id')
            if key is None:
                return
            # Remove stale index if overwriting existing doc
            if key in self.store[coll]:
                self._remove_from_index(coll, key, self.store[coll][key])
            self.store[coll][key] = doc
            self._add_to_index(coll, key, doc)

        elif op['operation'] == 'UPDATE':
            filt = op['filter']
            changes = op.get('update', {})
            if '$set' in changes:
                for key, doc in list(self.store[coll].items()):
                    if self._match(doc, filt):
                        old_doc = doc.copy()
                        doc.update(changes['$set'])
                        self.store[coll][key] = doc
                        self._remove_from_index(coll, key, old_doc)
                        self._add_to_index(coll, key, doc)

        elif op['operation'] == 'DELETE':
            filt = op['filter']
            if not isinstance(filt, dict):
                filt = {'_id': filt}
            to_remove = [k for k, doc in self.store[coll].items() if self._match(doc, filt)]
            for k in to_remove:
                self._remove_from_index(coll, k, self.store[coll][k])
                self.store[coll].pop(k, None)

    def put(self, collection: str, key: Any, document: Dict) -> None:
        document["_id"] = key
        return self._insert(collection, document)

    def _insert(self, collection: str, document: Dict) -> None:
        key = document.get('_id')
        self._validate_collection_id(collection, key)

        raw = json.dumps(document)
        if len(raw) > self.max_document_size:
            raise ValueError(f"Document exceeds {self.max_document_size} bytes in JSON form")

        op = {'operation': 'INSERT', 'collection': collection, 'document': document}
        with self.lock:
            self._apply(op)
            self.wal.append(op)

    def get(self, collection: str, key: Any) -> Optional[Dict]:
        return self._find_one(collection, {"_id": key})

    def _find_one(self, collection: str, filt: Dict) -> Optional[Dict]:
        with self.lock:
            # use index for simple equality queries
            if len(filt) == 1 and not any(isinstance(v, dict) for v in filt.values()):
                field, value = next(iter(filt.items()))
                idx = self.indexes.get(collection, {}).get(field, {})
                # ensure we re-validate match in case index was stale
                for _id in idx.get(value, set()):
                    doc = self.store[collection][_id]
                    if self._match(doc, filt):
                        return doc.copy()
            # fallback full scan
            for doc in self.store.get(collection, {}).values():
                if self._match(doc, filt):
                    return doc.copy()
        return None

    def update(self, collection: str, filt: Dict, update: Dict) -> int:
        self._validate_collection_id(collection, filt.get('_id', None))
        op = {'operation': 'UPDATE', 'collection': collection, 'filter': filt, 'update': update}
        count = 0
        with self.lock:
            for doc in self.store.get(collection, {}).values():
                if self._match(doc, filt):
                    count += 1
            if count:
                self._apply(op)
                self.wal.append(op)
        return count

    def delete(self, collection: str, key_or_filt: Dict) -> bool:
        op = {'operation': 'DELETE', 'collection': collection, 'filter': key_or_filt}
        with self.lock:
            before = len(self.store.get(collection, {}))
            self._apply(op)
            removed = len(self.store.get(collection, {})) < before
            if removed:
                self.wal.append(op)
            return removed

    def query(self, collection: str, filter: Dict) -> list:
        with self.lock:
            # use index for simple equality queries
            if len(filter) == 1 and not any(isinstance(v, dict) for v in filter.values()):
                field, value = next(iter(filter.items()))
                idx = self.indexes.get(collection, {}).get(field, {})
                # re-validate with match
                return [self.store[collection][_id].copy()
                        for _id in idx.get(value, set())
                        if self._match(self.store[collection][_id], filter)]
            # fallback full scan
            return [doc.copy() for doc in self.store.get(collection, {}).values() if self._match(doc, filter)]

    def _add_to_index(self, coll: str, key: Any, doc: Dict) -> None:
        for field, val in doc.items():
            fld_idx = self.indexes.setdefault(coll, {}).setdefault(field, {})
            fld_idx.setdefault(val, set()).add(key)

    def _remove_from_index(self, coll: str, key: Any, doc: Dict) -> None:
        for field, val in doc.items():
            vals_idx = self.indexes.get(coll, {}).get(field, {})
            ids = vals_idx.get(val)
            if ids:
                ids.discard(key)
                if not ids:
                    vals_idx.pop(val, None)

    def _match(self, doc: Dict, filt: Dict) -> bool:
        for field, cond in filt.items():
            val = doc.get(field)
            if isinstance(cond, dict):
                for op, threshold in cond.items():
                    if op == "$gt"  and not (val >  threshold): return False
                    if op == "$gte" and not (val >= threshold): return False
                    if op == "$lt"  and not (val <  threshold): return False
                    if op == "$lte" and not (val <= threshold): return False
                    if op == "$ne"  and not (val != threshold): return False
                    # add $eq, $in, etc. as needed
            else:
                if val != cond:
                    return False
        return True
