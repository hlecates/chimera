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

        # persistence
        self.wal = WriteAheadLog(wal_path)
        self.snapshot_mgr = SnapshotManager(snap_path)


    def startup(self) -> None:
        snapshot = self.snapshot_mgr.load('latest')
        self.store = snapshot if snapshot else {}

        for op in self.wal.replay():
            self._apply(op)

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

        if op['operation'] == 'INSERT':
            doc = op['document']
            key = doc.get('_id')
            if key is None:
                return
            self.store[coll][key] = doc

        elif op['operation'] == 'UPDATE':
            filt = op['filter']
            changes = op.get('update', {})
            if '$set' in changes:
                for key, doc in list(self.store[coll].items()):
                    if all(doc.get(k) == v for k, v in filt.items()):
                        doc.update(changes['$set'])
                        self.store[coll][key] = doc

        elif op['operation'] == 'DELETE':
            # op['filter'] may be a dict or a single id
            filt = op['filter']
            if not isinstance(filt, dict):
                filt = {'_id': filt}
            to_remove = [k for k, doc in self.store[coll].items()
                         if all(doc.get(fk) == fv for fk, fv in filt.items())]
            for k in to_remove:
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
            for doc in self.store.get(collection, {}).values():
                if all(doc.get(k) == v for k, v in filt.items()):
                    return doc.copy()
        return None


    def update(self, collection: str, filt: Dict, update: Dict) -> int:
        self._validate_collection_id(collection, filt.get('_id', None))
        op = {'operation': 'UPDATE', 'collection': collection, 'filter': filt, 'update': update}
        count = 0
        with self.lock:
            for doc in self.store.get(collection, {}).values():
                if all(doc.get(k) == v for k, v in filt.items()):
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
        

    def query(self, collection: str, filter: dict):
        with self.lock:
            return [
                doc.copy()
                for doc in self.store.get(collection, {}).values()
                if all(doc.get(k) == v for k, v in filter.items())
            ]