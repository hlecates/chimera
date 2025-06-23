import threading
import json
from typing import Any, Dict, List, Optional

from ChimeraDB.chimera.storage.wal import WriteAheadLog
from ChimeraDB.chimera.storage.snapshot import SnapshotManager
from ChimeraDB.chimera.engines.engine_interface import EngineInterface

class ColumnEngine(EngineInterface):
    def __init__(
        self,
        wal_path: str,
        snap_path: str,
        max_table_name_len: int = 128,
        max_id_name_len: int = 256
    ):
        # config limits
        self.max_table_name_len = max_table_name_len
        self.max_id_name_len = max_id_name_len

        # store
        #   table -> column -> { row_id / name -> value }
        self.store: Dict[str, Dict[str, Dict[Any, Any]]] = {}
        # indexes for fast equality:
        #   table -> column -> value -> set(row_id)
        #   ie self.indexes[table][column][value] = set_of_row_ids
        self.indexes: Dict[str, Dict[str, Dict[Any, set]]] = {}

        # persistence
        self.lock = threading.RLock()
        self.wal = WriteAheadLog(wal_path)
        self.snapshot_mgr = SnapshotManager(snap_path)
         
         
    def startup(self) -> None:
        pass


    def shutdown(self) -> None:
            with self.lock:
                self.snapshot_mgr.create("latest", self.store)
                self.wal.close()


    def recover(self) -> None:
        # identical to startup
        with self.lock:
            self.startup()

    
    def _validate_table_id(self, table: str, row_id: Any) -> None:
        if not isinstance(table, str) or not table:
            raise ValueError("Table name must be a non-empty string")
        if len(table) > self.max_table_name_len:
            raise ValueError(f"Table name exceeds {self.max_table_name_len} chars")
        if not row_id:
            raise ValueError("Row must have a non-empty primary key")
        if isinstance(row_id, str) and len(row_id) > self.max_id_name_len:
            raise ValueError(f"Row ID exceeds {self.max_id_name_len} chars")


    def _apply(self, op: Dict) -> None:
        pass


    def put(self, table: str, key: Any, row: Dict[str, Any]) -> None:
        pass 

    
    def _filter_row_ids(self, table: str, filter: Dict[str, Any]) -> set:
        if len(filter) == 1 and not any(isinstance(v, dict) for v in filter.values()):
            column, value = next(iter(filter.items()))
            return set(self.indexes.get(table, {}).get(column, {}).get(value, set()))
        
        all_ids = set()
        # For each column in the associated table (get().values()), add all the keys of the entries, ie row ids
        for coldata in self.store.get(table, {}).values():
            # Union
            all_ids |= set(coldata.keys())

        matches = set()
        for rid in all_ids:
            valid = True
            for col, cond in filter.items():
                # Get the table in the store, the the correct column, and then the value associated with the current id
                value = self.store[table].get(col, {}).get(rid)
                if isinstance(cond, dict):
                    for op_name, thr in cond.items():
                        if   op_name == "$gt" and not (value >  thr): valid = False
                        elif op_name == "$lt" and not (value <  thr): valid = False
                else:
                    # Equality condition
                    if value != cond:
                        valid = False
                if not valid:
                    break
            if valid:
                matches.add(rid)
        return matches


    def get(self, table: str, key: Any) -> Optional[Dict[str, Any]]:
        return self._find_one(table, {"_id": key})
    

    def _find_one(self, table: str, filter: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        pass


    def update(self, table: str, filt: Dict[str, Any], update: Dict[str, Any]) -> int:
        pass


    def delete(self, table: str, key_or_filt: Any) -> bool:
        pass


    def query(self, table: str, filt: Dict[str, Any]) -> List[Dict[str, Any]]:
        pass