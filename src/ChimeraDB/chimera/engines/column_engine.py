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
        # Load snapshot and WAL
        snapshot = self.snapshot_mgr.load('latest')
        self.store = snapshot if snapshot else {}

        # Replay WAL
        for op in self.wal.replay():
            self._apply(op)

        # Build indexes from current store
        self.indexes = {}
        for table, columns in self.store.items():
            self.indexes.setdefault(table, {})
            for column, values in columns.items():
                for row_id, value in values.items():
                    self._add_to_index(table, column, row_id, value)

        self.wal.rotate()

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
        table = op['table']
        self.store.setdefault(table, {})
        self.indexes.setdefault(table, {})

        if op['operation'] == 'INSERT':
            row_id = op['row_id']
            row_data = op['row_data']
            
            # Remove stale index entries if overwriting existing row
            if row_id in self.store[table].get('_id', {}):
                for col, val in self.store[table].items():
                    if col in self.indexes[table]:
                        self._remove_from_index(table, col, row_id, val.get(row_id))
            
            # Insert new data
            for col, val in row_data.items():
                self.store[table].setdefault(col, {})[row_id] = val
                self._add_to_index(table, col, row_id, val)

        elif op['operation'] == 'UPDATE':
            row_id = op['row_id']
            updates = op['updates']
            
            for col, new_val in updates.items():
                old_val = self.store[table].get(col, {}).get(row_id)
                self.store[table].setdefault(col, {})[row_id] = new_val
                
                # Update index
                if col in self.indexes[table]:
                    if old_val is not None:
                        self._remove_from_index(table, col, row_id, old_val)
                    self._add_to_index(table, col, row_id, new_val)

        elif op['operation'] == 'DELETE':
            row_id = op['row_id']
            
            # Remove from all columns and indexes
            for col, values in self.store[table].items():
                if row_id in values:
                    old_val = values[row_id]
                    del values[row_id]
                    if col in self.indexes[table]:
                        self._remove_from_index(table, col, row_id, old_val)

    def put(self, table: str, key: Any, row: Dict[str, Any]) -> None:
        self._validate_table_id(table, key)
        
        # Ensure row has _id field
        row['_id'] = key
        
        op = {'operation': 'INSERT', 'table': table, 'row_id': key, 'row_data': row}
        with self.lock:
            self._apply(op)
            self.wal.append(op)

    def _add_to_index(self, table: str, column: str, row_id: Any, value: Any) -> None:
        col_idx = self.indexes.setdefault(table, {}).setdefault(column, {})
        col_idx.setdefault(value, set()).add(row_id)

    def _remove_from_index(self, table: str, column: str, row_id: Any, value: Any) -> None:
        col_idx = self.indexes.get(table, {}).get(column, {})
        ids = col_idx.get(value)
        if ids:
            ids.discard(row_id)
            if not ids:
                col_idx.pop(value, None)

    def _filter_row_ids(self, table: str, filter: Dict[str, Any]) -> set:
        if len(filter) == 1 and not any(isinstance(v, dict) for v in filter.values()):
            column, value = next(iter(filter.items()))
            return set(self.indexes.get(table, {}).get(column, {}).get(value, set()))
        
        all_ids = set()
        # For each column in the associated table, add all the keys of the entries, ie row ids
        for coldata in self.store.get(table, {}).values():
            # Union
            all_ids |= set(coldata.keys())

        matches = set()
        for rid in all_ids:
            valid = True
            for col, cond in filter.items():
                # Get the table in the store, then the correct column, and then the value associated with the current id
                value = self.store[table].get(col, {}).get(rid)
                if isinstance(cond, dict):
                    for op_name, thr in cond.items():
                        if   op_name == "$gt" and not (value >  thr): valid = False
                        elif op_name == "$gte" and not (value >= thr): valid = False
                        elif op_name == "$lt" and not (value <  thr): valid = False
                        elif op_name == "$lte" and not (value <= thr): valid = False
                        elif op_name == "$ne" and not (value != thr): valid = False
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
        with self.lock:
            row_ids = self._filter_row_ids(table, filter)
            if not row_ids:
                return None
            
            # Return first match as a row
            row_id = next(iter(row_ids))
            result = {}
            for col, values in self.store.get(table, {}).items():
                if row_id in values:
                    result[col] = values[row_id]
            return result

    def update(self, table: str, filt: Dict[str, Any], update: Dict[str, Any]) -> int:
        self._validate_table_id(table, filt.get('_id', None))
        
        with self.lock:
            row_ids = self._filter_row_ids(table, filt)
            count = 0
            
            for row_id in row_ids:
                op = {'operation': 'UPDATE', 'table': table, 'row_id': row_id, 'updates': update}
                self._apply(op)
                self.wal.append(op)
                count += 1
                
        return count

    def delete(self, table: str, key_or_filt: Any) -> bool:
        if isinstance(key_or_filt, dict):
            filter_dict = key_or_filt
        else:
            filter_dict = {"_id": key_or_filt}
            
        with self.lock:
            row_ids = self._filter_row_ids(table, filter_dict)
            if not row_ids:
                return False
                
            for row_id in row_ids:
                op = {'operation': 'DELETE', 'table': table, 'row_id': row_id}
                self._apply(op)
                self.wal.append(op)
                
            return True

    def query(self, table: str, filt: Dict[str, Any]) -> List[Dict[str, Any]]:
        with self.lock:
            row_ids = self._filter_row_ids(table, filt)
            results = []
            
            for row_id in row_ids:
                row = {}
                for col, values in self.store.get(table, {}).items():
                    if row_id in values:
                        row[col] = values[row_id]
                results.append(row)
                
            return results