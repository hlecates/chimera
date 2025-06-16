import threading
import os
import json
import pickle
import base64

from storage.wal import WriteAheadLog
from engine_interface import StorageEngineInterface



class KeyValueEngine(StorageEngineInterface):
    def __init__(
            self, 
            wal_path: str, 
            snap_path: str,
            max_collection_name_len: int = 128,
            max_key_name_len: int = 256,
            max_value_size: int = 10 * 1024 * 1024
            ):
        
        self.wal_path = wal_path
        self.snap_path = snap_path

        self.max_collection_name_len = max_collection_name_len
        self.max_key_name_len = max_key_name_len
        self.max_value_size = max_value_size

        self.store = {}

        self.lock = threading.RLock()

        self.wal = WriteAheadLog(wal_path)

    def startup(self):
        pass

    def shutdown(self):
        pass

    def recover(self):
        pass

    def _validate_collection_key(self, collection: str, key: str):
        if not isinstance(collection, str) or not collection:
            raise ValueError("Collection name must be a non-empty string")
        if len(collection) > self.max_collection_name_len:
            raise ValueError(f"Collection name exceeds {self.max_collection_name_len} characters")
        if not isinstance(key, str) or not key:
            raise ValueError("Key must be a non-empty string")
        if len(key) > self.max_key_name_len:
            raise ValueError(f"Key exceeds {self.max_key_name_len} characters")


    def put(self, collection: str, key: str, value: bytes):
        self._validate_collection_key(collection, key)
        if not isinstance(value, (bytes, bytearray)):
            raise ValueError("Value must be of type bytes")
        if len(value) > self.max_value_size:
            raise ValueError(f"Value exceeds {self.max_value_size} bytes")

        entry = {'operation': 'PUT', 'collection': collection, 'key': key, 'value': base64.b64encode(value).decode('ascii')}

        with self.lock:
            self.wal.append(entry)
            self.store.setdefault(collection, {})[key] = value 

    def get(self, collection: str, key: str) -> bytes:
        self._validate_collection_key(collection, key)

        with self.lock:
            return self.store.get(collection, {}).get(key)

    def delete(self, collection: str, key: str):
        self._validate_collection_key(collection, key)

        entry = {'operation': 'DELETE', 'collection': collection, 'key': key}

        with self.lock:
            self.wal.append(entry)
            return self.store.get(collection, {}).pop(key, None) is not None

    def query(self, collection: str, filter: dict):
        raise NotImplementedError("Query not supported by this engine")