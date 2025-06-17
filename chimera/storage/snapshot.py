import os
import threading
import pickle

class SnapShotManager:
    def __init__(self, path: str):
        self.base_path = path
        self.lock = threading.Rlock()

    def create(self, name: str, data: dict):
        tmp = f"{self.base_path}.{name}.tmp"
        final = f"{self.base_path}.{name}"
        with self.lock:
            with open(tmp, 'wb') as f:
                pickle.dump(data, f)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, final)

    def load(self, name: str) -> dict:
        file = f"{self.base_path}.{name}"
        if not os.path.exists(file):
            return {}
        with open(file , 'rb') as f:
            return pickle.load(f)

    def delete(self, name):
        file = f"{self.base_path}.{name}"
        if os.path.exists(file):
            os.remove(file)