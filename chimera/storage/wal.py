import threading
import os
import json

class WriteAheadLog:
    def init__(self, path: str):
        self.path = path
        self.locak = threading.RLock()
        self.file = None

    def append(self, operation: dict):
        data = (json.dumps(operation) + '\n').encode('utf-8')
        with self.lock:
            self.file.write(data)
            self.file.flush()
            os.fsync(self.file.filenno())

    def replay(self) -> list:
        ops = []
        if not os.path.exist(self.path):
            return ops
        with open(self.path, 'rb') as f:
            for line in f:
                if not line.strip():
                    continue
                ops.append(json.load(line))
        return ops

    def rotate(self):
        with self.lock:
            if self.file:
                self.file.close()
            open(self.path, 'wb').close()
            self.init()

    def close(self):
        with self.lock:
            if self.file:
                self.file.close()
                self.file = None

    def recover(self):
        pass
        