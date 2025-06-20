import threading
import os
import json

class WriteAheadLog:
    def __init__(self, path: str):
        self.path = path
        self.lock = threading.RLock()
        # ensure directory exists
        directory = os.path.dirname(path)
        if directory and not os.path.isdir(directory):
            os.makedirs(directory, exist_ok=True)
        # open file in append+binary mode
        self.file = open(self.path, 'ab+')

    def append(self, operation: dict):
        data = (json.dumps(operation) + '\n').encode('utf-8')
        with self.lock:
            self.file.write(data)
            self.file.flush()
            os.fsync(self.file.fileno())

    def replay(self) -> list:
        ops = []
        if not os.path.exists(self.path):
            return ops
        with open(self.path, 'rb') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                ops.append(json.loads(line.decode('utf-8')))
        return ops

    def rotate(self):
        with self.lock:
            if self.file:
                self.file.close()
            # clear the log
            open(self.path, 'wb').close()
            # reopen
            self.file = open(self.path, 'ab+')

    def close(self):
        with self.lock:
            if self.file:
                self.file.close()
                self.file = None

    def recover(self):
        pass