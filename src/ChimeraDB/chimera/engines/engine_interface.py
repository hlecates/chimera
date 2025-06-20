from abc import ABC, abstractmethod

class EngineInterface(ABC):
    @abstractmethod
    def startup(self):
        raise NotImplementedError

    @abstractmethod
    def shutdown(self):
        raise NotImplementedError
    
    @abstractmethod
    def recover(self):
        raise NotImplementedError
    
    @abstractmethod
    def put(self, collection: str, key: str, value: bytes):
        raise NotImplementedError
    
    @abstractmethod
    def get(self, collection: str, key: str) -> bytes:
        raise NotImplementedError
    
    @abstractmethod
    def delete(self, collection: str, key: str):
        raise NotImplementedError
    
    @abstractmethod
    def query(self, collection: str, filter: dict):
        raise NotImplementedError("Query not suppoerted by this engine")