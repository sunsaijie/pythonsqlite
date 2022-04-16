import abc
from typing import List, Any

from src.field import Field


class DataBaseEngine(abc.ABC):
    @abc.abstractmethod
    def create(self, table_name: str, fields: List[Field]):
        pass
    
    @abc.abstractmethod
    def drop(self, table_name: str):
        pass

    @abc.abstractmethod
    def query(self, table_name: str, select: str="*", where=None):
        pass

    @abc.abstractmethod
    def delete(self, table_name: str, where=None):
        pass

    @abc.abstractmethod
    def update(self, table_name: str, where=None):
        pass

    @abc.abstractmethod
    def insert(self, table_name: str, data:List[List[Any]]):
        pass

