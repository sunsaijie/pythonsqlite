import abc
from typing import List
from src.field import Field

class DataTableInterface(abc.ABC):
    @abc.abstractmethod
    def create_dir_struct(self):
        pass


class DataTable(DataTableInterface):
    def __init__(self, tb_name: str, fields: List[Field], db_root: str):
        self._tb_name = tb_name
        self._fields = fields
        self._db_root = db_root
    
    def create_dir_struct(self):
        pass
    

