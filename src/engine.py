import abc
import os
import shutil
from typing import List, Any

from src.field import Field
from src import table


class DataBaseEngine(abc.ABC):
    @abc.abstractmethod
    def create(self, table_name: str, fields: List[Field]):
        pass
    
    @abc.abstractmethod
    def drop(self, table_name: str):
        pass

    # @abc.abstractmethod
    def query(self, table_name: str, select: str="*", where=None):
        pass

    # @abc.abstractmethod
    def delete(self, table_name: str, where=None):
        pass

    # @abc.abstractmethod
    def update(self, table_name: str, where=None):
        pass

    # @abc.abstractmethod
    def insert(self, table_name: str, data:List[List[Any]]):
        pass



class BasicDataBaesEngine(DataBaseEngine):
    def __init__(self, db_root: str):
        self._db_root = db_root
    
    def create(self, table_name: str, fields: List[Field]):
        tb_model = table.DataTable(table_name, fields, self._db_root)
        tb_model.create_dir_struct()
    
    def drop(self, table_name: str):
        table_path = os.path.join(self._db_root, table_name)
        if os.path.exists(table_path):
            decide = input("是否需要删除数据表(y/n)?")
            if decide.lower() == 'y':
                shutil.rmtree(table_path)
    
    def insert(self, table_name: str, data:List[List[Any]]):
        tb_model = table.DataTable.loading_model(table_name, self._db_root)
        tb_model.insert(data)
    
    def delete(self, table_name: str, **kwargs):
        tb_model = table.DataTable.loading_model(table_name, self._db_root)
        tb_model.delele(**kwargs)
    
    def update(self, table_name: str, update=None, where=None):
        tb_model = table.DataTable.loading_model(table_name, self._db_root)
        tb_model.update(update=update, where=where)
    
    def query(self, table_name: str, **kwargs):
        tb_model = table.DataTable.loading_model(table_name, self._db_root)
        for line in tb_model.query(**kwargs):
            print(line)
        

        

    
    
            
        
    







