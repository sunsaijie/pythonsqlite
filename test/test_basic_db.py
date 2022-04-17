import unittest
import os
import csv
import random

from unittest.mock import patch

from src.engine import BasicDataBaesEngine
from src import sqlexplainer
from src import config
from test.data.dataset import DataSet


DB_PATH = os.path.join(config.PROJECT_ROOT, "data")
DATASET_PATH = os.path.join(config.PROJECT_ROOT, 'test', 'data', "dataset.csv")


class TestBasicEngine(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.basic_engine = BasicDataBaesEngine(DB_PATH)

    # @unittest.skip
    def test_init_data(self):
        """初始化数据表"""
        sql = """CREATE TABLE personal_basic
        (
         name varchar(20)
         age int8
         address varchar(100)
         sex varchar(10)     
         is_working bool
        )"""
        sqlexplainer.ENGINE = BasicDataBaesEngine
        sqlexplainer.DB_PATH = DB_PATH
        sqlexplainer.explain_sql(sql)

    def test_insert(self):
        data = DataSet.read(10000)
        self.basic_engine.insert("personal_basic", data)

    def test_query(self):
        self.basic_engine.query("personal_basic", age=26)

    def test_delete(self):
        self.basic_engine.delete("personal_basic", age=25)
        self.basic_engine.query("personal_basic", age=25)

    def test_update(self):
        self.basic_engine.query("personal_basic", age=26)
        self.basic_engine.update("personal_basic", update={"is_working": False}, where={"age": 26})
        self.basic_engine.query("personal_basic", age=26)

