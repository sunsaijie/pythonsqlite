import unittest
from src import sqlexplainer, engine
from src.table import slot_table

from test.test_basic_db import DataSet

personal = 'personal'


class TestSlotTable(unittest.TestCase):
    def test_drop_table(self):
        a = engine.SlotDataBaseEngine(sqlexplainer.DB_PATH)
        a.drop(personal)

    def test_create(self):
        sql = """CREATE TABLE personal
        (
         name varchar(20)
         age int8
         address varchar(100)
         sex varchar(10)     
         is_working bool
        )"""
        sqlexplainer.ENGINE = engine.SlotDataBaseEngine
        sqlexplainer.explain_sql(sql)
        pass

    def test_insert(self):
        data = DataSet.read(10000)
        data = list(data)
        a = engine.SlotDataBaseEngine(sqlexplainer.DB_PATH)
        a.insert(personal, data)

    def test_delete(self):
        a = engine.SlotDataBaseEngine(sqlexplainer.DB_PATH)
        a.delete(personal, age=35)

    def test_query(self):
        a = engine.SlotDataBaseEngine(sqlexplainer.DB_PATH)
        a.query(personal, age=36)

    def test_update(self):
        a = engine.SlotDataBaseEngine(sqlexplainer.DB_PATH)
        a.update(personal, where={"age":36}, update={"name": "sunsaijie"})



