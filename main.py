#! -*- encoding: utf-8 -*-
from src import sqlexplainer
from src import engine
import random


def insert():
    s = engine.BasicDataBaesEngine(sqlexplainer.DB_PATH)
    data = []
    names = ["sunsaijie", "tangjing", "sunyinuo"]
    sexs = [True, False]
    for i in range(1000):
        data.append([
            random.choice(names),
            random.randint(1,20),
            random.choice(sexs)
            ])
    s.insert('data', data)

def query():
    s = engine.BasicDataBaesEngine(sqlexplainer.DB_PATH)
    s.query('data', age=16)
    pass

def delete():
    s = engine.BasicDataBaesEngine(sqlexplainer.DB_PATH)
    s.delete('data', age=15)
    pass

def update():
    s = engine.BasicDataBaesEngine(sqlexplainer.DB_PATH)
    s.update('data', update={"sex": False}, where={"age": 16})
    pass
    

if __name__ == "__main__":
    # delete()
    
    # update()
    query()
    # insert() 

    # s.delete('data', name="tangjing", sex=True)
#     sqlexplainer.explain_sql("""
# create table data 
# (
#     name Varchar(20)
#     age int8
#     sex bool
# )""")
    
