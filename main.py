#! -*- encoding: utf-8 -*-
from src import sqlexplainer


if __name__ == "__main__":
    sqlexplainer.explain_sql("""
create table data 
(
    name Varchar(20)
    age int8
    sex bool
)""")
    
