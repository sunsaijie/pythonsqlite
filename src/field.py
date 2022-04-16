import os
import abc
import re


_FIELDS = []

def _add_fields(cls):
    global _FIELDS
    _FIELDS.append(cls)


class Field(abc.ABC):
    def __init__(self, name, bit):
        self._name = name
        self._bit = bit
    
    @classmethod
    @abc.abstractmethod
    def from_sql(cls, sql):
        pass


@_add_fields
class VarCharField(Field):
    @classmethod
    def from_sql(cls, sql):
        ret = re.search(r"(\w+)\s+varchar\((\d+)\)", sql, re.I)
        if not ret:
            return False
        name = ret.group(1)
        bit = ret.group(2)
        print(cls)
        return cls(name, bit)


@_add_fields
class IntField(Field):
    @classmethod
    def from_sql(cls, sql):
        ret = re.search(r"(\w+)\s+int(\d+)", sql, re.I)
        if not ret:
            return False
        name = ret.group(1)
        bit = ret.group(2)
        return cls(name, bit)
    

@_add_fields
class BoolField(Field):
    @classmethod
    def from_sql(cls, sql):
        ret = re.search(r"(\w+)\s+bool", sql, re.I)
        if not ret:
            return False
        name = ret.group(1)
        bit = 1
        return cls(name, bit)


def transfor_to_field(sql):
    for field_parser in _FIELDS:
        obj = field_parser.from_sql(sql)
        if obj:
            return obj
    raise Exception(f"{sql} 不能识别为字段")
    



