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
        self._bit = int(bit)
    
    @classmethod
    @abc.abstractmethod
    def from_sql(cls, sql):
        pass
    
    @abc.abstractmethod
    def to_sql(self):
        pass
    
    @abc.abstractmethod
    def check_and_convert(self, value):
        pass

    @abc.abstractmethod
    def read_from_db(self, value):
        pass
    
    @property
    def name(self):
        return self._name

@_add_fields
class VarCharField(Field):
    @classmethod
    def from_sql(cls, sql):
        ret = re.search(r"(\w+)\s+varchar\((\d+)\)", sql, re.I)
        if not ret:
            return False
        name = ret.group(1)
        bit = ret.group(2)
        return cls(name, bit)
    
    def to_sql(self):
        return f"{self._name} varchar({self._bit})"
    
    def check_and_convert(self, value):
        convert_value = str(value)
        if len(convert_value) > self._bit:
            return convert_value[:self._bit]
        return convert_value
    
    def read_from_db(self, value):
        return str(value)


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
    
    def to_sql(self):
        return f"{self._name} int{self._bit}"
    
    def check_and_convert(self, value):
        convert_value = int(value)
        return convert_value
    
    def read_from_db(self, value):
        return int(value)
    

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
    
    def to_sql(self):
        return f"{self._name} bool"

    def check_and_convert(self, value):
        convert_value = bool(value)
        return 1 if convert_value else 0
    
    def read_from_db(self, value):
        if value in [0, "0"]:
            return False
        return True

def transfor_to_field(sql):
    for field_parser in _FIELDS:
        obj = field_parser.from_sql(sql)
        if obj:
            return obj
    raise Exception(f"{sql} 不能识别为字段")
    



