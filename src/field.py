import os
import abc
import re
import struct


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

    @property
    def bit(self):
        return self._bit
        # 20200014764454

    @abc.abstractmethod
    def binarizate(self, value):
        pass

    @abc.abstractmethod
    def debinarizate(self, value):
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

    def binarizate(self, value):
        encode_value = value.encode('utf-8')
        return struct.pack("<{}s".format(self._bit), encode_value)

    def debinarizate(self, value):
        binary_pack = struct.unpack("<{}s".format(self._bit), value)[0]
        return binary_pack.decode('utf-8').replace("\x00", "")


@_add_fields
class IntField(Field):
    @classmethod
    def from_sql(cls, sql):
        ret = re.search(r"(\w+)\s+int(\d+)", sql, re.I)
        if not ret:
            return False
        name = ret.group(1)
        bit = ret.group(2)
        return cls(name, 1)
    
    def to_sql(self):
        return f"{self._name} int{self._bit}"
    
    def check_and_convert(self, value):
        convert_value = int(value)
        return convert_value
    
    def read_from_db(self, value):
        return int(value)

    def binarizate(self, value):
        return struct.pack("<B", value)

    def debinarizate(self, value):
        return struct.unpack("<B", value)[0]


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

    def binarizate(self, value):
        return struct.pack("<B", value)

    def debinarizate(self, value):
        return struct.unpack("<B", value)[0]

def transfor_to_field(sql):
    for field_parser in _FIELDS:
        obj = field_parser.from_sql(sql)
        if obj:
            return obj
    raise Exception(f"{sql} 不能识别为字段")
    



