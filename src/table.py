import abc
import os
from collections import namedtuple
from typing import List, Tuple, Any
from src.field import Field, transfor_to_field


LineWithNo = namedtuple("LineNo", field_names=['line_no', "line"])

class DataTableInterface(abc.ABC):
    def __init__(self, tb_name: str, fields: List[Field], db_root: str):
        self._tb_name = tb_name
        self._fields = fields
        self._db_root = db_root
        self._tb_path = os.path.join(self._db_root, self._tb_name)

    @abc.abstractmethod
    def create_dir_struct(self):
        pass


class DataTable(DataTableInterface):
    @classmethod
    def loading_model(cls, tb_name, tb_root):
        meta = os.path.join(tb_root, tb_name, "meta")
        with open(meta, 'r') as fp:
            lines = fp.readlines()
        fields_strs = [x.strip() for x in lines if x.strip()]
        fields = [transfor_to_field(x)  for x in fields_strs]
        return cls(tb_name, fields, tb_root)
    
    def create_dir_struct(self):
        if os.path.exists(self._tb_path):
            raise Exception(f"{self._tb_name}已经存在")
        os.makedirs(self._tb_path)
        self._init_data()
        self._init_meta()
        self._init_slot()
        self._init_cursor()
    
    def _init_meta(self):
        meta_path = os.path.join(self._tb_path, "meta")
        with open(meta_path, 'w') as fp:
            for field in self._fields:
                fp.write(field.to_sql() + "\n")
        
    def _init_cursor(self):
        self._set_cursor(0)


    def _init_data(self):
        data_path = os.path.join(self._tb_path, "data")
        with open(data_path, 'w') as fp:
            fp.write("")


    def _init_slot(self):
        slot_path = os.path.join(self._tb_path, "slot")
        with open(slot_path, 'w') as fp:
            fp.write("")
    
    def _get_cursor(self):
        cursor_path = os.path.join(self._tb_path, "cursor")
        with open(cursor_path, 'r') as fp:
            index = fp.read()
        return int(index)
    
    def _set_cursor(self, index: int):
        cursor_path = os.path.join(self._tb_path, "cursor")
        with open(cursor_path, 'w') as fp:
            fp.write(str(index))
        return index
        
    
    def insert(self, data):
        convert_data = []
        for line in data:
            convert_data.append([x.check_and_convert(y) for x, y in zip(self._fields, line)])
        
        start_index = self._get_cursor()
        end_index = start_index + len(convert_data)

        data_path = os.path.join(self._tb_path, "data")
        with open(data_path, 'a') as fp1:
            for line in convert_data:
                csv_line = ",".join([str(x) for x in line]) + "\n"
                fp1.write(csv_line)
        
        slot_path = os.path.join(self._tb_path, 'slot')
        
        with open(slot_path, 'a') as fp:
            for index in range(start_index, end_index):
                fp.write("{}:V\n".format(index))
        
        self._set_cursor(end_index)

    def query(self, **kwargs):
        read_data = self._read_data(**kwargs)
        return [x.line for x in read_data]
    
    def _read_data(self, **kwargs) -> List[LineWithNo]:
        read_data = []

        index_where = {}
        for key, value in kwargs.items():
            for index, field in enumerate(self._fields):
                if key == field.name:
                    index_where[index] = value 
        
        data_path = os.path.join(self._tb_path, "data")
        with open(data_path, 'r') as fp1:
            for line_no, line in enumerate(fp1.readlines()):
                value_str_list = line.strip().split(",")
                read_value_list = [x.read_from_db(y) 
                        for x, y in zip(self._fields, value_str_list)]
                if not index_where:
                    read_data.append(LineWithNo(line_no, read_value_list))
                else:
                    for field_index, value in index_where.items():
                        read_value = read_value_list[field_index]
                        if read_value == value:
                            read_data.append(LineWithNo(line_no, read_value_list))
                            break
        return read_data
         
    
    def delele(self, **kwargs):
        if not kwargs:
            return

        read_data = self._read_data(**kwargs)
        delete_idx = [x.line_no for x in read_data]

        data_path = os.path.join(self._tb_path, "data")
        with open(data_path, 'r') as fp1:
            lines = fp1.readlines()
        
        new_lines = []
        for idx, line in enumerate(lines):
            if idx in delete_idx:
                continue
            new_lines.append(line)

        with open(data_path, 'w') as fp2:
            fp2.writelines(new_lines)
    
    def update(self, update=None, where=None):
        if not update:
            return
        if not where:
            return
        
        update_index_mapping = {}
        for key, value in update.items():
            for field_idx, field in enumerate(self._fields):
                if field.name == key:
                    update_index_mapping[field_idx] = value
        
        read_data = self._read_data(**where)
        
        for line_with_no in read_data:
            for filed_idx, value in update_index_mapping.items():
                filed_obj = self._fields[filed_idx]
                db_value = filed_obj.check_and_convert(value)
                line_with_no.line[filed_idx] = db_value
        
        data_path = os.path.join(self._tb_path, "data")
        with open(data_path, 'r') as fp1:
            lines = fp1.readlines()
        
        for update_no, update_line in read_data:
            lines[update_no] = ",".join([ str(x) for x in update_line]) + "\n"
        
        with open(data_path, 'w') as fp2:
            fp2.writelines(lines)
        


            

    
    

