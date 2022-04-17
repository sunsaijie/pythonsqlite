import os
from .basic_table import DataTableInterface

class SlotTable(DataTableInterface):
    """
    实现的额外功能
    1. 每一行， 每一个字段 按照固定字节数已二进制方式存储到文件
    2. 实现一个增量的slot，记录操作， 这样实现文件的增加操作
    3. 定时进行重新合并
    """
    def create_dir_struct(self):
        self._init_db_root()
        self._init_data()
        self._init_meta()
        self._init_cursor()
        self._init_slot()


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

    def _init_cursor(self):
        self._set_cursor(0)

    def _init_slot(self):
        slot_path = os.path.join(self._tb_path, "slot")
        with open(slot_path, 'w') as fp:
            fp.write("")

    @property
    def line_bit(self):
        return sum(x.bit for x in self._fields)

    def insert(self, data):
        content = b''
        for item in data:
            for field, value in zip(self._fields, item):
                after_check_value = field.check_and_convert(value)
                binary_pack = field.binarizate(after_check_value)
                content += binary_pack
        cursor = self._get_cursor()

        # 根据数据的条数，和每条的记录所占的字节数，
        # 直接算出下一次插入的起始cursor
        last_cursor = len(data) * self.line_bit + cursor

        data_path = os.path.join(self._tb_path, "data")

        with open(data_path, 'wb') as fp:
            fp.seek(cursor, 0)
            fp.write(content)

        self._set_cursor(last_cursor)

        # 写入slot
        first_slot = cursor // self.line_bit
        last_slot = first_slot + len(data)
        slot_path = os.path.join(self._tb_path, "slot")
        with open(slot_path, 'a') as fp:
            for slot in range(first_slot, last_slot):
                fp.write("{}:V\n".format(slot))

    def _condition_query_table(self, **kwargs):
        # 表里面是一个元组，代表要跳的位数, 正的代表要取出来做判断，
        # 负的代表不需要取出来，需做seek
        # 第二个值是要用来匹配的
        query_table = []

        for field in self._fields:
            if field.name in kwargs:
                value = kwargs.get(field.name)
                query_table.append((field.bit, value))
            else:
                query_table.append((-field.bit, None))
        return query_table

    def delete(self, **kwargs):
        if not kwargs:
            return

        query_table = self._condition_query_table(**kwargs)

        cursor_index = self._get_cursor()

        record_line_no = 0

        delete_line_idx = []
        data = os.path.join(self._tb_path, 'data')
        with open(data, 'rb') as fp:
            while True:
                if fp.tell() >= cursor_index:
                    break
                for filed_idx, (bit, value) in enumerate(query_table):
                    if bit > 0:
                        read_content = self._fields[filed_idx].debinarizate(fp.read(bit))
                        if read_content == value:
                            delete_line_idx.append(record_line_no)
                        else:
                            pass
                    else:
                        fp.seek(-bit, 1)
                record_line_no += 1

        slot_path = os.path.join(self._tb_path, "slot")
        with open(slot_path, 'a') as fp:
            for slot in delete_line_idx:
                fp.write("{}:D\n".format(slot))

    def query(self, **kwargs):
        query_sets = self._query(**kwargs)

        result = []
        for _, binary_line in query_sets:
            line = []
            cur_index = 0
            for field in self._fields:
                binary_part = binary_line[cur_index: cur_index + field.bit]
                line.append(field.debinarizate(binary_part))
                cur_index += field.bit
            result.append(line)
        return result

    def update(self, update=None, where=None):
        query_set = self._query(**where)
        

    def _query(self, **kwargs):
        """根据条件查询满足条件的列表，
        列表每一个元素为行号，行的二进制表示
        :param kwargs:
        :return:
        """
        slot = os.path.join(self._tb_path, "slot")
        with open(slot, 'r') as fp:
            slot_lines = fp.readlines()
            slot_lines = [x.split(":") for x in slot_lines if x.strip()]

        delete_ids = [int(x[0]) for x in slot_lines if x[1].strip().lower() == "d"]
        query_table = self._condition_query_table(**kwargs)

        cursor_index = self._get_cursor()
        record_line_no = 0
        data = os.path.join(self._tb_path, 'data')

        # 查询结果，包括行号和这一行的二进制内容的元组
        query_sets = []

        with open(data, 'rb') as fp:
            while True:
                if fp.tell() >= cursor_index:
                    break
                # 如果已经被删除，则直接跳过
                if record_line_no in delete_ids:
                    record_line_no += 1
                    fp.seek(self.line_bit)
                    continue
                line_binary = fp.read(self.line_bit)

                line_cur_index = 0
                for field_idx, (bit, value) in enumerate(query_table):
                    if bit > 0:
                        binary_part = line_binary[line_cur_index: line_cur_index + bit]
                        read_contant = self._fields[field_idx].debinarizate(binary_part)
                        if read_contant == value:
                            query_sets.append((record_line_no, line_binary))
                        else:
                            pass
                        line_cur_index += bit
                    else:
                        line_cur_index += (-bit)
                record_line_no += 1
        return query_sets




