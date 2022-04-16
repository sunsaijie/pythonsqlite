import abc
import re
from src import field


_BEHAVIERS = []

def _add_behavier(cls):
    global _BEHAVIERS
    _BEHAVIERS.append(cls)


class Behavier(abc.ABC):
    def __init__(self, sql):
        self._sql = sql

    @abc.abstractmethod
    def is_behavier(self):
        pass

    @abc.abstractmethod
    def do(self):
        pass


@_add_behavier
class CreateTable(Behavier):
    def is_behavier(self):
        """
        create table xxx 
        (
           name varchar(20)
           age int8
           sex bool
        )
        """
        print(self._sql)
        return re.match(r"\s*create\s+table\s+(\w+).*?\((.*)\)", self._sql, re.I | re.DOTALL)
    

    def do(self):
        ret = re.search(r"\s*create\s+table\s+(\w+).*?\((.*)\)", self._sql, re.I | re.DOTALL)
        tb_name = ret.group(1)
        origin_lines = ret.group(2).splitlines()
        strip_lines = map(lambda x: x.strip(), origin_lines)
        non_blank_lines = filter(lambda x: x, strip_lines)
        print(tb_name)
        for i in non_blank_lines:
            a = field.transfor_to_field(i)
            print(a)


def explain_sql(sql):
    for behavier_cls in _BEHAVIERS:
        behavier = behavier_cls(sql)
        if behavier.is_behavier():
            behavier.do()
            break
    else:
        raise Exception(f"{sql}不能识别")
    


