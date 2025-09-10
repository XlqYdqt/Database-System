from typing import List, Tuple, Dict

class Catalog:
    """
    简化版系统目录：管理表的元数据 (schema 等)
    schema 用列表保存，例如:
    [("id", "INT"), ("name", "TEXT"), ("age", "INT")]
    """

    def __init__(self):
        # {table_name: {"schema": [(col_name, col_type), ...]}}
        self.tables: Dict[str, Dict[str, List[Tuple[str, str]]]] = {}

    def create_table(self, table_name: str, schema: List[Tuple[str, str]]):
        """注册一个新表及其 schema"""
        if table_name in self.tables:
            raise RuntimeError(f"Table '{table_name}' already exists")
        self.tables[table_name] = {"schema": schema}

    def get_schema(self, table_name: str) -> List[Tuple[str, str]]:
        """获取表的 schema"""
        if table_name not in self.tables:
            raise RuntimeError(f"Table '{table_name}' does not exist")
        return self.tables[table_name]["schema"]

    def list_tables(self) -> List[str]:
        """返回所有表名"""
        return list(self.tables.keys())

    def drop_table(self, table_name: str):
        """删除表"""
        if table_name not in self.tables:
            raise RuntimeError(f"Table '{table_name}' does not exist")
        del self.tables[table_name]
