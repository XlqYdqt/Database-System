from typing import Dict, Tuple

class CatalogPage:
    """
    CatalogPage 存储在 page_id=0，作为所有表的元数据目录。
    它负责管理表的堆根页面ID和索引根页面ID。
    """
    def __init__(self):
        # {table_name: {'heap_root_page_id': int, 'index_root_page_id': int}}
        self.tables: Dict[str, Dict[str, int]] = {}

    def add_table(self, table_name: str, heap_root_page_id: int, index_root_page_id: int):
        """添加一个新表的元数据"""
        if table_name in self.tables:
            raise RuntimeError(f"Table '{table_name}' already exists in CatalogPage")
        self.tables[table_name] = {
            'heap_root_page_id': heap_root_page_id,
            'index_root_page_id': index_root_page_id
        }

    def get_table_metadata(self, table_name: str) -> Dict[str, int]:
        """获取表的元数据"""
        if table_name not in self.tables:
            raise RuntimeError(f"Table '{table_name}' not found in CatalogPage")
        return self.tables[table_name]

    def drop_table(self, table_name: str):
        """从目录中删除表的元数据"""
        if table_name not in self.tables:
            raise RuntimeError(f"Table '{table_name}' not found in CatalogPage")
        del self.tables[table_name]

    def list_tables(self) -> Dict[str, Dict[str, int]]:
        """返回所有表的元数据"""
        return self.tables

    def serialize(self) -> bytes:
        """将 CatalogPage 序列化为字节，以便写入磁盘"""
        # 这是一个简化的序列化，实际中可能需要更复杂的结构来处理变长字符串和字典
        import json
        return json.dumps(self.tables).encode('utf-8')

    @staticmethod
    def deserialize(data: bytes):
        """从字节反序列化 CatalogPage"""
        import json
        catalog_page = CatalogPage()
        catalog_page.tables = json.loads(data.decode('utf-8'))
        return catalog_page