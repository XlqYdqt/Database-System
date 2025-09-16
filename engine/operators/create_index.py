from typing import List, Any
from engine.storage_engine import StorageEngine

class CreateIndexOperator:
    """
    CREATE INDEX 操作的执行算子。
    负责解析创建索引的逻辑计划，并调用底层的 IndexManager 来完成实际的创建工作。
    """

    def __init__(self, table_name: str, column_name: str, is_unique: bool, storage_engine: StorageEngine):
        """
        初始化 CreateIndexOperator。

        Args:
            table_name (str): 需要创建索引的表名。
            column_name (str): 需要创建索引的列名。
            is_unique (bool): 索引是否是唯一索引。
            storage_engine (StorageEngine): 存储引擎实例。
        """
        self.table_name = table_name
        self.column_name = column_name
        self.is_unique = is_unique
        self.storage_engine = storage_engine

    def execute(self) -> List[Any]:
        """
        执行创建索引的操作。
        """
        # 1. 从存储引擎获取对应表的索引管理器
        index_manager = self.storage_engine.get_index_manager(self.table_name)
        if not index_manager:
            raise RuntimeError(f"无法找到表 '{self.table_name}' 的索引管理器。")

        # 2. 调用索引管理器的 create_index 方法
        # 这个方法会处理B+树的创建、数据的填充以及元数据的更新
        try:
            index_manager.create_index(self.column_name, self.is_unique)
        except Exception as e:
            # 将底层错误包装成对用户更友好的信息
            raise RuntimeError(f"为表 '{self.table_name}' 的列 '{self.column_name}' 创建索引失败: {e}")

        return []
