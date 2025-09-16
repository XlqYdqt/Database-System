from typing import List, Any
from engine.storage_engine import StorageEngine


class DropIndexOperator:
    """
    DROP INDEX 操作的执行算子。
    负责调用底层的 IndexManager 来完成实际的索引删除工作。
    """

    def __init__(self, index_name: str, storage_engine: StorageEngine):
        """
        初始化 DropIndexOperator。

        Args:
            index_name (str): 需要删除的索引名。
            storage_engine (StorageEngine): 存储引擎实例。
        """
        self.index_name = index_name
        self.storage_engine = storage_engine

    def execute(self) -> List[Any]:
        """
        执行删除索引的操作。
        """
        # 遍历所有表的索引管理器，找到拥有该索引的管理器
        found = False
        for table_name in self.storage_engine.index_managers:
            index_manager = self.storage_engine.get_index_manager(table_name)
            if index_manager and self.index_name in index_manager.indexes:
                try:
                    index_manager.drop_index(self.index_name)
                    found = True
                    break
                except Exception as e:
                    raise RuntimeError(f"删除索引 '{self.index_name}' 失败: {e}")

        if not found:
            raise RuntimeError(f"索引 '{self.index_name}' 不存在。")

        return []
