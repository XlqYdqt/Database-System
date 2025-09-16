from typing import List, Any
from engine.storage_engine import StorageEngine


class CreateIndexOperator:
    """
    CREATE INDEX 操作的执行算子。
    负责解析创建索引的逻辑计划，并调用底层的 IndexManager 来完成实际的创建工作。
    """

    def __init__(self, table_name: str, storage_engine: StorageEngine, index_name: str = None,
                 columns: List[str] = None, unique: bool = False, **kwargs):
        """
        初始化 CreateIndexOperator。
        [FIX] 调整构造函数以灵活处理来自执行器的不同参数传递方式。

        Args:
            table_name (str): 需要创建索引的表名。
            storage_engine (StorageEngine): 存储引擎实例。
            index_name (str): 要创建的索引的名称。
            columns (List[str]): 索引包含的列名列表。
            unique (bool): 索引是否是唯一索引。
        """
        self.table_name = table_name
        self.storage_engine = storage_engine

        # 从提供的参数或关键字参数中获取必需的信息
        self.index_name = index_name or kwargs.get('index_name')
        self.columns = columns or kwargs.get('columns')
        self.is_unique = unique or kwargs.get('unique', False)

        # 添加验证，确保关键信息已提供
        if not self.index_name or not self.columns:
            raise ValueError("CreateIndexOperator未能获取到必需的 'index_name' 和 'columns' 参数。")

    def execute(self) -> List[Any]:
        """
        执行创建索引的操作。
        """
        # 1. 从存储引擎获取对应表的索引管理器
        index_manager = self.storage_engine.get_index_manager(self.table_name)
        if not index_manager:
            raise RuntimeError(f"无法找到表 '{self.table_name}' 的索引管理器。")

        # 2. 调用索引管理器的 create_index 方法
        try:
            index_manager.create_index(
                index_name=self.index_name,
                columns=self.columns,
                is_unique=self.is_unique
            )
        except Exception as e:
            # 将底层错误包装成对用户更友好的信息
            raise RuntimeError(f"为表 '{self.table_name}' 创建索引 '{self.index_name}' 失败: {e}")

        return []

