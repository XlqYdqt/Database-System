from typing import Any, List

from sql.ast import Operator
from sql.planner import Delete
from engine.storage_engine import StorageEngine

class DeleteOperator:
    def __init__(self, table_name: str, child: Operator, storage_engine: StorageEngine, executor: Any, bplus_tree: Any):
        self.table_name = table_name
        self.child = child
        self.storage_engine = storage_engine
        self.executor = executor
        self.bplus_tree = bplus_tree

    def execute(self) -> List[Any]:
        deleted_rows = []
        for row in self.executor.execute(self.child):
            # row 是一个字典，包含所有列的值
            # 假设每行都有一个唯一的 'id' 字段作为主键
            # 需要根据实际情况确定主键列名
            primary_key_value = row.get('id') # 假设 'id' 是主键
            if primary_key_value is None:
                raise ValueError("Cannot delete row without a primary key (e.g., 'id' column).")

            # 从存储引擎中删除行
            self.storage_engine.delete_row(self.table_name, primary_key_value)

            # 从B+树索引中删除对应的键
            if self.bplus_tree:
                self.bplus_tree.delete(primary_key_value)
            deleted_rows.append(row)
        return deleted_rows