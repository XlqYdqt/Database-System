import struct
from typing import Any, List, Tuple, Dict, Optional

from engine.storage_engine import StorageEngine
from sql.ast import Operator
from sql.planner import Filter


class DeleteOperator(Operator):
    """
    DELETE 操作的执行算子 (重构版)
    职责分离：本算子只负责找出需要删除的行，具体的删除操作（包括数据和所有索引的维护）
    完全委托给 StorageEngine 的原子方法来完成。
    """

    def __init__(self, table_name: str, child: Operator, storage_engine: StorageEngine, executor: Any):
        self.table_name = table_name
        self.child = child
        self.storage_engine = storage_engine
        self.executor = executor

    def execute(self) -> List[Any]:
        """
        执行DELETE操作。
        1. 通过执行子计划（通常是 Filter 或 SeqScan）获取所有待删除行的RID和数据。
        2. 遍历结果，对每一行调用 StorageEngine 中封装好的、保证原子性的 delete_row 方法。
        """
        # Step 1: 找出所有需要被删除的行
        # self.child 通常是一个 FilterOperator 或 SeqScanOperator
        # 它的 execute() 方法已经实现了索引优化或全表扫描
        rows_to_delete: List[Tuple[Tuple[int, int], Dict[str, Any]]] = self.executor.execute(self.child)

        deleted_count = 0
        for rid, _ in rows_to_delete:
            try:
                # Step 2: 调用 StorageEngine 的原子删除方法
                # 这个方法会处理数据页的逻辑删除以及所有相关索引的条目删除
                if self.storage_engine.delete_row(self.table_name, rid):
                    deleted_count += 1
            except Exception as e:
                # 即使单行删除失败，也应继续尝试删除其他行，并打印警告
                print(f"警告：删除行 RID {rid} 时发生错误，已跳过: {e}")
                continue

        return [deleted_count]

