from typing import Any, List, Tuple, Dict, Optional

from engine.storage_engine import StorageEngine
from sql.ast import Operator


class DeleteOperator(Operator):
    """
    DELETE 操作的执行算子 (重构版)
    职责分离：本算子只负责找出需要删除的行，具体的删除操作
    完全委托给 StorageEngine 的原子方法来完成。
    """

    def __init__(self, table_name: str, child: Operator, storage_engine: StorageEngine, executor: Any, txn_id: Optional[int] = None):
        self.table_name = table_name
        self.child = child
        self.storage_engine = storage_engine
        self.executor = executor
        self.txn_id = txn_id

    def execute(self) -> List[Any]:
        """执行DELETE操作。"""
        # 1. 通过子计划（Filter或SeqScan）获取所有待删除行的RID
        rows_to_delete: List[Tuple[Tuple[int, int], Dict[str, Any]]] = self.executor.execute([self.child])

        deleted_count = 0
        for rid, _ in rows_to_delete:
            try:
                # 2. 调用 StorageEngine 的统一删除接口，并传入事务ID
                if self.storage_engine.delete_row(self.table_name, rid, self.txn_id):
                    deleted_count += 1
            except Exception as e:
                print(f"警告：删除行 RID {rid} 时发生错误，已跳过: {e}")
                continue

        return [f"{deleted_count} 行已删除"]
