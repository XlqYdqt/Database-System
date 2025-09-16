from typing import List, Optional, Any

from engine.operators.create_table import CreateTableOperator
from engine.operators.insert import InsertOperator
from engine.operators.project import ProjectOperator
from engine.operators.filter import FilterOperator
from engine.operators.seq_scan import SeqScanOperator
from engine.operators.update import UpdateOperator
from engine.operators.delete import DeleteOperator
from engine.operators.create_index import CreateIndexOperator

from engine.storage_engine import StorageEngine
from sql.planner import CreateTable, Insert, Project, Filter, SeqScan, Update, Delete, CreateIndex, Begin, Commit, Rollback


class Executor:
    """
    执行器 (重构版)
    负责遍历和执行算子树，实例化重构后的算子。
    """

    def __init__(self, storage_engine: StorageEngine):
        self.storage_engine = storage_engine
        self.txn_manager = storage_engine.txn_manager
        self.current_txn_id: Optional[int] = None

    def execute(self, plans: List[Any]) -> List[Any]:
        """执行一个或多个查询计划，返回结果集"""
        all_results = []
        for plan in plans:
            # 获取当前操作的事务ID
            txn_id = self.current_txn_id
            result = None

            if isinstance(plan, CreateTable):
                result = self._execute_create_table(plan)
            elif isinstance(plan, CreateIndex):
                result = self._execute_create_index(plan)
            elif isinstance(plan, Insert):
                result = self._execute_insert(plan, txn_id)
            elif isinstance(plan, Update):
                result = self._execute_update(plan, txn_id)
            elif isinstance(plan, Delete):
                result = self._execute_delete(plan, txn_id)
            elif isinstance(plan, SeqScan):
                result = self._execute_seq_scan(plan)
            elif isinstance(plan, Filter):
                result = self._execute_filter(plan)
            elif isinstance(plan, Project):
                result = self._execute_project(plan)
            elif isinstance(plan, Begin):
                result = self._execute_begin_transaction()
            elif isinstance(plan, Commit):
                result = self._execute_commit_transaction()
            elif isinstance(plan, Rollback):
                result = self._execute_rollback_transaction()
            else:
                raise ValueError(f"不支持的计划类型: {type(plan)}")

            if result is not None:
                # SELECT 等查询会返回多行结果，需要用 extend
                if isinstance(result, list):
                    all_results.extend(result)
                else:
                    all_results.append(result)

        return all_results

    def _execute_create_table(self, op: CreateTable) -> List[Any]:
        op = CreateTableOperator(op.table_name, op.columns, self.storage_engine)
        op.execute()
        return []

    def _execute_create_index(self, op: CreateIndex) -> List[Any]:
        op = CreateIndexOperator(
            table_name=op.table_name,
            column_name=op.column_name,
            is_unique=op.is_unique,
            storage_engine=self.storage_engine
        )
        op.execute()
        return []

    def _execute_insert(self, op: Insert, txn_id: Optional[int]) -> List[Any]:
        # 注意：INSERT VALUES 格式解析出的 op.values 是一个包含单行值的列表
        for row_values in op.values:
            insert_op = InsertOperator(op.table_name, row_values, self.storage_engine, txn_id)
            insert_op.execute()
        return []

    def _execute_update(self, op: Update, txn_id: Optional[int]) -> List[Any]:
        updates = list(op.assignments.items())
        update_op = UpdateOperator(op.table_name, op.child, updates, self.storage_engine, self, txn_id)
        return update_op.execute()

    def _execute_delete(self, op: Delete, txn_id: Optional[int]) -> List[Any]:
        delete_op = DeleteOperator(op.table_name, op.child, self.storage_engine, self, txn_id)
        return delete_op.execute()

    def _execute_seq_scan(self, op: SeqScan) -> List[Any]:
        seq_scan_op = SeqScanOperator(op.table_name, self.storage_engine)
        return seq_scan_op.execute()

    def _execute_filter(self, op: Filter) -> List[Any]:
        filter_op = FilterOperator(op.condition, op.child, self.storage_engine, self)
        return filter_op.execute()

    def _execute_project(self, op: Project) -> List[Any]:
        project_op = ProjectOperator(op.columns, op.child, self.storage_engine, self)
        return project_op.execute()

    def _execute_begin_transaction(self) -> List[Any]:
        if self.current_txn_id is not None:
            raise RuntimeError(f"无法启动新事务，因为事务 {self.current_txn_id} 仍在进行中。")
        self.current_txn_id = self.txn_manager.begin_transaction()
        return [f"事务 {self.current_txn_id} 已开始"]

    def _execute_commit_transaction(self) -> List[Any]:
        if self.current_txn_id is None:
            raise RuntimeError("没有活动的事务可以提交。")
        self.txn_manager.commit_transaction(self.current_txn_id)
        self.current_txn_id = None
        return ["事务已提交"]

    def _execute_rollback_transaction(self) -> List[Any]:
        if self.current_txn_id is None:
            raise RuntimeError("没有活动的事务可以回滚。")
        self.txn_manager.abort_transaction(self.current_txn_id)
        self.current_txn_id = None
        return ["事务已回滚"]