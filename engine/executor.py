from typing import List, Optional, Any

from engine.operators.sort import SortOperator
from sql.ast import BinaryExpression, Column, Literal
from engine.operators.create_table import CreateTableOperator
from engine.operators.insert import InsertOperator
from engine.operators.join import JoinOperator
from engine.operators.project import ProjectOperator
from engine.operators.filter import FilterOperator
from engine.operators.seq_scan import SeqScanOperator
from engine.operators.update import UpdateOperator
from engine.operators.delete import DeleteOperator
from engine.operators.create_index import CreateIndexOperator
# [NEW] 导入新的算子和计划类型
from engine.operators.drop_index import DropIndexOperator
from sql.planner import DropIndex, Sort

from engine.storage_engine import StorageEngine
from sql.planner import (
    CreateTable, Insert, Project, Filter, SeqScan,
    Update, Delete, CreateIndex, Begin, Commit, Rollback,
    Join   # ✅ 引入 Join
)


class Executor:
    """
    执行器 (重构版)
    遍历逻辑计划树，实例化对应算子。
    """

    def __init__(self, storage_engine: StorageEngine):
        self.storage_engine = storage_engine
        self.txn_manager = storage_engine.txn_manager
        self.current_txn_id: Optional[int] = None

    def execute(self, plans: List[Any]) -> List[Any]:
        """执行一个或多个查询计划，返回结果集"""
        all_results = []
        for plan in plans:
            txn_id = self.current_txn_id
            result = None

            if isinstance(plan, CreateTable):
                result = self._execute_create_table(plan)
            elif isinstance(plan, CreateIndex):
                result = self._execute_create_index(plan)
            # [NEW] 增加对 DropIndex 计划的处理
            elif isinstance(plan, DropIndex):
                result = self._execute_drop_index(plan)
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
            elif isinstance(plan, Join):   # ✅ 新增
                result = self._execute_join(plan)
            elif isinstance(plan, Begin):
                result = self._execute_begin_transaction()
            elif isinstance(plan, Commit):
                result = self._execute_commit_transaction()
            elif isinstance(plan, Rollback):
                result = self._execute_rollback_transaction()
            elif isinstance(plan, Sort):  # 新增
                result = self._execute_sort(plan)
            else:
                raise ValueError(f"不支持的计划类型: {type(plan)}")

            if result is not None:
                if isinstance(result, list):
                    all_results.extend(result)
                else:
                    all_results.append(result)

        return all_results

    # --- 新增 JOIN 执行 ---
    def _execute_join(self, op: Join) -> List[Any]:
        join_op = JoinOperator(
            join_type=op.join_type,
            condition=op.condition,
            left_child=op.left,
            right_child=op.right,
            storage_engine=self.storage_engine,
            executor=self
        )
        return join_op.execute()

    def _execute_sort(self, op: Sort) -> List[Any]:
        """
        执行 Sort 逻辑节点。
        op.keys: List[Column] 或列名
        op.orders: List[str] 对应每列的 'ASC' 或 'DESC'
        """
        sort_op = SortOperator(
            child=op.child,
            keys=op.sort_keys,  # 列列表
            orders=op.orders,  # 对应排序方式列表
            executor=self  # 当前 Executor
        )
        return sort_op.execute()

    # --- 其他保持不变 ---
    def _execute_create_table(self, op: CreateTable) -> List[Any]:
        op = CreateTableOperator(op.table_name, op.columns, self.storage_engine)
        op.execute()
        return []

    def _execute_create_index(self, op: CreateIndex) -> List[Any]:
        """
        [FIX] 修正创建索引算子的实例化逻辑。
        确保将 planner 生成的 'index_name', 'columns', 和 'unique'
        正确地作为关键字参数传递给 CreateIndexOperator 的构造函数。
        """
        if not op.columns:
            raise ValueError("CREATE INDEX 语句必须至少指定一列。")

        # 直接将 planner 传来的参数透传给 Operator
        operator = CreateIndexOperator(
            table_name=op.table_name,
            index_name=op.index_name,
            columns=op.columns,
            unique=op.unique,
            storage_engine=self.storage_engine
        )
        operator.execute()
        return []

    def _execute_drop_index(self, op: DropIndex) -> List[Any]:
        """[NEW] 执行删除索引的操作"""
        drop_op = DropIndexOperator(op.index_name, self.storage_engine)
        return drop_op.execute()

    def _execute_insert(self, op: Insert, txn_id: Optional[int]) -> List[Any]:
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
        table_name = op.table_name
        bplus_tree = None

        if isinstance(op.condition, BinaryExpression) and op.condition.op.value == '=':
            left, right = op.condition.left, op.condition.right

            if isinstance(left, Column) and isinstance(right, Literal):
                column_name = left.name
            elif isinstance(right, Column) and isinstance(left, Literal):
                column_name = right.name
            else:
                column_name = None

            if column_name:
                index_manager = self.storage_engine.get_index_manager(table_name)
                if index_manager:
                    bplus_tree = index_manager.get_index_for_column(column_name)

        filter_op = FilterOperator(
            condition=op.condition,
            child=op.child,
            storage_engine=self.storage_engine,
            executor=self,
            bplus_tree=bplus_tree
        )
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
