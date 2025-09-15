#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from typing import List, Optional, Any
from sql.ast import *
from sql.planner import *
from .operators import *
from .operators.insert import InsertOperator
from .operators.update import UpdateOperator
from .operators.delete import DeleteOperator

from .storage_engine import StorageEngine
from engine.transaction_manager import TransactionManager


class ExecutionContext:
    """执行上下文，包含执行过程中需要的状态信息"""

    def __init__(self):
        self.current_table = None  # 当前正在操作的表
        self.current_row = None  # 当前正在处理的行


class Executor:
    """执行器，负责遍历和执行算子树"""

    def __init__(self, storage_engine: StorageEngine):
        self.context = ExecutionContext()
        self.storage_engine = storage_engine
        self.txn_manager = storage_engine.txn_manager
        self.current_txn_id: Optional[int] = None
    def execute(self, plans: List[Operator], txn_id: Optional[int] = None) -> List[Any]:
        """执行查询计划，返回结果集"""
        all_results = []
        for plan in plans:
            if isinstance(plan, CreateTable):
                result = self._execute_create_table(plan)
            elif isinstance(plan, Insert):
                result = self._execute_insert(plan, txn_id if txn_id is not None else self.current_txn_id)
            elif isinstance(plan, Project):
                result = self._execute_project(plan)
            elif isinstance(plan, Filter):
                result = self._execute_filter(plan)
            elif isinstance(plan, SeqScan):
                result = self._execute_seq_scan(plan)
            elif isinstance(plan, Update):
                result = self._execute_update(plan, txn_id if txn_id is not None else self.current_txn_id)
            elif isinstance(plan, Delete):
                result = self._execute_delete(plan, txn_id if txn_id is not None else self.current_txn_id)
            elif isinstance(plan, Begin):
                result = self._execute_begin_transaction(plan)
            elif isinstance(plan, Commit):
                result = self._execute_commit_transaction(plan)
            elif isinstance(plan, Rollback):
                result = self._execute_rollback_transaction(plan)
            else:
                raise ValueError(f"Unsupported operator type: {type(plan)}")
            if result is not None:
                all_results.extend(result)
        return all_results

    def _execute_create_table(self, op: CreateTable) -> List[Any]:
        """执行CREATE TABLE操作"""
        # CreateTableOperator now handles the storage_engine.create_table call
        create_table_op = CreateTableOperator(op.table_name, op.columns, self.storage_engine)
        create_table_op.execute()
        return []

    def _execute_insert(self, op: Insert, txn_id: Optional[int] = None) -> List[Any]:
        """执行INSERT操作"""
        insert_op = InsertOperator(op.table_name, op.values, self.storage_engine, txn_id)
        insert_op.execute()
        return []

    def _execute_project(self, op: Project) -> List[Any]:
        """执行投影操作"""
        project_op = ProjectOperator(op.columns, op.child, self.storage_engine, self)
        return project_op.execute()

    def _execute_filter(self, op: Filter) -> List[Any]:
        """执行过滤操作"""
        bplus_tree = self.storage_engine.get_bplus_tree(op.table_name)
        # [FIX] Corrected the argument order for FilterOperator.
        # The executor (`self`) should be the 4th argument, and storage_engine the 3rd.
        filter_op = FilterOperator(op.condition, op.child, self.storage_engine, self, bplus_tree)
        return filter_op.execute()

    def _execute_seq_scan(self, op: SeqScan) -> List[Any]:
        """执行顺序扫描操作"""
        seq_scan_op = SeqScanOperator(op.table_name, self.storage_engine)
        return seq_scan_op.execute()

    def _execute_update(self, op: Update, txn_id: Optional[int] = None) -> List[Any]:
        """执行UPDATE操作"""
        # assignments 是 dict，转成 [(col, expr), ...]
        updates = list(op.assignments.items())

        # 获取B+树索引
        bplus_tree = self.storage_engine.get_bplus_tree(op.table_name)
        update_op = UpdateOperator(op.table_name, op.child, updates, self.storage_engine, self, bplus_tree, txn_id)
        return update_op.execute()

    def _execute_delete(self, op: Delete, txn_id: Optional[int] = None) -> List[Any]:
        """执行DELETE操作"""

        # 获取B+树索引
        bplus_tree = self.storage_engine.get_bplus_tree(op.table_name)
        delete_op = DeleteOperator(op.table_name, op.child, self.storage_engine, self, bplus_tree, txn_id)
        return delete_op.execute()

    def _execute_begin_transaction(self, op: Begin) -> List[Any]:
        """执行BEGIN TRANSACTION操作"""
        self.current_txn_id = self.txn_manager.begin_transaction()
        return [self.current_txn_id]

    def _execute_commit_transaction(self, op: Commit) -> List[Any]:
        """执行COMMIT TRANSACTION操作"""
        self.txn_manager.commit_transaction(self.current_txn_id)
        self.current_txn_id = None
        return []

    def _execute_rollback_transaction(self, op: Rollback) -> List[Any]:
        """执行ROLLBACK TRANSACTION操作"""
        self.txn_manager.rollback_transaction(self.current_txn_id)
        self.current_txn_id = None
        return []
