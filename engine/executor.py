#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from typing import List, Optional, Any
from sql.ast import *
from sql.planner import *
from .operators import *
from .operators.insert import InsertOperator
from .operators.update import UpdateOperator
from .operators.delete import DeleteOperator

from typing import Any, List

from engine.operators.create_table import CreateTableOperator
from engine.operators.insert import InsertOperator
from engine.operators.project import ProjectOperator
from engine.operators.filter import FilterOperator
from engine.operators.seq_scan import SeqScanOperator
from engine.operators.update import UpdateOperator
from engine.operators.delete import DeleteOperator
from engine.operators.create_index import CreateIndexOperator  # <-- 【新增】导入新的 CreateIndexOperator

from engine.storage_engine import StorageEngine
from sql.ast import Operator
from sql.planner import CreateTable, Insert, Project, Filter, SeqScan, Update, Delete, CreateIndex
from engine.transaction_manager import TransactionManager


class ExecutionContext:
    """执行上下文，包含执行过程中需要的状态信息"""

    def __init__(self):
        self.current_table = None
        self.current_row = None


class Executor:
    """
    执行器 (重构版)
    负责遍历和执行算子树，实例化重构后的算子。
    """

    def __init__(self, storage_engine: StorageEngine):
        self.context = ExecutionContext()
        self.storage_engine = storage_engine
        self.txn_manager = storage_engine.txn_manager
        self.current_txn_id: Optional[int] = None
    def execute(self, plans: List[Operator], txn_id: Optional[int] = None) -> List[Any]:
        """执行查询计划，返回结果集"""
        if isinstance(plan, CreateTable):
            return self._execute_create_table(plan)
        # <-- 【新增】处理 CreateIndex 计划的逻辑块
        elif isinstance(plan, CreateIndex):
            return self._execute_create_index(plan)
        elif isinstance(plan, Insert):
            return self._execute_insert(plan)
        elif isinstance(plan, Project):
            return self._execute_project(plan)
        elif isinstance(plan, Filter):
            return self._execute_filter(plan)
        elif isinstance(plan, SeqScan):
            return self._execute_seq_scan(plan)
        elif isinstance(plan, Update):
            return self._execute_update(plan)
        elif isinstance(plan, Delete):
            return self._execute_delete(plan)
        else:
            raise ValueError(f"Unsupported operator type: {type(plan)}")
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
        create_table_op = CreateTableOperator(op.table_name, op.columns, self.storage_engine)
        create_table_op.execute()
        return []

    # <-- 【新增】执行 CreateIndex 的具体方法
    def _execute_create_index(self, op: CreateIndex) -> List[Any]:
        """执行 CREATE INDEX 操作"""
        create_index_op = CreateIndexOperator(
            table_name=op.table_name,
            column_name=op.column_name,
            is_unique=op.is_unique,
            storage_engine=self.storage_engine
        )
        create_index_op.execute()
        return []

    def _execute_insert(self, op: Insert) -> List[Any]:
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
        filter_op = FilterOperator(op.condition, op.child, self.storage_engine, self)
        return filter_op.execute()

    def _execute_seq_scan(self, op: SeqScan) -> List[Any]:
        """执行顺序扫描操作"""
        seq_scan_op = SeqScanOperator(op.table_name, self.storage_engine)
        return seq_scan_op.execute()

    def _execute_update(self, op: Update, txn_id: Optional[int] = None) -> List[Any]:
        """执行UPDATE操作"""
        updates = list(op.assignments.items())
        update_op = UpdateOperator(op.table_name, op.child, updates, self.storage_engine, self)

        # 获取B+树索引
        bplus_tree = self.storage_engine.get_bplus_tree(op.table_name)
        update_op = UpdateOperator(op.table_name, op.child, updates, self.storage_engine, self, bplus_tree, txn_id)
        return update_op.execute()

    def _execute_delete(self, op: Delete, txn_id: Optional[int] = None) -> List[Any]:
        """执行DELETE操作"""
        delete_op = DeleteOperator(op.table_name, op.child, self.storage_engine, self)
        return delete_op.execute()


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
