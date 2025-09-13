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

class ExecutionContext:
    """执行上下文，包含执行过程中需要的状态信息"""
    def __init__(self):
        self.current_table = None  # 当前正在操作的表
        self.current_row = None    # 当前正在处理的行

class Executor:
    """执行器，负责遍历和执行算子树"""
    def __init__(self, storage_engine: StorageEngine):
        self.context = ExecutionContext()
        self.storage_engine = storage_engine


    
    def execute(self, plan: Operator) -> List[Any]:
        """执行查询计划，返回结果集"""
        if isinstance(plan, CreateTable):
            return self._execute_create_table(plan)
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
    
    def _execute_create_table(self, op: CreateTable) -> List[Any]:
        """执行CREATE TABLE操作"""
        # CreateTableOperator now handles the storage_engine.create_table call
        create_table_op = CreateTableOperator(op.table_name, op.columns, self.storage_engine)
        create_table_op.execute()
        return []
    
    def _execute_insert(self, op: Insert) -> List[Any]:
        """执行INSERT操作"""
        insert_op = InsertOperator(op.table_name, op.values, self.storage_engine)
        insert_op.execute()
        return []
    
    def _execute_project(self, op: Project) -> List[Any]:
        """执行投影操作"""
        project_op = ProjectOperator(op.columns, op.child, self.storage_engine, self)
        return project_op.execute()
    
    def _execute_filter(self, op: Filter) -> List[Any]:
        """执行过滤操作"""
        filter_op = FilterOperator(op.condition, op.child, self)
        return filter_op.execute()
    
    def _execute_seq_scan(self, op: SeqScan) -> List[Any]:
        """执行顺序扫描操作"""
        seq_scan_op = SeqScanOperator(op.table_name, self.storage_engine)
        return seq_scan_op.execute()

    def _execute_update(self, op: Update) -> List[Any]:
        """执行UPDATE操作"""
        # assignments 是 dict，转成 [(col, expr), ...]
        updates = list(op.assignments.items())

        # 获取B+树索引
        bplus_tree = self.storage_engine.get_bplus_tree(op.table_name)
        update_op = UpdateOperator(op.table_name, op.child, updates, self.storage_engine, self, bplus_tree)
        return update_op.execute()

    def _execute_delete(self, op: Delete) -> List[Any]:
        """执行DELETE操作"""


        # 获取B+树索引
        bplus_tree = self.storage_engine.get_bplus_tree(op.table_name)
        delete_op = DeleteOperator(op.table_name, op.child, self.storage_engine, self, bplus_tree)
        return delete_op.execute()