#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import List, Optional, Any
from sql.ast import *
from sql.planner import Operator
from .operators import *

class ExecutionContext:
    """执行上下文，包含执行过程中需要的状态信息"""
    def __init__(self):
        self.current_table = None  # 当前正在操作的表
        self.current_row = None    # 当前正在处理的行

class Executor:
    """执行器，负责遍历和执行算子树"""
    def __init__(self):
        self.context = ExecutionContext()
    
    def execute(self, plan: Operator) -> List[Any]:
        """执行查询计划，返回结果集"""
        if isinstance(plan, CreateTableOperator):
            return self._execute_create_table(plan)
        elif isinstance(plan, InsertOperator):
            return self._execute_insert(plan)
        elif isinstance(plan, ProjectOperator):
            return self._execute_project(plan)
        elif isinstance(plan, FilterOperator):
            return self._execute_filter(plan)
        elif isinstance(plan, SeqScanOperator):
            return self._execute_seq_scan(plan)
        else:
            raise ValueError(f"Unsupported operator type: {type(plan)}")
    
    def _execute_create_table(self, op: CreateTableOperator) -> List[Any]:
        """执行CREATE TABLE操作"""
        # TODO: 调用存储引擎创建表
        return []
    
    def _execute_insert(self, op: InsertOperator) -> List[Any]:
        """执行INSERT操作"""
        # TODO: 调用存储引擎插入数据
        return []
    
    def _execute_project(self, op: ProjectOperator) -> List[Any]:
        """执行投影操作"""
        # 先执行子算子
        child_results = self.execute(op.child)
        
        # 如果是 SELECT *，直接返回所有列
        if '*' in op.columns:
            return child_results
        
        # 否则只返回指定的列
        results = []
        for row in child_results:
            # TODO: 根据列名从行中提取数据
            results.append(row)
        return results
    
    def _execute_filter(self, op: FilterOperator) -> List[Any]:
        """执行过滤操作"""
        # 先执行子算子
        child_results = self.execute(op.child)
        
        # 应用过滤条件
        results = []
        for row in child_results:
            self.context.current_row = row
            if self._evaluate_expression(op.condition):
                results.append(row)
        return results
    
    def _execute_seq_scan(self, op: SeqScanOperator) -> List[Any]:
        """执行顺序扫描操作"""
        # TODO: 调用存储引擎扫描表
        return []
    
    def _evaluate_expression(self, expr: Expression) -> bool:
        """评估WHERE表达式"""
        if isinstance(expr, BinaryExpr):
            # 获取当前行中的列值
            left_value = self.context.current_row[expr.left]
            right_value = expr.right
            
            # 比较操作
            if expr.operator == '=':
                return left_value == right_value
            # TODO: 支持其他比较操作符
            
        return False