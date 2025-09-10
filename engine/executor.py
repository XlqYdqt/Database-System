#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import List, Optional, Any
from sql.ast import *
from sql.planner import Operator
from .operators import *
from .Catelog.catelog import Catalog
from .storage_engine import StorageEngine

class ExecutionContext:
    """执行上下文，包含执行过程中需要的状态信息"""
    def __init__(self):
        self.current_table = None  # 当前正在操作的表
        self.current_row = None    # 当前正在处理的行

class Executor:
    """执行器，负责遍历和执行算子树"""
    def __init__(self):
        self.context = ExecutionContext()
        self.catalog = Catalog()
        self.storage_engine = StorageEngine(self.catalog)
    
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
        # CreateTableOperator now handles the storage_engine.create_table call
        create_table_op = CreateTableOperator(op.table_name, op.columns, self.catalog, self.storage_engine)
        create_table_op.execute()
        return []
    
    def _execute_insert(self, op: InsertOperator) -> List[Any]:
        """执行INSERT操作"""
        schema = self.catalog.get_schema(op.table_name)
        # 确保插入的值与 schema 匹配
        if len(op.values) != len(schema):
            raise ValueError("Number of values does not match schema")
        
        # 将值序列化为字节
        row_data = self.encode_tuple(op.values, schema)
        # 调用存储引擎插入，存储引擎现在负责更新索引
        self.storage_engine.insert_row(op.table_name, row_data)

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
        table_name = self._get_base_table_name(op.child)
        schema = self.catalog.get_schema(table_name)
        
        # 创建列名到索引的映射
        col_name_to_index = {col_name: i for i, (col_name, _) in enumerate(schema)}
        
        for row in child_results:
            projected_row = []
            for col_name in op.columns:
                if col_name in col_name_to_index:
                    projected_row.append(row[col_name])
                else:
                    # 处理列不存在的情况，可以抛出错误或返回 None
                    raise ValueError(f"Column '{col_name}' not found in table '{table_name}'")
            results.append(projected_row)
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
        return op.execute()

    def _get_base_table_name(self, op: Operator) -> str:
        """Recursively finds the base table name from the operator tree."""
        if isinstance(op, SeqScanOperator):
            return op.table_name
        elif hasattr(op, 'child') and op.child is not None:
            return self._get_base_table_name(op.child)
        else:
            raise ValueError(f"Could not determine base table name from operator type: {type(op)}")
    
    def _evaluate_expression(self, expr: Expression) -> bool:
        """评估WHERE表达式"""
        if isinstance(expr, BinaryExpression):
            # 获取当前行中的列值
            if isinstance(expr.left, ColumnDefinition):
                col_name = expr.left.name
            else:
                col_name = expr.left
            left_value = self.context.current_row[col_name]

            right_value = expr.right
            
            # 比较操作
            if expr.operator == '=':
                return left_value == right_value
            elif expr.operator == '>':
                return left_value > right_value
            elif expr.operator == '<':
                return left_value < right_value
            elif expr.operator == '>=':
                return left_value >= right_value
            elif expr.operator == '<=':
                return left_value <= right_value
            elif expr.operator == '!=':
                return left_value != right_value
            else:
                raise ValueError(f"Unsupported comparison operator: {expr.operator}")
            
        return False