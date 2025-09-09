#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import List, Optional
from .ast import *

class Operator:
    """算子基类"""
    pass

class SeqScanOperator(Operator):
    """顺序扫描算子"""
    def __init__(self, table_name: str):
        self.table_name = table_name

class FilterOperator(Operator):
    """过滤算子"""
    def __init__(self, condition: Expression, child: Operator):
        self.condition = condition
        self.child = child

class ProjectOperator(Operator):
    """投影算子"""
    def __init__(self, columns: List[str], child: Operator):
        self.columns = columns
        self.child = child

class CreateTableOperator(Operator):
    """创建表算子"""
    def __init__(self, table_name: str, columns: List[ColumnDefinition]):
        self.table_name = table_name
        self.columns = columns

class InsertOperator(Operator):
    """插入算子"""
    def __init__(self, table_name: str, values: List[object]):
        self.table_name = table_name
        self.values = values

class LogicalPlanner:
    """逻辑计划生成器，将AST转换为算子树"""
    def __init__(self):
        pass
    
    def create_plan(self, stmt: Statement) -> Operator:
        """根据语句类型生成对应的算子树"""
        if isinstance(stmt, CreateTableStatement):
            return self._plan_create_table(stmt)
        elif isinstance(stmt, SelectStatement):
            return self._plan_select(stmt)
        elif isinstance(stmt, InsertStatement):
            return self._plan_insert(stmt)
        else:
            raise ValueError(f"Unsupported statement type: {type(stmt)}")
    
    def _plan_create_table(self, stmt: CreateTableStatement) -> Operator:
        """生成CREATE TABLE语句的算子"""
        return CreateTableOperator(stmt.table_name, stmt.columns)
    
    def _plan_select(self, stmt: SelectStatement) -> Operator:
        """生成SELECT语句的算子树"""
        # 1. 从表扫描开始
        plan = SeqScanOperator(stmt.table_name)
        
        # 2. 如果有WHERE条件，添加过滤
        if stmt.where:
            plan = FilterOperator(stmt.where, plan)
        
        # 3. 最后是投影
        return ProjectOperator(stmt.columns, plan)
    
    def _plan_insert(self, stmt: InsertStatement) -> Operator:
        """生成INSERT语句的算子"""
        return InsertOperator(stmt.table_name, stmt.values)