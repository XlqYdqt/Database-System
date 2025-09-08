#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import List, Optional

class Statement:
    """SQL语句的基类"""
    pass

class Expression:
    """表达式的基类"""
    pass

class BinaryExpr(Expression):
    """二元表达式，如 WHERE 子句中的条件"""
    def __init__(self, left: str, operator: str, right: object):
        self.left = left          # 左操作数（列名）
        self.operator = operator  # 操作符
        self.right = right        # 右操作数（值）

class ColumnDef:
    """列定义"""
    def __init__(self, name: str, type: str):
        self.name = name  # 列名
        self.type = type  # 数据类型

class CreateTableStatement(Statement):
    """CREATE TABLE 语句"""
    def __init__(self, table_name: str, columns: List[ColumnDef]):
        self.table_name = table_name  # 表名
        self.columns = columns        # 列定义列表

class SelectStatement(Statement):
    """SELECT 语句"""
    def __init__(self, columns: List[str], table_name: str, where: Optional[Expression] = None):
        self.columns = columns      # 选择的列
        self.table_name = table_name  # 表名
        self.where = where          # WHERE 条件

class InsertStatement(Statement):
    """INSERT 语句"""
    def __init__(self, table_name: str, values: List[object]):
        self.table_name = table_name  # 表名
        self.values = values          # 插入的值