#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import Dict, List, Optional
from .ast import *

class Table:
    """表的元数据"""
    def __init__(self, name: str, columns: List[ColumnDef]):
        self.name = name
        self.columns = {col.name: col for col in columns}

class SemanticAnalyzer:
    """语义分析器，检查SQL语句的语义正确性"""
    def __init__(self):
        self.tables: Dict[str, Table] = {}
    
    def analyze(self, stmt: Statement) -> None:
        """分析SQL语句的语义"""
        if isinstance(stmt, CreateTableStatement):
            self._analyze_create_table(stmt)
        elif isinstance(stmt, SelectStatement):
            self._analyze_select(stmt)
        elif isinstance(stmt, InsertStatement):
            self._analyze_insert(stmt)
        else:
            raise ValueError(f"Unsupported statement type: {type(stmt)}")
    
    def _analyze_create_table(self, stmt: CreateTableStatement) -> None:
        """分析CREATE TABLE语句"""
        if stmt.table_name in self.tables:
            raise ValueError(f"Table '{stmt.table_name}' already exists")
            
        # 检查列名是否重复
        column_names = set()
        for col in stmt.columns:
            if col.name in column_names:
                raise ValueError(f"Duplicate column name: {col.name}")
            column_names.add(col.name)
            
        # 检查数据类型是否支持
        supported_types = {'int', 'float', 'string', 'bool'}
        for col in stmt.columns:
            if col.type.lower() not in supported_types:
                raise ValueError(f"Unsupported data type: {col.type}")
                
        # 添加到表目录
        self.tables[stmt.table_name] = Table(stmt.table_name, stmt.columns)
    
    def _analyze_select(self, stmt: SelectStatement) -> None:
        """分析SELECT语句"""
        if stmt.table_name not in self.tables:
            raise ValueError(f"Table '{stmt.table_name}' does not exist")
            
        table = self.tables[stmt.table_name]
        
        # 检查列名
        if '*' not in stmt.columns:
            for col in stmt.columns:
                if col not in table.columns:
                    raise ValueError(f"Column '{col}' does not exist in table '{stmt.table_name}'")
                    
        # 检查WHERE条件
        if stmt.where:
            if isinstance(stmt.where, BinaryExpr):
                if stmt.where.left not in table.columns:
                    raise ValueError(f"Column '{stmt.where.left}' does not exist in table '{stmt.table_name}'")
    
    def _analyze_insert(self, stmt: InsertStatement) -> None:
        """分析INSERT语句"""
        if stmt.table_name not in self.tables:
            raise ValueError(f"Table '{stmt.table_name}' does not exist")
            
        table = self.tables[stmt.table_name]
        
        # 检查值的数量是否匹配列数
        if len(stmt.values) != len(table.columns):
            raise ValueError(f"Value count does not match column count in table '{stmt.table_name}'")
            
        # TODO: 检查值的类型是否匹配列的类型