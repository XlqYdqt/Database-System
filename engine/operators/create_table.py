#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import List, Any
from sql.ast import ColumnDefinition
from engine.storage_engine import StorageEngine

from engine.Catelog.catelog import Catalog

class CreateTableOperator:
    """创建表算子的具体实现"""
    def __init__(self, table_name: str, columns: List[ColumnDefinition], catalog: Catalog, storage_engine: StorageEngine):
        self.table_name = table_name
        self.columns = columns
        self.catalog = catalog
        self.storage_engine = storage_engine
    
    def execute(self) -> List[Any]:
        """执行创建表操作"""
        # 调用存储引擎创建表
        success = self.storage_engine.create_table(self.table_name)
        if not success:
            raise RuntimeError(f"Failed to create table '{self.table_name}'")
        return []