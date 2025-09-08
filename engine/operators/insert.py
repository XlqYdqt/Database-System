#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import List, Any
from ...sql.ast import *
from ..storage_engine import StorageEngine

class InsertOperator:
    """插入算子的具体实现"""
    def __init__(self, table_name: str, values: List[object]):
        self.table_name = table_name
        self.values = values
        self.storage_engine = StorageEngine()
    
    def execute(self) -> List[Any]:
        """执行插入操作"""
        # TODO: 将值转换为字节序列
        row_data = bytes()
        
        # 调用存储引擎插入数据
        success = self.storage_engine.insert_row(self.table_name, row_data)
        if not success:
            raise RuntimeError(f"Failed to insert into table '{self.table_name}'")
        return []