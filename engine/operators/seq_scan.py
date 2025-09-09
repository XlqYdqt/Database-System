#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import List, Any
from sql.ast import *
from engine.storage_engine import StorageEngine

class SeqScanOperator:
    """顺序扫描算子的具体实现"""
    def __init__(self, table_name: str):
        self.table_name = table_name
        self.storage_engine = StorageEngine()
    
    def execute(self) -> List[Any]:
        """执行顺序扫描操作"""
        # 调用存储引擎扫描表
        rows = self.storage_engine.scan_table(self.table_name)
        if not rows:
            return []
            
        # TODO: 将字节序列转换为行数据
        return rows