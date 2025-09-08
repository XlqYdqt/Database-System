#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import List, Any
from ...sql.ast import *

class ProjectOperator:
    """投影算子的具体实现"""
    def __init__(self, columns: List[str], child: Operator):
        self.columns = columns  # 需要投影的列
        self.child = child     # 子算子
    
    def execute(self) -> List[Any]:
        """执行投影操作"""
        # 先执行子算子获取数据
        rows = self.child.execute()
        
        # 如果是 SELECT *，直接返回所有列
        if '*' in self.columns:
            return rows
        
        # 否则只返回指定的列
        results = []
        for row in rows:
            # 从行中提取指定的列
            projected_row = {}
            for col in self.columns:
                if col in row:
                    projected_row[col] = row[col]
                else:
                    raise ValueError(f"Column '{col}' not found in row")
            results.append(projected_row)
        return results