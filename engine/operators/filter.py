#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import List, Any
from ...sql.ast import *

class FilterOperator:
    """过滤算子的具体实现"""
    def __init__(self, condition: Expression, child: Operator):
        self.condition = condition  # WHERE条件
        self.child = child         # 子算子
    
    def execute(self) -> List[Any]:
        """执行过滤操作"""
        # 先执行子算子获取数据
        rows = self.child.execute()
        
        # 应用过滤条件
        results = []
        for row in rows:
            if self._evaluate_condition(row):
                results.append(row)
        return results
    
    def _evaluate_condition(self, row: Any) -> bool:
        """评估WHERE条件"""
        if isinstance(self.condition, BinaryExpr):
            # 获取行中的列值
            left_value = row[self.condition.left]
            right_value = self.condition.right
            
            # 比较操作
            if self.condition.operator == '=':
                return left_value == right_value
            # TODO: 支持其他比较操作符
            
        return False