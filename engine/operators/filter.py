#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import List, Any
from sql.ast import *
from sql.planner import Operator, LogicalPlan
from typing import Any

class FilterOperator(Operator):
    """过滤算子的具体实现"""
    def __init__(self, condition: Expression, child: LogicalPlan, executor: Any):
        self.condition = condition  # WHERE条件
        self.child = child         # 子算子
        self.executor = executor

    def execute(self) -> List[Any]:
        """执行过滤操作，输入输出都是 (rid, row_dict)"""
        # 先执行子算子获取数据
        rows = self.executor.execute(self.child)  # [(rid, row_dict), ...]

        results = []
        for rid, row in rows:
            if self._evaluate_condition(row):  # ✅ 只把 row_dict 传给条件判断
                results.append((rid, row))  # ✅ 保留 rid
        return results

    def _evaluate_condition(self, row: Any) -> bool:
        if isinstance(self.condition, BinaryExpression):
            left_value = self._eval_expr(self.condition.left, row)
            right_value = self._eval_expr(self.condition.right, row)

            op = getattr(self.condition, "op", None) or getattr(self.condition, "operator", None)
            print(op.value)
            if op.value == '=':
                return left_value == right_value
            elif op.value == '>':
                return left_value > right_value
            elif op.value == '<':
                return left_value < right_value
            elif op.value == '>=':
                return left_value >= right_value
            elif op.value == '<=':
                return left_value <= right_value
            elif op.value == '!=':
                return left_value != right_value

        return False

    def _eval_expr(self, expr: Expression, row: Any) -> Any:
        """递归解析表达式（列、常量等）"""
        if isinstance(expr, Column):
            return row[expr.name]  # 从行中取列值
        elif isinstance(expr, Literal):
            return expr.value  # 字面量直接返回
        elif isinstance(expr, BinaryExpression):
            # 递归支持复杂条件，例如 (age > 20 AND id < 10)
            return self._evaluate_condition(row)
        else:
            raise NotImplementedError(f"Unsupported expression type: {expr}")
