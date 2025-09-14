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
            if self._evaluate_condition(self.condition, row):
                results.append((rid, row))
        return results

    def _evaluate_condition(self, condition: Expression, row: Any) -> bool:
        """【重构】递归地对条件表达式求值"""
        if not isinstance(condition, BinaryExpression):
            # 如果条件不是二元表达式，可以根据需要处理或返回False
            return False

        left_value = self._eval_expr(condition.left, row)
        right_value = self._eval_expr(condition.right, row)

        op = getattr(condition, "op", None) or getattr(condition, "operator", None)
        op_val = op.value.upper()  # 转换为大写以便不区分大小写比较

        # 【增强】处理逻辑运算符
        if op_val == 'AND':
            return left_value and right_value
        if op_val == 'OR':
            return left_value or right_value

        # 处理比较运算符
        if op_val == '=': return left_value == right_value
        if op_val == '>': return left_value > right_value
        if op_val == '<': return left_value < right_value
        if op_val == '>=': return left_value >= right_value
        if op_val == '<=': return left_value <= right_value
        if op_val in ('!=', '<>'): return left_value != right_value

        return False

    def _eval_expr(self, expr: Expression, row: Any) -> Any:
        """递归解析表达式的值（列、常量或子表达式）"""
        if isinstance(expr, Column):
            return row[expr.name]  # 从行中取列值
        elif isinstance(expr, Literal):
            return expr.value  # 字面量直接返回
        elif isinstance(expr, BinaryExpression):
            # 【修复】对于嵌套的二元表达式，递归调用 _evaluate_condition
            return self._evaluate_condition(expr, row)
        else:
            raise NotImplementedError(f"Unsupported expression type: {type(expr)}")