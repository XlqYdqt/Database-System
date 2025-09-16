#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import List, Any, Optional, Dict

from engine.operators.subquery import SubqueryOperator
from sql.ast import *
from sql.planner import Operator, LogicalPlan
from engine.storage_engine import StorageEngine


class FilterOperator(Operator):
    """过滤算子，支持普通比较 AND/OR 等，以及 IN / NOT IN 和标量子查询"""

    def __init__(self, condition: Expression, child: Operator,
                 storage_engine: StorageEngine, executor: Any):
        self.condition = condition
        self.child = child
        self.storage_engine = storage_engine
        self.executor = executor
        self._subquery_cache: Dict[int, List] = {}

    def execute(self) -> List[Any]:
        raw_rows = self.executor.execute([self.child])
        results = []
        for item in raw_rows:
            if isinstance(item, tuple) and len(item) == 2:
                rid, row = item
            elif isinstance(item, dict):
                rid, row = (None, item)
            else:
                rid, row = (None, item)

            try:
                if self._evaluate_condition(self.condition, row):
                    results.append((rid, row))
            except Exception as e:
                print(f"警告: 评估行 {row} 时出错: {e}")
                continue
        return results

    def _evaluate_condition(self, condition: Expression, row: Any) -> bool:
        if isinstance(condition, BinaryExpression):
            op = getattr(condition, "op", None) or getattr(condition, "operator", None)
            op_val = op.value.upper() if hasattr(op, "value") else str(op).upper()

            if op_val == "AND":
                return self._evaluate_condition(condition.left, row) and self._evaluate_condition(condition.right, row)
            if op_val == "OR":
                return self._evaluate_condition(condition.left, row) or self._evaluate_condition(condition.right, row)

        if isinstance(condition, InExpression):
            left_val = self._eval_expr(condition.expression, row)
            values = self._eval_expr(condition.values, row)
            result = left_val in values
            is_not = getattr(condition, 'is_not', False)
            return not result if is_not else result

        if isinstance(condition, BinaryExpression):
            left_val = self._eval_expr(condition.left, row)
            right_val = self._eval_expr(condition.right, row)

            op = getattr(condition, "op", None) or getattr(condition, "operator", None)
            op_val = op.value.upper() if hasattr(op, "value") else str(op).upper()

            if op_val in ("=", "=="): return left_val == right_val
            if op_val == ">": return left_val > right_val
            if op_val == "<": return left_val < right_val
            if op_val == ">=": return left_val >= right_val
            if op_val == "<=": return left_val <= right_val
            if op_val in ("!=", "<>"): return left_val != right_val
            raise NotImplementedError(f"不支持的二元运算符: {op_val}")

        return bool(self._eval_expr(condition, row))

    def _eval_expr(self, expr: Any, row: Any) -> Any:
        if isinstance(expr, Literal):
            return expr.value
        if isinstance(expr, Column):
            if isinstance(row, dict):
                return row.get(expr.name)
            raise ValueError("无法对非字典类型的行解析列")

        # [FIX] 根据 ast.py 文件，正确的属性名是 .select_statement
        if isinstance(expr, SubqueryExpression):
            subquery_node = expr.select_statement
            cache_key = id(subquery_node)
            if cache_key not in self._subquery_cache:
                subq = SubqueryOperator(subquery_node, self.executor)
                self._subquery_cache[cache_key] = subq.execute()

            result = self._subquery_cache[cache_key]

            if len(result) > 1:
                raise RuntimeError("标量子查询返回了多于一行的结果")
            return result[0] if result else None

        if isinstance(expr, (list, tuple)):
            return [self._eval_expr(v, row) for v in expr]

        if isinstance(expr, (SelectStatement, LogicalPlan, Operator)):
            cache_key = id(expr)
            if cache_key not in self._subquery_cache:
                subq = SubqueryOperator(expr, self.executor)
                self._subquery_cache[cache_key] = subq.execute()
            return self._subquery_cache[cache_key]

        raise NotImplementedError(f"不支持的值表达式类型: {type(expr)}")

