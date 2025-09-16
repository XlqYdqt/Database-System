#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import List, Any, Optional, Dict

from engine.operators.subquery import SubqueryOperator
from sql.ast import *
from sql.planner import Operator, LogicalPlan
from engine.storage_engine import StorageEngine


class FilterOperator(Operator):
    """过滤算子，支持普通比较 AND/OR 等，以及 IN / NOT IN 子查询（非相关子查询）"""

    def __init__(self, condition: Expression, child: Operator,
                 storage_engine: StorageEngine, executor: Any):
        self.condition = condition
        self.child = child
        self.storage_engine = storage_engine
        self.executor = executor
        # 🚀 子查询缓存，避免重复执行
        self._subquery_cache: Dict[int, List] = {}

    def execute(self) -> List[Any]:
        # 全表扫描路径
        raw_rows = self.executor.execute([self.child])
        results = []
        for item in raw_rows:
            # 统一处理输入行格式
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
                # 在评估单行时出错，打印警告并继续
                print(f"警告: 评估行 {row} 时出错: {e}")
                continue
        return results

    def _evaluate_condition(self, condition: Expression, row: Any) -> bool:
        """
        递归地评估任意条件表达式，正确处理逻辑运算符和子查询。
        """
        # --- [FIX] 优先处理 AND/OR 逻辑运算符 ---
        if isinstance(condition, BinaryExpression):
            op = getattr(condition, "op", None) or getattr(condition, "operator", None)
            op_val = op.value.upper() if hasattr(op, "value") else str(op).upper()

            # 如果是 AND 或 OR，则递归调用 _evaluate_condition
            if op_val == "AND":
                return self._evaluate_condition(condition.left, row) and self._evaluate_condition(condition.right, row)
            if op_val == "OR":
                return self._evaluate_condition(condition.left, row) or self._evaluate_condition(condition.right, row)

        # --- IN / NOT IN 表达式 ---
        if isinstance(condition, InExpression):
            left_val = self._eval_expr(condition.expression, row)
            values = None
            # 判断 IN 的右侧是子查询还是静态列表
            if isinstance(condition.values, (SelectStatement, LogicalPlan, Operator)):
                cache_key = id(condition.values)
                if cache_key not in self._subquery_cache:
                    subq = SubqueryOperator(condition.values, self.executor)
                    self._subquery_cache[cache_key] = subq.execute()
                values = self._subquery_cache[cache_key]
            else:
                values = self._eval_expr(condition.values, row)

            result = left_val in values
            is_not = getattr(condition, 'is_not', False)
            return not result if is_not else result

        # --- 其他二元比较 (=, >, <, etc.) ---
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
            # AND/OR 已在前面处理
            raise NotImplementedError(f"不支持的二元运算符: {op_val}")

        # --- 单值表达式 (例如 WHERE a;) ---
        return bool(self._eval_expr(condition, row))

    def _eval_expr(self, expr: Any, row: Any) -> Any:
        """将表达式节点计算成一个 Python 值 (str, int, list, etc.)"""
        if isinstance(expr, Literal):
            return expr.value
        if isinstance(expr, Column):
            if isinstance(row, dict):
                return row.get(expr.name)
            raise ValueError("无法对非字典类型的行解析列")

        # 支持静态列表 (e.g., IN (1, 2, 3))
        if isinstance(expr, (list, tuple)):
            return [self._eval_expr(v, row) for v in expr]

        # 如果表达式本身就是一个子查询
        if isinstance(expr, (SelectStatement, LogicalPlan, Operator)):
            subq = SubqueryOperator(expr, self.executor)
            return subq.execute()

        # 其他类型的表达式不应在此函数中求值
        raise NotImplementedError(f"不支持的值表达式类型: {type(expr)}")

