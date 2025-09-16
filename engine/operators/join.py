#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import List, Any, Tuple, Dict
from sql.ast import Expression, Column, BinaryExpression, Literal
from sql.planner import Operator
from engine.storage_engine import StorageEngine


class JoinOperator(Operator):
    """JOIN 算子，支持 INNER JOIN，后续可扩展 LEFT/RIGHT/FULL/CROSS"""

    def __init__(self, join_type: str, condition: Expression,
                 left_child: Operator, right_child: Operator,
                 storage_engine: StorageEngine, executor: Any):
        self.join_type = join_type.upper()
        self.condition = condition
        self.left_child = left_child
        self.right_child = right_child
        self.storage_engine = storage_engine
        self.executor = executor

    def execute(self) -> List[Tuple[Any, Dict[str, Any]]]:
        """
        执行 JOIN：
        - 目前实现 Nested Loop Join（暴力匹配，后续可优化为 Hash Join / Merge Join）
        - 返回结果行为 dict，key 是 "table.col" 形式，避免冲突
        """
        left_rows = self.executor.execute([self.left_child])  # [(rid, row_dict), ...]
        right_rows = self.executor.execute([self.right_child])

        results = []

        for l_rid, l_row in left_rows:
            matched = False
            for r_rid, r_row in right_rows:
                combined = self._merge_rows(l_row, r_row)
                if self._evaluate_condition(self.condition, combined):
                    results.append((None, combined))
                    matched = True

            # LEFT JOIN：左边没匹配上也要保留
            if self.join_type == "LEFT" and not matched:
                combined = self._merge_rows(l_row, None)
                results.append((None, combined))

        # RIGHT JOIN：右边没匹配上也要保留
        if self.join_type == "RIGHT":
            for r_rid, r_row in right_rows:
                matched = False
                for l_rid, l_row in left_rows:
                    combined = self._merge_rows(l_row, r_row)
                    if self._evaluate_condition(self.condition, combined):
                        matched = True
                        break
                if not matched:
                    combined = self._merge_rows(None, r_row)
                    results.append((None, combined))

        # FULL JOIN = LEFT + RIGHT
        if self.join_type == "FULL":
            # 已经包含 INNER + LEFT
            right_unmatched = []
            for r_rid, r_row in right_rows:
                matched = False
                for l_rid, l_row in left_rows:
                    combined = self._merge_rows(l_row, r_row)
                    if self._evaluate_condition(self.condition, combined):
                        matched = True
                        break
                if not matched:
                    right_unmatched.append(r_row)
            for r_row in right_unmatched:
                combined = self._merge_rows(None, r_row)
                results.append((None, combined))

        # CROSS JOIN（笛卡尔积）
        if self.join_type == "CROSS":
            for l_rid, l_row in left_rows:
                for r_rid, r_row in right_rows:
                    combined = self._merge_rows(l_row, r_row)
                    results.append((None, combined))

        return results

    def _merge_rows(self, left_row: Dict[str, Any], right_row: Dict[str, Any]) -> Dict[str, Any]:
        """
        合并左右两行，列名用 table.col 避免歧义
        """
        combined = {}
        if left_row:
            for k, v in left_row.items():
                combined[f"left.{k}"] = v
        if right_row:
            for k, v in right_row.items():
                combined[f"right.{k}"] = v
        return combined

    def _evaluate_condition(self, condition: Expression, row: Dict[str, Any]) -> bool:
        """
        评估 JOIN 条件（通常是 BinaryExpression: users.id = orders.user_id）
        """
        if condition is None:
            return True  # CROSS JOIN 的情况

        if isinstance(condition, BinaryExpression):
            left_val = self._eval_expr(condition.left, row)
            right_val = self._eval_expr(condition.right, row)

            op = getattr(condition, "op", None)
            op_val = op.value.upper() if op and hasattr(op, "value") else str(op).upper()

            if op_val in ("=", "=="):
                return left_val == right_val
            if op_val == "!=":
                return left_val != right_val
            if op_val == ">":
                return left_val > right_val
            if op_val == "<":
                return left_val < right_val
            if op_val == ">=":
                return left_val >= right_val
            if op_val == "<=":
                return left_val <= right_val
            return False

        return False

    def _eval_expr(self, expr: Expression, row: Dict[str, Any]) -> Any:
        """支持 Column / Literal"""
        if isinstance(expr, Column):
            # 这里 Column 可能写成 users.id 或 orders.user_id
            col_name = expr.name
            # 直接匹配 row 中的 key（前面 merge 已经加了 left. right. 前缀）
            for k in row.keys():
                if k.endswith(f".{col_name}"):
                    return row[k]
            return None
        if isinstance(expr, Literal):
            return expr.value
        if isinstance(expr, BinaryExpression):
            return self._evaluate_condition(expr, row)
        return None
