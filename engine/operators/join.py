#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import List, Any, Tuple, Dict
from sql.ast import Expression, Column, BinaryExpression, Literal
from sql.planner import Operator
from engine.storage_engine import StorageEngine


class JoinOperator(Operator):
    """JOIN 算子，支持 INNER/LEFT/RIGHT/FULL/CROSS JOIN"""

    def __init__(self, join_type: str, condition: Expression,
                 left_child: Operator, right_child: Operator,
                 storage_engine: StorageEngine, executor: Any,
                 left_table: str = None, right_table: str = None):
        self.join_type = join_type.upper() if join_type else "INNER"
        self.condition = condition
        self.left_child = left_child
        self.right_child = right_child
        self.storage_engine = storage_engine
        self.executor = executor

        # 表名或别名（如果 child 是 SeqScan 就能拿到 table_name）
        self.left_alias = getattr(left_child, "table_name", left_table or "left")
        self.right_alias = getattr(right_child, "table_name", right_table or "right")

    def execute(self) -> List[Tuple[Any, Dict[str, Any]]]:
        left_rows = self.executor.execute([self.left_child])  # [(rid, row_dict), ...]
        right_rows = self.executor.execute([self.right_child])

        results = []

        # INNER JOIN 默认逻辑
        for l_rid, l_row in left_rows:
            matched = False
            for r_rid, r_row in right_rows:
                combined = self._merge_rows(l_row, r_row)
                if self._evaluate_condition(self.condition, combined):
                    results.append((None, combined))
                    matched = True

            # LEFT JOIN 需要保留未匹配的左边行
            if self.join_type == "LEFT" and not matched:
                combined = self._merge_rows(l_row, None)
                results.append((None, combined))

        # RIGHT JOIN：保留未匹配的右边行
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

    def _merge_rows(self, left_row: Dict[str, Any], right_row: Dict[str, Any],
                    left_alias: str = "users", right_alias: str = "orders") -> Dict[str, Any]:
        """
        合并左右两行，列名用 table.col 避免歧义。
        如果某边是 None（LEFT/RIGHT JOIN 未匹配），则补充对应表的列为 None。
        """
        combined = {}

        # 左表
        if left_row:
            for k, v in left_row.items():
                combined[f"{left_alias}.{k}"] = v
        else:
            # 左表没匹配时 → 填充空值
            schema = self.storage_engine.catalog_page.get_table_metadata(left_alias)['schema']
            for col_name in schema.keys():
                combined[f"{left_alias}.{col_name}"] = None

        # 右表
        if right_row:
            for k, v in right_row.items():
                combined[f"{right_alias}.{k}"] = v
        else:
            schema = self.storage_engine.catalog_page.get_table_metadata(right_alias)['schema']
            for col_name in schema.keys():
                combined[f"{right_alias}.{col_name}"] = None

        return combined

    def _evaluate_condition(self, condition: Expression, row: Dict[str, Any]) -> bool:
        if condition is None:
            return True  # CROSS JOIN 情况

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
        if isinstance(expr, Column):
            col_name = expr.name
            for k in row.keys():
                if k.endswith(f".{col_name}"):
                    return row[k]
            return None
        if isinstance(expr, Literal):
            return expr.value
        if isinstance(expr, BinaryExpression):
            return self._evaluate_condition(expr, row)
        return None
