#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import List, Any, Optional, Dict, Tuple

from engine.operators.subquery import SubqueryOperator
from sql.ast import *
from sql.planner import Operator, LogicalPlan
from engine.storage_engine import StorageEngine
from engine.b_plus_tree import BPlusTree


class FilterOperator(Operator):
    """
    过滤算子 (集成版)
    - 支持索引查找 (Index Seek) 以提高性能。
    - 支持子查询 (IN / NOT IN / 标量比较)。
    - 在无可用索引时，回退到全表扫描过滤。
    """

    def __init__(self, condition: Expression, child: Operator,
                 storage_engine: StorageEngine, executor: Any,
                 bplus_tree: Optional[BPlusTree] = None):
        self.condition = condition
        self.child = child
        self.storage_engine = storage_engine
        self.executor = executor
        self.bplus_tree = bplus_tree  # 由执行器传入的可用索引
        self._subquery_cache: Dict[int, List] = {}

    def execute(self) -> List[Any]:
        # --- 路径 A: 索引查找 (Index Seek) ---
        # 如果执行器传入了B+树索引，并且条件确实是简单的等值查询
        if self.bplus_tree and self._is_simple_equality_condition():
            table_name = self._get_base_table_name()
            column_name, value = self._extract_condition_parts()

            if column_name and value is not None:
                col_def = self.storage_engine.catalog_page.get_table_metadata(table_name)['schema'][column_name]
                key_bytes = self.storage_engine._prepare_key_for_b_tree(value, col_def.data_type)

                rid = self.bplus_tree.search(key_bytes)
                if rid:
                    row_data = self.storage_engine.read_row(table_name, rid)
                    if row_data:
                        row_dict = self.storage_engine._decode_row(table_name, row_data)
                        # 索引只能保证部分条件满足（例如在 AND 子句中），
                        # 因此仍需用完整条件再次过滤以确保正确性。
                        if self._evaluate_condition(self.condition, row_dict):
                            return [(rid, row_dict)]
                return []

        # --- 路径 B: 全表扫描 + 过滤 ---
        raw_rows = self.executor.execute([self.child])
        results = []
        for item in raw_rows:
            rid, row = (item[0], item[1]) if isinstance(item, tuple) and len(item) == 2 else (None, item)
            try:
                if self._evaluate_condition(self.condition, row):
                    results.append((rid, row))
            except Exception as e:
                print(f"警告: 评估行 {row} 时出错: {e}")
                continue
        return results

    def _evaluate_condition(self, condition: Expression, row: Any) -> bool:
        if isinstance(condition, BinaryExpression):
            # 优先处理逻辑运算符 AND/OR，因为它们需要递归调用本函数
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
            # 处理剩下的比较运算符
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

        # 处理标量子查询
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

        # 处理 IN 后面的静态列表, e.g., IN (1, 2, 3)
        if isinstance(expr, (list, tuple)):
            return [self._eval_expr(v, row) for v in expr]

        # 处理 IN 后面的子查询
        if isinstance(expr, (SelectStatement, LogicalPlan, Operator)):
            cache_key = id(expr)
            if cache_key not in self._subquery_cache:
                subq = SubqueryOperator(expr, self.executor)
                self._subquery_cache[cache_key] = subq.execute()
            return self._subquery_cache[cache_key]

        raise NotImplementedError(f"不支持的值表达式类型: {type(expr)}")

    # --- 辅助函数 ---

    def _is_simple_equality_condition(self) -> bool:
        """检查条件是否是 `column = literal` 的形式"""
        if isinstance(self.condition, BinaryExpression) and self.condition.op.value == '=':
            if (isinstance(self.condition.left, Column) and isinstance(self.condition.right, Literal)) or \
                    (isinstance(self.condition.right, Column) and isinstance(self.condition.left, Literal)):
                return True
        return False

    def _extract_condition_parts(self) -> Tuple[Optional[str], Any]:
        """从 `column = literal` 条件中提取列名和值"""
        if not self._is_simple_equality_condition():
            return None, None

        if isinstance(self.condition.left, Column):
            return self.condition.left.name, self.condition.right.value
        else:
            return self.condition.right.name, self.condition.left.value

    def _get_base_table_name(self) -> str:
        op = self.child
        while not hasattr(op, 'table_name'):
            if hasattr(op, 'child'):
                op = op.child
            else:
                raise ValueError("无法从查询计划中确定基表名")
        return op.table_name

