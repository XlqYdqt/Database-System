#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import List, Any, Optional, Dict
from sql.ast import *
from sql.planner import Operator, LogicalPlan
from engine.storage_engine import StorageEngine
from engine.b_plus_tree import BPlusTree


class FilterOperator(Operator):
    """过滤算子的具体实现"""

    def __init__(self, condition: Expression, child: LogicalPlan, storage_engine: StorageEngine, executor: Any,
                 bplus_tree: Optional[BPlusTree] = None):
        self.condition = condition
        self.child = child
        self.storage_engine = storage_engine
        self.executor = executor
        self.bplus_tree = bplus_tree

    def execute(self) -> List[Any]:
        """
        执行过滤操作。
        如果可能，优先使用B+树索引进行查找（Index Seek）；
        否则，回退到全表扫描并逐行过滤。
        """
        # --- 路径 A: 索引查找 (Index Seek) ---
        if self.bplus_tree and self._can_use_index():
            results = []
            pk_value = self._extract_pk_value()
            if pk_value is not None:
                schema = self.storage_engine.catalog_page.get_table_metadata(self._get_base_table_name())['schema']
                pk_col_def, _ = self.storage_engine._get_pk_info(schema)
                pk_bytes = self.storage_engine._prepare_key_for_b_tree(pk_value, pk_col_def.data_type)

                rid = self.bplus_tree.search(pk_bytes)
                if rid:
                    row_data_bytes = self.storage_engine.read_row(self._get_base_table_name(), rid)
                    if row_data_bytes:
                        row_data_dict = self._deserialize_row_data(row_data_bytes, schema)
                        results.append((rid, row_data_dict))
            return results

        # --- 路径 B: 全表扫描 + 过滤 ---
        rows = self.executor.execute(self.child)
        results = []
        for rid, row in rows:
            if self._evaluate_condition(self.condition, row):
                results.append((rid, row))
        return results

    def _evaluate_condition(self, condition: Expression, row: Any) -> bool:
        """对顶层条件表达式求值，期望结果为布尔值"""
        result = self._eval_expr(condition, row)
        return bool(result)

    def _eval_expr(self, expr: Expression, row: Any) -> Any:
        """[FIX] Refactored expression evaluation for clarity and correctness.
        This function now properly handles nested expressions and logical operators.
        """
        if isinstance(expr, Literal):
            return expr.value
        if isinstance(expr, Column):
            return row.get(expr.name)
        if isinstance(expr, BinaryExpression):
            left_val = self._eval_expr(expr.left, row)
            right_val = self._eval_expr(expr.right, row)

            op = getattr(expr, "op", None) or getattr(expr, "operator", None)
            op_val = op.value.upper()

            if op_val == '=': return left_val == right_val
            if op_val == '>': return left_val > right_val
            if op_val == '<': return left_val < right_val
            if op_val == '>=': return left_val >= right_val
            if op_val == '<=': return left_val <= right_val
            if op_val in ('!=', '<>'): return left_val != right_val
            if op_val == 'AND': return left_val and right_val
            if op_val == 'OR': return left_val or right_val

            raise NotImplementedError(f"Unsupported operator in expression: {op_val}")

        raise NotImplementedError(f"Unsupported expression type: {type(expr)}")

    # --- 以下是用于索引查找的辅助方法 ---

    def _can_use_index(self) -> bool:
        """判断 WHERE 条件是否是 `主键 = 常量` 的形式。"""
        if isinstance(self.condition, BinaryExpression):
            condition = self.condition
            try:
                schema = self.storage_engine.catalog_page.get_table_metadata(self._get_base_table_name())['schema']
                pk_col_name = self.storage_engine._get_pk_info(schema)[0].name
            except (TypeError, KeyError, ValueError):
                return False

            if (isinstance(condition.left, Column) and condition.left.name == pk_col_name and
                    isinstance(condition.right, Literal) and condition.op.value == '='):
                return True
        return False

    def _extract_pk_value(self) -> Any:
        """从 WHERE 条件中提取主键的值。"""
        if self._can_use_index():
            return self.condition.right.value
        return None

    def _get_base_table_name(self) -> str:
        """从子算子中递归获取基表名。"""
        op = self.child
        while not hasattr(op, 'table_name'):
            if hasattr(op, 'child'):
                op = op.child
            else:
                raise ValueError("Could not determine base table name from the query plan")
        return op.table_name

    def _deserialize_row_data(self, row_bytes: bytes, schema: Dict[str, ColumnDefinition]) -> Dict[str, Any]:
        """将字节流反序列化为字典形式的行数据。"""
        row_dict = {}
        offset = 0
        for col_def in schema.values():
            value, offset = self.storage_engine._decode_value(row_bytes, offset, col_def.data_type)
            row_dict[col_def.name] = value
        return row_dict
