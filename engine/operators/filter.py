#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import List, Any, Optional, Dict, Tuple
from sql.ast import *
from sql.planner import Operator, LogicalPlan
from engine.storage_engine import StorageEngine


class FilterOperator(Operator):
    """
    过滤算子 (重构版)
    逻辑与之前版本基本一致，专注于发现可用的索引并高效地查找数据，
    或在无法使用索引时进行全表扫描过滤。
    """

    def __init__(self, condition: Expression, child: LogicalPlan, storage_engine: StorageEngine, executor: Any):
        self.condition = condition
        self.child = child
        self.storage_engine = storage_engine
        self.executor = executor

    def execute(self) -> List[Tuple[Tuple[int, int], Dict[str, Any]]]:
        """
        执行过滤操作。
        首先尝试寻找最优索引进行查找 (Index Seek)；
        如果找不到合适的索引，则回退到全表扫描并逐行过滤。
        """
        table_name = self._get_base_table_name()

        # --- 路径 A: 索引查找 (Index Seek) ---
        indexed_column, value = self._find_optimial_index_condition()
        if indexed_column:
            index_manager = self.storage_engine.get_index_manager(table_name)
            bplus_tree = index_manager.get_index_for_column(indexed_column)

            if bplus_tree:
                schema = self.storage_engine.catalog_page.get_table_metadata(table_name)['schema']
                col_def = schema[indexed_column]

                key_bytes = self.storage_engine._prepare_key_for_b_tree(value, col_def.data_type)
                rid = bplus_tree.search(key_bytes)

                if rid:
                    row_data_bytes = self.storage_engine.read_row(table_name, rid)
                    if row_data_bytes:
                        row_data_dict = self.storage_engine._decode_row(table_name, row_data_bytes)
                        if self._evaluate_condition(self.condition, row_data_dict):
                            return [(rid, row_data_dict)]
                return []

        # --- 路径 B: 全表扫描 + 过滤 ---
        rows = self.executor.execute(self.child)
        results = []
        for rid, row in rows:
            if self._evaluate_condition(self.condition, row):
                results.append((rid, row))
        return results

    def _evaluate_condition(self, condition: Expression, row: Dict[str, Any]) -> bool:
        result = self._eval_expr(condition, row)
        return bool(result)

    def _eval_expr(self, expr: Expression, row: Dict[str, Any]) -> Any:
        if isinstance(expr, Literal): return expr.value
        if isinstance(expr, Column): return row.get(expr.name)
        if isinstance(expr, BinaryExpression):
            left_val = self._eval_expr(expr.left, row)
            right_val = self._eval_expr(expr.right, row)
            op = getattr(expr, "op", getattr(expr, "operator", None))
            op_val = op.value.upper()
            if op_val == '=': return left_val == right_val
            if op_val == '>': return left_val > right_val
            if op_val == '<': return left_val < right_val
            if op_val == '>=': return left_val >= right_val
            if op_val == '<=': return left_val <= right_val
            if op_val in ('!=', '<>'): return left_val != right_val
            if op_val == 'AND': return left_val and right_val
            if op_val == 'OR': return left_val or right_val
            raise NotImplementedError(f"Unsupported operator: {op_val}")
        raise NotImplementedError(f"Unsupported expression type: {type(expr)}")

    def _find_optimial_index_condition(self) -> Tuple[Optional[str], Any]:
        """分析WHERE条件，寻找 `indexed_column = constant` 形式的子句。"""
        # 此处是一个简化的实现，仅处理顶层条件。
        # 实际的优化器会递归地分析整个条件树。
        if isinstance(self.condition, BinaryExpression) and self.condition.op.value == '=':
            column_name, value = None, None
            if isinstance(self.condition.left, Column) and isinstance(self.condition.right, Literal):
                column_name, value = self.condition.left.name, self.condition.right.value
            elif isinstance(self.condition.right, Column) and isinstance(self.condition.left, Literal):
                column_name, value = self.condition.right.name, self.condition.left.value

            if column_name:
                index_manager = self.storage_engine.get_index_manager(self._get_base_table_name())
                if index_manager and index_manager.get_index_for_column(column_name):
                    return column_name, value
        return None, None

    def _get_base_table_name(self) -> str:
        op = self.child
        while not hasattr(op, 'table_name'):
            if hasattr(op, 'child'):
                op = op.child
            else:
                raise ValueError("无法从查询计划中确定基表名称")
        return op.table_name

