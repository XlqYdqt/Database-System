#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import List, Any, Optional, Dict
from sql.ast import *
from sql.planner import Operator, LogicalPlan, SeqScan
from engine.storage_engine import StorageEngine
from engine.exceptions import TableNotFoundError


class FilterOperator(Operator):
    """过滤算子的具体实现"""

    def __init__(self, condition: Expression, child: LogicalPlan, executor: Any,
                 storage_engine: StorageEngine, bplus_tree: Optional[Any] = None):
        self.condition = condition
        self.child = child
        self.executor = executor
        # 【代码修改】新增属性以支持索引查找
        self.storage_engine = storage_engine
        self.bplus_tree = bplus_tree
        self.table_name = self._get_base_table_name(child)

    def execute(self) -> List[Any]:
        """
        执行过滤操作。
        [INDEX-SEEK FIX] 如果 WHERE 条件可以使用主键索引，则执行索引查找 (Index Seek)；
        否则，回退到顺序扫描 (Sequential Scan)。
        """
        # --- 路径 A: 索引查找 (Index Seek) ---
        if self.bplus_tree and self._can_use_index():
            pk_value = self._extract_pk_value()
            if pk_value is not None:
                try:
                    schema = self.storage_engine.catalog_page.get_table_metadata(self.table_name)['schema']
                    pk_col_def, _ = self.storage_engine._get_pk_info(schema)
                    pk_bytes = self.storage_engine._prepare_key_for_b_tree(pk_value, pk_col_def.data_type)

                    rid = self.bplus_tree.search(pk_bytes)
                    if rid:
                        row_data_bytes = self.storage_engine.read_row(self.table_name, rid)
                        if row_data_bytes:
                            row_data_dict = self._deserialize_row_data(row_data_bytes, schema)
                            # 索引查找只可能返回一行或零行
                            return [(rid, row_data_dict)]
                except (TableNotFoundError, ValueError, KeyError):
                    # 如果在索引查找过程中发生任何错误，安全地返回空结果
                    return []
            # 如果主键值为空或未在索引中找到，则返回空列表
            return []

        # --- 路径 B: 全表扫描 + 过滤 ---
        rows = self.executor.execute(self.child)
        results = []
        for rid, row in rows:
            if self._evaluate_condition(self.condition, row):
                results.append((rid, row))
        return results

    def _evaluate_condition(self, condition: Expression, row: Any) -> bool:
        """【重构】递归地对条件表达式求值"""
        if not isinstance(condition, BinaryExpression):
            return False

        left_value = self._eval_expr(condition.left, row)
        right_value = self._eval_expr(condition.right, row)

        op = getattr(condition, "op", None) or getattr(condition, "operator", None)
        op_val = op.value.upper()

        if op_val == 'AND':
            return left_value and right_value
        if op_val == 'OR':
            return left_value or right_value

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
            return row[expr.name]
        elif isinstance(expr, Literal):
            return expr.value
        elif isinstance(expr, BinaryExpression):
            return self._evaluate_condition(expr, row)
        else:
            raise NotImplementedError(f"Unsupported expression type: {type(expr)}")

    # --- 用于索引查找的辅助方法 ---

    def _can_use_index(self) -> bool:
        """判断 WHERE 条件是否是 `主键 = 常量` 的形式。"""
        if isinstance(self.condition, BinaryExpression):
            condition = self.condition
            try:
                schema = self.storage_engine.catalog_page.get_table_metadata(self.table_name)['schema']
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

    def _get_base_table_name(self, op: LogicalPlan) -> str:
        """递归地从逻辑计划中找到基表名。"""
        current_op = op
        while not isinstance(current_op, SeqScan):
            if hasattr(current_op, 'child') and current_op.child is not None:
                current_op = current_op.child
            else:
                raise ValueError(f"无法从算子树中确定基表名: {type(op)}")
        return current_op.table_name

    def _deserialize_row_data(self, row_bytes: bytes, schema: Dict[str, ColumnDefinition]) -> Dict[str, Any]:
        """将字节流反序列化为字典形式的行数据。"""
        row_dict = {}
        offset = 0
        for col_def in schema.values():
            value, offset = self.storage_engine._decode_value(row_bytes, offset, col_def.data_type)
            row_dict[col_def.name] = value
        return row_dict
