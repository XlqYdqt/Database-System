#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import List, Any

from engine.operators import SeqScanOperator
from sql.ast import *
from sql.planner import Operator

class ProjectOperator:
    """投影算子的具体实现"""
    def __init__(self, columns: List[str], child: Operator, storage_engine: Any, executor: Any):
        self.columns = columns  # 需要投影的列
        self.child = child     # 子算子
        self.storage_engine = storage_engine
        self.executor = executor
    
    def execute(self) -> List[Any]:
        """执行投影操作"""
        # 先执行子算子获取数据
        rows = self.executor.execute(self.child)
        # 先把 Column 对象转换成字符串列名
        column_names = [col.name if isinstance(col, Column) else col for col in self.columns]

        # 如果是 SELECT *，直接返回所有列
        if '*' in column_names:
            return rows
        # 否则只返回指定的列
        results = []
        # 获取子操作符的表名
        table_name = self._get_base_table_name(self.child)
        schema = self.storage_engine.catalog_page.get_table_metadata(table_name)['schema']

        # 创建列名到索引的映射
        col_name_to_index = {col_name: i for i, (col_name, _) in enumerate(schema.items())}

        for row in rows:
            projected_row = []
            for col in self.columns:
                col_name = col.name if isinstance(col, Column) else col  # 统一成字符串
                if col_name in col_name_to_index:
                    projected_row.append(row[col_name])  # row 是 dict，用字符串取值
                else:
                    raise ValueError(f"Column '{col_name}' not found in table '{table_name}'")
            results.append(projected_row)
        return results

    def _get_base_table_name(self, op) -> str:
        """递归获取基表名，兼容 Operator 或 LogicalPlan"""
        from engine.operators import SeqScanOperator

        # 1. 如果是 SeqScanOperator，直接返回表名
        if isinstance(op, SeqScanOperator):
            return op.table_name

        # 2. 如果是 FilterOperator 或 ProjectOperator，尝试递归子算子
        if hasattr(op, 'child') and op.child is not None:
            return self._get_base_table_name(op.child)

        # 3. 如果是 LogicalPlan，尝试获取其 table_name 属性
        if hasattr(op, 'table_name'):
            return op.table_name

        # 4. 如果还有 children 列表，也可以递归
        if hasattr(op, 'children') and op.children:
            for child in op.children:
                try:
                    return self._get_base_table_name(child)
                except ValueError:
                    continue

        raise ValueError(f"Could not determine base table name from type {type(op)}")
