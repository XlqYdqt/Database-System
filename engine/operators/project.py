#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import List, Any
from sql.ast import Column
from sql.planner import Operator


class ProjectOperator:
    """投影算子的具体实现"""
    def __init__(self, columns: List[str], child: Operator, storage_engine: Any, executor: Any):
        self.columns = columns   # 需要投影的列（Column 或 str）
        self.child = child       # 子算子
        self.storage_engine = storage_engine
        self.executor = executor

    def execute(self) -> List[Any]:
        """执行投影操作"""
        rows = self.executor.execute([self.child])  # [(rid, row_dict), ...]

        # 统一列名 → 字符串（table.col 格式）
        column_names = []
        for col in self.columns:
            if isinstance(col, Column):
                if hasattr(col, "table") and col.table:   # 显式带表名
                    column_names.append(f"{col.table}.{col.name}")
                else:  # 没有表名前缀（可能是单表查询）
                    column_names.append(col.name)
            else:
                column_names.append(str(col))

        # SELECT * 直接返回所有列
        if '*' in column_names:
            return [row_dict for _, row_dict in rows]

        results = []
        for _, row_dict in rows:
            projected_row = {}
            for col_name in column_names:
                if col_name in row_dict:
                    projected_row[col_name] = row_dict[col_name]
                else:
                    # 给出更清晰的错误信息
                    raise ValueError(
                        f"Column '{col_name}' not found in row: {list(row_dict.keys())}"
                    )
            results.append(projected_row)

        return results
