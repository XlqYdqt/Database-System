#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import List, Any
from sql.ast import *
from sql.planner import Operator

class ProjectOperator:
    """投影算子的具体实现"""
    def __init__(self, columns: List[str], child: Operator, storage_engine: Any, executor: Any):
        self.columns = columns  # 需要投影的列
        self.child = child     # 子算子
    
    def execute(self) -> List[Any]:
        """执行投影操作"""
        # 先执行子算子获取数据
        rows = self.child.execute()
        
        # 如果是 SELECT *，直接返回所有列
        if '*' in self.columns:
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
            for col_name in self.columns:
                if col_name in col_name_to_index:
                    projected_row.append(row[col_name])
                else:
                    # 处理列不存在的情况，可以抛出错误或返回 None
                    raise ValueError(f"Column '{col_name}' not found in table '{table_name}'")
            results.append(projected_row)
        return results

    def _get_base_table_name(self, op: Operator) -> str:
        """Recursively finds the base table name from the operator tree."""
        # 检查操作符是否是 SeqScanOperator 的实例
        if isinstance(op, SeqScanOperator):
            return op.table_name
        # 如果操作符有子操作符，则递归调用
        elif hasattr(op, 'child') and op.child is not None:
            return self._get_base_table_name(op.child)
        # 如果无法确定基表名，则抛出错误
        else:
            raise ValueError(f"Could not determine base table name from operator type: {type(op)}")