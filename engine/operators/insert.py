#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import List, Any, Dict
from sql.ast import *
from engine.storage_engine import StorageEngine
from engine.exceptions import PrimaryKeyViolationError, UniquenessViolationError


class InsertOperator:
    """
    INSERT 操作的执行算子 (重构版)
    职责单一：将SQL的VALUES转换为字典和字节流，并调用 StorageEngine 的原子插入方法。
    """

    def __init__(self, table_name: str, values: List[object], storage_engine: StorageEngine):
        self.table_name = table_name
        self.values = values
        self.storage_engine = storage_engine

    def execute(self) -> List[Any]:
        """
        执行插入操作。
        """
        metadata = self.storage_engine.catalog_page.get_table_metadata(self.table_name)
        if not metadata or 'schema' not in metadata:
            raise RuntimeError(f"无法找到表 '{self.table_name}' 的 schema。")
        schema = metadata['schema']

        # Step 1: 将SQL字面量转换为Python字典和字节流
        row_dict = self._create_row_dict(self.values, schema)
        row_data_bytes = self.storage_engine._serialize_row(self.table_name, row_dict)

        try:
            # Step 2: 调用 StorageEngine 的原子插入方法
            self.storage_engine.insert_row(self.table_name, row_data_bytes, row_dict)
        except (PrimaryKeyViolationError, UniquenessViolationError) as e:
            # 将底层的存储错误转换为对用户更友好的运行时错误
            raise RuntimeError(f"插入失败：{e}")
        except (MemoryError, IOError) as e:
            raise RuntimeError(f"存储引擎错误：{e}")
        except Exception as e:
            raise RuntimeError(f"向表 '{self.table_name}' 插入数据时发生未知失败: {e}")

        return []

    def _create_row_dict(self, values: list, schema: Dict[str, ColumnDefinition]) -> Dict[str, Any]:
        """根据 schema 将SQL字面量列表转换为Python字典。"""
        if len(values) != len(schema):
            raise ValueError(f"列数不匹配：表 '{self.table_name}' 需要 {len(schema)} 个值，但提供了 {len(values)} 个。")

        row_dict = {}
        for col_def, val_expr in zip(schema.values(), values):
            # 从 Literal 节点中提取真实值
            value = val_expr.value if isinstance(val_expr, Literal) else val_expr
            row_dict[col_def.name] = value

        return row_dict

