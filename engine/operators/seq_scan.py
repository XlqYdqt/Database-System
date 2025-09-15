#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import List, Any, Dict
from engine.storage_engine import StorageEngine
from sql.ast import ColumnDefinition

class SeqScanOperator:
    """全表扫描算子的具体实现"""
    def __init__(self, table_name: str, storage_engine: StorageEngine):
        self.table_name = table_name
        self.storage_engine = storage_engine

    def execute(self) -> List[Any]:
        """
        执行顺序扫描操作，返回 (rid, row_dict)
        """
        rows_with_rid = self.storage_engine.scan_table(self.table_name)
        if not rows_with_rid:
            return []

        schema = self.storage_engine.catalog_page.get_table_metadata(self.table_name)['schema']

        decoded_rows = []
        for rid, raw_row_data in rows_with_rid:
            # [BUG FIX] 使用 StorageEngine 中统一的、更健壮的解码逻辑
            # 而不是在本地重新实现一遍，这修复了因页面空洞导致的数据错位解码问题。
            row_dict = self._decode_row(raw_row_data, schema)
            decoded_rows.append((rid, row_dict))
        return decoded_rows

    def _decode_row(self, raw_data: bytes, schema: Dict[str, ColumnDefinition]) -> Dict[str, Any]:
        """
        辅助函数，使用存储引擎的解码器将一行原始字节数据转换为字典。
        """
        row_dict = {}
        offset = 0
        for col_name, col_def in schema.items():
            try:
                # 调用 storage_engine 的核心解码方法
                value, new_offset = self.storage_engine._decode_value(raw_data, offset, col_def.data_type)
                row_dict[col_name] = value
                offset = new_offset
            except Exception as e:
                # 如果解码失败，打印错误并为该列设置一个None值，以增加程序的容错性
                print(f"警告: 解码列 '{col_name}' 时出错: {e}")
                row_dict[col_name] = None
                # 尝试继续解码后面的列，但这可能也会失败
                break
        return row_dict
