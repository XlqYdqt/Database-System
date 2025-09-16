#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import List, Any, Dict, Tuple
from engine.storage_engine import StorageEngine
from sql.ast import ColumnDefinition


class SeqScanOperator:
    """
    全表扫描算子 (优化版)
    将行数据解码的逻辑统一委托给 StorageEngine，确保解码逻辑的一致性和健壮性。
    """

    def __init__(self, table_name: str, storage_engine: StorageEngine):
        self.table_name = table_name
        self.storage_engine = storage_engine

    def execute(self) -> List[Tuple[Tuple[int, int], Dict[str, Any]]]:
        """
        执行顺序扫描操作。
        1. 从存储引擎获取所有行的原始字节数据。
        2. 调用存储引擎中心化的解码方法，将字节流转换为字典。
        """
        # Step 1: 获取 (rid, raw_bytes) 列表
        rows_with_rid = self.storage_engine.scan_table(self.table_name)
        if not rows_with_rid:
            return []

        decoded_rows = []
        for rid, raw_row_data in rows_with_rid:
            try:
                # Step 2: 调用 StorageEngine 的解码方法，而不是自己实现
                # [OPTIMIZATION]
                row_dict = self.storage_engine._decode_row(self.table_name, raw_row_data)
                decoded_rows.append((rid, row_dict))
            except Exception as e:
                # 如果单行解码失败，打印警告并继续处理下一行
                print(f"警告: 解码行 RID {rid} 时出错，已跳过: {e}")
                continue

        return decoded_rows
