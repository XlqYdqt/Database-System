#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import List, Any
from engine.storage_engine import StorageEngine


class SeqScanOperator:
    """全表扫描算子的具体实现"""
    def __init__(self, table_name: str, storage_engine: StorageEngine):
        self.table_name = table_name
        self.storage_engine = storage_engine


    def execute(self) -> List[Any]:
        """执行顺序扫描操作"""
        # 调用存储引擎扫描表
        rows = self.storage_engine.scan_table(self.table_name)
        if not rows:
            return []

        # 从 StorageEngine 获取 schema
        schema = self.storage_engine.catalog_page.get_table_metadata(self.table_name)['schema']

        # 逐行解码
        decoded_rows = []
        for raw in rows:
            values = self.decode_tuple(raw, schema)
            row_dict = {col_name: val for col_name, val in zip(schema.keys(), values)}
            decoded_rows.append(row_dict)
        return decoded_rows

    def decode_tuple(self, raw: bytes, schema: dict):
        values, offset = [], 0
        for col_def in schema.values():
            col_type = col_def.data_type.name
            if col_type == "INT":
                val = int.from_bytes(raw[offset:offset + 4], "little", signed=True)
                offset += 4
                values.append(val)
            elif col_type == "TEXT" or col_type == "STRING":
                length = int.from_bytes(raw[offset:offset + 4], "little")
                offset += 4
                val = raw[offset:offset + length].decode("utf-8")
                offset += length
                values.append(val)
            elif col_type == "FLOAT":
                import struct
                val = struct.unpack("<f", raw[offset:offset + 4])[0]
                offset += 4
                values.append(val)
            else:
                raise NotImplementedError(f"Unsupported type: {col_type}")
        return values
