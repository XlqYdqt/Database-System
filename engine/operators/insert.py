#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import List, Any
from sql.ast import *
from engine.storage_engine import StorageEngine





class InsertOperator:
    """插入算子的具体实现"""

    def __init__(self, table_name: str, values: List[object], storage_engine: StorageEngine):
        self.table_name = table_name
        self.values = values

        self.storage_engine = storage_engine

    def execute(self) -> List[Any]:
        """执行插入操作"""
        # 从 Catalog 获取 schema
        schema = self.storage_engine.catalog_page.get_table_metadata(self.table_name)['schema']
        print(schema)

        # 将值序列化为字节
        row_data = self.encode_tuple(self.values, schema)

        # 调用存储引擎插入
        success = self.storage_engine.insert_row(self.table_name, row_data)
        if not success:
            raise RuntimeError(f"Failed to insert into table '{self.table_name}'")
        return []

    def encode_tuple(self, values: list, schema: list) -> bytes:
        """根据 schema 将 Python 值编码为字节"""
        row_data = bytearray()
        for col_def, val in zip(schema.values(), values):
            col_name = col_def.name
            col_type = col_def.data_type.name
            if col_type == "INT":
                # Check if val is a Literal object and extract its value
                if isinstance(val, Literal):
                    val = val.value
                row_data.extend(int(val).to_bytes(4, "little", signed=True))
            elif col_type == "TEXT" or col_type == "STRING":
                # Check if val is a Literal object and extract its value
                if isinstance(val, Literal):
                    val = val.value
                encoded = val.encode("utf-8")
                row_data.extend(len(encoded).to_bytes(2, "little"))
                row_data.extend(encoded)
            elif col_type == "FLOAT":
                # Check if val is a Literal object and extract its value
                if isinstance(val, Literal):
                    val = val.value
                import struct
                row_data.extend(struct.pack("<f", float(val)))
            else:
                raise NotImplementedError(f"Unsupported type: {col_type}")
        return bytes(row_data)
