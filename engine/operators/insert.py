#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import List, Any, Dict
from sql.ast import *
from engine.storage_engine import StorageEngine
# [FIX] 引入新的异常类型
from engine.exceptions import PrimaryKeyViolationError
from typing import List, Any, Dict, Optional

class InsertOperator:
    """INSERT 操作的执行算子"""

    def __init__(self, table_name: str, values: List[object], storage_engine: StorageEngine, txn_id: Optional[int] = None):
        self.table_name = table_name
        self.values = values
        self.storage_engine = storage_engine
        self.txn_id = txn_id

    def execute(self) -> List[Any]:
        """
        执行插入操作，并捕获存储引擎可能抛出的特定异常。
        """
        metadata = self.storage_engine.catalog_page.get_table_metadata(self.table_name)
        if not metadata or 'schema' not in metadata:
            raise RuntimeError(f"无法找到表 '{self.table_name}' 的 schema。")
        schema = metadata['schema']

        row_data_bytes = self._encode_tuple(self.values, schema)

        # [FIX] 使用 try...except 块来捕获并处理来自存储引擎的特定错误
        try:
            self.storage_engine.insert_row(self.table_name, row_data_bytes,self.txn_id)
        except PrimaryKeyViolationError as e:
            # 将底层的存储错误转换为对用户更友好的运行时错误
            raise RuntimeError(f"插入失败：{e}")
        except (MemoryError, IOError) as e:
            # 捕获系统级错误（如缓冲池满、磁盘IO问题）
            raise RuntimeError(f"存储引擎错误：{e}")
        except Exception as e:
            # 捕获其他未知错误
            raise RuntimeError(f"向表 '{self.table_name}' 插入数据时发生未知失败: {e}")

        return []

    def _encode_tuple(self, values: list, schema: Dict[str, ColumnDefinition]) -> bytes:
        """根据 schema 将 Python 值编码为字节流。"""
        row_data = bytearray()

        if len(values) != len(schema):
            raise ValueError(f"列数不匹配：表 '{self.table_name}' 需要 {len(schema)} 个值，但提供了 {len(values)} 个。")

        for col_def, val_expr in zip(schema.values(), values):
            value = val_expr.value if isinstance(val_expr, Literal) else val_expr
            col_type = col_def.data_type

            if col_type == DataType.INT:
                row_data.extend(int(value).to_bytes(4, "little", signed=True))
            elif col_type in (DataType.TEXT, DataType.STRING):
                encoded_str = str(value).encode("utf-8")
                row_data.extend(len(encoded_str).to_bytes(4, "little"))
                row_data.extend(encoded_str)
            elif col_type == DataType.FLOAT:
                import struct
                row_data.extend(struct.pack("<f", float(value)))
            else:
                raise NotImplementedError(f"不支持的数据类型: {col_type}")

        return bytes(row_data)
