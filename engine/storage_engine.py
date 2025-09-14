#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from typing import List, Dict, Optional, Any, Tuple
import struct

from sql.ast import ColumnDefinition, DataType, ColumnConstraint
from engine.constants import ROW_LENGTH_PREFIX_SIZE
from engine.b_plus_tree import BPlusTree, INVALID_PAGE_ID
from engine.catalog_page import CatalogPage
from engine.table_heap_page import TableHeapPage
from engine.data_page import DataPage
# [FIX] 引入 TableNotFoundError 异常，用于更精确的错误处理
from engine.exceptions import TableAlreadyExistsError, PrimaryKeyViolationError, TableNotFoundError
from storage.buffer_pool_manager import BufferPoolManager


class StorageEngine:
    """
    存储引擎。
    作为数据库的底层核心，它负责管理表的元数据、物理存储、索引维护，
    并封装与缓冲池管理器的交互。
    """
    B_PLUS_TREE_KEY_SIZE = 16

    def __init__(self, buffer_pool_manager: BufferPoolManager):
        self.bpm = buffer_pool_manager
        self.indexes: Dict[str, BPlusTree] = {}

        is_dirty = False
        catalog_page_raw = self.bpm.fetch_page(0)
        if catalog_page_raw is None:
            catalog_page_raw = self.bpm.new_page()
            if catalog_page_raw is None or catalog_page_raw.page_id != 0:
                raise RuntimeError("关键错误：缓冲池无法分配或获取 page_id=0 作为目录页。")

        try:
            if any(b != 0 for b in catalog_page_raw.data):
                self.catalog_page = CatalogPage.deserialize(catalog_page_raw.data)
                is_dirty = False
            else:
                self.catalog_page = CatalogPage()
                catalog_page_raw.data = bytearray(self.catalog_page.serialize())
                is_dirty = True
        finally:
            self.bpm.unpin_page(0, is_dirty)

        self._load_all_indexes()

    def _load_all_indexes(self) -> None:
        """在系统启动时，从目录中加载所有表的B+树索引。"""
        for table_name, metadata in self.catalog_page.tables.items():
            if 'index_root_page_id' in metadata:
                index_root_id = metadata['index_root_page_id']
                self.indexes[table_name] = BPlusTree(self.bpm, index_root_id)

    def _prepare_key_for_b_tree(self, value: Any, col_type: DataType) -> bytes:
        """将Python值转换为B+树期望的、固定长度、可比较的字节键。"""
        if col_type == DataType.INT:
            key_bytes = value.to_bytes(8, 'big', signed=True)
        elif col_type in (DataType.TEXT, DataType.STRING):
            key_bytes = str(value).encode('utf-8')
        else:
            raise NotImplementedError(f"不支持的主键类型用于索引: {col_type.name}")

        if len(key_bytes) > self.B_PLUS_TREE_KEY_SIZE:
            raise ValueError(f"索引键过长。最大长度 {self.B_PLUS_TREE_KEY_SIZE} 字节，得到 {len(key_bytes)} 字节。")

        return key_bytes.ljust(self.B_PLUS_TREE_KEY_SIZE, b'\x00')

    def _flush_catalog_page(self) -> None:
        """将目录页（CatalogPage）的内容序列化并强制写回磁盘。"""
        catalog_page_raw = self.bpm.fetch_page(0)
        if catalog_page_raw:
            try:
                catalog_page_raw.data = bytearray(self.catalog_page.serialize())
                self.bpm.unpin_page(0, True)
            except Exception as e:
                self.bpm.unpin_page(0, False)
                raise e
        else:
            raise RuntimeError("在缓冲池中找不到目录页，无法刷新。")

    def get_bplus_tree(self, table_name: str) -> Optional[BPlusTree]:
        """获取指定表的B+树索引对象。"""
        return self.indexes.get(table_name)

    def create_table(self, table_name: str, columns: List[ColumnDefinition]) -> bool:
        """创建新表。如果缓冲池已满，则会抛出 MemoryError。"""
        if table_name in self.catalog_page.tables:
            raise TableAlreadyExistsError(f"表 '{table_name}' 已存在。")

        table_heap_page = self.bpm.new_page()
        if not table_heap_page:
            raise MemoryError("缓冲池已满，无法为表创建新的堆页面。")

        try:
            schema_dict = {col.name: col for col in columns}
            self.catalog_page.add_table(table_name, table_heap_page.page_id, INVALID_PAGE_ID, schema_dict)
            self._flush_catalog_page()
            self.indexes[table_name] = BPlusTree(self.bpm, INVALID_PAGE_ID)
            empty_heap = TableHeapPage()
            table_heap_page.data = bytearray(empty_heap.serialize())
        finally:
            self.bpm.unpin_page(table_heap_page.page_id, True)

        return True

    def insert_row(self, table_name: str, row_data: bytes) -> bool:
        """
        插入一行数据。
        - 成功: 返回 True
        - 失败: 抛出 specific exception (e.g., PrimaryKeyViolationError, MemoryError, IOError)
        """
        table_metadata = self.catalog_page.get_table_metadata(table_name)
        if not table_metadata:
            raise TableNotFoundError(table_name)

        heap_page_id = table_metadata['heap_root_page_id']
        heap_page_raw = self.bpm.fetch_page(heap_page_id)
        if not heap_page_raw:
            raise IOError(f"无法为表 '{table_name}' 获取堆页面 {heap_page_id}。")

        target_page_raw = None
        heap_page_is_dirty = False

        try:
            table_heap = TableHeapPage.deserialize(heap_page_raw.data)

            total_record_length = len(row_data) + ROW_LENGTH_PREFIX_SIZE
            record_to_insert = total_record_length.to_bytes(ROW_LENGTH_PREFIX_SIZE, "little") + row_data

            for page_id in reversed(table_heap.get_page_ids()):
                page_raw = self.bpm.fetch_page(page_id)
                if page_raw:
                    try:
                        data_page = DataPage(page_raw.page_id, page_raw.data)
                        if data_page.get_free_space() >= len(record_to_insert):
                            target_page_raw = page_raw
                            break
                    finally:
                        if target_page_raw is None or page_raw.page_id != target_page_raw.page_id:
                            self.bpm.unpin_page(page_id, False)

            if not target_page_raw:
                target_page_raw = self.bpm.new_page()
                if not target_page_raw:
                    raise MemoryError("缓冲池已满，无法为插入创建新的数据页。")

                table_heap.add_page_id(target_page_raw.page_id)
                heap_page_raw.data = bytearray(table_heap.serialize())
                heap_page_is_dirty = True

            target_data_page = DataPage(target_page_raw.page_id, target_page_raw.data)
            row_offset = target_data_page.insert_record(record_to_insert)
            target_page_raw.data = bytearray(target_data_page.get_data())

            schema = table_metadata['schema']
            try:
                pk_col_def, pk_col_index = self._get_pk_info(schema)
                pk_value, _ = self._decode_value_from_row(row_data, pk_col_index, schema)
                pk_bytes = self._prepare_key_for_b_tree(pk_value, pk_col_def.data_type)
                rid_tuple = (target_page_raw.page_id, row_offset)

                bplus_tree = self.indexes.get(table_name)
                if bplus_tree is None:
                    return True

                insert_result = bplus_tree.insert(pk_bytes, rid_tuple)

                if insert_result is None:
                    # [TRANSACTION FIX] 关键的回滚逻辑！
                    # 如果索引插入失败（主键冲突），我们必须撤销刚才的数据插入操作，
                    # 否则就会在数据页上留下没有索引的“幽灵数据”。
                    # 我们通过逻辑删除刚刚插入的行来实现回滚。
                    self.delete_row_by_rid(table_name, rid_tuple)
                    raise PrimaryKeyViolationError(pk_value)

                root_changed = insert_result
                if root_changed:
                    self.update_index_root(table_name, bplus_tree.root_page_id)

            except ValueError:
                # 表中没有主键，跳过索引更新
                pass

            return True

        finally:
            self.bpm.unpin_page(heap_page_id, heap_page_is_dirty)
            if target_page_raw:
                self.bpm.unpin_page(target_page_raw.page_id, True)

    def scan_table(self, table_name: str) -> List[Tuple[Tuple[int, int], bytes]]:
        """
        扫描全表，返回所有行数据及其RID。
        如果表不存在，则抛出 TableNotFoundError。
        """
        table_metadata = self.catalog_page.get_table_metadata(table_name)
        if not table_metadata:
            raise TableNotFoundError(table_name)

        heap_page_id = table_metadata['heap_root_page_id']
        heap_page_raw = self.bpm.fetch_page(heap_page_id)
        if not heap_page_raw:
            raise IOError(f"无法为表 '{table_name}' 获取堆页面 {heap_page_id}。")

        results = []
        try:
            table_heap = TableHeapPage.deserialize(heap_page_raw.data)
            for data_page_id in table_heap.get_page_ids():
                page_raw = self.bpm.fetch_page(data_page_id)
                if not page_raw: continue
                try:
                    data_page = DataPage(page_raw.page_id, page_raw.data)
                    for offset, record in data_page.get_all_records():
                        rid = (data_page_id, offset)
                        row_data = record[ROW_LENGTH_PREFIX_SIZE:]
                        results.append((rid, row_data))
                finally:
                    self.bpm.unpin_page(data_page_id, False)
        finally:
            self.bpm.unpin_page(heap_page_id, False)

        return results

    def _get_pk_info(self, schema: Dict[str, ColumnDefinition]) -> Tuple[ColumnDefinition, int]:
        """辅助函数，从schema中获取主键的定义和列索引位置。"""
        for i, col_def in enumerate(schema.values()):
            if any(c[0] == ColumnConstraint.PRIMARY_KEY for c in col_def.constraints):
                return col_def, i
        raise ValueError("表中未定义主键")

    def _decode_value_from_row(self, row_data: bytes, col_index: int, schema: Dict[str, ColumnDefinition]) -> Tuple[
        Any, int]:
        """根据列的索引从行数据中解码出特定一个值。"""
        offset = 0
        for i, col_def in enumerate(schema.values()):
            value, new_offset = self._decode_value(row_data, offset, col_def.data_type)
            if i == col_index:
                return value, new_offset
            offset = new_offset
        raise IndexError("列索引超出范围")

    def read_row(self, table_name: str, rid: Tuple[int, int]) -> Optional[bytes]:
        """根据RID（记录ID）读取单行数据。"""
        page_id, offset = rid
        page = self.bpm.fetch_page(page_id)
        if not page: return None
        try:
            data_page = DataPage(page.page_id, page.data)
            record = data_page.get_record(offset)
            return record[ROW_LENGTH_PREFIX_SIZE:] if record else None
        finally:
            self.bpm.unpin_page(page_id, False)

    def _decode_value(self, row_data: bytes, offset: int, col_type: DataType) -> Tuple[Any, int]:
        """根据数据类型从字节流的指定偏移量解码一个值。"""
        try:
            if col_type == DataType.INT:
                value = int.from_bytes(row_data[offset: offset + 4], "little", signed=True)
                offset += 4
            elif col_type in (DataType.TEXT, DataType.STRING):
                length = int.from_bytes(row_data[offset: offset + 4], "little")
                offset += 4
                value = row_data[offset: offset + length].decode("utf-8")
                offset += length
            elif col_type == DataType.FLOAT:
                value, = struct.unpack("<f", row_data[offset: offset + 4])
                offset += 4
            else:
                raise NotImplementedError(f"不支持的解码类型: {col_type.name}")
            return value, offset
        except (struct.error, IndexError, UnicodeDecodeError) as e:
            raise ValueError(f"从偏移量 {offset} 解码类型 {col_type.name} 失败: {e}")

    def delete_row_by_rid(self, table_name: str, rid: Tuple[int, int]) -> bool:
        """根据RID删除一行数据（逻辑删除）。"""
        page_id, offset = rid
        page = self.bpm.fetch_page(page_id)
        if not page:
            raise IOError(f"无法为删除操作获取页面 {page_id}。")

        deleted = False
        try:
            data_page = DataPage(page.page_id, page.data)
            deleted = data_page.delete_record(offset)
            if deleted:
                page.data = bytearray(data_page.get_data())
            return deleted
        finally:
            self.bpm.unpin_page(page_id, deleted)

    def update_index_root(self, table_name: str, new_root_id: int) -> None:
        """更新并持久化一个表的索引根页面ID。"""
        metadata = self.catalog_page.get_table_metadata(table_name)
        if not metadata:
            raise TableNotFoundError(table_name)
        metadata['index_root_page_id'] = new_root_id
        self._flush_catalog_page()

    def update_row_by_rid(self, table_name: str, rid: Tuple[int, int], new_row_data: bytes) -> Optional[
        Tuple[int, int]]:
        """根据RID更新一行数据。如果行移动，会返回新的RID。"""
        page_id, old_offset = rid
        page = self.bpm.fetch_page(page_id)
        if not page:
            raise IOError(f"无法为更新操作获取页面 {page_id}。")

        try:
            data_page = DataPage(page.page_id, page.data)
            total_record_length = len(new_row_data) + ROW_LENGTH_PREFIX_SIZE
            new_record = total_record_length.to_bytes(ROW_LENGTH_PREFIX_SIZE, 'little') + new_row_data

            new_offset, moved = data_page.update_record(old_offset, new_record)
            page.data = bytearray(data_page.get_data())
            return (page_id, new_offset)
        except (ValueError, IndexError):
            return None
        finally:
            self.bpm.unpin_page(page_id, True)

