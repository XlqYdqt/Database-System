#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import List, Dict, Optional, Any, Tuple

from sql.ast import ColumnDefinition, DataType, ColumnConstraint
from storage.buffer_pool_manager import Page
from storage.buffer_pool_manager import BufferPoolManager
from storage.lru_replacer import LRUReplacer
from storage.disk_manager import DiskManager
from engine.constants import PAGE_SIZE, ROW_LENGTH_PREFIX_SIZE

from engine.b_plus_tree import BPlusTree # Import BPlusTree
from engine.catalog_page import CatalogPage # Import CatalogPage
from engine.table_heap_page import TableHeapPage # Import TableHeapPage
from engine.data_page import DataPage # Import DataPage
from engine.exceptions import TableAlreadyExistsError

class StorageEngine:
    """存储引擎，负责行数据和页面之间的映射"""
    def __init__(self, buffer_pool_size: int = 1024):

        self.file_manager = DiskManager("data", PAGE_SIZE)
        self.lru_replacer = LRUReplacer(buffer_pool_size)
        self.buffer_pool = BufferPoolManager(buffer_pool_size, self.file_manager, self.lru_replacer)


        self.indexes: Dict[str, BPlusTree] = {} # Change to BPlusTree

        # Load or create CatalogPage
        catalog_page_raw = self.buffer_pool.fetch_page(0)
        if catalog_page_raw:
            print(f"Raw catalog page data: {catalog_page_raw.data}")
            self.catalog_page = CatalogPage.deserialize(catalog_page_raw.data)
            print(f"Deserialized catalog page tables: {self.catalog_page.tables}")
            print(self.catalog_page.list_tables())
            self.buffer_pool.unpin_page(0, False)
        else:
            self.catalog_page = CatalogPage()
            # Pin the new catalog page and mark it dirty
            new_page_for_catalog = self.buffer_pool.new_page()
            if new_page_for_catalog and new_page_for_catalog.page_id == 0:
                new_page_for_catalog.data = bytearray(self.catalog_page.serialize())
                self.buffer_pool.flush_page(new_page_for_catalog.page_id)
                self.buffer_pool.unpin_page(new_page_for_catalog.page_id, True)
            else:
                raise RuntimeError("Failed to create initial CatalogPage at page ID 0. new_page() did not return page 0 or failed.")

    def _flush_catalog_page(self):
        """将 CatalogPage 刷新到磁盘"""
        catalog_page_raw = self.buffer_pool.fetch_page(0)
        if catalog_page_raw:
            catalog_page_raw.data = bytearray(self.catalog_page.serialize())
            self.buffer_pool.flush_page(catalog_page_raw.page_id)
            self.buffer_pool.unpin_page(0, True)
        else:
            raise RuntimeError("CatalogPage not found in buffer pool for flushing")



    # ------------------------
    # 表管理
    # ------------------------
    def create_table(self, table_name: str, columns: List[ColumnDefinition]) -> bool:
        """为新表分配首页并初始化索引和元数据"""
        print(self.catalog_page.list_tables())
        catalog_page_raw = self.buffer_pool.fetch_page(0)
        # No need to deserialize catalog_page here, it's already loaded in __init__
        if not self.buffer_pool.fetch_page(0):
            raise RuntimeError("CatalogPage (page 0) not found in buffer pool.")

        print("Creating table '{}'".format(table_name))


        if table_name in self.catalog_page.list_tables():
            raise TableAlreadyExistsError(table_name)

        # 为新表分配一个 TableHeapPage 来管理数据页
        table_heap_page_raw = self.buffer_pool.new_page()
        if not table_heap_page_raw:
            return False
        table_heap_page_id = table_heap_page_raw.page_id
        table_heap_page = TableHeapPage()
        table_heap_page.add_page_id(table_heap_page_id) # Add its own page_id as per requirement

        # 为新表分配第一个数据页
        first_data_page = self.buffer_pool.new_page()
        if not first_data_page:
            self.buffer_pool.unpin_page(table_heap_page_id, False) # Clean up
            return False
        first_data_page_id = first_data_page.page_id
        self.buffer_pool.unpin_page(first_data_page_id, True)

        # 将第一个数据页的 ID 添加到 TableHeapPage 中
        table_heap_page.add_page_id(first_data_page_id)
        print(table_heap_page.get_page_ids())

        table_heap_page_raw.data = bytearray(table_heap_page.serialize())
        print(table_heap_page_raw.data)
        self.buffer_pool.flush_page(table_heap_page_raw.page_id)
        self.buffer_pool.unpin_page(table_heap_page_id, True)

        # 为索引分配根页面 (index root page)
        # 假设索引也需要一个根页面，这里简化处理，实际B+树可能内部管理页面分配
        # 这里我们为B+树的根节点分配一个页面ID，并将其存储在CatalogPage中
        # B+Tree 内部会管理其页面的分配和持久化
        # Placeholder, B+Tree will manage its own root page ID
        rotate = self.buffer_pool.new_page()
        index_root_page_id = rotate.page_id
        table_heap_page.add_page_id(index_root_page_id)

        # Initialize a B+ tree for the new table
        b_plus_tree = BPlusTree(self.buffer_pool, index_root_page_id)
        self.indexes[table_name] = b_plus_tree

        # 将表的元数据添加到 CatalogPage，现在存储 table_heap_page_id
        # Convert schema list of tuples to dictionary for CatalogPage
        # schema_dict = {col_name: col_type for col_name, col_type in schema}
        # Convert schema list of ColumnDefinition objects to a dictionary for CatalogPage
        schema_dict = {col.name: col for col in columns}
        self.catalog_page.add_table(table_name, table_heap_page_id, index_root_page_id, schema_dict)
        catalog_page_raw.data = bytearray(self.catalog_page.serialize())
        self.buffer_pool.flush_page(catalog_page_raw.page_id)
        self.buffer_pool.unpin_page(0, True)
        return True

    def insert_row(self, table_name: str, row_data: bytes) -> bool:
        print("1")
        """插入一行数据"""
        catalog_page_raw = self.buffer_pool.fetch_page(0)
        if not catalog_page_raw:
            raise RuntimeError("CatalogPage (page 0) not found in buffer pool.")
        # No need to deserialize catalog_page here, it's already loaded in __init__
        # 1. 获取表的元数据
        table_metadata = self.catalog_page.get_table_metadata(table_name)
        table_heap_page_id = table_metadata['heap_root_page_id'] # 实际上是 table_heap_page_id

        # 获取 TableHeapPage
        table_heap_page_raw = self.buffer_pool.fetch_page(table_heap_page_id)
        if not table_heap_page_raw:
            return False
        table_heap_page = TableHeapPage.deserialize(table_heap_page_raw.data)

        # 找到最后一个数据页
        data_page_ids = table_heap_page.get_page_ids()
        last_data_page_id = data_page_ids[-1] if data_page_ids else None
        
        current_data_page_raw: Optional[Page] = None
        current_data_page: Optional[DataPage] = None

        if last_data_page_id is not None:
            current_data_page_raw = self.buffer_pool.fetch_page(last_data_page_id)
            if not current_data_page_raw:
                self.buffer_pool.unpin_page(table_heap_page_id, False)
                return False
            current_data_page = DataPage(current_data_page_raw.page_id, current_data_page_raw.data)




        # 如果没有数据页，或者最后一个数据页已满（或不足以容纳新行），则分配新页
        # 否则，将使用现有的未满数据页
        if current_data_page is None or self._page_is_full(current_data_page, len(row_data)):
            if current_data_page_raw:
                # If the existing page was full, unpin it and mark dirty
                self.buffer_pool.unpin_page(current_data_page_raw.page_id, True)

            new_page_raw = self.buffer_pool.new_page()
            if not new_page_raw:
                self.buffer_pool.unpin_page(table_heap_page_id, False)
                return False

            current_data_page = DataPage(new_page_raw.page_id, new_page_raw.data)
            current_data_page_raw = new_page_raw # Update the raw page reference

            table_heap_page.add_page_id(current_data_page.get_page_id())
            table_heap_page_raw.data = bytearray(table_heap_page.serialize())
            self.buffer_pool.unpin_page(table_heap_page_id, True) # Mark table_heap_page as dirty
        
        if not current_data_page or not current_data_page_raw:
            # This should not happen if logic is correct, but for safety
            self.buffer_pool.unpin_page(table_heap_page_id, False)
            return False

        # 2. 插入数据
        row_id = self._page_insert_row(current_data_page, row_data)
        if row_id is None:
            # If insertion failed, unpin the page and return False
            self.buffer_pool.unpin_page(current_data_page_raw.page_id, False) # Unpin the raw page
            self.buffer_pool.unpin_page(table_heap_page_id, False)
            return False

        # Mark the data page as dirty and unpin it after insertion
        current_data_page_raw.data = bytearray(current_data_page.get_data())


        # 3. 更新索引
        schema = self.catalog_page.get_table_metadata(table_name)['schema']

        pk_col_name = None
        pk_col_type = None
        pk_col_index = -1

        for i, (col_name, col_def) in enumerate(schema.items()):
            # constraints 存在于 ColumnDefinition.constraints
            if (ColumnConstraint.PRIMARY_KEY, None) in col_def.constraints:
                pk_col_name = col_name
                pk_col_type = col_def.data_type  # 注意这里是枚举 DataType
                pk_col_index = i
                break

        if pk_col_name is None:
            print(f"Error: No primary key defined for table {table_name}")
            return False

        # 解码主键值
        pk_value, _ = self._decode_value(row_data, pk_col_index, pk_col_type)

        # 存储主键到 (page_id, row_id) 的映射到B+树
        if table_name not in self.indexes:
            # This should ideally not happen if create_table is called first
            index_root_page_id = table_metadata['index_root_page_id']
            self.indexes[table_name] = BPlusTree(self.buffer_pool, index_root_page_id)
            # Serialize RID (page_id, row_id) to bytes

        rid_bytes = current_data_page_raw.page_id.to_bytes(4, 'little') + row_id.to_bytes(4, 'little')

        # Convert pk_value to bytes for B+ tree key
        # This is a simplified conversion, actual implementation might need more robust serialization
        if pk_col_type.name == "INT":
            pk_bytes = pk_value.to_bytes(4, 'little', signed=True)
        elif pk_col_type.name == "TEXT":
            pk_bytes = pk_value.encode('utf-8')
        else:
            raise NotImplementedError(f"Unsupported primary key type for indexing: {pk_col_type.name}")

        self.indexes[table_name].insert(pk_bytes, rid_bytes)

        self.buffer_pool.flush_page(current_data_page_raw.page_id)
        self.buffer_pool.unpin_page(current_data_page_raw.page_id, True)  # Mark as dirty and unpin

        return True

    @staticmethod
    def _page_is_full(data_page: DataPage, data_size: int) -> bool:
        """检查数据页是否已满"""
        print("2")
        print(data_size)
        print(data_page.get_free_space())
        return data_page.get_free_space() < data_size

    def _page_insert_row(self, data_page: DataPage, row_data: bytes) -> Optional[int]:
        """在数据页中插入一行数据"""
        print("执行_page_insert_row")
        try:
            from engine.constants import ROW_LENGTH_PREFIX_SIZE

            # 给每一行加上长度前缀
            row_length = len(row_data)
            record = row_length.to_bytes(ROW_LENGTH_PREFIX_SIZE, "little") + row_data

            offset = data_page.insert_record(record)
            return offset
        except ValueError:
            return None

        # Serialize RID (page_id, row_id) to bytes
        rid_bytes = page.page_id.to_bytes(4, 'little') + row_id.to_bytes(4, 'little')

        # Convert pk_value to bytes for B+ tree key
        # This is a simplified conversion, actual implementation might need more robust serialization
        if pk_col_type == "INT":
            pk_bytes = pk_value.to_bytes(4, 'little', signed=True)
        elif pk_col_type == "TEXT":
            pk_bytes = pk_value.encode('utf-8')
        else:
            raise NotImplementedError(f"Unsupported primary key type for indexing: {pk_col_type}")

        self.indexes[table_name].insert(pk_bytes, tuple(rid_bytes))

        self.buffer_pool.unpin_page(page.page_id, True) # Mark data page as dirty

        return True

    def delete_row(self, table_name: str, pk_value: Any) -> bool:
        """根据主键删除行数据"""
        if table_name not in self.indexes:
            return False

        # Convert pk_value to bytes for B+ tree search
        schema = self.catalog_page.get_table_metadata(table_name)['schema']
        pk_col_name, pk_col_type = schema[0]

        if pk_col_type == "INT":
            pk_bytes = pk_value.to_bytes(4, 'little', signed=True)
        elif pk_col_type == "TEXT":
            pk_bytes = pk_value.encode('utf-8')
        else:
            raise NotImplementedError(f"Unsupported primary key type for indexing: {pk_col_type}")

        rid_bytes = self.indexes[table_name].search(pk_bytes)
        if not rid_bytes:
            return False # Key not found in index

        # Deserialize RID (page_id, row_id) from bytes
        page_id = int.from_bytes(rid_bytes[0:4], 'little')
        row_id = int.from_bytes(rid_bytes[4:8], 'little')

        page = self.buffer_pool.fetch_page(page_id)
        if not page:
            return False

        # Delete the row from the page.
        delete_success = self._page_delete_row(page, row_id)
        if not delete_success:
            self.buffer_pool.unpin_page(page_id, False)
            return False

        # # Delete the entry from the B+ tree.
        # self.indexes[table_name].delete(pk_bytes)

        self.buffer_pool.unpin_page(page_id, True) # Mark data page as dirty
        return True

    def scan_table(self, table_name: str) -> List[bytes]:
        """扫描整个表"""
        results = []
        # 从 CatalogPage 获取表的 table_heap_page_id
        table_metadata = self.catalog_page.get_table_metadata(table_name)
        table_heap_page_id = table_metadata['heap_root_page_id'] # 实际上是 table_heap_page_id

        # 获取 TableHeapPage
        table_heap_page_raw = self.buffer_pool.fetch_page(table_heap_page_id)
        if not table_heap_page_raw:
            return results # No table heap page found, return empty results
        table_heap_page = TableHeapPage.deserialize(table_heap_page_raw.data)
        self.buffer_pool.unpin_page(table_heap_page_id, False)

        # 遍历 TableHeapPage 中记录的所有数据页，排除 TableHeapPage 自身的 page_id
        for data_page_id in table_heap_page.get_page_ids():
            if data_page_id == table_heap_page_id:
                continue
            page = self.buffer_pool.fetch_page(data_page_id)
            if not page:
                continue # Skip if page cannot be fetched

            # 从页中读取所有行
            data_page = DataPage(page.page_id, page.data)
            for offset, row_data in data_page.get_all_records():
                results.append(row_data)

            self.buffer_pool.unpin_page(data_page_id, False)
            # A proper implementation would involve a page directory or linked list of pages
            # For now, we'll just assume we can just try to create a new page if the root is full
            # and update the heap_root_page_id in catalog_page if a new root is needed.
            # This logic needs to be improved significantly for a real database.

        return results

    def _get_page(self, page_id: int) -> Optional[Page]:
        """获取指定页面"""
        return self.buffer_pool.fetch_page(page_id)

    def _get_last_page(self, table_name: str) -> Optional[Page]:
        """获取表的最后一页"""
        # 从 CatalogPage 获取表的 heap_root_page_id
        table_metadata = self.catalog_page.get_table_metadata(table_name)
        heap_root_page_id = table_metadata['heap_root_page_id']

        # 遍历数据页找到最后一页
        current_page_id = heap_root_page_id
        last_page = None
        while True:
            page = self.buffer_pool.fetch_page(current_page_id)
            if not page:
                break # No more pages
            last_page = page
            # 假设页面是连续分配的，或者有一个方式找到下一个页面
            # 实际中，需要从页面头部读取下一个页面的ID
            # For now, we'll just increment page_id, which is a simplification.
            self.buffer_pool.unpin_page(current_page_id, False)
            current_page_id += 1 # This is a very naive way to find next page
            # A proper implementation would involve a page directory or linked list of pages

        return last_page

    # def _create_new_page(self, table_name: str) -> Optional[Page]:
    #     """创建新页面"""
    #     # 从 CatalogPage 获取表的 heap_root_page_id
    #     table_metadata = self.catalog_page.get_table_metadata(table_name)
    #     heap_root_page_id = table_metadata['heap_root_page_id']
    #
    #     # 分配一个新的页面
    #     new_page = self.buffer_pool.new_page()
    #     if new_page:
    #         # 实际中，这里需要将新页面链接到表的现有页面链中
    #         # For now, we'll just return the new page.
    #         pass
    #     return new_page

    def _decode_value(self, row_data: bytes, offset: int, col_type) -> tuple[Any, int]:
        # 兼容 DataType 枚举和 str
        if isinstance(col_type, DataType):
            col_type = col_type.value
        value = None
        if col_type == "INT":
            value = int.from_bytes(row_data[offset : offset + 4], "little", signed=True)
            offset += 4
        elif col_type == "TEXT" or col_type == "STRING":
            length = int.from_bytes(row_data[offset : offset + 4], "little")
            offset += 4
            value = row_data[offset : offset + length].decode("utf-8")
            offset += length
        elif col_type == "FLOAT":
            import struct
            value = struct.unpack("<f", row_data[offset : offset + 4])[0]
            offset += 4
        else:
            raise NotImplementedError(f"Unsupported type for decoding: {col_type}")
        return value, offset

    def get_row_by_key(self, table_name: str, pk_value: Any) -> Optional[bytes]:
        """根据主键从索引中获取行数据"""
        if table_name not in self.indexes:
            return None

        # Convert pk_value to bytes for B+ tree search
        schema = self.catalog_page.get_table_metadata(table_name)['schema']
        pk_col_name, pk_col_type = schema[0]

        if pk_col_type == "INT":
            pk_bytes = pk_value.to_bytes(4, 'little', signed=True)
        elif pk_col_type == "TEXT":
            pk_bytes = pk_value.encode('utf-8')
        else:
            raise NotImplementedError(f"Unsupported primary key type for indexing: {pk_col_type}")

        rid_bytes = self.indexes[table_name].search(pk_bytes)
        if not rid_bytes:
            return None

        # Deserialize RID (page_id, row_id) from bytes
        page_id = int.from_bytes(rid_bytes[0:4], 'little')
        row_id = int.from_bytes(rid_bytes[4:8], 'little')

        page = self.buffer_pool.fetch_page(page_id)
        if not page:
            return None
        return self._page_get_row(page, row_id)

    # def delete_row(self, table_name: str, pk_value: Any) -> bool:
    #     """根据主键删除行数据"""
    #     if table_name not in self.indexes:
    #         return False
    #
    #     # Convert pk_value to bytes for B+ tree search
    #     schema = self.catalog.get_schema(table_name)
    #     pk_col_name, pk_col_type = schema[0]
    #
    #     if pk_col_type == "INT":
    #         pk_bytes = pk_value.to_bytes(4, 'little', signed=True)
    #     elif pk_col_type == "TEXT":
    #         pk_bytes = pk_value.encode('utf-8')
    #     else:
    #         raise NotImplementedError(f"Unsupported primary key type for indexing: {pk_col_type}")
    #
    #     rid_bytes = self.indexes[table_name].search(pk_bytes)
    #     if not rid_bytes:
    #         return False # Key not found in index
    #
    #     # Deserialize RID (page_id, row_id) from bytes
    #     page_id = int.from_bytes(rid_bytes[0:4], 'little')
    #     row_id = int.from_bytes(rid_bytes[4:8], 'little')
    #
    #     page = self.buffer_pool.fetch_page(page_id)
    #     if not page:
    #         return False
    #
    #     # Delete row from page
    #     success = self._page_delete_row(page, row_id)
    #     if not success:
    #         return False
    #
    #     # Delete from B+ tree index
    #     return self.indexes[table_name].delete(pk_bytes)

    def update_row(self, table_name: str, pk_value: Any, new_row_data: bytes) -> bool:
        """更新一行数据"""
        # 1. 获取表的元数据和索引
        table_metadata = self.catalog_page.get_table_metadata(table_name)
        if not table_metadata:
            return False

        if table_name not in self.indexes:
            index_root_page_id = table_metadata['index_root_page_id']
            self.indexes[table_name] = BPlusTree(self.buffer_pool, index_root_page_id)

        # 2. 根据主键查找旧记录的 RID (page_id, row_id)
        schema = self.catalog_page.get_table_metadata(table_name)['schema']
        pk_col_name, pk_col_type = schema[0]

        if pk_col_type == "INT":
            pk_bytes = pk_value.to_bytes(4, 'little', signed=True)
        elif pk_col_type == "TEXT":
            pk_bytes = pk_value.encode('utf-8')
        else:
            raise NotImplementedError(f"Unsupported primary key type for indexing: {pk_col_type}")

        rid_bytes = self.indexes[table_name].search(pk_bytes)
        if not rid_bytes:
            return False # 旧记录不存在

        old_page_id = int.from_bytes(rid_bytes[0:4], 'little')
        old_row_id = int.from_bytes(rid_bytes[4:8], 'little')

        # 3. 获取旧记录所在的页面
        old_page = self.buffer_pool.fetch_page(old_page_id)
        if not old_page:
            return False

        # 4. 尝试原地更新
        old_row_offset, old_row_length = self._page_get_row_info(old_page, old_row_id)
        if old_row_offset is not None and self._page_update_row_in_place(old_page, old_row_offset, new_row_data):
            self.buffer_pool.unpin_page(old_page_id, True) # Mark page dirty
            return True # 原地更新成功

        # 5. 如果无法原地更新（新数据太大），则执行删除旧记录并插入新记录的逻辑
        # 删除旧记录
        old_row_data_retrieved = self._page_delete_row(old_page, old_row_id)
        if not old_row_data_retrieved:
            self.buffer_pool.unpin_page(old_page_id, False)
            return False
        self.buffer_pool.unpin_page(old_page_id, True) # Mark old page as dirty

        # 插入新记录 (这会处理页面空间和 TableHeapPage 更新，并更新 B+ 树)
        return self.insert_row(table_name, new_row_data)

    # --- 页面内部数据管理辅助方法 ---
    # 定义页面内部结构常量
    # 假设页面头部有 4 字节用于存储下一个空闲偏移量 (next_free_offset)
    # 假设每个行数据前有 2 字节存储其长度
    PAGE_HEADER_SIZE = 4  # Bytes for next_free_offset
    ROW_LENGTH_PREFIX_SIZE = 2  # Bytes for row length

    def _page_is_full(self, page: DataPage, row_size: int) -> bool:
        """检查页面是否有足够的空间容纳新行"""
        return page.is_full() or page.get_free_space() < row_size

    def _page_insert_row(self, page: DataPage, row_data: bytes) -> Optional[int]:
        """在页面中插入一行数据，返回行ID（偏移量）"""
        try:
            offset = page.insert_record(row_data)
            return offset
        except ValueError:
            return None
        page.data[current_offset : current_offset + len(row_data)] = row_data

        # 更新 next_free_offset
        new_next_free_offset = current_offset + len(row_data)
        page.data[0:self.PAGE_HEADER_SIZE] = new_next_free_offset.to_bytes(self.PAGE_HEADER_SIZE, 'little')

        # 返回行ID，这里简化为行数据的起始偏移量
        return next_free_offset

    def _page_get_row(self, page: Page, row_offset: int) -> Optional[bytes]:
        """从页面中获取指定偏移量的行数据"""
        if row_offset < 0 or row_offset >= len(page.data):
            return None  # 偏移量无效或在头部区域

        try:
            # 读取行长度
            row_length = int.from_bytes(page.data[row_offset : row_offset + ROW_LENGTH_PREFIX_SIZE], 'little')
            data_start_offset = row_offset + ROW_LENGTH_PREFIX_SIZE

            # 如果 row_length 为 0，表示该行已被逻辑删除
            if row_length == 0:
                return None

            # 读取行数据
            row_data = page.data[data_start_offset : data_start_offset + row_length]
            return bytes(row_data)
        except IndexError:
            return None  # 数据越界

    def _page_update_row_in_place(self, page: Page, row_offset: int, new_row_data: bytes) -> bool:
        """更新页面中的一行数据"""
        data_page = DataPage(page.page_id, page.data)
        try:
            # For simplicity, assuming update does not change record size significantly
            # In a real system, this would involve more complex free space management
            # This method needs to be adjusted based on how DataPage handles updates.
            # For now, we'll assume DataPage.update_record can handle the update given an offset.
            # The current DataPage.update_record assumes the record size doesn't change.
            # If the new_row_data length is different, this will be an issue.
            # For now, let's assume the new_row_data is the same length as the old one for in-place update.
            # This needs to be consistent with how records are stored and updated in DataPage.
            data_page.update_record(row_offset, new_row_data)
            page.data = bytearray(data_page.get_data()) # Update the raw page data
            page.is_dirty = True
            return True
        except IndexError:
            return False

    def _page_delete_row(self, page: Page, row_offset: int) -> bool:
        """从页面中删除指定偏移量的行数据"""
        # 简单的删除：将行数据标记为已删除（例如，将长度设为0）
        # 实际的删除会涉及空闲空间管理，例如使用位图或空闲链表。
        # 这里我们只是将该行的数据长度标记为0，表示逻辑删除。
        if row_offset < 0 or row_offset >= len(page.data):
            return False  # 偏移量无效或在头部区域

        try:
            from engine.constants import ROW_LENGTH_PREFIX_SIZE
            # 读取行长度
            row_length = int.from_bytes(page.data[row_offset : row_offset + ROW_LENGTH_PREFIX_SIZE], 'little')

            # 将长度标记为0，表示删除
            page.data[row_offset : row_offset + ROW_LENGTH_PREFIX_SIZE] = (0).to_bytes(ROW_LENGTH_PREFIX_SIZE, 'little')
            # 清空数据（可选，但有助于调试和避免数据泄露）
            # page.data[row_offset + self.ROW_LENGTH_PREFIX_SIZE : row_offset + self.ROW_LENGTH_PREFIX_SIZE + row_length] = bytearray(row_length)
            return True
        except IndexError:
            return False  # 数据越界
