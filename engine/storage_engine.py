#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import List, Dict, Optional, Any
from storage.buffer_pool_manager import Page
from storage.buffer_pool_manager import BufferPoolManager
from storage.disk_manager import DiskManager
from engine.Catelog.catelog import Catalog # Import Catalog
from storage.b_plus_tree import BPlusTree # Import BPlusTree

class StorageEngine:
    """存储引擎，负责行数据和页面之间的映射"""
    def __init__(self, catalog: Catalog, buffer_pool_size: int = 1024):
        self.buffer_pool = BufferPoolManager(buffer_pool_size)
        self.file_manager = DiskManager("data")
        self.catalog = catalog # Store the catalog instance
        self.indexes: Dict[str, BPlusTree] = {} # Change to BPlusTree

    def _get_table_metadata_page(self, table_name: str) -> Optional[Page]:
        """获取表的元数据页 (page_id=0)"""
        return self.buffer_pool.fetch_page(0, table_name)

    def _update_table_metadata(self, table_name: str, total_pages: int):
        """更新表的元数据，特别是总页数"""
        metadata_page = self._get_table_metadata_page(table_name)
        if not metadata_page:
            # This should not happen if create_table is called first
            return
        # 简单地将 total_pages 写入元数据页的开始部分
        # 实际应用中可能需要更复杂的序列化和反序列化
        metadata_page.write_bytes(total_pages.to_bytes(4, 'little'), 0)
        self.buffer_pool.unpin_page(metadata_page.page_id, True)

    def _read_table_metadata(self, table_name: str) -> Dict[str, Any]:
        """读取表的元数据"""
        metadata_page = self._get_table_metadata_page(table_name)
        if not metadata_page:
            return {"total_pages": 0}
        # 简单地从元数据页的开始部分读取 total_pages
        total_pages = int.from_bytes(metadata_page.read_bytes(0, 4), 'little')
        self.buffer_pool.unpin_page(metadata_page.page_id, False)
        return {"total_pages": total_pages}


    
    # ------------------------
    # 表管理
    # ------------------------
    def create_table(self, table_name: str) -> bool:
        """为新表分配首页 (page_id=0) 并初始化索引和元数据"""
        # 尝试获取 page_id=0 作为元数据页
        metadata_page = self.buffer_pool.new_page(table_name, 0) # 明确请求 page_id=0
        if not metadata_page:
            return False

        # 初始化元数据：总页数从1开始（包含元数据页本身）
        self._update_table_metadata(table_name, 1) # metadata page is page 0

        # Initialize a B+ tree for the new table
        self.indexes[table_name] = BPlusTree()
        return True
    
    def insert_row(self, table_name: str, row_data: bytes) -> bool:
        """插入一行数据"""
        # 1. 获取表的最后一页
        page = self._get_last_page(table_name)
        if not page or page.is_full(len(row_data)):
            # 如果页面已满，创建新页
            page = self._create_new_page(table_name)
            if not page:
                return False
        
        # 2. 插入数据
        row_id = page.insert_row(row_data)
        if row_id is None:
            return False

        # 3. 更新索引
        schema = self.catalog.get_schema(table_name)
        # 假设第一个字段是主键
        pk_col_name, pk_col_type = schema[0]

        # 解码主键值
        pk_value, _ = self._decode_value(row_data, 0, pk_col_type)

        # 存储主键到 (page_id, row_id) 的映射到B+树
        if table_name not in self.indexes:
            # This should ideally not happen if create_table is called first
            self.indexes[table_name] = BPlusTree()
        
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

        self.indexes[table_name].insert(pk_bytes, rid_bytes)

        return True
    
    def scan_table(self, table_name: str) -> List[bytes]:
        """扫描整个表"""
        results = []
        # 从元数据中获取总页数
        metadata = self._read_table_metadata(table_name)
        total_pages = metadata["total_pages"]
        if total_pages == 0:
            return results # No pages in table

        # 从 page_id=1 开始扫描数据页，因为 page_id=0 是元数据页
        page_id = 1
        
        # Use B+ tree to scan all RIDs and fetch rows
        if table_name in self.indexes:
            # This is a simplified scan, a full B+ tree scan would iterate through leaf nodes
            # For now, we'll keep the existing page-by-page scan for full table scan
            # and use B+ tree for direct key lookups.
            pass # Keep existing scan logic for full table scan

        while True:
            page = self._get_page(table_name, page_id)
            if not page:
                break
                
            # 从页中读取所有行
            row_id = 0
            while True:
                row_data = page.get_row(row_id)
                if not row_data:
                    break
                results.append(row_data)
                row_id += 1
            
            page_id += 1
            
        return results
    
    def _get_page(self, table_name: str, page_id: int) -> Optional[Page]:
        """获取指定页面"""
        return self.buffer_pool.fetch_page(page_id, table_name)
    
    def _get_last_page(self, table_name: str) -> Optional[Page]:
        """获取表的最后一页"""
        # 从元数据中获取总页数
        metadata = self._read_table_metadata(table_name)
        total_pages = metadata["total_pages"]

        if total_pages <= 1: # 只有元数据页或没有数据页
            return None

        # 最后一页的ID是 total_pages - 1 (因为 page_id 从 0 开始，且 page_id=0 是元数据页)
        last_page_id = total_pages - 1

        # 先从缓存池中查找
        page = self.buffer_pool.fetch_page(last_page_id, table_name)
        if page:
            return page

        # 如果不在缓存池中，尝试从文件读取 (这应该由 BufferPoolManager 内部处理)
        # 注意: DiskManager.read_page 通常不直接由 StorageEngine 调用，而是通过 BufferPoolManager
        # 这里保留是为了逻辑完整性，但实际操作应依赖 BufferPoolManager 的 fetch_page
        # page = self.file_manager.read_page(table_name, last_page_id)
        # if page:
        #     self.buffer_pool.pin_page(last_page_id) # fetch_page 已经处理了 pin
        # return page
        return None # 如果 fetch_page 失败，则返回 None
    
    def _create_new_page(self, table_name: str) -> Optional[Page]:
        """创建新页面"""
        # 从元数据中获取下一个可用的 page_id
        metadata = self._read_table_metadata(table_name)
        next_page_id = metadata["total_pages"]

        page = self.buffer_pool.new_page(table_name, next_page_id)
        if page:
            # 更新元数据中的总页数
            self._update_table_metadata(table_name, next_page_id + 1)
        return page

    def _decode_value(self, row_data: bytes, offset: int, col_type: str) -> tuple[Any, int]:
        """根据 schema 解码单个值"""
        if col_type == "INT":
            value = int.from_bytes(row_data[offset:offset + 4], "little", signed=True)
            offset += 4
        elif col_type == "TEXT":
            length = int.from_bytes(row_data[offset:offset + 2], "little")
            offset += 2
            value = row_data[offset:offset + length].decode("utf-8")
            offset += length
        else:
            raise NotImplementedError(f"Unsupported type: {col_type}")
        return value, offset

    def get_row_by_key(self, table_name: str, pk_value: Any) -> Optional[bytes]:
        """根据主键从索引中获取行数据"""
        if table_name not in self.indexes:
            return None

        # Convert pk_value to bytes for B+ tree search
        schema = self.catalog.get_schema(table_name)
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
        return page.get_row(row_id)

    def delete_row(self, table_name: str, pk_value: Any) -> bool:
        """根据主键删除行数据"""
        if table_name not in self.indexes:
            return False

        # Convert pk_value to bytes for B+ tree search
        schema = self.catalog.get_schema(table_name)
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

        # Delete row from page
        success = page.delete_row(row_id)
        if not success:
            return False

        # Delete from B+ tree index
        return self.indexes[table_name].delete(pk_bytes)

    def update_row(self, table_name: str, pk_value: Any, new_row_data: bytes) -> bool:
        """根据主键更新行数据"""
        # TODO: 实现更新逻辑，可能需要先删除旧行，再插入新行，或者直接在页内更新
        # 考虑到变长数据，直接在页内更新可能比较复杂，通常是删除旧的RID，插入新的RID
        if table_name not in self.indexes:
            return False

        # Convert pk_value to bytes for B+ tree search
        schema = self.catalog.get_schema(table_name)
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

        # For update, we'll delete the old row and insert the new one.
        # This simplifies handling variable-length data and ensures data integrity.
        # First, delete the old row from the page.
        delete_success = page.delete_row(row_id)
        if not delete_success:
            return False

        # Then, delete the old entry from the B+ tree.
        self.indexes[table_name].delete(pk_bytes)

        # Now, insert the new row data.
        # This will create a new RID and update the B+ tree with the new RID.
        return self.insert_row(table_name, new_row_data)
