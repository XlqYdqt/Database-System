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
        self.next_page_id = 0
        self.file_manager = DiskManager("data")
        self.catalog = catalog # Store the catalog instance
        self.indexes: Dict[str, BPlusTree] = {} # Change to BPlusTree
    
    # ------------------------
    # 表管理
    # ------------------------
    def create_table(self, table_name: str) -> bool:
        """为新表分配首页并初始化索引"""
        page = self.buffer_pool.new_page()
        if not page:
            return False
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
        page_id = 0
        
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
        return self.buffer_pool.fetch_page(page_id)
    
    def _get_last_page(self, table_name: str) -> Optional[Page]:
        """获取表的最后一页"""
        # 获取表文件大小
        file_size = self.file_manager.get_table_size(table_name)
        if file_size == 0:
            return None
            
        # 计算最后一页的页面ID
        last_page_id = (file_size - 1) // Page.PAGE_SIZE
        
        # 先从缓存池中查找
        page = self.buffer_pool.get_page(last_page_id)
        if page:
            return page
            
        # 如果不在缓存池中，从文件读取
        page = self.file_manager.read_page(table_name, last_page_id)
        if page:
            # 将页面放入缓存池
            self.buffer_pool.pin_page(last_page_id)
        return page
    
    def _create_new_page(self, table_name: str) -> Optional[Page]:
        """创建新页面"""
        page = self.buffer_pool.new_page()
        if page:
            # TODO: 将页面与表关联
            self.next_page_id += 1
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
