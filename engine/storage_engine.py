#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import List, Dict, Optional, Any
# from storage.page import Page
from storage.buffer_pool_manager import BufferPoolManager
from storage.disk_manager import DiskManager

class StorageEngine:
    """存储引擎，负责行数据和页面之间的映射"""
    def __init__(self, buffer_pool_size: int = 1024):
        self.buffer_pool = BufferPoolManager(buffer_pool_size)
        self.next_page_id = 0
        self.file_manager = DiskManager("data")
    
    def create_table(self, table_name: str) -> bool:
        """创建表文件"""
        return self.file_manager.create_table_file(table_name)
    
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
        return page.insert_row(row_data)
    
    def scan_table(self, table_name: str) -> List[bytes]:
        """扫描整个表"""
        results = []
        page_id = 0
        
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
        # TODO: 实现页面获取
        return None
    
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