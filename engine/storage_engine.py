#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import List, Dict, Optional, Any
from ..storage.lru_replacer import Page
from ..storage.buffer_pool_manager import BufferPool

class StorageEngine:
    """存储引擎，负责行数据和页面之间的映射"""
    def __init__(self, buffer_pool_size: int = 1024):
        self.buffer_pool = BufferPool(buffer_pool_size)
        self.next_page_id = 0
    
    def create_table(self, table_name: str) -> bool:
        """创建表文件"""
        # TODO: 创建表文件
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
        return page.insert_row(row_data)
    
    def scan_table(self, table_name: str) -> List[bytes]:
        """扫描整个表"""
        results = []
        page_id = 0
        
        while True:
            page = self._get_page(table_name, page_id)
            if not page:
                break
                
            # TODO: 从页中读取所有行
            page_id += 1
            
        return results
    
    def _get_page(self, table_name: str, page_id: int) -> Optional[Page]:
        """获取指定页面"""
        # TODO: 实现页面获取
        return None
    
    def _get_last_page(self, table_name: str) -> Optional[Page]:
        """获取表的最后一页"""
        # TODO: 实现最后页面获取
        return None
    
    def _create_new_page(self, table_name: str) -> Optional[Page]:
        """创建新页面"""
        page = self.buffer_pool.new_page()
        if page:
            # TODO: 将页面与表关联
            self.next_page_id += 1
        return page