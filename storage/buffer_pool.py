#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import Dict, Optional
from collections import OrderedDict
from .page import Page

class BufferPool:
    """缓存管理器，使用LRU策略管理内存中的页"""
    def __init__(self, pool_size: int):
        self.pool_size = pool_size  # 缓存池大小（页数）
        self.pages: Dict[int, Page] = {}  # 页ID到页的映射
        self.lru_cache = OrderedDict()  # LRU缓存，记录页的访问顺序
    
    def get_page(self, page_id: int) -> Optional[Page]:
        """获取指定ID的页，如果不在内存中则返回None"""
        if page_id in self.pages:
            # 更新LRU缓存
            self.lru_cache.move_to_end(page_id)
            return self.pages[page_id]
        return None
    
    def new_page(self) -> Optional[Page]:
        """分配一个新页"""
        # TODO: 实现页面分配
        return None
    
    def pin_page(self, page_id: int) -> None:
        """固定一个页，防止被替换"""
        if page_id in self.pages:
            self.pages[page_id].pin_count += 1
    
    def unpin_page(self, page_id: int) -> None:
        """解除页的固定状态"""
        if page_id in self.pages:
            page = self.pages[page_id]
            if page.pin_count > 0:
                page.pin_count -= 1
    
    def flush_page(self, page_id: int) -> None:
        """将页写回磁盘"""
        if page_id in self.pages:
            page = self.pages[page_id]
            if page.is_dirty:
                # TODO: 实现页面写回
                page.is_dirty = False
    
    def _evict_page(self) -> bool:
        """驱逐一个页面"""
        # 查找可以驱逐的页（未被固定且最久未使用）
        for page_id in list(self.lru_cache.keys()):
            page = self.pages[page_id]
            if page.pin_count == 0:
                if page.is_dirty:
                    self.flush_page(page_id)
                del self.pages[page_id]
                del self.lru_cache[page_id]
                return True
        return False