#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import List, Dict, Optional

class Page:
    """页结构，用于存储数据行"""
    PAGE_SIZE = 4096  # 页大小，单位字节
    
    def __init__(self, page_id: int):
        self.page_id = page_id
        self.data = bytearray(self.PAGE_SIZE)  # 页数据
        self.is_dirty = False  # 是否被修改
        self.pin_count = 0     # 引用计数
        
    def get_free_space(self) -> int:
        """获取页中的空闲空间大小"""
        # TODO: 实现空闲空间计算
        return self.PAGE_SIZE
    
    def insert_row(self, row_data: bytes) -> bool:
        """插入一行数据"""
        # TODO: 实现数据插入
        return False
    
    def delete_row(self, row_id: int) -> bool:
        """删除一行数据"""
        # TODO: 实现数据删除
        return False
    
    def update_row(self, row_id: int, new_data: bytes) -> bool:
        """更新一行数据"""
        # TODO: 实现数据更新
        return False
    
    def get_row(self, row_id: int) -> Optional[bytes]:
        """获取一行数据"""
        # TODO: 实现数据读取
        return None
    
    def is_full(self, row_size: int) -> bool:
        """检查是否有足够空间存储新行"""
        return self.get_free_space() < row_size