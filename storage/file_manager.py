#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
from typing import Optional
from .page import Page

class FileManager:
    """文件管理器，负责页面的磁盘读写"""
    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        os.makedirs(data_dir, exist_ok=True)
    
    def create_table_file(self, table_name: str) -> bool:
        """创建表文件"""
        file_path = self._get_table_file_path(table_name)
        try:
            if not os.path.exists(file_path):
                open(file_path, 'wb').close()
            return True
        except Exception as e:
            print(f"Error creating table file: {e}")
            return False
    
    def read_page(self, table_name: str, page_id: int) -> Optional[Page]:
        """从磁盘读取页面"""
        file_path = self._get_table_file_path(table_name)
        try:
            with open(file_path, 'rb') as f:
                # 定位到页面位置
                f.seek(page_id * Page.PAGE_SIZE)
                data = f.read(Page.PAGE_SIZE)
                if not data:
                    return None
                    
                # 创建页面并加载数据
                page = Page(page_id)
                page.data = bytearray(data)
                return page
        except Exception as e:
            print(f"Error reading page: {e}")
            return None
    
    def write_page(self, table_name: str, page: Page) -> bool:
        """将页面写入磁盘"""
        file_path = self._get_table_file_path(table_name)
        try:
            with open(file_path, 'r+b') as f:
                # 定位到页面位置
                f.seek(page.page_id * Page.PAGE_SIZE)
                f.write(page.data)
                return True
        except Exception as e:
            print(f"Error writing page: {e}")
            return False
    
    def get_table_size(self, table_name: str) -> int:
        """获取表文件大小（字节数）"""
        file_path = self._get_table_file_path(table_name)
        try:
            return os.path.getsize(file_path)
        except Exception:
            return 0
    
    def _get_table_file_path(self, table_name: str) -> str:
        """获取表文件路径"""
        return os.path.join(self.data_dir, f"{table_name}.db")