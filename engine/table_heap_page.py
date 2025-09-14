from typing import List
import struct

from engine.constants import PAGE_SIZE


class TableHeapPage:
    """
    表堆页（TableHeapPage），它不存实际数据，而是作为目录，
    存储一个表所有数据页（DataPage）的 page_id 列表。
    格式: [MAGIC(4字节)][COUNT(4字节)][PAGE_ID_1(4字节)][PAGE_ID_2]...
    """
    PAGE_ID_SIZE = 4
    MAGIC = b'THP1'  # 格式签名，用于识别页面类型
    HEADER_SIZE = 8

    def __init__(self, page_ids: List[int] = None):
        self.page_ids = page_ids if page_ids is not None else []

    def add_page_id(self, page_id: int):
        """添加一个数据页的 page_id 到列表中。"""
        self.page_ids.append(page_id)

    def get_page_ids(self) -> List[int]:
        """获取所有数据页的 page_id 列表。"""
        return self.page_ids

    def serialize(self) -> bytes:
        """将 TableHeapPage 序列化为字节。"""
        count = len(self.page_ids)
        parts = [self.MAGIC, struct.pack('<I', count)]  # 写入头部
        if count > 0:
            parts.append(struct.pack(f'<{count}I', *self.page_ids))
        serialized_data = b''.join(parts)

        padding_size = PAGE_SIZE - len(serialized_data)
        if padding_size < 0:
            raise ValueError(f"序列化后的表堆页大小 ({len(serialized_data)}) 超出页面限制 ({PAGE_SIZE})")
        return serialized_data + b'\0' * padding_size

    @staticmethod
    def deserialize(data: bytes):
        """从字节反序列化出 TableHeapPage 对象。"""
        if not data or len(data) < TableHeapPage.HEADER_SIZE or data[:4] != TableHeapPage.MAGIC:
            # 如果没有数据，或长度不足，或MAGIC签名不匹配，则返回空对象
            return TableHeapPage([])

        count = struct.unpack('<I', data[4:8])[0]
        # 安全检查，防止因count损坏导致读取越界
        max_possible = (len(data) - TableHeapPage.HEADER_SIZE) // TableHeapPage.PAGE_ID_SIZE
        count = min(count, max_possible)

        page_ids = []
        if count > 0:
            start = TableHeapPage.HEADER_SIZE
            end = start + count * TableHeapPage.PAGE_ID_SIZE
            page_ids = list(struct.unpack(f'<{count}I', data[start:end]))
        return TableHeapPage(page_ids)
