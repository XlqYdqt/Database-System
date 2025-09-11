from typing import List
import struct

class TableHeapPage:
    """
    TableHeapPage 存储一个表的所有数据页的 page_id 列表。
    这个页面本身将由 BufferPoolManager 管理。
    """
    PAGE_ID_SIZE = 4 # Each page_id is an integer, typically 4 bytes

    def __init__(self, page_ids: List[int] = None):
        self.page_ids = page_ids if page_ids is not None else []

    def add_page_id(self, page_id: int):
        """添加一个数据页的 page_id 到列表中"""
        self.page_ids.append(page_id)

    def get_page_ids(self) -> List[int]:
        """获取所有数据页的 page_id 列表"""
        return self.page_ids

    def serialize(self) -> bytes:
        """将 TableHeapPage 序列化为字节"""
        # 简单地将所有 page_id 拼接起来
        # 假设 page_id 是 int，使用 struct.pack 转换为字节
        serialized_data = b''
        for page_id in self.page_ids:
            serialized_data += struct.pack('<I', page_id) # <I for unsigned int (little-endian)
        return serialized_data

    @staticmethod
    def deserialize(data: bytes):
        """从字节反序列化 TableHeapPage"""
        page_ids = []
        # 每次读取 PAGE_ID_SIZE 字节，直到数据结束
        for i in range(0, len(data), TableHeapPage.PAGE_ID_SIZE):
            page_id = struct.unpack('<I', data[i:i + TableHeapPage.PAGE_ID_SIZE])[0]
            page_ids.append(page_id)
        return TableHeapPage(page_ids)

    def __len__(self):
        """返回序列化后的大小（字节）"""
        return len(self.page_ids) * self.PAGE_ID_SIZE

    def __str__(self):
        return f"TableHeapPage(page_ids={self.page_ids})"

    def __repr__(self):
        return self.__str__()
    