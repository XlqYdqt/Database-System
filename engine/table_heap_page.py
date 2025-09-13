from typing import List
import struct

class TableHeapPage:
    """
    TableHeapPage 存储一个表的所有数据页的 page_id 列表。

    新的序列化格式（向后兼容）：
      [MAGIC(4 bytes) = b'THP1'][COUNT(4 bytes, little-endian unsigned int)][PAGE_ID_1(4)][PAGE_ID_2(4)]...

    旧的（历史）格式：只是连续的 4-byte unsigned ints（没有头部）。
    反序列化会优先识别带 MAGIC 的新版格式；如果没有 magic，则退回到旧格式的解析。
    这样可以保证既能使用更安全的有头格式，又能兼容磁盘上已有的旧页面。
    """

    PAGE_ID_SIZE = 4  # 每个 page_id 占用的字节数
    MAGIC = b'THP1'   # 文件格式签名（4 字节）

    def __init__(self, page_ids: List[int] = None):
        self.page_ids = page_ids if page_ids is not None else []

    def add_page_id(self, page_id: int):
        """添加一个数据页的 page_id 到列表中"""
        self.page_ids.append(page_id)

    def get_page_ids(self) -> List[int]:
        """获取所有数据页的 page_id 列表"""
        return self.page_ids

    def serialize(self) -> bytes:
        """将 TableHeapPage 序列化为字节（使用 MAGIC + COUNT + ids）。

        返回值示例（page_ids=[122, 123]）:
          b'THP1' + b'\x02\x00\x00\x00' + b'z\x00\x00\x00' + b'{\x00\x00\x00'
        """
        count = len(self.page_ids)
        parts = [self.MAGIC, struct.pack('<I', count)]
        if count > 0:
            # 一次打包所有 page_id（little-endian unsigned ints）
            parts.append(struct.pack(f'<{count}I', *self.page_ids))
        serialized_data = b''.join(parts)

        # 填充到 PAGE_SIZE
        from engine.constants import PAGE_SIZE
        padding_size = PAGE_SIZE - len(serialized_data)
        if padding_size < 0:
            raise ValueError(f"Serialized data size ({len(serialized_data)}) exceeds PAGE_SIZE ({PAGE_SIZE})")
        return serialized_data + b'\x00' * padding_size

    @staticmethod
    def deserialize(data: bytes):
        """从字节反序列化 TableHeapPage。

        - 如果数据以 MAGIC 开头，则按新格式解析（MAGIC + COUNT + ids）
        - 否则按旧格式解析（纯 4-byte ints 顺序）
        """
        if not data:
            return TableHeapPage([])

        # 新格式检测（MAGIC）
        if len(data) >= 4 and data[:4] == TableHeapPage.MAGIC:
            # 至少需要 8 字节（MAGIC + COUNT）
            if len(data) < 8:
                return TableHeapPage([])
            count = struct.unpack('<I', data[4:8])[0]
            max_possible = (len(data) - 8) // TableHeapPage.PAGE_ID_SIZE
            # 防止损坏的 count 导致越界，取可用的最小值作为实际数量
            count = min(count, max_possible)
            page_ids = []
            if count > 0:
                start = 8
                end = start + count * TableHeapPage.PAGE_ID_SIZE
                fmt = f'<{count}I'
                page_ids = list(struct.unpack(fmt, data[start:end]))
            return TableHeapPage(page_ids)

        # 旧格式（无头）：把整个数据按 4 字节切块解析为 page_id
        valid_len = (len(data) // TableHeapPage.PAGE_ID_SIZE) * TableHeapPage.PAGE_ID_SIZE
        page_ids = []
        for i in range(0, valid_len, TableHeapPage.PAGE_ID_SIZE):
            page_id = struct.unpack('<I', data[i:i + TableHeapPage.PAGE_ID_SIZE])[0]
            page_ids.append(page_id)
        return TableHeapPage(page_ids)

    def __len__(self):
        """返回序列化后的字节数（包含 MAGIC + COUNT 头部）。"""
        return 8 + len(self.page_ids) * self.PAGE_ID_SIZE

    def __str__(self):
        return f"TableHeapPage(page_ids={self.page_ids})"

    def __repr__(self):
        return self.__str__()
