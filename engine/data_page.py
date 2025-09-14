# data_page.py
from typing import List, Tuple, Optional

from engine.constants import PAGE_SIZE, ROW_LENGTH_PREFIX_SIZE


class DataPage:
    """数据页（DataPage），负责存储表的实际行记录。"""

    def __init__(self, page_id: int, data: bytes = b''):
        self.page_id = page_id
        self.data = bytearray(data) if data else bytearray(PAGE_SIZE)

        # [优化] 通过扫描页面来确定真实的空闲空间指针，而不是简单地取len(data)
        # 这使得页面布局的管理更加健壮。
        self.free_space_pointer = self._calculate_free_space_pointer()

    def _calculate_free_space_pointer(self) -> int:
        """扫描页面以找到第一个可用的空闲空间起始位置。"""
        offset = 0
        while offset < PAGE_SIZE:
            # 如果剩余空间不足以读取一个长度前缀，说明已到末尾
            if offset + ROW_LENGTH_PREFIX_SIZE > PAGE_SIZE:
                return offset

            record_len = int.from_bytes(self.data[offset:offset + ROW_LENGTH_PREFIX_SIZE], "little")

            # 遇到第一个长度为0的记录，且后面都是0，说明这是空闲空间的开始
            if record_len == 0:
                return offset

            offset += record_len
        return offset

    def get_free_space(self) -> int:
        """返回页面中剩余的可用空间大小。"""
        return PAGE_SIZE - self.free_space_pointer

    def insert_record(self, record_data: bytes) -> int:
        """在页面末尾插入一条新记录。"""
        if self.get_free_space() < len(record_data):
            raise ValueError("页面空间不足，无法插入记录。")
        offset = self.free_space_pointer
        self.data[offset:offset + len(record_data)] = record_data
        self.free_space_pointer += len(record_data)
        return offset

    def update_record(self, offset: int, new_record: bytes) -> Tuple[int, bool]:
        """
        更新指定偏移量的记录。
        - 如果新记录不长于旧记录，则原地更新。
        - 如果新记录更长，则将旧记录标记为删除，并在页面末尾插入新记录。
        返回 (最终记录的偏移量, 是否发生移动)。
        """
        if offset < 0 or offset + ROW_LENGTH_PREFIX_SIZE > len(self.data):
            raise IndexError("无效的记录偏移量。")

        # [修正] 读取旧记录的完整长度（包括长度前缀）
        existing_total_length = int.from_bytes(self.data[offset: offset + ROW_LENGTH_PREFIX_SIZE], "little")

        # 情形A: 新记录长度小于等于旧记录，可以原地更新
        if len(new_record) <= existing_total_length:
            self.data[offset:offset + len(new_record)] = new_record
            # 将旧记录剩余的部分用空字节覆盖，防止脏数据
            zero_start = offset + len(new_record)
            zero_end = offset + existing_total_length
            self.data[zero_start:zero_end] = b'\0' * (zero_end - zero_start)
            return offset, False  # 未移动

        # 情形B: 新记录更长，需要移动
        if self.get_free_space() < len(new_record):
            raise ValueError("页面空间不足，无法更新记录。")

        # 将原记录标记为已删除（长度置0）
        self.delete_record(offset)

        # 在页面末尾插入新记录
        new_offset = self.insert_record(new_record)
        return new_offset, True  # 已移动

    def get_data(self) -> bytes:
        """返回页面的字节数据。"""
        return bytes(self.data)

    def get_all_records(self) -> List[Tuple[int, bytes]]:
        """遍历并返回页面中所有有效的记录。"""
        records = []
        current_offset = 0
        while current_offset < self.free_space_pointer:
            if current_offset + ROW_LENGTH_PREFIX_SIZE > self.free_space_pointer:
                break

            record_length = int.from_bytes(self.data[current_offset: current_offset + ROW_LENGTH_PREFIX_SIZE], "little")

            # [修正] 如果记录长度为0，说明是已删除记录或填充区，应跳过并继续扫描
            if record_length == 0:
                # 找到下一个非零字节，作为可能的下一条记录的开始
                # 这是一个简化的处理方式，更复杂的系统会使用槽位图（slot directory）
                next_offset = current_offset + 1
                while next_offset < self.free_space_pointer and self.data[next_offset] == 0:
                    next_offset += 1

                # 如果后面全是0，则扫描结束
                if next_offset >= self.free_space_pointer:
                    break
                current_offset = next_offset
                continue

            record_end = current_offset + record_length
            if record_end > self.free_space_pointer:
                # 记录长度异常，可能数据损坏，终止扫描
                break

            records.append((current_offset, bytes(self.data[current_offset:record_end])))
            current_offset = record_end
        return records

    def get_record(self, offset: int) -> Optional[bytes]:
        """获取指定偏移量的单条记录。"""
        if offset < 0 or offset + ROW_LENGTH_PREFIX_SIZE > len(self.data):
            return None
        record_length = int.from_bytes(self.data[offset:offset + ROW_LENGTH_PREFIX_SIZE], "little")
        if record_length == 0:  # 已删除或无效记录
            return None
        return self.data[offset:offset + record_length]

    def delete_record(self, offset: int) -> bool:
        """逻辑删除一条记录（通过将其长度置0），不回收空间。"""
        if offset < 0 or offset + ROW_LENGTH_PREFIX_SIZE > len(self.data):
            return False
        self.data[offset:offset + ROW_LENGTH_PREFIX_SIZE] = (0).to_bytes(ROW_LENGTH_PREFIX_SIZE, "little")
        return True