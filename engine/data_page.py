# data_page.py
from typing import List, Tuple, Optional

from engine.constants import PAGE_SIZE, ROW_LENGTH_PREFIX_SIZE


class DataPage:
    """数据页（DataPage），负责存储表的实际行记录。"""

    def __init__(self, page_id: int, data: bytes = b''):
        self.page_id = page_id
        self.data = bytearray(data) if data else bytearray(PAGE_SIZE)
        self.free_space_pointer = self._calculate_free_space_pointer()

    def _calculate_free_space_pointer(self) -> int:
        """
        计算空闲空间指针。
        会扫描整个页面，正确地跳过有效或已删除的记录，找到所有记录中实际的最高偏移量，
        从而确保 free_space_pointer 指向真正的可用空间起始位置。
        """
        offset = 0
        end_of_data = 0
        while offset < PAGE_SIZE:
            if offset + ROW_LENGTH_PREFIX_SIZE > PAGE_SIZE:
                break

            try:
                # 使用 signed=True 来正确读取正负长度
                record_len = int.from_bytes(self.data[offset:offset + ROW_LENGTH_PREFIX_SIZE], "little", signed=True)
            except IndexError:
                break

            # 长度为0表示数据结束
            if record_len == 0:
                break

            abs_len = abs(record_len)
            current_record_end = offset + abs_len
            if current_record_end > end_of_data:
                end_of_data = current_record_end

            offset += abs_len

        return end_of_data

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

        existing_total_length = int.from_bytes(self.data[offset: offset + ROW_LENGTH_PREFIX_SIZE], "little",
                                               signed=True)
        if existing_total_length <= 0:
            raise ValueError("不能更新一个已经被删除的记录。")

        if len(new_record) <= existing_total_length:
            # 原地更新
            self.data[offset:offset + len(new_record)] = new_record
            # 如果新记录比旧的短，用空字节填充剩余部分
            zero_start = offset + len(new_record)
            zero_end = offset + existing_total_length
            if zero_end > zero_start:
                self.data[zero_start:zero_end] = b'\0' * (zero_end - zero_start)
            return offset, False

        # 如果新记录更长，且空间不足
        if self.get_free_space() < len(new_record):
            raise ValueError("页面空间不足，无法更新记录。")

        # 逻辑删除旧记录，并在末尾插入新记录
        self.delete_record(offset)
        new_offset = self.insert_record(new_record)
        return new_offset, True

    def get_data(self) -> bytes:
        """返回页面的字节数据。"""
        return bytes(self.data)

    def get_all_records(self) -> List[Tuple[int, bytes]]:
        """
        [BUG FIX] 重写了记录扫描逻辑，使其更加健壮。
        新逻辑可以安全地处理被删除的记录（负长度），
        通过读取长度的绝对值来正确跳到下一条记录，避免错位。
        """
        records = []
        current_offset = 0
        while current_offset < self.free_space_pointer:
            if current_offset + ROW_LENGTH_PREFIX_SIZE > self.free_space_pointer:
                break

            try:
                # 使用 signed=True 来读取可能为负的长度
                record_length = int.from_bytes(self.data[current_offset: current_offset + ROW_LENGTH_PREFIX_SIZE],
                                               "little", signed=True)
            except IndexError:
                break

            # 如果长度为0，说明可能到了数据的末尾或者是一片未初始化的区域，停止扫描
            if record_length == 0:
                break

            # 获取记录长度的绝对值，用于计算下一条记录的偏移量
            abs_length = abs(record_length)
            record_end = current_offset + abs_length

            # 健全性检查
            if record_end > self.free_space_pointer:
                break

            # 如果长度是正数，说明这是一个有效记录
            if record_length > 0:
                records.append((current_offset, bytes(self.data[current_offset:record_end])))

            # 无论记录是否被删除，都跳过整个记录的长度
            current_offset = record_end

        return records

    def get_record(self, offset: int) -> Optional[bytes]:
        """获取指定偏移量的单条记录。"""
        if offset < 0 or offset + ROW_LENGTH_PREFIX_SIZE > len(self.data):
            return None
        record_length = int.from_bytes(self.data[offset:offset + ROW_LENGTH_PREFIX_SIZE], "little", signed=True)
        # 长度为正才有效
        if record_length <= 0:
            return None
        return self.data[offset:offset + record_length]

    def delete_record(self, offset: int) -> bool:
        """
        逻辑删除一条记录，通过将其长度前缀取反来实现。
        这样既能标记记录为已删除，又能保留其原始长度信息，以便扫描时能正确跳过。
        """
        if offset < 0 or offset + ROW_LENGTH_PREFIX_SIZE > len(self.data):
            return False

        old_record_length = int.from_bytes(self.data[offset:offset + ROW_LENGTH_PREFIX_SIZE], "little", signed=True)

        # 如果记录已经被删除 (长度为负或0)，则无需操作
        if old_record_length <= 0:
            return True

        # 将长度取反并写回
        deleted_length_bytes = (-old_record_length).to_bytes(ROW_LENGTH_PREFIX_SIZE, "little", signed=True)
        self.data[offset:offset + ROW_LENGTH_PREFIX_SIZE] = deleted_length_bytes

        return True

