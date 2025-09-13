# data_page.py
from typing import List, Tuple

from engine.constants import PAGE_SIZE

class DataPage:
    def __init__(self, page_id: int, data: bytes = b''):
        self.page_id = page_id
        # If data is provided and consists only of null bytes, treat it as an empty page.
        # This is crucial for correctly initializing pages read from disk that might be all zeros.
        if data and data == b'\x00' * len(data):
            self.data = bytearray(PAGE_SIZE)
            self.free_space_pointer = 0
        else:
            self.data = bytearray(data) if data else bytearray(PAGE_SIZE)
            self.free_space_pointer = len(data) if data else 0

    def get_free_space(self) -> int:
        return PAGE_SIZE - self.free_space_pointer

    def is_full(self) -> bool:
        return self.get_free_space() == 0

    def insert_record(self, record_data: bytes) -> int:
        from engine.constants import ROW_LENGTH_PREFIX_SIZE

        row_length = len(record_data)
        total_size = ROW_LENGTH_PREFIX_SIZE + row_length

        if self.get_free_space() < total_size:
            raise ValueError("Not enough free space on page")

        offset = self.free_space_pointer
        # 写入记录长度前缀（行级）
        self.data[offset:offset + ROW_LENGTH_PREFIX_SIZE] = row_length.to_bytes(
            ROW_LENGTH_PREFIX_SIZE, "little"
        )
        # 写入记录本体
        self.data[offset + ROW_LENGTH_PREFIX_SIZE: offset + total_size] = record_data

        self.free_space_pointer += total_size
        return offset

    def update_record(self, offset: int, record_data: bytes):
        record_size = len(record_data)
        # For simplicity, assuming update does not change record size significantly
        # In a real system, this would involve more complex free space management
        if offset + record_size > self.free_space_pointer:
            raise IndexError("Record update out of bounds")
        self.data[offset:offset + record_size] = record_data

    def get_data(self) -> bytes:
        return bytes(self.data)

    def get_page_id(self) -> int:
        return self.page_id

    def get_all_records(self) -> List[Tuple[int, bytes]]:
        """迭代页面中的所有记录，返回 (offset, record_data)，自动跳过填充的0"""
        from engine.constants import ROW_LENGTH_PREFIX_SIZE

        records = []
        current_offset = 0

        while current_offset + ROW_LENGTH_PREFIX_SIZE <= self.free_space_pointer:
            # 读取记录长度前缀
            row_length = int.from_bytes(
                self.data[current_offset: current_offset + ROW_LENGTH_PREFIX_SIZE],
                "little"
            )

            if row_length == 0:
                # 检查后面是否全是填充的 0
                if all(b == 0 for b in self.data[current_offset: self.free_space_pointer]):
                    break  # 已经进入填充区，直接退出
                else:
                    # 真的是逻辑删除的行，只跳过前缀
                    current_offset += ROW_LENGTH_PREFIX_SIZE
                    continue

            data_start_offset = current_offset + ROW_LENGTH_PREFIX_SIZE
            data_end_offset = data_start_offset + row_length

            # 如果越界，说明后面都是填充的0，直接退出
            if data_end_offset > len(self.data):
                break

            record_data = self.data[data_start_offset: data_end_offset]

            # 如果记录全是 0，说明是填充，直接退出循环
            if all(b == 0 for b in record_data):
                break

            records.append((current_offset, bytes(record_data)))
            current_offset = data_end_offset

        return records
