# data_page.py
from typing import List, Tuple

from engine.constants import PAGE_SIZE, ROW_LENGTH_PREFIX_SIZE


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

    def update_record(self, offset: int, new_row_bytes: bytes) -> Tuple[int, bool]:
        """
        更新一条记录。

        :param offset: 记录起始偏移（指向长度前缀的起始位置）
        :param new_row_bytes: 不含长度前缀的记录内容 bytes
        :return: (new_offset, moved)
                 new_offset: 记录最终所在位置（如果未移动则等于 offset）
                 moved: 如果为 True 表示记录被移到了页尾（offset 无效，需要上层更新 RID/index）
        可能抛出 IndexError/ValueError（越界或空间不足）
        """
        # 边界检查：必须能读取旧的长度前缀
        if offset < 0 or offset + ROW_LENGTH_PREFIX_SIZE > len(self.data):
            raise IndexError("Invalid record offset")

        # 读取旧记录长度（行级，不含前缀）
        existing_length = int.from_bytes(
            self.data[offset: offset + ROW_LENGTH_PREFIX_SIZE], "little"
        )
        existing_total = ROW_LENGTH_PREFIX_SIZE + existing_length

        new_length = len(new_row_bytes)
        new_total = ROW_LENGTH_PREFIX_SIZE + new_length

        # 情形 A：新长度小于等于旧长度 -> 原地写入（覆盖），并将多余字节清0
        if new_length <= existing_length:
            # 写长度前缀
            self.data[offset: offset + ROW_LENGTH_PREFIX_SIZE] = new_length.to_bytes(
                ROW_LENGTH_PREFIX_SIZE, "little"
            )
            # 写数据
            start = offset + ROW_LENGTH_PREFIX_SIZE
            self.data[start: start + new_length] = new_row_bytes
            # 将旧记录剩余的字节置0（避免残留内容被误读）
            for i in range(start + new_length, offset + existing_total):
                self.data[i] = 0
            # free_space_pointer 不变
            return offset, False

        # 情形 B：新长度大于旧长度 -> 尝试在页尾追加（并将原记录逻辑删除）
        extra_needed = new_total - existing_total
        if self.get_free_space() < new_total:
            # 尝试判断：如果释放掉旧记录空间（将其标记为 deleted）是否足够 —— 这里我们选择不回收旧空间立即复用，
            # 因为回收需要移动后续记录或维护空闲链表，比较复杂。直接失败或让上层决定页分裂。
            raise ValueError("Not enough free space on page for in-place expansion; need page split or compaction")

        # 标记原记录为删除（长度置 0）
        self.data[offset: offset + ROW_LENGTH_PREFIX_SIZE] = (0).to_bytes(ROW_LENGTH_PREFIX_SIZE, "little")

        # 在页尾写入新的记录（带长度前缀）
        new_offset = self.free_space_pointer
        self.data[new_offset: new_offset + ROW_LENGTH_PREFIX_SIZE] = new_length.to_bytes(
            ROW_LENGTH_PREFIX_SIZE, "little"
        )
        start = new_offset + ROW_LENGTH_PREFIX_SIZE
        self.data[start: start + new_length] = new_row_bytes

        # 更新 free_space_pointer
        self.free_space_pointer += new_total

        return new_offset, True

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
