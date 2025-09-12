# data_page.py

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
        record_size = len(record_data)
        if self.get_free_space() < record_size:
            raise ValueError("Not enough free space on page")

        offset = self.free_space_pointer
        self.data[offset:offset + record_size] = record_data
        self.free_space_pointer += record_size
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