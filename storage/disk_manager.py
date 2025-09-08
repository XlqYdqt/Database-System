import os


# --- Class Docstring ---
# DiskManager 负责所有底层的磁盘I/O操作。
# 它的核心职责是将数据库文件抽象成一个由固定大小的“页”组成的集合。
# 上层模块（如 BufferPoolManager）通过 page_id 与之交互，
# 无需关心文件句柄、字节偏移量等底层细节。
class DiskManager:
    """
    Manages all disk I/O operations for the database file.
    """

    def __init__(self, db_filename: str, page_size: int = 4096):
        """
        初始化 DiskManager。

        Args:
            db_filename (str): 数据库文件的路径。
            page_size (int): 每个数据页的大小（以字节为单位）。
        """
        self.page_size = page_size
        self.db_filename = db_filename

        # --- 文件初始化 ---
        # 这里的逻辑确保我们总是能以 'r+b' (读写二进制) 模式打开文件。
        # 如果文件不存在，它会先被 'w+b' 模式创建一个空文件，然后立即关闭，
        # 之后再由下面的 open() 以 'r+b' 模式成功打开。
        if not os.path.exists(db_filename):
            with open(db_filename, 'w+b') as f:
                pass

        self.db_file = open(db_filename, 'r+b')

        # --- 元数据管理 ---
        # 在初始化时，通过计算文件总大小来预先加载总页数。
        # 这避免了每次分配新页时都需要查询文件系统，提高了效率。
        self.db_file.seek(0, os.SEEK_END)  # 移动文件指针到末尾
        file_size = self.db_file.tell()  # 获取当前位置（即文件大小）
        self.num_pages = file_size // self.page_size

    # --- 1. 资源管理 (Context Manager) ---
    # 实现 __enter__ 和 __exit__ 方法，使得 DiskManager
    # 可以被用在 'with' 语句中。这可以确保文件句柄在使用完毕后
    # 无论是否发生异常，都会被自动关闭，是管理资源的首选方式。
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    # --- 2. 页面读取 ---
    def read_page(self, page_id: int) -> bytearray:
        """
        从磁盘文件中读取一个完整的页。

        Args:
            page_id (int): 要读取的页的ID。

        Returns:
            bytearray: 包含页面数据的字节数组。
        """
        # 边界检查：确保请求的 page_id 是有效的，防止读取文件范围外的数据。
        if page_id >= self.num_pages:
            raise IndexError(f"Page ID {page_id} is out of bounds (total pages: {self.num_pages}).")

        # 计算字节偏移量，这是定位页在文件中位置的关键。
        offset = page_id * self.page_size
        self.db_file.seek(offset)

        # 读取一整个页大小的数据并返回，接口清晰易用。
        page_data = self.db_file.read(self.page_size)
        return bytearray(page_data)

    # --- 3. 页面写入 ---
    def write_page(self, page_id: int, page_data: bytearray):
        """
        将一个页的数据写入到磁盘文件中。

        Args:
            page_id (int): 要写入的页的ID。
            page_data (bytearray): 要写入的页面数据。
        """
        # 边界检查：确保不会写入到不存在的页。
        if page_id >= self.num_pages:
            raise IndexError(f"Page ID {page_id} is out of bounds (total pages: {self.num_pages}).")

        # 数据完整性检查：确保写入的数据大小正好是一个页的大小。
        if len(page_data) != self.page_size:
            raise ValueError(f"Data to write has size {len(page_data)}, but page size is {self.page_size}.")

        # 计算偏移量并移动文件指针。
        offset = page_id * self.page_size
        self.db_file.seek(offset)
        self.db_file.write(page_data)

        # 关键步骤：调用 flush() 将操作系统内存缓冲区的数据强制写入物理磁盘。
        # 这确保了数据的持久性，防止因程序崩溃或断电导致数据丢失。
        self.db_file.flush()

    # --- 4. 页面分配 (已修正) ---
    def allocate_page(self) -> int:
        """
        在数据库文件的末尾分配一个新页，并返回其 page_id。
        这通过向文件追加一个大小为 page_size 的空字节块来实现。

        Returns:
            int: 新分配的页的ID。
        """
        # 新页的ID就是当前的总页数 (因为 page_id 从0开始)。
        # 这是在返回前的值，所以逻辑是正确的。
        new_page_id = self.num_pages

        # 直接操作文件句柄来扩展文件，这是最底层的操作。
        # 我们不能调用 self.write_page()，因为它会进行边界检查并失败。
        self.db_file.seek(0, os.SEEK_END)
        self.db_file.write(bytearray(self.page_size))  # 在末尾追加一个空页
        self.db_file.flush()

        # 更新内部维护的页计数器。
        self.num_pages += 1

        return new_page_id

    def get_num_pages(self) -> int:
        """返回数据库文件中的总页数。"""
        return self.num_pages

    def close(self):
        """关闭文件句柄。"""
        self.db_file.close()
