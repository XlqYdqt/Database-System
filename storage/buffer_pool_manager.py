from collections import deque
from disk_manager import DiskManager
from lru_replacer import LRUReplacer


# 导入 threading 模块，为并发环境下的锁机制提供支持（当前为注释状态）
# import threading

# --- 页面对象 ---
# Page 对象是 BufferPoolManager 中管理的基本单位。
# 它包含了页的实际数据以及相关的元数据，如 page_id, pin_count 等。
class Page:
    """
    Page is the basic unit of data transfer between memory and disk.
    """

    def __init__(self):
        """初始化一个空的 Page 对象。"""
        self.page_id: int | None = None
        self.data: bytearray = bytearray()
        self.pin_count: int = 0
        self.is_dirty: bool = False

    def reset(self):
        """重置 Page 对象的元数据，以便重用。"""
        self.page_id = None
        self.data.clear()
        self.pin_count = 0
        self.is_dirty = False

    def __repr__(self) -> str:
        """为 Page 对象提供一个便于调试的字符串表示形式。"""
        return f"Page(page_id={self.page_id}, pin_count={self.pin_count}, is_dirty={self.is_dirty})"


# --- 缓冲池管理器 ---
class BufferPoolManager:
    """
    BufferPoolManager is responsible for fetching pages from disk to memory
    and flushing pages from memory to disk.
    """

    def __init__(self, pool_size: int, disk_manager: DiskManager, lru_replacer: LRUReplacer):
        """
        初始化 BufferPoolManager。
        Args:
            pool_size (int): 缓冲池中的帧数。
            disk_manager (DiskManager): DiskManager 实例。
            lru_replacer (LRUReplacer): LRUReplacer 实例。
        """
        self.pool_size = pool_size
        self.disk_manager = disk_manager
        self.lru_replacer = lru_replacer

        self.pages = [Page() for _ in range(pool_size)]
        self.page_table = {}
        self.free_list = deque(range(pool_size))

        # 在并发环境中，需要一个锁（或称为 latch）来保护缓冲池的内部数据结构。
        # self.latch = threading.Lock()

    def _find_free_frame(self) -> int | None:
        """私有辅助方法，用于寻找一个可用的帧。"""
        if self.free_list:
            return self.free_list.popleft()
        return self.lru_replacer.victim()

    def fetch_page(self, page_id: int) -> Page | None:
        """
        获取一个数据页。如果页在缓冲池中，直接返回；否则从磁盘加载。
        """
        # with self.latch: # 在并发环境中，应在此处加锁
        # 1. 首先检查页是否已在缓冲池中（缓存命中）。
        if page_id in self.page_table:
            frame_id = self.page_table[page_id]
            page = self.pages[frame_id]
            page.pin_count += 1
            self.lru_replacer.pin(frame_id)
            return page

        # 2. 如果不在缓冲池中（缓存未命中），寻找一个可用的帧。
        frame_id = self._find_free_frame()
        if frame_id is None:
            return None  # 所有帧都被钉住，无法获取新页。

        # 3. 如果找到的帧之前被占用，处理旧的页。
        old_page = self.pages[frame_id]
        if old_page.page_id is not None:
            if old_page.is_dirty:
                self.disk_manager.write_page(old_page.page_id, old_page.data)
            del self.page_table[old_page.page_id]

        # 4. 从磁盘读取新页的数据，并更新 Page 对象的元数据。
        try:
            page_data = self.disk_manager.read_page(page_id)
        except IndexError:
            # 如果请求的 page_id 在磁盘上不存在，DiskManager 会抛出异常。
            # 在这种情况下，我们无法获取页面，将释放的帧归还并返回 None。
            self.free_list.append(frame_id)
            return None

        page = self.pages[frame_id]
        page.page_id = page_id
        page.data = page_data
        page.pin_count = 1
        page.is_dirty = False

        # 5. 更新页表，将新页钉住，并返回 Page 对象。
        self.page_table[page_id] = frame_id
        self.lru_replacer.pin(frame_id)
        return page

    def unpin_page(self, page_id: int, is_dirty: bool) -> bool:
        """
        当上层模块使用完一个页后，调用此方法来“解钉”。
        """
        # with self.latch:
        if page_id not in self.page_table:
            return False

        frame_id = self.page_table[page_id]
        page = self.pages[frame_id]

        if page.pin_count <= 0:
            return False

        page.pin_count -= 1
        # 只有当调用者明确指出页面已被修改时，才设置 is_dirty 标志。
        if is_dirty:
            page.is_dirty = True

        if page.pin_count == 0:
            self.lru_replacer.unpin(frame_id)

        return True

    def new_page(self) -> Page | None:
        """
        在数据库中创建一个新页，并将其加载到缓冲池中。
        """
        # with self.latch:
        # 1. 寻找一个可用的帧。
        frame_id = self._find_free_frame()
        if frame_id is None:
            return None

        # 2. 如果找到的帧之前被占用，处理旧的页。
        old_page = self.pages[frame_id]
        if old_page.page_id is not None:
            if old_page.is_dirty:
                self.disk_manager.write_page(old_page.page_id, old_page.data)
            del self.page_table[old_page.page_id]

        # 3. 调用 DiskManager 在磁盘上分配一个新页。
        new_page_id = self.disk_manager.allocate_page()

        # 4. 在帧中设置新页的元数据。
        page = self.pages[frame_id]
        page.page_id = new_page_id
        page.data = bytearray(self.disk_manager.page_size)
        page.pin_count = 1
        # 新创建的页应被视为“脏”页，因为它在内存中的状态（空页）
        # 与磁盘上的状态（可能不存在或为垃圾数据）不同，需要最终被写回。
        page.is_dirty = True

        # 5. 更新页表，将新页钉住。
        self.page_table[new_page_id] = frame_id
        self.lru_replacer.pin(frame_id)

        return page

    def delete_page(self, page_id: int) -> bool:
        """
        从缓冲池中删除一个页。
        """
        # with self.latch:
        if page_id not in self.page_table:
            return True

        frame_id = self.page_table[page_id]
        page = self.pages[frame_id]

        if page.pin_count > 0:
            return False

        del self.page_table[page_id]
        self.lru_replacer.pin(frame_id)
        page.reset()
        self.free_list.append(frame_id)

        # 注意：此方法仅从缓冲池中移除页面。
        # 一个完整的实现还需要调用 DiskManager 来回收磁盘空间。

        return True

    def flush_page(self, page_id: int) -> bool:
        """将指定页强制刷回磁盘。"""
        # with self.latch:
        if page_id not in self.page_table:
            return False

        frame_id = self.page_table[page_id]
        page = self.pages[frame_id]

        if page.page_id is None:
            return False

        self.disk_manager.write_page(page.page_id, page.data)
        page.is_dirty = False

        return True

    def flush_all_pages(self) -> bool:
        """将缓冲池中所有脏页刷回磁盘。"""
        # with self.latch:
        for page in self.pages:
            if page.page_id is not None and page.is_dirty:
                self.flush_page(page.page_id)
        return True

