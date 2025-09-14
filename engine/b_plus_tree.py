import struct
import bisect
import threading
from storage.buffer_pool_manager import BufferPoolManager, Page

# [FIX] 定义一个常量来表示无效的页面ID，而不是使用有特殊含义的 0
INVALID_PAGE_ID = -1


# --- 辅助类：定义页面布局和序列化/反序列化 ---

class BPlusTreePage:
    """
    B+树页面的基类，封装了所有页面的通用头部信息和操作。
    """
    # 页面头部格式：'b' -> is_leaf (1字节), 'H' -> num_keys (2字节)
    HEADER_FORMAT = 'bH'
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

    def __init__(self, page: Page):
        self.page = page
        self.data = page.data
        # 初始化时，进行基本检查，防止操作无效数据
        if not self.data or len(self.data) < self.HEADER_SIZE:
            self.is_leaf = False
            self.num_keys = 0
            return

        # 从页面数据中解包头部信息
        header_data = self.data[:self.HEADER_SIZE]
        is_leaf_byte, self.num_keys = struct.unpack(self.HEADER_FORMAT, header_data)
        self.is_leaf = bool(is_leaf_byte)

    def serialize_header(self):
        """将头部信息（节点类型、键数量）序列化回页面数据中。"""
        header_data = struct.pack(self.HEADER_FORMAT, int(self.is_leaf), self.num_keys)
        self.data[:self.HEADER_SIZE] = header_data

    def get_num_keys(self) -> int:
        """返回当前节点中的键数量。"""
        return self.num_keys

    def get_min_keys(self) -> int:
        """返回节点允许的最小键数（通常是最大值的一半，向上取整）。"""
        return (self.get_max_keys() + 1) // 2

    def get_max_keys(self) -> int:
        """计算并返回此页面能容纳的最大键数。子类必须重写此方法。"""
        raise NotImplementedError("子类必须实现 get_max_keys")


class InternalPage(BPlusTreePage):
    """
    内部节点页面的包装类。
    页面布局: [ 头部 | 指针_0 | 键_1 | 指针_1 | 键_2 | 指针_2 | ... ]
    """
    KEY_FORMAT = '16s'  # 键格式，16字节字符串
    POINTER_FORMAT = 'i'  # 指针格式，4字节整数 (page_id)
    KEY_SIZE = struct.calcsize(KEY_FORMAT)
    POINTER_SIZE = struct.calcsize(POINTER_FORMAT)
    CELL_SIZE = KEY_SIZE + POINTER_SIZE  # 每个（键+指针）单元的大小

    def __init__(self, page: Page):
        super().__init__(page)
        self.is_leaf = False
        self.pointers = []
        self.keys = []
        # 只有在页面数据有效时才进行反序列化
        if self.data and len(self.data) >= self.HEADER_SIZE:
            self._deserialize_body()

    def _deserialize_body(self):
        """从页面字节数据中读取所有键和指针到内存。"""
        offset = self.HEADER_SIZE
        # 增加边界检查，防止数据损坏导致读取越界
        if offset + self.POINTER_SIZE > len(self.data): return

        # 读取第一个指针 (ptr_0)
        ptr_data = self.data[offset: offset + self.POINTER_SIZE]
        self.pointers.append(struct.unpack(self.POINTER_FORMAT, ptr_data)[0])
        offset += self.POINTER_SIZE

        # 依次读取 (key_i, ptr_i) 对
        for _ in range(self.num_keys):
            if offset + self.CELL_SIZE > len(self.data): break  # 安全检查
            key_data = self.data[offset: offset + self.KEY_SIZE]
            self.keys.append(struct.unpack(self.KEY_FORMAT, key_data)[0])
            offset += self.KEY_SIZE

            ptr_data = self.data[offset: offset + self.POINTER_SIZE]
            self.pointers.append(struct.unpack(self.POINTER_FORMAT, ptr_data)[0])
            offset += self.POINTER_SIZE

    def serialize(self):
        """将内存中的键和指针列表序列化回页面的字节数据中。"""
        self.num_keys = len(self.keys)
        self.serialize_header()
        offset = self.HEADER_SIZE

        # 写入第一个指针
        struct.pack_into(self.POINTER_FORMAT, self.data, offset, self.pointers[0])
        offset += self.POINTER_SIZE

        # 依次写入后续的 (键, 指针) 对
        for i in range(self.num_keys):
            struct.pack_into(self.KEY_FORMAT, self.data, offset, self.keys[i])
            offset += self.KEY_SIZE
            struct.pack_into(self.POINTER_FORMAT, self.data, offset, self.pointers[i + 1])
            offset += self.POINTER_SIZE

    def lookup(self, key) -> int:
        """根据给定的键，查找应该访问的下一个子节点的 page_id。"""
        # bisect_right 在有序列表 keys 中进行二分查找，返回 key 的插入点。
        # 这个索引值恰好对应于 pointers 列表中下一个要访问的子节点的索引。
        idx = bisect.bisect_right(self.keys, key)
        return self.pointers[idx]

    def is_full(self) -> bool:
        """检查页面是否已满。"""
        current_size = self.HEADER_SIZE + self.POINTER_SIZE + (self.num_keys * self.CELL_SIZE)
        return current_size + self.CELL_SIZE > len(self.data)

    def insert(self, key, pointer: int):
        """在内部节点中插入一个新的 (键, 指针) 对，并保持键的有序性。"""
        insert_idx = bisect.bisect_left(self.keys, key)
        self.keys.insert(insert_idx, key)
        self.pointers.insert(insert_idx + 1, pointer)
        self.num_keys = len(self.keys)

    def get_max_keys(self) -> int:
        """计算内部节点能容纳的最大键数。"""
        return (len(self.data) - self.HEADER_SIZE - self.POINTER_SIZE) // self.CELL_SIZE

    def remove(self, key):
        """根据键移除一个键和它右边的指针。"""
        try:
            key_index = self.keys.index(key)
            self.keys.pop(key_index)
            # 移除键右侧的指针
            self.pointers.pop(key_index + 1)
            self.num_keys -= 1
        except ValueError:
            # 如果键不存在，则不执行任何操作
            pass


class LeafPage(BPlusTreePage):
    """
    叶子节点页面的包装类。
    页面布局: [ 头部 | 前驱页面ID | 后继页面ID | 键_1 | RID_1 | 键_2 | RID_2 | ... ]
    """
    KEY_FORMAT = '16s'
    KEY_SIZE = struct.calcsize(KEY_FORMAT)
    RID_FORMAT = 'ii'  # RID (Record ID) 由 (page_id, offset) 组成
    RID_SIZE = struct.calcsize(RID_FORMAT)
    CELL_SIZE = KEY_SIZE + RID_SIZE
    SIBLING_POINTER_FORMAT = 'i'
    SIBLING_POINTER_SIZE = struct.calcsize(SIBLING_POINTER_FORMAT)
    LEAF_HEADER_SIZE = BPlusTreePage.HEADER_SIZE + 2 * SIBLING_POINTER_SIZE

    def __init__(self, page: Page):
        super().__init__(page)
        self.is_leaf = True
        self.key_rid_pairs = []
        self.prev_page_id = 0
        self.next_page_id = 0
        if self.data and len(self.data) >= self.HEADER_SIZE:
            self._deserialize_body()

    def _deserialize_body(self):
        """从页面字节数据中读取兄弟指针和所有 (键, RID) 对。"""
        offset = self.HEADER_SIZE
        # 读取前驱和后继兄弟节点的 page_id
        if len(self.data) >= self.LEAF_HEADER_SIZE:
            self.prev_page_id, self.next_page_id = struct.unpack_from(
                f'2{self.SIBLING_POINTER_FORMAT}', self.data, offset
            )
            offset += 2 * self.SIBLING_POINTER_SIZE

        # 读取 (键, RID) 对
        for _ in range(self.num_keys):
            if offset + self.CELL_SIZE > len(self.data): break
            key = struct.unpack_from(self.KEY_FORMAT, self.data, offset)[0]
            offset += self.KEY_SIZE
            rid = struct.unpack_from(self.RID_FORMAT, self.data, offset)
            offset += self.RID_SIZE
            self.key_rid_pairs.append((key, rid))

    def serialize(self):
        """将内存中的数据结构序列化回页面的字节数据中。"""
        self.num_keys = len(self.key_rid_pairs)
        self.serialize_header()
        offset = self.HEADER_SIZE

        # 写入兄弟指针
        struct.pack_into(f'2{self.SIBLING_POINTER_FORMAT}', self.data, offset, self.prev_page_id, self.next_page_id)
        offset += 2 * self.SIBLING_POINTER_SIZE

        # 写入 (键, RID) 对
        for key, rid in self.key_rid_pairs:
            struct.pack_into(self.KEY_FORMAT, self.data, offset, key)
            offset += self.KEY_SIZE
            struct.pack_into(self.RID_FORMAT, self.data, offset, *rid)
            offset += self.RID_SIZE

    def lookup(self, key) -> tuple | None:
        """在叶子节点中查找键，如果找到则返回对应的 RID。"""
        keys = [pair[0] for pair in self.key_rid_pairs]
        # 二分查找定位
        idx = bisect.bisect_left(keys, key)
        # 确认是否精确匹配
        if idx < len(keys) and keys[idx] == key:
            return self.key_rid_pairs[idx][1]
        return None

    def is_full(self) -> bool:
        """检查页面是否已满。"""
        current_size = self.LEAF_HEADER_SIZE + (self.num_keys * self.CELL_SIZE)
        return current_size + self.CELL_SIZE > len(self.data)

    def insert(self, key, rid: tuple):
        """在叶子节点中插入一个新的 (键, RID) 对，并保持有序。"""
        new_pair = (key, rid)
        keys = [pair[0] for pair in self.key_rid_pairs]
        insert_idx = bisect.bisect_left(keys, key)
        self.key_rid_pairs.insert(insert_idx, new_pair)
        self.num_keys = len(self.key_rid_pairs)

    def get_max_keys(self) -> int:
        """计算叶子节点能容纳的最大键数。"""
        return (len(self.data) - self.LEAF_HEADER_SIZE) // self.CELL_SIZE

    def remove(self, key) -> bool:
        """根据键移除一个 (键, RID) 对。"""
        keys = [pair[0] for pair in self.key_rid_pairs]
        idx = bisect.bisect_left(keys, key)
        if idx < len(keys) and keys[idx] == key:
            self.key_rid_pairs.pop(idx)
            self.num_keys -= 1
            return True
        return False


# --- 并发控制辅助类 ---

class TransactionContext:
    """
    事务上下文，用于在单次B+树操作中跟踪和管理所有锁定的页面（Latches）。
    这确保了操作的原子性：即使中途出错，所有资源也能被正确释放和回滚。
    """

    def __init__(self, tree):
        self.tree = tree
        # 存储已加锁的页面包装器对象
        self.latched_pages_wrappers = []
        # 记录本次操作中新创建的页面ID，用于错误回滚
        self.newly_created_page_ids = set()
        # 记录本次操作中要删除的页面ID
        self.deleted_page_ids = set()

    def add_latched_page(self, page_wrapper):
        """将一个已加锁的页面加入上下文管理。"""
        self.latched_pages_wrappers.append(page_wrapper)

    def release_all_latches(self, is_dirty_list=None, is_error: bool = False):
        """
        释放所有持有的页面锁（latch）并解钉（unpin）页面。
        """
        # 如果是因错误而释放，需要删除所有本次新创建的页面
        if is_error:
            for page_id in self.newly_created_page_ids:
                self.tree.bpm.delete_page(page_id)

        if is_dirty_list is None: is_dirty_list = [False] * len(self.latched_pages_wrappers)

        # 以相反的顺序释放锁和解钉，符合加锁顺序
        for i, wrapper in reversed(list(enumerate(self.latched_pages_wrappers))):
            page_id = wrapper.page.page_id
            is_dirty = is_dirty_list[i] if i < len(is_dirty_list) else False
            self.tree.bpm.unpin_page(page_id, is_dirty)
            self.tree._release_latch(page_id)

        # 释放为新页面和已删除页面持有的锁
        for page_id in self.newly_created_page_ids: self.tree._release_latch(page_id)
        for page_id in self.deleted_page_ids:
            self.tree._release_latch(page_id)
            if not is_error: self.tree.bpm.delete_page(page_id)

        # 清理上下文状态
        self.latched_pages_wrappers.clear()
        self.newly_created_page_ids.clear()
        self.deleted_page_ids.clear()

    def release_ancestors_latches(self):
        """
        实现锁耦合（Latch Crabbing）的关键。
        当发现当前遍历到的节点是“安全”的（即后续操作不会影响到祖先节点），
        就释放所有祖先节点的锁，只保留当前节点的锁。
        """
        last_page = self.latched_pages_wrappers.pop()
        self.release_all_latches(is_dirty_list=[True] * len(self.latched_pages_wrappers))
        self.latched_pages_wrappers.append(last_page)

    def release_ancestors_latches_for_delete(self):
        """删除操作的锁优化，逻辑与插入类似。"""
        self.release_ancestors_latches()


# --- B+树主类 ---
class BPlusTree:
    """
    B+树索引的主类，负责管理整个树的结构和操作（插入、删除、查找）。
    """

    def __init__(self, buffer_pool_manager: BufferPoolManager, root_page_id: int):
        self.bpm = buffer_pool_manager
        self.root_page_id = root_page_id
        # 用于保护 _latch_manager 的内部锁
        self._manager_lock = threading.Lock()
        # 存储每个 page_id 对应的锁 (latch)
        self._latch_manager = {}

    def _get_latch(self, page_id: int) -> threading.Lock:
        """获取或创建一个与 page_id 关联的锁。"""
        with self._manager_lock:
            if page_id not in self._latch_manager:
                self._latch_manager[page_id] = threading.Lock()
            return self._latch_manager[page_id]

    def _acquire_latch(self, page_id: int):
        """获取页面锁。"""
        self._get_latch(page_id).acquire()

    def _release_latch(self, page_id: int):
        """释放页面锁。"""
        self._get_latch(page_id).release()

    def search(self, key) -> tuple | None:
        """从B+树中查找一个键，返回其对应的RID (线程安全)。"""
        # [FIX] 使用 INVALID_PAGE_ID 进行判断
        if self.root_page_id is None or self.root_page_id == INVALID_PAGE_ID:
            return None
        current_page_id = self.root_page_id
        try:
            self._acquire_latch(current_page_id)
            page_obj = self.bpm.fetch_page(current_page_id)
            # 如果页面获取失败或为空，则无法继续
            if not page_obj or not page_obj.data:
                self.bpm.unpin_page(current_page_id, is_dirty=False)
                self._release_latch(current_page_id)
                return None

            # 从根节点开始向下遍历
            while True:
                page_wrapper = BPlusTreePage(page_obj)
                if page_wrapper.is_leaf:
                    # 到达叶子节点，进行查找
                    leaf_wrapper = LeafPage(page_obj)
                    rid = leaf_wrapper.lookup(key)
                    self.bpm.unpin_page(current_page_id, is_dirty=False)
                    self._release_latch(current_page_id)
                    return rid
                else:
                    # 在内部节点，找到下一个要访问的子节点
                    internal_wrapper = InternalPage(page_obj)
                    next_page_id = internal_wrapper.lookup(key)
                    # 释放当前节点的锁和 pin
                    self.bpm.unpin_page(current_page_id, is_dirty=False)
                    self._release_latch(current_page_id)

                    # 准备访问下一个节点
                    current_page_id = next_page_id
                    self._acquire_latch(current_page_id)
                    page_obj = self.bpm.fetch_page(current_page_id)
                    if not page_obj or not page_obj.data:
                        self._release_latch(current_page_id)
                        return None
        except Exception as e:
            print(f"Error during search: {e}")
            # 发生异常时，尝试释放可能持有的锁
            try:
                self._release_latch(current_page_id)
            except (threading.ThreadError, RuntimeError):
                pass
            return None

    def insert(self, key, rid: tuple) -> bool | None:
        """
        [DEADLOCK FIX & PK FIX] 修复了死锁和主键唯一性检查问题。
        现在此方法返回 bool | None:
        - None: 插入失败 (主键重复)
        - True / False: 插入成功 (bool 值表示根节点是否改变)
        """
        context = TransactionContext(self)
        try:
            # Case 1: 树为空，创建第一个节点（根节点）
            if self.root_page_id is None or self.root_page_id == INVALID_PAGE_ID:
                root_changed = self._start_new_tree(key, rid, context)
                # [FIX] 关键修复：在成功创建新树后，必须调用 release_all_latches
                # 来释放为新根节点获取的锁。
                context.release_all_latches()
                return root_changed

            # Case 2: 树已存在，查找目标叶子节点
            leaf_page_wrapper = self._find_leaf_page_with_latching(key, context)
            if leaf_page_wrapper is None:
                raise RuntimeError("无法找到用于插入的叶子节点，缓冲池可能已满。")

            # 检查键是否已存在
            if leaf_page_wrapper.lookup(key) is not None:
                context.release_all_latches()
                # [FIX] 返回 None 表示主键冲突
                return None

            # 在叶子节点中插入
            leaf_page_wrapper.insert(key, rid)

            # 如果叶子节点未满，操作完成
            if not leaf_page_wrapper.is_full():
                context.release_all_latches(is_dirty_list=[True] * len(context.latched_pages_wrappers))
                return False  # 根节点未改变

            # --- 节点分裂逻辑 ---
            # 如果叶子节点已满，则进行分裂，并可能级联影响父节点
            root_changed = self._split_leaf_and_insert_parent(leaf_page_wrapper, context)

            # 标记所有涉及的页面为脏页并释放资源
            dirty_flags = [True] * len(context.latched_pages_wrappers)
            context.release_all_latches(is_dirty_list=dirty_flags)
            return root_changed

        except Exception as e:
            print(f"Error during insert: {e}")
            context.release_all_latches(is_error=True)  # 保证异常时回滚并释放所有资源
            return False

    def delete(self, key) -> bool:
        """从B+树中删除一个键及其关联的值。"""
        context = TransactionContext(self)
        try:
            # [FIX] 使用 INVALID_PAGE_ID 进行判断
            if self.root_page_id is None or self.root_page_id == INVALID_PAGE_ID: return False
            old_root_id = self.root_page_id

            # 查找包含该键的叶子节点
            leaf_page_wrapper = self._find_leaf_for_delete_with_latching(key, context)
            if leaf_page_wrapper is None:
                raise RuntimeError("无法找到用于删除的叶子节点。")

            # 从叶子节点中删除键
            if not leaf_page_wrapper.remove(key):
                context.release_all_latches()
                return False  # 键不存在

            # 检查删除后是否发生下溢 (underflow)
            if leaf_page_wrapper.get_num_keys() < leaf_page_wrapper.get_min_keys():
                self._handle_underflow(leaf_page_wrapper, context)

            # 标记所有修改过的页面为脏页并释放资源
            dirty_flags = [True] * len(context.latched_pages_wrappers)
            context.release_all_latches(dirty_flags)
            # 返回根节点是否发生了变化
            return self.root_page_id != old_root_id

        except Exception as e:
            print(f"Error during delete: {e}")
            context.release_all_latches(is_error=True)
            return False

    def _find_leaf_page_with_latching(self, key, context: TransactionContext) -> LeafPage | None:
        """
        辅助方法：使用锁耦合（latch crabbing）协议从根安全地遍历到目标叶子节点（用于插入）。
        """
        current_page_id = self.root_page_id
        self._acquire_latch(current_page_id)
        page_obj = self.bpm.fetch_page(current_page_id)
        if not page_obj:
            self._release_latch(current_page_id)
            return None

        page_wrapper = BPlusTreePage(page_obj)
        context.add_latched_page(page_wrapper)

        while not page_wrapper.is_leaf:
            internal_wrapper = InternalPage(page_obj)
            next_page_id = internal_wrapper.lookup(key)
            self._acquire_latch(next_page_id)
            next_page_obj = self.bpm.fetch_page(next_page_id)
            if not next_page_obj:
                self._release_latch(next_page_id)
                raise RuntimeError(f"在遍历过程中无法获取页面 {next_page_id}。")

            # 锁耦合的核心：检查子节点是否“安全”。
            # 对插入操作而言，"安全"意味着子节点未满。
            next_page_wrapper = BPlusTreePage(next_page_obj)
            is_child_safe = not (not next_page_wrapper.is_leaf and InternalPage(next_page_obj).is_full())

            if is_child_safe:
                # 如果子节点是安全的，则释放所有祖先节点的锁。
                context.release_ancestors_latches()

            context.add_latched_page(BPlusTreePage(next_page_obj))
            page_obj = next_page_obj
            page_wrapper = BPlusTreePage(page_obj)

        return LeafPage(page_obj)

    def _find_leaf_for_delete_with_latching(self, key, context: TransactionContext) -> LeafPage | None:
        """
        辅助方法：使用锁耦合协议安全地遍历到目标叶子节点（用于删除）。
        """
        current_page_id = self.root_page_id
        self._acquire_latch(current_page_id)
        page_obj = self.bpm.fetch_page(current_page_id)
        if not page_obj:
            self._release_latch(current_page_id)
            return None

        page_wrapper = BPlusTreePage(page_obj)
        context.add_latched_page(page_wrapper)

        while not page_wrapper.is_leaf:
            internal_wrapper = InternalPage(page_obj)
            # 对删除操作而言，"安全"意味着节点键数大于最小限制。
            if internal_wrapper.get_num_keys() > internal_wrapper.get_min_keys():
                context.release_ancestors_latches_for_delete()

            next_page_id = internal_wrapper.lookup(key)
            self._acquire_latch(next_page_id)
            next_page_obj = self.bpm.fetch_page(next_page_id)
            if not next_page_obj:
                self._release_latch(next_page_id)
                raise RuntimeError(f"在遍历过程中无法获取页面 {next_page_id}。")

            context.add_latched_page(BPlusTreePage(next_page_obj))
            page_obj = next_page_obj
            page_wrapper = BPlusTreePage(page_obj)

        leaf_wrapper = LeafPage(page_obj)
        if leaf_wrapper.get_num_keys() > leaf_wrapper.get_min_keys():
            context.release_ancestors_latches_for_delete()

        return leaf_wrapper

    def _start_new_tree(self, key, rid, context: TransactionContext) -> bool:
        """当树为空时，创建一个新的根节点（叶子节点）。"""
        page_obj = self.bpm.new_page()
        if not page_obj:
            raise MemoryError("缓冲池已满，无法为根节点创建新页面。")

        page_id = page_obj.page_id
        self._acquire_latch(page_id)
        context.newly_created_page_ids.add(page_id)

        self.root_page_id = page_id
        leaf_node = LeafPage(page_obj)
        leaf_node.insert(key, rid)
        leaf_node.serialize()
        # 新页面是脏页，需要 unpin
        self.bpm.unpin_page(page_id, is_dirty=True)
        return True  # 根节点已改变

    def _split_leaf_and_insert_parent(self, leaf_page_wrapper: LeafPage, context: TransactionContext) -> bool:
        """辅助函数：分裂一个叶子节点，并将中间键推送到父节点。"""
        # 1. 创建一个新的兄弟叶子节点
        new_page_obj = self.bpm.new_page()
        if not new_page_obj:
            raise MemoryError("缓冲池已满，无法为分裂创建新页面。")

        new_page_id = new_page_obj.page_id
        self._acquire_latch(new_page_id)
        context.newly_created_page_ids.add(new_page_id)
        new_leaf_wrapper = LeafPage(new_page_obj)

        # 2. 将原节点的一半数据移动到新节点
        mid_idx = leaf_page_wrapper.get_num_keys() // 2
        new_leaf_wrapper.key_rid_pairs = leaf_page_wrapper.key_rid_pairs[mid_idx:]
        leaf_page_wrapper.key_rid_pairs = leaf_page_wrapper.key_rid_pairs[:mid_idx]

        leaf_page_wrapper.num_keys = len(leaf_page_wrapper.key_rid_pairs)
        new_leaf_wrapper.num_keys = len(new_leaf_wrapper.key_rid_pairs)

        # 3. 更新兄弟指针，形成双向链表
        new_leaf_wrapper.next_page_id = leaf_page_wrapper.next_page_id
        new_leaf_wrapper.prev_page_id = leaf_page_wrapper.page.page_id
        leaf_page_wrapper.next_page_id = new_page_id

        # 4. 更新原节点的右兄弟的前驱指针（如果存在）
        if new_leaf_wrapper.next_page_id != 0:
            self._update_sibling_pointer(new_leaf_wrapper.next_page_id, new_page_id)

        # 5. 将修改序列化回页面
        leaf_page_wrapper.serialize()
        new_leaf_wrapper.serialize()
        self.bpm.unpin_page(new_page_id, is_dirty=True)

        # 6. 将新节点的第一个键作为分隔键，插入到父节点中
        middle_key = new_leaf_wrapper.key_rid_pairs[0][0]
        return self._insert_into_parent(middle_key, new_page_id, context)

    def _update_sibling_pointer(self, sibling_page_id: int, new_prev_id: int):
        """安全地获取一个兄弟节点并更新其前驱指针。"""
        self._acquire_latch(sibling_page_id)
        try:
            sibling_page_obj = self.bpm.fetch_page(sibling_page_id)
            if sibling_page_obj:
                sibling_wrapper = LeafPage(sibling_page_obj)
                sibling_wrapper.prev_page_id = new_prev_id
                sibling_wrapper.serialize()
                self.bpm.unpin_page(sibling_page_id, is_dirty=True)
        finally:
            self._release_latch(sibling_page_id)

    def _insert_into_parent(self, key, right_child_pid: int, context: TransactionContext) -> bool:
        """递归地将分裂产生的键和指针插入到父节点中。"""
        # 从上下文中弹出子节点，栈顶即为父节点
        popped_child_wrapper = context.latched_pages_wrappers.pop()
        left_child_pid = popped_child_wrapper.page.page_id

        # Case 1: 如果栈为空，说明原节点是根节点，需要创建一个新的根
        if not context.latched_pages_wrappers:
            new_root_page_obj = self.bpm.new_page()
            if not new_root_page_obj:
                raise MemoryError("缓冲池已满，无法创建新的根页面。")

            new_root_id = new_root_page_obj.page_id
            self._acquire_latch(new_root_id)
            context.newly_created_page_ids.add(new_root_id)

            new_root = InternalPage(new_root_page_obj)
            self.root_page_id = new_root_id
            new_root.keys = [key]
            new_root.pointers = [left_child_pid, right_child_pid]
            new_root.serialize()
            self.bpm.unpin_page(new_root_id, is_dirty=True)
            return True  # 根节点已改变

        # Case 2: 父节点存在，直接插入
        parent_node = InternalPage(context.latched_pages_wrappers[-1].page)
        parent_node.insert(key, right_child_pid)

        # 如果父节点未满，则操作完成
        if not parent_node.is_full():
            parent_node.serialize()
            return False  # 根节点未改变

        # Case 3: 父节点也满了，需要递归分裂父节点
        new_internal_page_obj = self.bpm.new_page()
        if not new_internal_page_obj:
            raise MemoryError("缓冲池已满，无法为内部分裂创建新页面。")

        new_internal_id = new_internal_page_obj.page_id
        self._acquire_latch(new_internal_id)
        context.newly_created_page_ids.add(new_internal_id)
        new_internal_node = InternalPage(new_internal_page_obj)

        # 分裂内部节点，并将中间键向上推
        mid_idx = parent_node.get_num_keys() // 2
        key_to_push_up = parent_node.keys[mid_idx]
        new_internal_node.keys = parent_node.keys[mid_idx + 1:]
        new_internal_node.pointers = parent_node.pointers[mid_idx + 1:]
        parent_node.keys = parent_node.keys[:mid_idx]
        parent_node.pointers = parent_node.pointers[:mid_idx + 1]

        parent_node.num_keys = len(parent_node.keys)
        new_internal_node.num_keys = len(new_internal_node.keys)

        parent_node.serialize()
        new_internal_node.serialize()
        self.bpm.unpin_page(new_internal_id, is_dirty=True)

        return self._insert_into_parent(key_to_push_up, new_internal_id, context)

    def _handle_underflow(self, node: BPlusTreePage, context: TransactionContext):
        """处理节点下溢（键数少于最小限制）。"""
        # Case 1: 根节点下溢，特殊处理
        if node.page.page_id == self.root_page_id:
            self._adjust_root(node)
            return

        # 获取父节点和当前节点在父节点中的位置
        context.latched_pages_wrappers.pop()
        parent_node = InternalPage(context.latched_pages_wrappers[-1].page)
        child_index = parent_node.pointers.index(node.page.page_id)

        # 优先尝试与左兄弟进行“借用”或“合并”
        if child_index > 0:
            left_sibling_page_id = parent_node.pointers[child_index - 1]
            if self._try_borrow_or_merge_with_sibling(node, left_sibling_page_id, parent_node, child_index - 1, context,
                                                      is_left=True):
                return

        # 如果左兄弟不行，再尝试右兄弟
        if child_index < parent_node.get_num_keys():
            right_sibling_page_id = parent_node.pointers[child_index + 1]
            if self._try_borrow_or_merge_with_sibling(node, right_sibling_page_id, parent_node, child_index, context,
                                                      is_left=False):
                return

    def _try_borrow_or_merge_with_sibling(self, node, sibling_pid, parent, key_idx, context, is_left):
        """尝试从一个兄弟节点借用一个键，如果不行则与它合并。"""
        self._acquire_latch(sibling_pid)
        try:
            sibling_page = self.bpm.fetch_page(sibling_pid)
            if not sibling_page: return False

            sibling_wrapper = LeafPage(sibling_page) if node.is_leaf else InternalPage(sibling_page)
            # 如果兄弟节点足够“富裕”，则向它借一个键
            if sibling_wrapper.get_num_keys() > sibling_wrapper.get_min_keys():
                if is_left:
                    self._redistribute(sibling_wrapper, node, parent, key_idx)
                else:
                    self._redistribute(node, sibling_wrapper, parent, key_idx)
            # 否则，与兄弟节点合并
            else:
                if is_left:
                    self._merge(sibling_wrapper, node, parent, key_idx, context)
                else:
                    self._merge(node, sibling_wrapper, parent, key_idx, context)

            self.bpm.unpin_page(sibling_pid, is_dirty=True)
            return True
        finally:
            self._release_latch(sibling_pid)

    def _redistribute(self, left_node, right_node, parent_node, key_index):
        """重新分配：从一个兄弟节点移动一个元素到另一个节点。"""
        if left_node.is_leaf:  # 叶子节点重新分配
            moving_pair = left_node.key_rid_pairs.pop()
            right_node.key_rid_pairs.insert(0, moving_pair)
            parent_node.keys[key_index] = right_node.key_rid_pairs[0][0]
        else:  # 内部节点重新分配
            # 涉及到父节点、左节点、右节点的三方数据移动
            moving_key = parent_node.keys[key_index]
            right_node.keys.insert(0, moving_key)
            moving_pointer = left_node.pointers.pop()
            right_node.pointers.insert(0, moving_pointer)
            parent_node.keys[key_index] = left_node.keys.pop()

        left_node.num_keys -= 1
        right_node.num_keys += 1
        left_node.serialize()
        right_node.serialize()
        parent_node.serialize()

    def _merge(self, left_node, right_node, parent_node, key_index, context):
        """将右节点的所有内容合并到左节点。"""
        # 从父节点拉下分隔键
        if not left_node.is_leaf:
            separator_key = parent_node.keys[key_index]
            left_node.keys.append(separator_key)
            left_node.keys.extend(right_node.keys)
            left_node.pointers.extend(right_node.pointers)
        else:  # 叶子节点合并
            left_node.key_rid_pairs.extend(right_node.key_rid_pairs)
            left_node.next_page_id = right_node.next_page_id
            if right_node.next_page_id != 0:
                self._update_sibling_pointer(right_node.next_page_id, left_node.page.page_id)

        # 更新元数据和序列化
        left_node.num_keys = len(left_node.keys) if not left_node.is_leaf else len(left_node.key_rid_pairs)
        left_node.serialize()

        # 从父节点中移除相关的键和指针
        parent_node.keys.pop(key_index)
        parent_node.pointers.pop(key_index + 1)
        parent_node.num_keys -= 1
        parent_node.serialize()

        # 将被合并的右节点标记为待删除
        context.deleted_page_ids.add(right_node.page.page_id)

        # 递归检查父节点是否也发生下溢
        if parent_node.page.page_id != self.root_page_id and parent_node.get_num_keys() < parent_node.get_min_keys():
            self._handle_underflow(parent_node, context)
        elif parent_node.page.page_id == self.root_page_id:
            self._adjust_root(parent_node)

    def _adjust_root(self, root_node: BPlusTreePage):
        """
        调整根节点。如果根在删除后变为空，树的高度可能会降低。
        """
        if not root_node.is_leaf and root_node.get_num_keys() == 0:
            new_root_id = root_node.pointers[0]
            self.root_page_id = new_root_id
        elif root_node.is_leaf and root_node.get_num_keys() == 0:
            # [FIX] 使用 INVALID_PAGE_ID 标记空树
            self.root_page_id = INVALID_PAGE_ID


