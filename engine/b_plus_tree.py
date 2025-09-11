import struct
import bisect
import threading
from storage.buffer_pool_manager import BufferPoolManager, Page


# --- 辅助类：定义页面布局和序列化/反序列化 ---

class BPlusTreePage:
    """
    B+树页面的基类，封装了所有页面的通用头部信息和操作。
    这个类本身不直接实例化，而是由 InternalPage 和 LeafPage 继承。
    它作为一个Python对象，包装了从BufferPoolManager获取的原始Page对象，
    并提供了方便的方法来读写page.data中的字节。
    """
    # struct 通过此字符串的含义来定义的数据结构格式
    HEADER_FORMAT = 'bH'  # 节点类型(1 byte), 键数量(2 bytes)
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

    def __init__(self, page: Page):
        self.page = page
        self.data = page.data
        # 在初始化时，从原始字节中反序列化出头部信息
        header_data = self.data[:self.HEADER_SIZE]
        is_leaf_byte, self.num_keys = struct.unpack(self.HEADER_FORMAT, header_data)
        self.is_leaf = bool(is_leaf_byte)

    def serialize_header(self):
        """将头部信息序列化回page.data。"""
        header_data = struct.pack(self.HEADER_FORMAT, int(self.is_leaf), self.num_keys)
        self.data[:self.HEADER_SIZE] = header_data



class InternalPage(BPlusTreePage):
    """
    内部节点页面的包装类。
    布局: [ HEADER | ptr_0 | key_1 | ptr_1 | key_2 | ptr_2 | ... ]
    key是用来划分键值范围的标志
    """
    # 假设 key 和 page_id （指针）都是4字节整数
    # todo 此处的key可以根据需要来修改 目前索引key的类型只有整数
    KEY_FORMAT = '>i'
    POINTER_FORMAT = '>i'
    KEY_SIZE = struct.calcsize(KEY_FORMAT)
    POINTER_SIZE = struct.calcsize(POINTER_FORMAT)
    CELL_SIZE = KEY_SIZE + POINTER_SIZE

    def __init__(self, page: Page):
        super().__init__(page)
        self.is_leaf = False  # 明确设置类型
        self.pointers = []
        self.keys = []
        self._deserialize_body()

    def _deserialize_body(self):
        """从page.data中读取所有键和指针。"""
        offset = self.HEADER_SIZE

        ptr_data = self.data[offset: offset + self.POINTER_SIZE]
        self.pointers.append(struct.unpack(self.POINTER_FORMAT, ptr_data)[0])
        offset += self.POINTER_SIZE

        # 读取交替的 key 和 pointer
        for _ in range(self.num_keys):
            key_data = self.data[offset: offset + self.KEY_SIZE]
            self.keys.append(struct.unpack(self.KEY_FORMAT, key_data)[0])
            offset += self.KEY_SIZE

            ptr_data = self.data[offset: offset + self.POINTER_SIZE]
            self.pointers.append(struct.unpack(self.POINTER_FORMAT, ptr_data)[0])
            offset += self.POINTER_SIZE

    def serialize(self):
        """
        将整个内部节点的逻辑结构（内存中的keys和pointers列表）
        序列化回底层的page.data字节数组中。
        """
        self.num_keys = len(self.keys)
        self.serialize_header()
        offset = self.HEADER_SIZE

        # 写入第一个指针 ptr_0
        struct.pack_into(self.POINTER_FORMAT, self.data, offset, self.pointers[0])
        offset += self.POINTER_SIZE

        # 依次写入 key_i 和 ptr_i
        for i in range(self.num_keys):
            struct.pack_into(self.KEY_FORMAT, self.data, offset, self.keys[i])
            offset += self.KEY_SIZE
            struct.pack_into(self.POINTER_FORMAT, self.data, offset, self.pointers[i + 1])
            offset += self.POINTER_SIZE

    def lookup(self, key) -> int:
        """
        根据给定的key，查找应该访问的下一个子节点的page_id。
        这是B+树导航的核心。
        它利用二分查找来快速定位，而不是线性扫描。
        """
        # bisect_right 在有序列表 keys 中查找 key 的插入点。
        # 二分查找索引
        # 'right' 保证了如果 key 存在，插入点会在所有相等元素的右侧。
        # 这个返回的索引值，恰好就是我们在 pointers 列表中需要访问的指针的索引。
        # 例如：keys=[100, 500], pointers=[p0, p1, p2]
        #   - lookup(50): bisect_right返回0 -> pointers[0]
        #   - lookup(250): bisect_right返回1 -> pointers[1]
        #   - lookup(900): bisect_right返回2 -> pointers[2]
        idx = bisect.bisect_right(self.keys, key)
        return self.pointers[idx]

    def is_full(self) -> bool:
        """
        检查当前页面是否已满，即无法再插入一个新的 (key, pointer) 对。
        """
        # 计算当前已用空间
        current_size = self.HEADER_SIZE + self.POINTER_SIZE + (self.num_keys * self.CELL_SIZE)
        # 检查剩余空间是否足够容纳一个新的 (key, pointer) 对
        return current_size + self.CELL_SIZE > len(self.data)

    def insert(self, key, pointer: int):
        """
        在内部节点中插入一个新的 (key, pointer) 对，并保持keys的有序性。
        这个方法在子节点分裂后被调用，用于将分裂产生的“路标”插入到父节点中。
        """
        # 使用 bisect_left 找到 key 应该插入的位置，以保持列表有序
        insert_idx = bisect.bisect_left(self.keys, key)

        # 在keys和pointers列表中相应的位置插入新元素
        self.keys.insert(insert_idx, key)
        # 新的 pointer 应该在新的 key 之后
        self.pointers.insert(insert_idx + 1, pointer)

        # 更新头部信息中的键数量
        self.num_keys = len(self.keys)


class LeafPage(BPlusTreePage):
    """
    叶子节点页面的包装类。
    布局: [ HEADER | prev_pid | next_pid | key_1 | rid_1 | key_2 | rid_2 | ... ]
    """
    KEY_FORMAT = 'i'
    KEY_SIZE = struct.calcsize(KEY_FORMAT)
    # 假设 RID 是 (page_id, slot_num)，分别为4字节和2字节
    RID_FORMAT = 'ih'
    RID_SIZE = struct.calcsize(RID_FORMAT)
    CELL_SIZE = KEY_SIZE + RID_SIZE

    # 兄弟指针也放在头部区域
    SIBLING_POINTER_FORMAT = 'i'
    SIBLING_POINTER_SIZE = struct.calcsize(SIBLING_POINTER_FORMAT)
    LEAF_HEADER_SIZE = BPlusTreePage.HEADER_SIZE + 2 * SIBLING_POINTER_SIZE

    def __init__(self, page: Page):
        super().__init__(page)
        self.is_leaf = True
        self.key_rid_pairs = []
        self.prev_page_id = 0
        self.next_page_id = 0
        self._deserialize_body()

    def _deserialize_body(self):
        """从page.data中读取所有键值对和兄弟指针。"""
        # 读取兄弟指针
        offset = self.HEADER_SIZE
        if len(self.data) >= self.LEAF_HEADER_SIZE:
            self.prev_page_id = struct.unpack_from(self.SIBLING_POINTER_FORMAT, self.data, offset)[0]
            offset += self.SIBLING_POINTER_SIZE
            self.next_page_id = struct.unpack_from(self.SIBLING_POINTER_FORMAT, self.data, offset)[0]
            offset += self.SIBLING_POINTER_SIZE

        # 读取 (key, rid) 对
        for _ in range(self.num_keys):
            if len(self.data) >= offset + self.CELL_SIZE:
                key = struct.unpack_from(self.KEY_FORMAT, self.data, offset)[0]
                offset += self.KEY_SIZE
                rid = struct.unpack_from(self.RID_FORMAT, self.data, offset)
                offset += self.RID_SIZE
                self.key_rid_pairs.append((key, rid))

    def serialize(self):
        """将整个叶子节点的逻辑结构序列化回page.data。"""
        self.num_keys = len(self.key_rid_pairs)
        self.serialize_header()
        offset = self.HEADER_SIZE

        # 写入兄弟指针
        struct.pack_into(self.SIBLING_POINTER_FORMAT, self.data, offset, self.prev_page_id)
        offset += self.SIBLING_POINTER_SIZE
        struct.pack_into(self.SIBLING_POINTER_FORMAT, self.data, offset, self.next_page_id)
        offset += self.SIBLING_POINTER_SIZE

        # 写入 (key, rid) 对
        for key, rid in self.key_rid_pairs:
            struct.pack_into(self.KEY_FORMAT, self.data, offset, key)
            offset += self.KEY_SIZE
            struct.pack_into(self.RID_FORMAT, self.data, offset, *rid)
            offset += self.RID_SIZE

    def lookup(self, key) -> tuple | None:
        """在叶子节点中查找key，返回对应的RID。"""
        # 提取所有键用于二分查找
        keys = [pair[0] for pair in self.key_rid_pairs]

        # 使用 bisect_left 找到可能匹配的位置
        idx = bisect.bisect_left(keys, key)

        # 检查找到的位置是否真的匹配
        if idx < len(keys) and keys[idx] == key:
            return self.key_rid_pairs[idx][1]  # 返回RID

        return None

    def is_full(self) -> bool:
        """检查页面是否已满。"""
        current_size = self.LEAF_HEADER_SIZE + (self.num_keys * self.CELL_SIZE)
        return current_size + self.CELL_SIZE > len(self.data)

    def insert(self, key, rid: tuple):
        """在叶子节点中插入一个新的 (key, RID) 对，保持有序。"""
        # 创建一个元组用于比较和插入
        new_pair = (key, rid)

        # 提取键以找到插入点
        keys = [pair[0] for pair in self.key_rid_pairs]
        insert_idx = bisect.bisect_left(keys, key)

        # 插入新的键值对
        self.key_rid_pairs.insert(insert_idx, new_pair)

        # 更新键数量
        self.num_keys = len(self.key_rid_pairs)


# --- 并发控制辅助类 ---

class TransactionContext:
    """
    用于在单次B+树操作中跟踪和管理所有锁定的页面（Latches）。
    这确保了即使在操作过程中发生错误，所有获取的锁也能被正确释放。
    """

    def __init__(self, tree):
        self.tree = tree
        # 存储已加锁的页面包装器 (InternalPage or LeafPage)
        self.latched_pages_wrappers = []
        self.newly_created_page_ids = set()

    def add_latched_page(self, page_wrapper):
        self.latched_pages_wrappers.append(page_wrapper)

    def release_all_latches(self, is_dirty_list=None):
        """释放所有持有的锁并解钉页面。"""
        # 未提供则默认都不是脏页
        if is_dirty_list is None:
            is_dirty_list = [False] * len(self.latched_pages_wrappers)

        for i, wrapper in reversed(list(enumerate(self.latched_pages_wrappers))):
            page_id = wrapper.page.page_id
            is_dirty = is_dirty_list[i] if i < len(is_dirty_list) else False
            self.tree.bpm.unpin_page(page_id, is_dirty)
            self.tree._release_latch(page_id)

        # 释放新创建页面的锁
        for page_id in self.newly_created_page_ids:
            # 它们已经被unpin和标记为dirty，这里只需释放锁
            self.tree._release_latch(page_id)

        self.latched_pages_wrappers.clear()
        self.newly_created_page_ids.clear()

    def release_ancestors_latches(self):
        """
        实现锁耦合（Latch Crabbing）的关键。
        当发现当前节点是“安全”的时，释放所有祖先节点的锁。
        """
        last_page = self.latched_pages_wrappers.pop()
        # 脏页标记应传递给 release_all_latches
        # 因为被释放的祖先节点可能已经被修改（如果上层也分裂了）
        self.release_all_latches(is_dirty_list=[True] * len(self.latched_pages_wrappers))
        self.latched_pages_wrappers.append(last_page)


# --- B+树主类 ---
class BPlusTree:
    """
    B+树索引的主类，负责管理整个树的结构和操作。
    """

    def __init__(self, buffer_pool_manager: BufferPoolManager, root_page_id: int):
        self.bpm = buffer_pool_manager
        self.root_page_id = root_page_id
        self._manager_lock = threading.Lock()
        # 存储 page_id -> threading.Lock 的映射
        self._latch_manager = {}

    def _get_latch(self, page_id: int) -> threading.Lock:
        """获取或创建一个与page_id关联的锁存器(Latch)。"""
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
        if self.root_page_id is None or self.root_page_id == 0:
            return None

        current_page_id = self.root_page_id

        # 使用try...finally确保即使发生异常，锁也能被释放
        try:
            self._acquire_latch(current_page_id)

            while True:
                page_obj = self.bpm.fetch_page(current_page_id)
                if page_obj is None:
                    # fetch失败，释放当前锁并返回
                    self._release_latch(current_page_id)
                    return None

                is_leaf = bool(struct.unpack_from('b', page_obj.data, 0)[0])

                if is_leaf:
                    leaf_wrapper = LeafPage(page_obj)
                    rid = leaf_wrapper.lookup(key)
                    self.bpm.unpin_page(current_page_id, is_dirty=False)
                    # 这是路径的终点，我们持有锁，所以最后释放
                    self._release_latch(current_page_id)
                    return rid
                else:  # 是内部节点
                    internal_wrapper = InternalPage(page_obj)
                    next_page_id = internal_wrapper.lookup(key)

                    # 锁耦合：先获取子节点锁，再释放父节点锁
                    self._acquire_latch(next_page_id)
                    parent_page_id = current_page_id
                    current_page_id = next_page_id

                    # 释放父节点
                    self.bpm.unpin_page(parent_page_id, is_dirty=False)
                    self._release_latch(parent_page_id)

        except Exception as e:
            print(f"Error during search: {e}")
            # 异常情况下，我们可能仍然持有current_page_id的锁，需要尝试释放
            # 注意: 这里可能需要更复杂的逻辑来确定锁是否真的被持有，
            # 但对于简单场景，尝试释放是合理的。
            try:
                self._release_latch(current_page_id)
            except threading.ThreadError:
                pass  # 锁未被此线程持有
            return None

    def insert(self, key, rid: tuple) -> bool:
        """向B+树中插入一个新的 (key, RID) 对 (线程安全)。"""
        context = TransactionContext(self)

        try:
            if self.root_page_id is None or self.root_page_id == 0:
                return self._start_new_tree(key, rid)

            leaf_page_wrapper = self._find_leaf_page_with_latching(key, context)

            if leaf_page_wrapper.lookup(key) is not None:
                context.release_all_latches()
                return False  # 不允许重复键

            leaf_page_wrapper.insert(key, rid)

            if not leaf_page_wrapper.is_full():
                # 叶子节点未满，操作完成
                context.release_all_latches(is_dirty_list=[True] * len(context.latched_pages_wrappers))
                return False  # root没有改变

            # --- 节点分裂逻辑 ---
            # 1. 创建新兄弟节点
            new_page_obj = self.bpm.new_page()
            # **关键修正**: 立即为新页面加锁！
            self._acquire_latch(new_page_obj.page_id)
            context.newly_created_page_ids.add(new_page_obj.page_id)

            new_leaf_wrapper = LeafPage(new_page_obj)

            # 2. 移动一半数据
            mid_idx = len(leaf_page_wrapper.key_rid_pairs) // 2
            new_leaf_wrapper.key_rid_pairs = leaf_page_wrapper.key_rid_pairs[mid_idx:]
            leaf_page_wrapper.key_rid_pairs = leaf_page_wrapper.key_rid_pairs[:mid_idx]

            # 3. 更新键数量和兄弟指针
            leaf_page_wrapper.num_keys = len(leaf_page_wrapper.key_rid_pairs)
            new_leaf_wrapper.num_keys = len(new_leaf_wrapper.key_rid_pairs)
            new_leaf_wrapper.next_page_id = leaf_page_wrapper.next_page_id
            new_leaf_wrapper.prev_page_id = leaf_page_wrapper.page.page_id
            leaf_page_wrapper.next_page_id = new_leaf_wrapper.page.page_id

            if new_leaf_wrapper.next_page_id != 0:
                # 获取原始的右兄弟节点
                self._acquire_latch(new_leaf_wrapper.next_page_id)
                next_sibling_page_obj = self.bpm.fetch_page(new_leaf_wrapper.next_page_id)

                # 把它也加入到事务上下文中，以确保锁能被正确释放
                # 注意：这里我们只是为了修改它，所以用一个临时的包装器
                next_sibling_wrapper = LeafPage(next_sibling_page_obj)
                context.add_latched_page(next_sibling_wrapper)  # 确保锁被管理

                # 更新它的前向指针
                next_sibling_wrapper.prev_page_id = new_leaf_wrapper.page.page_id
                next_sibling_wrapper.serialize()


            leaf_page_wrapper.serialize()
            new_leaf_wrapper.serialize()

            middle_key = new_leaf_wrapper.key_rid_pairs[0][0]

            # 5. 将中间键插入父节点
            root_changed = self._insert_into_parent(middle_key, new_leaf_wrapper.page.page_id, context)

            # 标记所有涉及的页面为脏页
            dirty_flags = [True] * len(context.latched_pages_wrappers)
            # **修正**: 新页面在unpin时也必须标记为dirty
            self.bpm.unpin_page(new_leaf_wrapper.page.page_id, is_dirty=True)

            context.release_all_latches(dirty_flags)
            return root_changed

        except Exception as e:
            print(f"Error during insert: {e}")
            context.release_all_latches()  # 确保异常时释放所有锁
            return False

    def delete(self, key):
        """todo（可选任务）从B+树中删除一个键。"""
        pass

    def _find_leaf_page_with_latching(self, key, context: TransactionContext) -> BPlusTreePage:
        """
        辅助方法，从根节点开始，使用锁耦合/闩锁爬行协议找到目标叶子节点。
        返回: 目标叶子节点的包装器 (LeafPage)
        """
        current_page_id = self.root_page_id
        self._acquire_latch(current_page_id)
        page_obj = self.bpm.fetch_page(current_page_id)

        is_leaf = bool(struct.unpack_from('b', page_obj.data, 0)[0])
        page_wrapper = LeafPage(page_obj) if is_leaf else InternalPage(page_obj)
        context.add_latched_page(page_wrapper)

        while not page_wrapper.is_leaf:
            internal_wrapper = page_wrapper
            next_page_id = internal_wrapper.lookup(key)

            self._acquire_latch(next_page_id)
            next_page_obj = self.bpm.fetch_page(next_page_id)

            is_leaf = bool(struct.unpack_from('b', next_page_obj.data, 0)[0])
            next_page_wrapper = LeafPage(next_page_obj) if is_leaf else InternalPage(next_page_obj)

            # 核心优化：如果子节点是“安全”的（未满），则释放所有祖先锁
            if not next_page_wrapper.is_full():
                context.release_ancestors_latches()

            context.add_latched_page(next_page_wrapper)
            page_wrapper = next_page_wrapper

        return page_wrapper

    def _start_new_tree(self, key, rid):
        """当树为空时，创建第一个节点。"""
        page_obj = self.bpm.new_page()
        #  即使是新树的根节点，也应该获取锁
        self._acquire_latch(page_obj.page_id)
        try:
            self.root_page_id = page_obj.page_id

            leaf_node = LeafPage(page_obj)
            leaf_node.insert(key, rid)
            leaf_node.serialize()

            self.bpm.unpin_page(self.root_page_id, is_dirty=True)
        finally:
            # 确保锁被释放
            self._release_latch(page_obj.page_id)
        return True

    def _insert_into_parent(self, key, right_child_pid: int, context: TransactionContext) -> bool:
        """递归地将分裂产生的键和指针插入到父节点中。"""
        # 此时，所有需要的父节点都已经被加锁并存储在context中
        # 移除当前叶子/内部节点，留下父节点栈
        popped_child_wrapper = context.latched_pages_wrappers.pop()
        left_child_pid = popped_child_wrapper.page.page_id

        if not context.latched_pages_wrappers:
            # 情况1: 左孩子是根节点，需要创建一个新的根
            new_root_page_obj = self.bpm.new_page()
            # **关键修正**: 为新根节点加锁
            self._acquire_latch(new_root_page_obj.page_id)
            context.newly_created_page_ids.add(new_root_page_obj.page_id)

            new_root = InternalPage(new_root_page_obj)
            self.root_page_id = new_root.page.page_id

            new_root.keys = [key]
            new_root.pointers = [left_child_pid, right_child_pid]
            new_root.serialize()

            self.bpm.unpin_page(self.root_page_id, is_dirty=True)
            return True  # root id changed

        # 从context获取父节点
        parent_node = context.latched_pages_wrappers[-1]
        parent_node.insert(key, right_child_pid)

        if not parent_node.is_full():
            parent_node.serialize()
            return False  # root id not changed

        # 情况3: 父节点也满了，需要递归分裂
        new_internal_page_obj = self.bpm.new_page()
        # **关键修正**: 为分裂出的新内部节点加锁
        self._acquire_latch(new_internal_page_obj.page_id)
        context.newly_created_page_ids.add(new_internal_page_obj.page_id)

        new_internal_node = InternalPage(new_internal_page_obj)

        mid_idx = len(parent_node.keys) // 2
        key_to_push_up = parent_node.keys[mid_idx]

        new_internal_node.keys = parent_node.keys[mid_idx + 1:]
        new_internal_node.pointers = parent_node.pointers[mid_idx + 1:]
        parent_node.keys = parent_node.keys[:mid_idx]
        parent_node.pointers = parent_node.pointers[:mid_idx + 1]

        parent_node.num_keys = len(parent_node.keys)
        new_internal_node.num_keys = len(new_internal_node.keys)

        parent_node.serialize()
        new_internal_node.serialize()
        self.bpm.unpin_page(new_internal_page_obj.page.page_id, is_dirty=True)

        return self._insert_into_parent(key_to_push_up, new_internal_page_obj.page.page_id, context)
