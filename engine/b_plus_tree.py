import struct
import bisect
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

    def __init__(self, page: Page, is_leaf: bool):
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

    # 其他通用方法，如 is_full(), get_key_at() 等可以根据需要添加


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
        super().__init__(page, is_leaf=False)
        self.pointers = []
        self.keys = []
        # _deserialize_body 会根据头部中的 num_keys 填充 keys 和 pointers
        self._deserialize_body()

    def _deserialize_body(self):
        """从page.data中读取所有键和指针。"""
        offset = self.HEADER_SIZE
        # 确保数据长度足够读取第一个指针
        if len(self.data) >= offset + self.POINTER_SIZE:
            ptr_data = self.data[offset: offset + self.POINTER_SIZE]
            self.pointers.append(struct.unpack(self.POINTER_FORMAT, ptr_data)[0])
            offset += self.POINTER_SIZE

        # 读取交替的 key 和 pointer
        for _ in range(self.num_keys):
            # 确保数据长度足够读取一个 (key, pointer) 对
            if len(self.data) >= offset + self.CELL_SIZE:
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
    KEY_FORMAT = '>i'
    KEY_SIZE = struct.calcsize(KEY_FORMAT)
    # 假设 RID 是 (page_id, slot_num)，分别为4字节和2字节
    RID_FORMAT = '>ih'
    RID_SIZE = struct.calcsize(RID_FORMAT)
    CELL_SIZE = KEY_SIZE + RID_SIZE

    # 兄弟指针也放在头部区域
    SIBLING_POINTER_FORMAT = '>i'
    SIBLING_POINTER_SIZE = struct.calcsize(SIBLING_POINTER_FORMAT)
    LEAF_HEADER_SIZE = BPlusTreePage.HEADER_SIZE + 2 * SIBLING_POINTER_SIZE

    def __init__(self, page: Page):
        super().__init__(page, is_leaf=True)
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


# --- B+树主类 ---
class BPlusTree:
    """
    B+树索引的主类，负责管理整个树的结构和操作。
    """

    def __init__(self, buffer_pool_manager: BufferPoolManager, root_page_id: int):
        self.bpm = buffer_pool_manager
        self.root_page_id = root_page_id

    def search(self, key) -> tuple | None:
        """从B+树中查找一个键，返回其对应的RID。"""
        if self.root_page_id is None:
            return None

        leaf_page_wrapper, _ = self._find_leaf_page(key)
        rid = leaf_page_wrapper.lookup(key)

        self.bpm.unpin_page(leaf_page_wrapper.page.page_id, is_dirty=False)
        return rid

    def insert(self, key, rid: tuple):
        """向B+树中插入一个新的 (key, RID) 对。"""
        if self.root_page_id is None:
            return self._start_new_tree(key, rid)

        leaf_page_wrapper, parent_stack = self._find_leaf_page(key, is_for_insert=True)
        leaf_page_wrapper.insert(key, rid)

        if not leaf_page_wrapper.is_full():
            leaf_page_wrapper.serialize()
            self.bpm.unpin_page(leaf_page_wrapper.page.page_id, is_dirty=True)
            return

        # --- 节点分裂逻辑 ---
        # 1. 创建新兄弟节点
        new_page_obj = self.bpm.new_page()
        new_leaf_wrapper = LeafPage(new_page_obj)

        # 2. 移动一半数据
        mid_idx = len(leaf_page_wrapper.key_rid_pairs) // 2
        new_leaf_wrapper.key_rid_pairs = leaf_page_wrapper.key_rid_pairs[mid_idx:]
        leaf_page_wrapper.key_rid_pairs = leaf_page_wrapper.key_rid_pairs[:mid_idx]

        # 3. 更新兄弟指针
        new_leaf_wrapper.next_page_id = leaf_page_wrapper.next_page_id
        new_leaf_wrapper.prev_page_id = leaf_page_wrapper.page.page_id
        leaf_page_wrapper.next_page_id = new_leaf_wrapper.page.page_id

        # 4. 获取要推到父节点的中间键
        middle_key = new_leaf_wrapper.key_rid_pairs[0][0]

        # 5. 序列化两个修改过的叶子节点
        leaf_page_wrapper.serialize()
        new_leaf_wrapper.serialize()

        # 6. 将中间键插入父节点
        self._insert_into_parent(leaf_page_wrapper, middle_key, new_leaf_wrapper, parent_stack)

        # 7. 解钉所有相关页面
        self.bpm.unpin_page(leaf_page_wrapper.page.page_id, is_dirty=True)
        self.bpm.unpin_page(new_leaf_wrapper.page.page_id, is_dirty=True)

    def delete(self, key):
        """todo（可选任务）从B+树中删除一个键。"""
        pass

    def _find_leaf_page(self, key, is_for_insert=False) -> (LeafPage, list):
        """
        辅助方法，从根节点开始遍历，找到目标叶子节点。
        返回: (LeafPage包装器, 父节点ID栈)
        """
        parent_stack = []
        current_page_id = self.root_page_id

        while True:
            page_obj = self.bpm.fetch_page(current_page_id)
            # 根据头部第一个字节判断页面类型
            is_leaf = bool(struct.unpack_from('>b', page_obj.data, 0)[0])

            if is_leaf:
                return LeafPage(page_obj), parent_stack

            node = InternalPage(page_obj)
            parent_stack.append(current_page_id)
            next_page_id = node.lookup(key)
            self.bpm.unpin_page(current_page_id, is_dirty=False)
            current_page_id = next_page_id

    def _start_new_tree(self, key, rid):
        """当树为空时，创建第一个节点。"""
        page_obj = self.bpm.new_page()
        self.root_page_id = page_obj.page_id
        # !!todo 重要: 实际应用中，必须将新的 self.root_page_id 持久化到 Catalog (Page 0) 中 !!

        leaf_node = LeafPage(page_obj)
        leaf_node.insert(key, rid)
        leaf_node.serialize()

        self.bpm.unpin_page(self.root_page_id, is_dirty=True)

    def _insert_into_parent(self, left_child: BPlusTreePage, key, right_child: BPlusTreePage, parent_stack: list):
        """递归地将分裂产生的键和指针插入到父节点中。"""
        if not parent_stack:
            # 情况1: 左孩子是根节点，需要创建一个新的根
            new_root_page_obj = self.bpm.new_page()
            new_root = InternalPage(new_root_page_obj)
            self.root_page_id = new_root.page.page_id
            # !!todo 重要: 同样需要将新的 self.root_page_id 持久化到 Catalog !!

            new_root.keys = [key]
            new_root.pointers = [left_child.page.page_id, right_child.page.page_id]
            new_root.serialize()

            self.bpm.unpin_page(self.root_page_id, is_dirty=True)
            return

        # 情况2: 存在父节点
        parent_page_id = parent_stack.pop()
        parent_page_obj = self.bpm.fetch_page(parent_page_id)
        parent_node = InternalPage(parent_page_obj)
        parent_node.insert(key, right_child.page.page_id)

        if not parent_node.is_full():
            parent_node.serialize()
            self.bpm.unpin_page(parent_page_id, is_dirty=True)
            return

        # 情况3: 父节点也满了，需要递归分裂
        # ... 实现内部节点的分裂逻辑，与叶子节点分裂类似 ...
        new_internal_page_obj = self.bpm.new_page()
        new_internal_node = InternalPage(new_internal_page_obj)

        # 2. 移动一半键和指针
        mid_idx = len(parent_node.keys) // 2

        # 中间的键将被推到上层
        key_to_push_up = parent_node.keys[mid_idx]

        # 新节点获取后半部分的键和指针
        new_internal_node.keys = parent_node.keys[mid_idx + 1:]
        new_internal_node.pointers = parent_node.pointers[mid_idx + 1:]

        # 旧节点保留前半部分
        parent_node.keys = parent_node.keys[:mid_idx]
        parent_node.pointers = parent_node.pointers[:mid_idx + 1]

        # 3. 将中间键推到上层（递归调用 _insert_into_parent）
        self._insert_into_parent(parent_node, key_to_push_up, new_internal_node, parent_stack)

        # 4. 序列化并解钉所有相关页面
        parent_node.serialize()
        new_internal_node.serialize()
        self.bpm.unpin_page(parent_page_id, is_dirty=True)
        self.bpm.unpin_page(new_internal_page_obj.page_id, is_dirty=True)