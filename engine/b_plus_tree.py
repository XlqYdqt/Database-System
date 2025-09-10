import struct
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
    # 假设 key 和 page_id 都是4字节整数
    KEY_FORMAT = 'i'
    POINTER_FORMAT = 'i'
    KEY_SIZE = struct.calcsize(KEY_FORMAT)
    POINTER_SIZE = struct.calcsize(POINTER_FORMAT)

    def __init__(self, page: Page):
        super().__init__(page, is_leaf=False)
        self.pointers = []
        self.keys = []
        self._deserialize_body()

    def _deserialize_body(self):
        """从page.data中读取所有键和指针。"""
        offset = self.HEADER_SIZE
        # 读取第一个指针
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
        """将整个内部节点的逻辑结构序列化回page.data。"""
        self.serialize_header()
        offset = self.HEADER_SIZE
        # 写入所有指针和键
        # ... 实现具体的写入逻辑 ...

    def lookup(self, key) -> int:
        """根据给定的key，查找应该访问的下一个子节点的page_id。"""
        # ... 实现二分查找或顺序查找 ...
        # 返回一个 page_id
        pass

    # ... 其他方法，如分裂(split), 插入(insert)等 ...


class LeafPage(BPlusTreePage):
    """
    叶子节点页面的包装类。
    布局: [ HEADER | key_1 | rid_1 | key_2 | rid_2 | ... | prev_pid | next_pid ]
    """
    # 假设 RID 是 (page_id, slot_num)，分别为4字节和2字节
    RID_FORMAT = 'ih'
    RID_SIZE = struct.calcsize(RID_FORMAT)
    # 假设 prev/next page_id 是4字节整数
    SIBLING_POINTER_FORMAT = 'i'
    SIBLING_POINTER_SIZE = struct.calcsize(SIBLING_POINTER_FORMAT)

    def __init__(self, page: Page):
        super().__init__(page, is_leaf=True)
        self.key_rid_pairs = []
        self.prev_page_id = 0
        self.next_page_id = 0
        self._deserialize_body()

    def _deserialize_body(self):
        """从page.data中读取所有键值对和兄弟指针。"""
        # ... 实现具体的反序列化逻辑 ...
        pass

    def serialize(self):
        """将整个叶子节点的逻辑结构序列化回page.data。"""
        # ... 实现具体的序列化逻辑 ...
        pass

    # ... 其他方法，如分裂(split), 插入(insert), 查找(find)等 ...


# --- B+树主类 ---

class BPlusTree:
    """
    B+树索引的主类，负责管理整个树的结构和操作。
    它通过与BufferPoolManager交互来获取、创建和释放页面。
    """

    def __init__(self, buffer_pool_manager: BufferPoolManager, root_page_id: int):
        self.bpm = buffer_pool_manager
        self.root_page_id = root_page_id

    def search(self, key) -> tuple | None:
        """
        从B+树中查找一个键，返回其对应的RID。
        """
        if self.root_page_id is None:
            return None

        # 1. 从根节点开始，调用 _find_leaf_page 辅助方法
        leaf_page_obj = self._find_leaf_page(key)

        # 2. 在叶子节点中查找 key
        # ... 实现叶子节点内的查找逻辑 ...

        # 3. 别忘了 unpin 页面
        self.bpm.unpin_page(leaf_page_obj.page.page_id, is_dirty=False)

        # return rid or None
        pass

    def insert(self, key, rid: tuple):
        """
        向B+树中插入一个新的 (key, RID) 对。
        """
        if self.root_page_id is None:
            # 如果树是空的，创建一个新的根节点（同时也是叶子节点）
            return self._start_new_tree(key, rid)

        # 1. 查找应该插入的叶子节点
        leaf_page_obj = self._find_leaf_page(key, is_for_insert=True)

        # 2. 尝试在叶子节点中插入
        # ... 实现叶子节点内的插入逻辑 ...

        # 3. 如果叶子节点满了，则需要分裂
        if leaf_page_obj.is_full():
            # a. 调用 bpm.new_page() 创建新兄弟节点
            # b. 将一半数据移动到新节点
            # c. 将新节点插入到父节点中（这可能导致父节点也分裂，需要递归处理）
            # d. 记得 unpin 所有修改过的页面，并标记 is_dirty=True
            pass

        # 4. 如果没满，直接 unpin 并标记 is_dirty=True
        self.bpm.unpin_page(leaf_page_obj.page.page_id, is_dirty=True)
        pass

    def delete(self, key):
        """
        从B+树中删除一个键及其关联的RID。
        （实现比插入更复杂，涉及到合并或重新分配，初期可作为可选任务）
        """
        pass

    def _find_leaf_page(self, key, is_for_insert=False) -> LeafPage:
        """
        一个辅助方法，从根节点开始遍历，找到包含目标key的叶子节点。
        - is_for_insert: 如果是为插入操作查找，需要记录遍历路径以备分裂时使用。
        """
        current_page_id = self.root_page_id

        while True:
            # 1. 获取页面
            page_obj = self.bpm.fetch_page(current_page_id)

            # 2. 包装成 InternalPage 或 LeafPage
            #    （需要根据头部信息判断）
            node = InternalPage(page_obj)  # 假设是内部节点

            if node.is_leaf:
                # 找到了叶子节点，返回
                # 注意：此时叶子节点的page是pin住的，由调用者负责unpin
                return LeafPage(page_obj)

            # 3. 在内部节点中查找下一个page_id
            next_page_id = node.lookup(key)

            # 4. 解钉当前页面
            self.bpm.unpin_page(current_page_id, is_dirty=False)

            # 5. 进入下一层
            current_page_id = next_page_id

    def _start_new_tree(self, key, rid):
        """
        当树完全为空时，创建第一个节点（根节点，也是叶子节点）。
        """
        # 1. 调用 self.bpm.new_page() 来获取一个新页面
        page_obj = self.bpm.new_page()
        self.root_page_id = page_obj.page_id
        # ... 在此更新Catalog中的root_page_id ...

        # 2. 将此页面包装成 LeafPage
        leaf_node = LeafPage(page_obj)

        # 3. 插入第一个键值对
        # ... 实现插入逻辑 ...

        # 4. 将修改序列化回 page.data
        leaf_node.serialize()

        # 5. 解钉页面，标记为脏页
        self.bpm.unpin_page(self.root_page_id, is_dirty=True)