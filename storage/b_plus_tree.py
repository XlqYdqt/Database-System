# -*- coding: utf-8 -*-

from typing import List, Tuple, Optional, Any

MAX_KEYS = 4 # 示例值，实际根据页面大小和键值对大小确定

class Node:
    """B+树节点基类"""
    def __init__(self, is_leaf: bool):
        self.is_leaf = is_leaf
        self.keys: List[bytes] = []
        self.parent: Optional[InternalNode] = None

    def is_full(self) -> bool:
        return len(self.keys) == MAX_KEYS

    def is_half_full(self) -> bool:
        return len(self.keys) >= MAX_KEYS // 2

class InternalNode(Node):
    """B+树内部节点"""
    def __init__(self):
        super().__init__(is_leaf=False)
        self.children: List[Node] = []

class LeafNode(Node):
    """B+树叶子节点"""
    def __init__(self):
        super().__init__(is_leaf=True)
        self.values: List[bytes] = [] # 存储实际数据（RID）
        self.next_leaf: Optional[LeafNode] = None
        self.prev_leaf: Optional[LeafNode] = None

class BPlusTree:
    """B+树索引"""
    def __init__(self):
        self.root: Optional[Node] = None

    def _find_leaf(self, key: bytes) -> LeafNode:
        """查找给定键应该插入的叶子节点"""
        if not self.root:
            return None

        node = self.root
        while not node.is_leaf:
            # 找到第一个大于或等于key的键的索引
            i = 0
            while i < len(node.keys) and key >= node.keys[i]:
                i += 1
            node = node.children[i]
        return node

    def _insert_into_leaf(self, leaf: LeafNode, key: bytes, value: bytes):
        """将键值对插入叶子节点，并处理分裂"""
        # 找到插入位置
        i = 0
        while i < len(leaf.keys) and key > leaf.keys[i]:
            i += 1
        leaf.keys.insert(i, key)
        leaf.values.insert(i, value)

        if leaf.is_full():
            self._split_leaf(leaf)

    def _split_leaf(self, leaf: LeafNode):
        """分裂叶子节点"""
        mid = MAX_KEYS // 2
        new_leaf = LeafNode()
        new_leaf.keys = leaf.keys[mid:]
        new_leaf.values = leaf.values[mid:]
        new_leaf.parent = leaf.parent

        # 更新原叶子节点
        leaf.keys = leaf.keys[:mid]
        leaf.values = leaf.values[:mid]

        # 维护叶子节点链表
        new_leaf.next_leaf = leaf.next_leaf
        if leaf.next_leaf:
            leaf.next_leaf.prev_leaf = new_leaf
        leaf.next_leaf = new_leaf
        new_leaf.prev_leaf = leaf

        # 将新叶子节点提升到父节点
        if leaf.parent:
            self._insert_into_parent(leaf.parent, new_leaf.keys[0], leaf, new_leaf)
        else:
            # 如果没有父节点，创建新的根节点
            new_root = InternalNode()
            new_root.keys = [new_leaf.keys[0]]
            new_root.children = [leaf, new_leaf]
            leaf.parent = new_root
            new_leaf.parent = new_root
            self.root = new_root

    def _insert_into_parent(self, parent: InternalNode, key: bytes, left_child: Node, right_child: Node):
        """将键和子节点插入父节点，并处理分裂"""
        # 找到插入位置
        i = 0
        while i < len(parent.keys) and key > parent.keys[i]:
            i += 1
        parent.keys.insert(i, key)
        parent.children.pop(i)
        parent.children.insert(i, left_child)
        parent.children.insert(i + 1, right_child)

        left_child.parent = parent
        right_child.parent = parent

        if parent.is_full():
            self._split_internal(parent)

    def _split_internal(self, internal: InternalNode):
        """分裂内部节点"""
        mid = MAX_KEYS // 2
        new_internal = InternalNode()
        new_internal.keys = internal.keys[mid + 1:]
        new_internal.children = internal.children[mid + 1:]
        new_internal.parent = internal.parent

        # 更新子节点的父指针
        for child in new_internal.children:
            child.parent = new_internal

        # 提升中间键
        promoted_key = internal.keys[mid]

        # 更新原内部节点
        internal.keys = internal.keys[:mid]
        internal.children = internal.children[:mid + 1]

        if internal.parent:
            self._insert_into_parent(internal.parent, promoted_key, internal, new_internal)
        else:
            # 如果没有父节点，创建新的根节点
            new_root = InternalNode()
            new_root.keys = [promoted_key]
            new_root.children = [internal, new_internal]
            internal.parent = new_root
            new_internal.parent = new_root
            self.root = new_root

    def insert(self, key: bytes, value: bytes) -> bool:
        """插入键值对"""
        if not self.root:
            # 树为空，创建第一个叶子节点作为根
            leaf = LeafNode()
            leaf.keys.append(key)
            leaf.values.append(value)
            self.root = leaf
            return True

        leaf = self._find_leaf(key)
        if not leaf:
            return False # Should not happen if root exists

        # 检查键是否已存在（如果B+树用于唯一索引）
        # 这里假设键可以重复，或者由上层逻辑保证唯一性
        # 如果需要唯一性，可以在这里添加检查

        self._insert_into_leaf(leaf, key, value)
        return True

    def search(self, key: bytes) -> Optional[bytes]:
        """查找键对应的值"""
        if not self.root:
            return None

        leaf = self._find_leaf(key)
        if not leaf:
            return None

        for i, k in enumerate(leaf.keys):
            if k == key:
                return leaf.values[i]
        return None

    def delete(self, key: bytes) -> bool:
        """删除键值对"""
        if not self.root:
            return False

        leaf = self._find_leaf(key)
        if not leaf:
            return False

        # 找到键的索引
        idx = -1
        for i, k in enumerate(leaf.keys):
            if k == key:
                idx = i
                break

        if idx == -1:
            return False # Key not found

        # 删除键值对
        leaf.keys.pop(idx)
        leaf.values.pop(idx)

        # 处理下溢
        if not leaf.is_half_full() and self.root != leaf:
            self._handle_underflow(leaf)
        elif not leaf.keys and self.root == leaf:
            # 如果根节点是叶子节点且为空，则树为空
            self.root = None

        return True

    def _handle_underflow(self, node: Node):
        """处理节点下溢"""
        if not node.parent:
            return # 根节点不处理下溢（除非它为空）

        parent = node.parent
        # 找到当前节点在父节点中的索引
        idx_in_parent = -1
        for i, child in enumerate(parent.children):
            if child == node:
                idx_in_parent = i
                break

        # 尝试从兄弟节点借键
        # 尝试从左兄弟借
        if idx_in_parent > 0:
            left_sibling = parent.children[idx_in_parent - 1]
            if left_sibling.is_half_full():
                self._redistribute(left_sibling, node, parent, idx_in_parent - 1)
                return

        # 尝试从右兄弟借
        if idx_in_parent < len(parent.children) - 1:
            right_sibling = parent.children[idx_in_parent + 1]
            if right_sibling.is_half_full():
                self._redistribute(node, right_sibling, parent, idx_in_parent)
                return

        # 无法借键，进行合并
        if idx_in_parent > 0:
            # 与左兄弟合并
            self._merge(left_sibling, node, parent, idx_in_parent - 1)
        else:
            # 与右兄弟合并
            self._merge(node, right_sibling, parent, idx_in_parent)

    def _redistribute(self, left_node: Node, right_node: Node, parent: InternalNode, parent_key_idx: int):
        """重新分配键"""
        if left_node.is_leaf and right_node.is_leaf:
            # 叶子节点重新分配
            # 从左兄弟移动一个键到右兄弟
            if len(left_node.keys) > MAX_KEYS // 2:
                key_to_move = left_node.keys.pop()
                value_to_move = left_node.values.pop()
                right_node.keys.insert(0, key_to_move)
                right_node.values.insert(0, value_to_move)
                parent.keys[parent_key_idx] = right_node.keys[0] # 更新父节点键
            # 从右兄弟移动一个键到左兄弟
            elif len(right_node.keys) > MAX_KEYS // 2:
                key_to_move = right_node.keys.pop(0)
                value_to_move = right_node.values.pop(0)
                left_node.keys.append(key_to_move)
                left_node.values.append(value_to_move)
                parent.keys[parent_key_idx] = right_node.keys[0] # 更新父节点键
        else:
            # 内部节点重新分配
            # 从左兄弟移动一个键到右兄弟
            if len(left_node.keys) > MAX_KEYS // 2:
                promoted_key = parent.keys[parent_key_idx]
                key_to_move = left_node.keys.pop()
                child_to_move = left_node.children.pop()

                parent.keys[parent_key_idx] = key_to_move
                right_node.keys.insert(0, promoted_key)
                right_node.children.insert(0, child_to_move)
                child_to_move.parent = right_node
            # 从右兄弟移动一个键到左兄弟
            elif len(right_node.keys) > MAX_KEYS // 2:
                promoted_key = parent.keys[parent_key_idx]
                key_to_move = right_node.keys.pop(0)
                child_to_move = right_node.children.pop(0)

                parent.keys[parent_key_idx] = key_to_move
                left_node.keys.append(promoted_key)
                left_node.children.append(child_to_move)
                child_to_move.parent = left_node

    def _merge(self, left_node: Node, right_node: Node, parent: InternalNode, parent_key_idx: int):
        """合并节点"""
        if left_node.is_leaf and right_node.is_leaf:
            # 合并叶子节点
            left_node.keys.extend(right_node.keys)
            left_node.values.extend(right_node.values)
            left_node.next_leaf = right_node.next_leaf
            if right_node.next_leaf:
                right_node.next_leaf.prev_leaf = left_node
        else:
            # 合并内部节点
            promoted_key = parent.keys[parent_key_idx]
            left_node.keys.append(promoted_key)
            left_node.keys.extend(right_node.keys)
            left_node.children.extend(right_node.children)
            for child in right_node.children:
                child.parent = left_node

        # 从父节点移除被合并的键和子节点
        parent.keys.pop(parent_key_idx)
        parent.children.pop(parent_key_idx + 1) # 移除右节点

        # 如果父节点下溢，递归处理
        if not parent.is_half_full() and self.root != parent:
            self._handle_underflow(parent)
        elif not parent.keys and self.root == parent:
            # 如果根节点是内部节点且为空，则新的根节点是左节点
            self.root = left_node
            left_node.parent = None

    def update(self, key: bytes, new_value: bytes) -> bool:
        """更新键对应的值"""
        if not self.root:
            return False

        leaf = self._find_leaf(key)
        if not leaf:
            return False

        # 找到键的索引
        idx = -1
        for i, k in enumerate(leaf.keys):
            if k == key:
                idx = i
                break

        if idx == -1:
            return False # Key not found

        # 更新值
        leaf.values[idx] = new_value
        return True