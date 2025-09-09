from collections import OrderedDict

# --- LRU 替换策略 ---
# LRUReplacer 负责跟踪所有在缓冲池中“未被钉住” (unpinned) 的帧。
# 当缓冲池需要淘汰一个页面来为新页腾出空间时，
# BufferPoolManager 会向 LRUReplacer 请求一个“受害者”帧。
# 它使用“最近最少使用” (Least Recently Used) 算法来决定哪个帧应该被淘汰。
class LRUReplacer:
    """
    LRUReplacer manages page eviction using the LRU policy.
    """
    def __init__(self, capacity: int):
        """
        初始化 LRUReplacer。
        Args:
            capacity (int): 缓冲池中帧的总数。
        """
        self.capacity = capacity
        # OrderedDict 可以作为 LRU 缓存的完美数据结构。
        # 我们把它当作一个集合来用，只关心键 (frame_id)。
        # popitem(last=False) 可以 O(1) 移除最旧的项。
        # move_to_end() 可以 O(1) 将一项标记为最新。
        self.cache = OrderedDict()

    def victim(self) -> int | None:
        """
        淘汰一个最近最少使用的帧。
        Returns:
            int | None: 被淘汰的帧的 ID。如果没有可淘汰的帧，则返回 None。
        """
        if not self.cache:
            return None
        # `last=False` 使得 popitem 表现为 FIFO (先进先出)，
        # 这正是 LRU 算法所需要的。
        frame_id, _ = self.cache.popitem(last=False)
        return frame_id

    def pin(self, frame_id: int):
        """
        当一个页被“钉住”时，它不应被淘汰。
        我们将其从 LRU 候选者中移除。
        Args:
            frame_id (int): 被钉住的帧的 ID。
        """
        if frame_id in self.cache:
            del self.cache[frame_id]

    def unpin(self, frame_id: int):
        """
        当一个页的 pin_count 变为 0 时，它成为可淘汰的候选者。
        我们将其加入到 LRU 跟踪列表的“最新”一端。
        Args:
            frame_id (int): 被解钉的帧的 ID。
        """
        if frame_id not in self.cache and len(self.cache) < self.capacity:
            # 将其添加到末尾，表示“最近使用”。
            self.cache[frame_id] = None
