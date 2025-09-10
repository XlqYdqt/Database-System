# -*- coding: utf-8 -*-

"""
数据库存储引擎组件的单元测试。
要运行这些测试，请确保 disk_manager.py, lru_replacer.py,
和 buffer_pool_manager.py 文件位于同一目录中。
"""

import unittest
import os
import threading
import time
import random

# 假设你的代码分别在以下三个文件中
# 如果 Page 类不在 buffer_pool_manager.py 中，请相应调整导入
from disk_manager import DiskManager
from lru_replacer import LRUReplacer
from buffer_pool_manager import BufferPoolManager, Page


class TestDiskManager(unittest.TestCase):
    """DiskManager 的测试套件。"""

    def setUp(self):
        """为每个测试设置一个临时的数据库文件。"""
        self.db_filename = "test_db.db"
        # 确保每次测试开始前文件都是全新的
        if os.path.exists(self.db_filename):
            os.remove(self.db_filename)
        self.disk_manager = DiskManager(self.db_filename, page_size=128)

    def tearDown(self):
        """在每个测试后清理临时的数据库文件。"""
        self.disk_manager.close()
        if os.path.exists(self.db_filename):
            os.remove(self.db_filename)

    def test_read_write_page(self):
        """测试向页面写入数据并读回。"""
        page_id = self.disk_manager.allocate_page()
        self.assertEqual(page_id, 0)

        data_to_write = bytearray(b'A' * self.disk_manager.page_size)
        self.disk_manager.write_page(page_id, data_to_write)

        data_read = self.disk_manager.read_page(page_id)
        self.assertEqual(data_to_write, data_read)

    def test_allocate_page(self):
        """测试分配多个页面。"""
        self.assertEqual(self.disk_manager.get_num_pages(), 0)

        page_id1 = self.disk_manager.allocate_page()
        self.assertEqual(page_id1, 0)
        self.assertEqual(self.disk_manager.get_num_pages(), 1)

        page_id2 = self.disk_manager.allocate_page()
        self.assertEqual(page_id2, 1)
        self.assertEqual(self.disk_manager.get_num_pages(), 2)

    def test_read_invalid_page(self):
        """测试读取一个不存在的页面会引发 IndexError。"""
        with self.assertRaises(IndexError):
            self.disk_manager.read_page(0)

    def test_write_invalid_data(self):
        """测试写入大小不正确的数据会引发 ValueError。"""
        page_id = self.disk_manager.allocate_page()
        invalid_data = bytearray(b'short')
        with self.assertRaises(ValueError):
            self.disk_manager.write_page(page_id, invalid_data)


class TestLRUReplacer(unittest.TestCase):
    """LRUReplacer 的测试套件。"""

    def setUp(self):
        self.lru_replacer = LRUReplacer(capacity=5)

    def test_unpin_and_victim(self):
        """测试基本的解钉和 victim 选择。"""
        for i in range(5):
            self.lru_replacer.unpin(i)

        # 0 应该是最近最少使用的
        self.assertEqual(self.lru_replacer.victim(), 0)
        # 1 应该是下一个受害者
        self.assertEqual(self.lru_replacer.victim(), 1)

    def test_pin(self):
        """测试钉住操作会从候选受害者中移除一个帧。"""
        self.lru_replacer.unpin(0)
        self.lru_replacer.unpin(1)

        self.lru_replacer.pin(0)  # 钉住帧 0

        # 现在受害者应该是 1，而不是 0
        self.assertEqual(self.lru_replacer.victim(), 1)
        self.assertIsNone(self.lru_replacer.victim())

    def test_victim_all_pinned(self):
        """测试当所有帧都被钉住时，victim 返回 None。"""
        self.lru_replacer.unpin(0)
        self.lru_replacer.unpin(1)

        self.lru_replacer.pin(0)
        self.lru_replacer.pin(1)

        self.assertIsNone(self.lru_replacer.victim())

    def test_pin_unpin_sequence(self):
        """测试解钉、钉住和再次解钉的序列。"""
        self.lru_replacer.unpin(0)
        self.lru_replacer.unpin(1)

        self.assertEqual(self.lru_replacer.victim(), 0)

        self.lru_replacer.unpin(0)  # 再次解钉 0 会使其变为最近使用的
        self.assertEqual(self.lru_replacer.victim(), 1)


class TestBufferPoolManager(unittest.TestCase):
    """BufferPoolManager 的测试套件。"""

    def setUp(self):
        self.db_filename = "test_bpm.db"
        self.page_size = 128
        self.pool_size = 5

        if os.path.exists(self.db_filename):
            try:
                os.remove(self.db_filename)
            except OSError as e:
                print(f"Error removing file {self.db_filename}: {e}")

        self.disk_manager = DiskManager(self.db_filename, self.page_size)
        self.lru_replacer = LRUReplacer(self.pool_size)
        self.bpm = BufferPoolManager(self.pool_size, self.disk_manager, self.lru_replacer)

    def tearDown(self):
        self.bpm.close()
        if os.path.exists(self.db_filename):
            try:
                os.remove(self.db_filename)
            except OSError as e:
                print(f"Error removing file in tearDown {self.db_filename}: {e}")

    def test_new_page_basic(self):
        """测试当缓冲池有空间时创建新页面。"""
        page1 = self.bpm.new_page()
        self.assertIsNotNone(page1)
        self.assertEqual(page1.page_id, 0)
        self.assertEqual(page1.pin_count, 1)

        # 解钉 page1，使其可以被驱逐
        self.assertTrue(self.bpm.unpin_page(page1.page_id, is_dirty=True))

        page2 = self.bpm.new_page()
        self.assertIsNotNone(page2)
        self.assertEqual(page2.page_id, 1)

    def test_new_page_all_pinned(self):
        """测试当所有帧都已满且被钉住时，new_page 返回 None。"""
        pages = []
        for _ in range(self.pool_size):
            page = self.bpm.new_page()
            self.assertIsNotNone(page)
            pages.append(page)

        # 所有页面都被钉住，应该无法创建新页面
        self.assertIsNone(self.bpm.new_page())

        # 清理
        for page in pages:
            self.bpm.unpin_page(page.page_id, is_dirty=False)

    def test_fetch_page_hit_and_miss(self):
        """测试获取页面（缓存命中和未命中）。"""
        page0 = self.bpm.new_page()
        test_data = b'test_data'
        page0.data[:] = test_data.ljust(self.page_size, b'\0')
        self.assertTrue(self.bpm.unpin_page(0, is_dirty=True))

        # 强制将 page 0 刷到磁盘
        self.assertTrue(self.bpm.flush_page(0))

        # **修正**: 创建一个新的BPM实例来模拟冷缓存，但共享同一个DiskManager
        # 这样可以确保是从磁盘上读取，而不是从内存缓存中。
        lru_replacer2 = LRUReplacer(self.pool_size)
        bpm2 = BufferPoolManager(self.pool_size, self.disk_manager, lru_replacer2)

        fetched_page0 = bpm2.fetch_page(0)
        self.assertIsNotNone(fetched_page0)
        self.assertEqual(fetched_page0.page_id, 0)
        self.assertEqual(fetched_page0.data, test_data.ljust(self.page_size, b'\0'))

        # 在同一个BPM实例中再次获取，测试缓存命中
        fetched_page0_again = bpm2.fetch_page(0)
        self.assertIsNotNone(fetched_page0_again)
        self.assertEqual(fetched_page0_again.pin_count, 2)

        self.assertTrue(bpm2.unpin_page(0, False))
        self.assertTrue(bpm2.unpin_page(0, False))
        # **修正**: 不在此处关闭 bpm2，因为它会关闭共享的 disk_manager
        # bpm2.close()

    def test_eviction_of_dirty_page(self):
        """测试脏页在被驱逐时会被刷新到磁盘。"""
        pages = []
        for i in range(self.pool_size):
            page = self.bpm.new_page()
            page_data = f"page_{i}".encode()
            page.data[:] = page_data.ljust(self.page_size, b'\0')
            pages.append(page)
            self.assertTrue(self.bpm.unpin_page(i, is_dirty=True))

        # 这应该会驱逐 page 0
        new_page = self.bpm.new_page()
        self.assertIsNotNone(new_page)
        self.assertTrue(self.bpm.unpin_page(new_page.page_id, False))

        # **修正**: 创建一个新的BPM来确保我们是从磁盘读取，而不是从缓存读取
        lru_replacer2 = LRUReplacer(self.pool_size)
        bpm2 = BufferPoolManager(self.pool_size, self.disk_manager, lru_replacer2)

        fetched_page0 = bpm2.fetch_page(0)
        self.assertIsNotNone(fetched_page0)
        expected_data = b"page_0".ljust(self.page_size, b'\0')
        self.assertEqual(fetched_page0.data, expected_data)
        # **修正**: 不在此处关闭 bpm2
        # bpm2.close()

    def test_delete_pinned_page(self):
        """测试被钉住的页面不能被删除。"""
        page = self.bpm.new_page()
        self.assertFalse(self.bpm.delete_page(page.page_id))
        self.assertTrue(self.bpm.unpin_page(page.page_id, False))

    def test_delete_page_success(self):
        """测试成功删除一个页面，并验证其帧被释放。"""
        # 1. 创建一个页面并解钉
        page = self.bpm.new_page()
        page_id = page.page_id
        self.assertTrue(self.bpm.unpin_page(page_id, False))

        # 2. 删除该页面
        self.assertTrue(self.bpm.delete_page(page_id))

        # 3. 验证该页面的帧已被释放
        # 我们可以通过成功创建 pool_size 个新页面来证明这一点。
        # 如果帧没有被释放，我们最多只能创建 pool_size - 1 个页面。
        pages = []
        for i in range(self.pool_size):
            p = self.bpm.new_page()
            # 断言每个新页面的创建都成功了
            self.assertIsNotNone(p, f"第{i + 1}次创建新页面失败, page {page_id} 的帧可能未被正确释放。")
            pages.append(p)

        # 清理
        for p in pages:
            if p:
                self.bpm.unpin_page(p.page_id, False)

    def test_flush_all_pages(self):
        """测试 flush_all_pages 只刷新脏页。"""
        # 1. 创建 page0 和 page1，它们默认都是脏的
        page0 = self.bpm.new_page()  # page_id 0
        data0 = b'data0_v1'
        page0.data[:] = data0.ljust(self.page_size, b'\0')
        self.bpm.unpin_page(0, is_dirty=True)

        page1 = self.bpm.new_page()  # page_id 1
        data1 = b'data1'
        page1.data[:] = data1.ljust(self.page_size, b'\0')
        self.bpm.unpin_page(1, is_dirty=True)

        # 2. 单独刷新 page1，这会将其写盘并把 is_dirty 设为 False
        self.assertTrue(self.bpm.flush_page(1))

        # 3. 再次修改 page0，确保它是脏的
        page0 = self.bpm.fetch_page(0)
        data0_v2 = b'data0_v2'
        page0.data[:] = data0_v2.ljust(self.page_size, b'\0')
        self.bpm.unpin_page(0, is_dirty=True)

        # 4. 调用 flush_all_pages。这应该只刷新 page0。
        self.bpm.flush_all_pages()

        # 5. 从磁盘重新读取以验证
        # page0 应该包含最新数据
        self.assertEqual(self.disk_manager.read_page(0), data0_v2.ljust(self.page_size, b'\0'))
        # page1 应该包含它被 flush_page 时的旧数据，因为 flush_all 没有再次动它
        self.assertEqual(self.disk_manager.read_page(1), data1.ljust(self.page_size, b'\0'))

    def test_concurrent_fetch_and_unpin(self):
        """测试对同一页面的并发获取和解钉。"""
        page = self.bpm.new_page()
        page_id = page.page_id
        self.assertTrue(self.bpm.unpin_page(page_id, is_dirty=False))

        num_threads = 10
        iterations = 50

        def task():
            for _ in range(iterations):
                p = self.bpm.fetch_page(page_id)
                self.assertIsNotNone(p)
                time.sleep(random.uniform(0, 0.001))  # 模拟工作
                self.assertTrue(self.bpm.unpin_page(page_id, is_dirty=False))

        threads = [threading.Thread(target=task) for _ in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # 所有线程结束后，页面应在缓冲池中且 pin_count 为 0
        final_page = self.bpm.fetch_page(page_id)
        self.assertEqual(final_page.pin_count, 1)
        self.bpm.unpin_page(page_id, False)
        # 检查内部状态（这需要为测试临时访问，不推荐在生产代码中这样做）
        self.assertEqual(self.bpm.pages[self.bpm.page_table[page_id]].pin_count, 0)

    def test_concurrent_new_page(self):
        """测试并发创建新页面，确保所有线程最终都成功。"""
        num_threads = 10

        def task():
            # 每个线程尝试创建一个页面，直到成功为止。
            # 这模拟了缓冲池经常满载的高并发场景。
            p = None
            while p is None:
                p = self.bpm.new_page()
                if p is None:
                    # 如果缓冲池满了并且所有页都被钉住，就短暂等待后重试。
                    time.sleep(0.005)

            # 页面创建成功后，模拟工作，然后解钉，以便其他线程可以继续。
            time.sleep(random.uniform(0, 0.01))
            self.assertTrue(self.bpm.unpin_page(p.page_id, is_dirty=False))

        threads = [threading.Thread(target=task) for _ in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # 在所有线程都成功创建并解钉它们的页面后，
        # 磁盘上的总页面数应与线程数相匹配。
        self.assertEqual(self.disk_manager.get_num_pages(), num_threads)


if __name__ == '__main__':
    unittest.main()

