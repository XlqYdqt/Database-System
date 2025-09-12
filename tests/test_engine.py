# #!/usr/bin/env python3
# # -*- coding: utf-8 -*-
#
# import unittest
# from engine.operators.seq_scan import SeqScanOperator
# from engine.storage_engine import StorageEngine
# from engine.Catelog.catelog import Catalog
#
# class TestSeqScan(unittest.TestCase):
#     """顺序扫描算子测试"""
#     def setUp(self):
#         self.catalog = Catalog()
#         self.storage_engine = StorageEngine(self.catalog)
#
#     def test_empty_table_scan(self):
#         """测试空表扫描"""
#         # 创建一个空表
#         table_name = "empty_test_table"
#
#         # 执行顺序扫描
#         scan_op = SeqScanOperator(table_name)
#         result = scan_op.execute()
#
#         # 验证结果为空列表
#         self.assertEqual(result, [])
#
#     def test_single_row_scan(self):
#         """测试单行数据扫描"""
#         # 准备测试数据
#         table_name = "single_row_test_table"
#         test_row = bytes([1, 2, 3])  # 模拟一行数据的字节序列
#
#         # 插入测试数据
#         self.storage_engine.insert_row(table_name, test_row)
#
#         # 执行顺序扫描
#         scan_op = SeqScanOperator(table_name)
#         result = scan_op.execute()
#
#         # 验证结果
#         self.assertEqual(len(result), 1)
#         self.assertEqual(result[0], test_row)
#
#     def test_multiple_rows_scan(self):
#         """测试多行数据扫描"""
#         # 准备测试数据
#         table_name = "multiple_rows_test_table"
#         test_rows = [
#             bytes([1, 2, 3]),
#             bytes([4, 5, 6]),
#             bytes([7, 8, 9])
#         ]
#
#         # 插入测试数据
#         for row in test_rows:
#             self.storage_engine.insert_row(table_name, row)
#
#         # 执行顺序扫描
#         scan_op = SeqScanOperator(table_name)
#         result = scan_op.execute()
#
#         # 验证结果
#         self.assertEqual(len(result), len(test_rows))
#         for expected, actual in zip(test_rows, result):
#             self.assertEqual(expected, actual)
#
#     def test_table_not_found(self):
#         """测试扫描不存在的表"""
#         table_name = "non_existent_table"
#
#         # 执行顺序扫描
#         scan_op = SeqScanOperator(table_name)
#         result = scan_op.execute()
#
#         # 验证结果为空列表
#         self.assertEqual(result, [])
#
# if __name__ == '__main__':
#     unittest.main()