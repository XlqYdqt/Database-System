#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from typing import List

from sql.lexer import Lexer
from sql.parser import Parser
from sql.semantic import SemanticAnalyzer
from sql.planner import Planner
from engine.executor import Executor
from engine.storage_engine import StorageEngine
from storage.disk_manager import DiskManager
from storage.buffer_pool_manager import BufferPoolManager
from storage.lru_replacer import LRUReplacer
DB_FILE_PATH = "minidb.db"
BUFFER_POOL_SIZE = 100  # 假设缓冲池大小为100个页面


def main():
    """主程序入口，处理SQL输入并执行"""
    # 按照正确的依赖顺序初始化所有底层组件
    disk_manager = DiskManager(DB_FILE_PATH)
    lru_replacer = LRUReplacer(BUFFER_POOL_SIZE)
    buffer_pool_manager = BufferPoolManager(BUFFER_POOL_SIZE, disk_manager, lru_replacer)

    # 将正确初始化的 buffer_pool_manager 注入到 StorageEngine
    storage_engine = StorageEngine(buffer_pool_manager)
    executor = Executor(storage_engine)

    try:
        while True:
            try:
                # 读取SQL输入
                sql = input('miniDB> ')
                if sql.lower() in ('exit', 'quit'):
                    break
                if not sql.strip():
                    continue

                # 1. 词法分析
                lexer = Lexer(sql)
                tokens = lexer.tokenize()

                # 2. 语法分析
                parser = Parser(tokens)
                ast = parser.parse()

                # 3. 语义分析
                # 注意：SemanticAnalyzer 应该直接使用实时的 catalog_page，而不是它的拷贝
                semantic_analyzer = SemanticAnalyzer(storage_engine.catalog_page)
                analyzed_ast = semantic_analyzer.analyze(ast)

                # 4. 逻辑计划生成
                planner = Planner()
                logical_plan = planner.plan(analyzed_ast)

                # 5. 执行计划
                # 确保 logical_plan 始终是一个列表
                if not isinstance(logical_plan, list):
                    logical_plan = [logical_plan]
                result = executor.execute(logical_plan)
                # print(f"Result:{result}")

                if result:
                    first_item = result[0]
                    if isinstance(first_item, dict):
                        # 使用表格格式化器
                        print(format_table(result))
                        # print(f"{len(result)} row(s) in set")
                    else:
                        # UPDATE / DELETE / INSERT 返回影响的行数
                        print(f"Rows affected: {result[0]}")

                    # print("OK")


            except KeyboardInterrupt:
                print("\nExiting...")
                break
            except Exception as e:
                print(f'Error: {e}')

    finally:
        # 【关键】确保在程序退出时，将缓冲池中的所有脏页刷回磁盘
        print("\nShutting down. Flushing all pages to disk...")
        buffer_pool_manager.flush_all_pages()
        disk_manager.close()
        print("Shutdown complete.")

def format_table(rows: List[dict]) -> str:
    """将查询结果格式化成 MySQL 风格的表格"""
    if not rows:
        return "Empty set"

    # 获取列名
    headers = list(rows[0].keys())

    # 计算每列的宽度（取 max(列名宽度, 数据最大宽度)）
    col_widths = {}
    for h in headers:
        max_data_len = max(len(str(row[h])) for row in rows)
        col_widths[h] = max(len(h), max_data_len)

    # 构造表头分隔线
    sep_line = "+" + "+".join("-" * (col_widths[h] + 2) for h in headers) + "+"

    # 构造表头
    header_line = "| " + " | ".join(h.ljust(col_widths[h]) for h in headers) + " |"

    # 构造数据行
    row_lines = []
    for row in rows:
        row_line = "| " + " | ".join(str(row[h]).ljust(col_widths[h]) for h in headers) + " |"
        row_lines.append(row_line)

    # 拼接结果
    result = [sep_line, header_line, sep_line] + row_lines + [sep_line]
    return "\n".join(result)

if __name__ == '__main__':
    print('Welcome to MiniDB! Type "exit" or "quit" to leave.')
    main()
