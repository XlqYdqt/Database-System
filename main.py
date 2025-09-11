#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from sql.lexer import Lexer
from sql.parser import Parser
from sql.semantic import SemanticAnalyzer
from sql.planner import Planner
from engine.executor import Executor
from engine.Catelog.catelog import Catalog
from engine.storage_engine import StorageEngine

def main():
    """主程序入口，处理SQL输入并执行"""
    catalog = Catalog()
    storage_engine = StorageEngine(catalog)
    executor = Executor()
    executor.catalog = catalog # Ensure executor uses the same catalog instance
    executor.storage_engine = storage_engine # Ensure executor uses the same storage engine instance

    while True:
        try:
            # 读取SQL输入
            sql = input('miniDB> ')
            if sql.lower() in ('exit', 'quit'):
                break

            # 1. 词法分析
            lexer = Lexer(sql)
            tokens = lexer.tokenize()

            # 2. 语法分析
            parser = Parser(tokens)
            ast = parser.parse()

            # 3. 语义分析
            semantic_analyzer = SemanticAnalyzer()
            # Pass the current catalog's tables and indexes to the semantic analyzer
            semantic_analyzer.tables = catalog.tables
            semantic_analyzer.indexes = catalog.indexes
            semantic_analyzer.roles = catalog.roles
            analyzed_ast = semantic_analyzer.analyze(ast)
            # Update the catalog with any changes from semantic analysis (e.g., new tables/indexes)
            catalog.tables = semantic_analyzer.tables
            catalog.indexes = semantic_analyzer.indexes
            catalog.roles = semantic_analyzer.roles

            # 4. 逻辑计划生成
            planner = Planner(catalog)
            logical_plan = planner.build_plan(analyzed_ast)

            # 5. 执行计划
            result = executor.execute(logical_plan)

            if result is not None:
                for row in result:
                    print(row)
            print("SQL executed successfully.")
            
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f'Error: {e}')

if __name__ == '__main__':
    print('Welcome to MiniDB!')
    main()