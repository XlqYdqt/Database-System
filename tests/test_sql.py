#!/usr/bin/env python3
"""
SQL编译器测试脚本
用于测试lexer、parser、semantic、planner模块
"""

import sys
import os
# 获取项目根目录路径（tests目录的父目录）
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# 添加项目根目录到Python路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from sql.lexer import Lexer, TokenType
from sql.parser import Parser
from sql.semantic import SemanticAnalyzer
from sql.planner import Planner


def test_lexer():
    """测试词法分析器"""
    print("=" * 50)
    print("测试词法分析器")
    print("=" * 50)

    # 测试SQL语句
    sql_statements = [
        "CREATE TABLE users (id INT PRIMARY KEY, name STRING, age INT);",
        "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 25);",
        "SELECT name, age FROM users WHERE age > 20;",
        "UPDATE users SET age = 26 WHERE name = 'Alice';",
        "DELETE FROM users WHERE id = 1;",
        "BEGIN; COMMIT; ROLLBACK;",
        "GRANT SELECT ON users TO alice;",
        "CREATE INDEX idx_name ON users(name);",
        "EXPLAIN SELECT * FROM users;"
    ]

    for i, sql in enumerate(sql_statements, 1):
        print(f"\n{i}. SQL: {sql}")

        # 创建词法分析器并分词
        lexer = Lexer(sql)
        tokens = lexer.tokenize()

        # 打印所有token
        for token in tokens:
            print(f"   {token}")


def test_parser():
    """测试语法分析器"""
    print("\n" + "=" * 50)
    print("测试语法分析器")
    print("=" * 50)

    # 测试SQL语句
    sql_statements = [
        "CREATE TABLE users (id INT PRIMARY KEY, name STRING, age INT);",
        "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 25);",
        "SELECT name, age FROM users WHERE age > 20;",
        "UPDATE users SET age = 26 WHERE name = 'Alice';",
        "DELETE FROM users WHERE id = 1;",
        "BEGIN; COMMIT; ROLLBACK;",
        "GRANT SELECT ON users TO alice;",
        "CREATE INDEX idx_name ON users(name);",
        "EXPLAIN SELECT * FROM users;"
    ]

    for i, sql in enumerate(sql_statements, 1):
        print(f"\n{i}. SQL: {sql}")

        # 词法分析
        lexer = Lexer(sql)
        tokens = lexer.tokenize()

        # 语法分析
        parser = Parser(tokens)
        try:
            statement = parser.parse()
            print(f"   AST: {statement}")
        except Exception as e:
            print(f"   解析错误: {e}")


def test_semantic():
    """测试语义分析器"""
    print("\n" + "=" * 50)
    print("测试语义分析器")
    print("=" * 50)

    analyzer = SemanticAnalyzer()

    # 测试SQL语句
    sql_statements = [
        "CREATE TABLE users (id INT PRIMARY KEY, name STRING, age INT);",
        "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 25);",
        "SELECT name, age FROM users WHERE age > 20;",
        "UPDATE users SET age = 26 WHERE name = 'Alice';",
        "DELETE FROM users WHERE id = 1;",
        "GRANT SELECT ON users TO alice;"
    ]

    for i, sql in enumerate(sql_statements, 1):
        print(f"\n{i}. SQL: {sql}")

        lexer = Lexer(sql)
        tokens = lexer.tokenize()

        parser = Parser(tokens)
        try:
            statement = parser.parse()
            analyzed_stmt = analyzer.analyze(statement)
            print(f"   语义分析成功: {analyzed_stmt}")
        except Exception as e:
            print(f"   错误: {e}")


def test_planner():
    """测试逻辑计划生成器"""
    print("\n" + "=" * 50)
    print("测试逻辑计划生成器")
    print("=" * 50)

    analyzer = SemanticAnalyzer()
    planner = Planner()

    sql_statements = [
        "CREATE TABLE users (id INT PRIMARY KEY, name STRING, age INT);",
        "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 25);",
        "SELECT name, age FROM users WHERE age > 20;",
        "UPDATE users SET age = 26 WHERE name = 'Alice';",
        "DELETE FROM users WHERE id = 1;",
        "CREATE INDEX idx_name ON users(name);"
    ]

    for i, sql in enumerate(sql_statements, 1):
        print(f"\n{i}. SQL: {sql}")

        lexer = Lexer(sql)
        tokens = lexer.tokenize()

        parser = Parser(tokens)
        try:
            statement = parser.parse()
            analyzed_stmt = analyzer.analyze(statement)
            plan = planner.plan(analyzed_stmt)
            print(f"   逻辑计划: {plan}")
        except Exception as e:
            print(f"   错误: {e}")


def test_error_cases():
    """测试错误情况"""
    print("\n" + "=" * 50)
    print("测试错误情况")
    print("=" * 50)

    error_sqls = [
        "CREATE TABLE users (id INT PRIMARY KEY, name STRING, age INT)",  # 缺少分号
        "INSERT INTO nonexistent (id, name) VALUES (1, 'Alice');",  # 表不存在
        "UPDATE users SET invalid_column = 1 WHERE id = 1;",  # 列不存在
        "DELETE FROM nonexistent WHERE id = 1;"  # 表不存在
    ]

    analyzer = SemanticAnalyzer()

    # 先创建一个表，以便测试其他错误
    create_sql = "CREATE TABLE users (id INT PRIMARY KEY, name STRING, age INT);"
    lexer = Lexer(create_sql)
    tokens = lexer.tokenize()
    parser = Parser(tokens)
    create_stmt = parser.parse()
    analyzer.analyze(create_stmt)

    for i, sql in enumerate(error_sqls, 1):
        print(f"\n{i}. SQL: {sql}")

        lexer = Lexer(sql)
        tokens = lexer.tokenize()

        parser = Parser(tokens)
        try:
            statement = parser.parse()
            analyzed_stmt = analyzer.analyze(statement)
            print(f"   意外成功: {analyzed_stmt}")
        except Exception as e:
            print(f"   预期错误: {e}")


def test_sql_expansions():
    """测试扩展的SQL语句"""
    print("\n" + "=" * 50)
    print("测试扩展的SQL语句")
    print("=" * 50)

    sql_statements = [
        "BEGIN;",
        "INSERT INTO users (id, name, age) VALUES (2, 'Bob', 30);",
        "COMMIT;",
        "BEGIN; UPDATE users SET age = 31 WHERE name = 'Bob'; ROLLBACK;",
        "CREATE INDEX idx_users_name_age ON users(name, age);",
        "SELECT name, age FROM users WHERE age > 20 AND name LIKE 'A%' ORDER BY age DESC, name ASC;",
        "EXPLAIN SELECT name FROM users WHERE age > 25;",
        "GRANT SELECT, UPDATE ON users TO bob;",
        "REVOKE UPDATE ON users FROM bob;",
        "CREATE ROLE data_analyst;",
        "GRANT SELECT ON users TO data_analyst;",
        "GRANT data_analyst TO alice;",
    ]

    for i, sql in enumerate(sql_statements, 1):
        print(f"\n{i}. SQL: {sql}")

        lexer = Lexer(sql)
        tokens = lexer.tokenize()

        parser = Parser(tokens)
        try:
            statement = parser.parse()
            print(f"   AST: {statement}")

            # 进一步进行语义分析和计划生成为必要步骤
            analyzer = SemanticAnalyzer()
            analyzed_stmt = analyzer.analyze(statement)
            print(f"   语义分析成功: {analyzed_stmt}")

            planner = Planner()
            plan = planner.plan(analyzed_stmt)
            print(f"   逻辑计划: {plan}")

        except Exception as e:
            print(f"   错误: {e}")


if __name__ == "__main__":
    test_lexer()
    test_parser()
    test_semantic()
    test_planner()
    test_error_cases()
    test_sql_expansions()  # 新增的测试调用

    print("\n" + "=" * 50)
    print("所有测试完成")
    print("=" * 50)
