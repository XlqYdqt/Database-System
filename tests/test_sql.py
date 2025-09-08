#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import unittest
from ..sql.lexer import Lexer, TokenType, Token
from ..sql.parser import Parser
from ..sql.ast import *
from ..sql.semantic import SemanticAnalyzer

class TestLexer(unittest.TestCase):
    """词法分析器测试"""
    def test_select(self):
        lexer = Lexer("SELECT id, name FROM users WHERE age = 18")
        tokens = lexer.scan_tokens()
        
        # 验证token类型
        self.assertEqual(tokens[0].type, TokenType.SELECT)
        self.assertEqual(tokens[1].type, TokenType.IDENTIFIER)
        self.assertEqual(tokens[2].type, TokenType.COMMA)
        self.assertEqual(tokens[3].type, TokenType.IDENTIFIER)
        self.assertEqual(tokens[4].type, TokenType.FROM)
        self.assertEqual(tokens[5].type, TokenType.IDENTIFIER)
        self.assertEqual(tokens[6].type, TokenType.WHERE)
        self.assertEqual(tokens[7].type, TokenType.IDENTIFIER)
        self.assertEqual(tokens[8].type, TokenType.EQUALS)
        self.assertEqual(tokens[9].type, TokenType.NUMBER)
    
    def test_create_table(self):
        lexer = Lexer("CREATE TABLE users (id int, name string)")
        tokens = lexer.scan_tokens()
        
        self.assertEqual(tokens[0].type, TokenType.CREATE)
        self.assertEqual(tokens[1].type, TokenType.TABLE)
        self.assertEqual(tokens[2].type, TokenType.IDENTIFIER)
        self.assertEqual(tokens[3].type, TokenType.LPAREN)
        self.assertEqual(tokens[4].type, TokenType.IDENTIFIER)
        self.assertEqual(tokens[5].type, TokenType.IDENTIFIER)
        self.assertEqual(tokens[6].type, TokenType.COMMA)
        self.assertEqual(tokens[7].type, TokenType.IDENTIFIER)
        self.assertEqual(tokens[8].type, TokenType.IDENTIFIER)
        self.assertEqual(tokens[9].type, TokenType.RPAREN)

class TestParser(unittest.TestCase):
    """语法分析器测试"""
    def test_select(self):
        lexer = Lexer("SELECT id, name FROM users WHERE age = 18")
        parser = Parser(lexer.scan_tokens())
        stmt = parser.parse()
        
        self.assertIsInstance(stmt, SelectStatement)
        self.assertEqual(stmt.columns, ['id', 'name'])
        self.assertEqual(stmt.table_name, 'users')
        self.assertIsInstance(stmt.where, BinaryExpr)
        self.assertEqual(stmt.where.left, 'age')
        self.assertEqual(stmt.where.operator, '=')
        self.assertEqual(stmt.where.right, 18)
    
    def test_create_table(self):
        lexer = Lexer("CREATE TABLE users (id int, name string)")
        parser = Parser(lexer.scan_tokens())
        stmt = parser.parse()
        
        self.assertIsInstance(stmt, CreateTableStatement)
        self.assertEqual(stmt.table_name, 'users')
        self.assertEqual(len(stmt.columns), 2)
        self.assertEqual(stmt.columns[0].name, 'id')
        self.assertEqual(stmt.columns[0].type, 'int')
        self.assertEqual(stmt.columns[1].name, 'name')
        self.assertEqual(stmt.columns[1].type, 'string')

class TestSemanticAnalyzer(unittest.TestCase):
    """语义分析器测试"""
    def setUp(self):
        self.analyzer = SemanticAnalyzer()
        
        # 创建测试表
        lexer = Lexer("CREATE TABLE users (id int, name string, age int)")
        parser = Parser(lexer.scan_tokens())
        stmt = parser.parse()
        self.analyzer.analyze(stmt)
    
    def test_select_nonexistent_table(self):
        lexer = Lexer("SELECT id FROM nonexistent")
        parser = Parser(lexer.scan_tokens())
        stmt = parser.parse()
        
        with self.assertRaises(ValueError):
            self.analyzer.analyze(stmt)
    
    def test_select_nonexistent_column(self):
        lexer = Lexer("SELECT nonexistent FROM users")
        parser = Parser(lexer.scan_tokens())
        stmt = parser.parse()
        
        with self.assertRaises(ValueError):
            self.analyzer.analyze(stmt)
    
    def test_create_duplicate_table(self):
        lexer = Lexer("CREATE TABLE users (id int)")
        parser = Parser(lexer.scan_tokens())
        stmt = parser.parse()
        
        with self.assertRaises(ValueError):
            self.analyzer.analyze(stmt)

if __name__ == '__main__':
    unittest.main()