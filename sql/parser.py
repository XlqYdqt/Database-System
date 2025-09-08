#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import List, Optional
from .lexer import Token, TokenType
from .ast import *

class Parser:
    """语法分析器，将token流转换为抽象语法树"""
    def __init__(self, tokens: List[Token]):
        self.tokens = tokens    # token列表
        self.current = 0        # 当前token位置
    
    def parse(self) -> Statement:
        """解析SQL语句，生成AST"""
        try:
            # 根据第一个token判断语句类型
            if self.match(TokenType.SELECT):
                return self.select_statement()
            elif self.match(TokenType.CREATE):
                return self.create_statement()
            elif self.match(TokenType.INSERT):
                return self.insert_statement()
            else:
                raise SyntaxError(f"Unexpected token: {self.peek()}")
        except Exception as e:
            raise SyntaxError(f"Parse error: {e}")
    
    def select_statement(self) -> SelectStatement:
        """解析SELECT语句"""
        columns = self.select_list()
        
        if not self.match(TokenType.FROM):
            raise SyntaxError("Expected 'FROM' after SELECT list")
            
        table = self.consume(TokenType.IDENTIFIER, "Expected table name")
        condition = None
        
        if self.match(TokenType.WHERE):
            condition = self.expression()
            
        return SelectStatement(columns, table.lexeme, condition)
    
    def select_list(self) -> List[str]:
        """解析SELECT列表"""
        columns = []
        
        if self.match(TokenType.STAR):
            columns.append("*")
        else:
            columns.append(self.consume(TokenType.IDENTIFIER, "Expected column name").lexeme)
            while self.match(TokenType.COMMA):
                columns.append(self.consume(TokenType.IDENTIFIER, "Expected column name").lexeme)
                
        return columns
    
    def create_statement(self) -> CreateTableStatement:
        """解析CREATE TABLE语句"""
        if not self.match(TokenType.TABLE):
            raise SyntaxError("Expected 'TABLE' after CREATE")
            
        name = self.consume(TokenType.IDENTIFIER, "Expected table name")
        columns = self.column_definitions()
        
        return CreateTableStatement(name.lexeme, columns)
    
    def column_definitions(self) -> List[ColumnDef]:
        """解析列定义"""
        self.consume(TokenType.LPAREN, "Expected '(' after table name")
        columns = []
        
        # 至少需要一列
        columns.append(self.column_definition())
        while self.match(TokenType.COMMA):
            columns.append(self.column_definition())
            
        self.consume(TokenType.RPAREN, "Expected ')'")
        return columns
    
    def column_definition(self) -> ColumnDef:
        """解析单个列定义"""
        name = self.consume(TokenType.IDENTIFIER, "Expected column name")
        type = self.consume(TokenType.IDENTIFIER, "Expected column type")
        return ColumnDef(name.lexeme, type.lexeme)
    
    def insert_statement(self) -> InsertStatement:
        """解析INSERT语句"""
        if not self.match(TokenType.INTO):
            raise SyntaxError("Expected 'INTO' after INSERT")
            
        table = self.consume(TokenType.IDENTIFIER, "Expected table name")
        values = self.value_list()
        
        return InsertStatement(table.lexeme, values)
    
    def value_list(self) -> List[object]:
        """解析值列表"""
        if not self.match(TokenType.VALUES):
            raise SyntaxError("Expected 'VALUES'")
            
        self.consume(TokenType.LPAREN, "Expected '('")
        values = []
        
        values.append(self.value())
        while self.match(TokenType.COMMA):
            values.append(self.value())
            
        self.consume(TokenType.RPAREN, "Expected ')'")
        return values
    
    def value(self) -> object:
        """解析单个值"""
        token = self.advance()
        
        if token.type == TokenType.STRING:
            return token.literal
        elif token.type == TokenType.NUMBER:
            return token.literal
        else:
            raise SyntaxError(f"Expected value, got {token.type}")
    
    def expression(self) -> Expression:
        """解析WHERE表达式"""
        left = self.consume(TokenType.IDENTIFIER, "Expected column name")
        operator = self.consume(TokenType.EQUALS, "Expected '='")
        right = self.value()
        
        return BinaryExpr(left.lexeme, operator.lexeme, right)
    
    # 辅助方法
    def match(self, type: TokenType) -> bool:
        """检查当前token是否匹配指定类型，如果匹配则消费该token"""
        if self.check(type):
            self.advance()
            return True
        return False
    
    def check(self, type: TokenType) -> bool:
        """检查当前token是否为指定类型"""
        if self.is_at_end():
            return False
        return self.peek().type == type
    
    def advance(self) -> Token:
        """获取当前token并移动到下一个"""
        if not self.is_at_end():
            self.current += 1
        return self.previous()
    
    def is_at_end(self) -> bool:
        """检查是否到达token列表末尾"""
        return self.peek().type == TokenType.EOF
    
    def peek(self) -> Token:
        """获取当前token"""
        return self.tokens[self.current]
    
    def previous(self) -> Token:
        """获取前一个token"""
        return self.tokens[self.current - 1]
    
    def consume(self, type: TokenType, message: str) -> Token:
        """消费指定类型的token，如果类型不匹配则抛出异常"""
        if self.check(type):
            return self.advance()
            
        raise SyntaxError(f"{message} at {self.peek()}")