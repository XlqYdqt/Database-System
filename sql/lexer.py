#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from enum import Enum, auto

class TokenType(Enum):
    """Token类型枚举"""
    # 关键字
    SELECT = auto()
    FROM = auto()
    WHERE = auto()
    INSERT = auto()
    INTO = auto()
    VALUES = auto()
    CREATE = auto()
    TABLE = auto()
    
    # 标识符和字面量
    IDENTIFIER = auto()  # 表名、列名等
    STRING = auto()      # 字符串值
    NUMBER = auto()      # 数字值
    
    # 运算符和标点
    COMMA = auto()       # ,
    STAR = auto()        # *
    EQUALS = auto()      # =
    LPAREN = auto()      # (
    RPAREN = auto()      # )
    
    # 其他
    EOF = auto()         # 文件结束
    ERROR = auto()       # 词法错误

class Token:
    """Token类，表示一个词法单元"""
    def __init__(self, type: TokenType, lexeme: str, literal: object = None):
        self.type = type      # token类型
        self.lexeme = lexeme  # 原始文本
        self.literal = literal # 字面量值

class Lexer:
    """词法分析器，将SQL文本转换为token流"""
    def __init__(self, source: str):
        self.source = source  # 源代码
        self.tokens = []      # token列表
        self.start = 0        # 当前词素起始位置
        self.current = 0      # 当前字符位置
        
        # 关键字映射表
        self.keywords = {
            'select': TokenType.SELECT,
            'from': TokenType.FROM,
            'where': TokenType.WHERE,
            'insert': TokenType.INSERT,
            'into': TokenType.INTO,
            'values': TokenType.VALUES,
            'create': TokenType.CREATE,
            'table': TokenType.TABLE
        }
    
    def scan_tokens(self) -> list[Token]:
        """扫描源代码，生成token列表"""
        while not self.is_at_end():
            self.start = self.current
            self.scan_token()
            
        self.tokens.append(Token(TokenType.EOF, ""))
        return self.tokens
    
    def scan_token(self):
        """扫描单个token"""
        c = self.advance()
        
        if c.isspace():
            return
        
        if c.isalpha():
            self.identifier()
        elif c.isdigit():
            self.number()
        else:
            # 处理运算符和标点
            match c:
                case ',':
                    self.add_token(TokenType.COMMA)
                case '*':
                    self.add_token(TokenType.STAR)
                case '=':
                    self.add_token(TokenType.EQUALS)
                case '(':
                    self.add_token(TokenType.LPAREN)
                case ')':
                    self.add_token(TokenType.RPAREN)
                case '"':
                    self.string()
                case _:
                    self.add_token(TokenType.ERROR)
    
    def identifier(self):
        """处理标识符和关键字"""
        while self.peek().isalnum():
            self.advance()
            
        text = self.source[self.start:self.current].lower()
        type = self.keywords.get(text, TokenType.IDENTIFIER)
        self.add_token(type)
    
    def number(self):
        """处理数字字面量"""
        while self.peek().isdigit():
            self.advance()
            
        if self.peek() == '.' and self.peek_next().isdigit():
            self.advance()
            while self.peek().isdigit():
                self.advance()
        
        value = float(self.source[self.start:self.current])
        self.add_token(TokenType.NUMBER, value)
    
    def string(self):
        """处理字符串字面量"""
        while self.peek() != '"' and not self.is_at_end():
            self.advance()
            
        if self.is_at_end():
            self.add_token(TokenType.ERROR)
            return
            
        self.advance()  # 结束引号
        value = self.source[self.start + 1:self.current - 1]
        self.add_token(TokenType.STRING, value)
    
    # 辅助方法
    def is_at_end(self) -> bool:
        return self.current >= len(self.source)
    
    def advance(self) -> str:
        self.current += 1
        return self.source[self.current - 1]
    
    def peek(self) -> str:
        if self.is_at_end():
            return '\0'
        return self.source[self.current]
    
    def peek_next(self) -> str:
        if self.current + 1 >= len(self.source):
            return '\0'
        return self.source[self.current + 1]
    
    def add_token(self, type: TokenType, literal: object = None):
        text = self.source[self.start:self.current]
        self.tokens.append(Token(type, text, literal))