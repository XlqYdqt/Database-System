"""
增强版 SQL 词法分析器
支持事务、并发控制、索引、优化、权限管理等关键字
"""

import re
from typing import List, Optional
from enum import Enum


class TokenType(Enum):
    """Token类型枚举"""
    KEYWORD = 'KEYWORD'
    IDENTIFIER = 'IDENTIFIER'
    NUMBER = 'NUMBER'
    STRING = 'STRING'
    OPERATOR = 'OPERATOR'
    PUNCTUATION = 'PUNCTUATION'
    EOF = 'EOF'


class Token:
    """Token类"""
    def __init__(self, token_type: TokenType, value: str, line: int, column: int):
        self.type = token_type
        self.value = value
        self.line = line
        self.column = column

    def __repr__(self):
        return f"Token({self.type.value}, '{self.value}', line={self.line}, column={self.column})"


class Lexer:
    """增强版词法分析器"""

    KEYWORDS = {
        # DML / DDL
        'CREATE', 'TABLE', 'INSERT', 'INTO', 'SELECT', 'FROM', 'WHERE',
        'UPDATE', 'SET', 'DELETE', 'VALUES', 'INT', 'FLOAT', 'STRING',
        'BOOL', 'PRIMARY', 'KEY', 'AND', 'OR', 'NOT', 'ORDER', 'BY',

        # 事务控制
        'BEGIN', 'COMMIT', 'ROLLBACK', 'SAVEPOINT', 'SET', 'TRANSACTION',

        # 并发控制
        'LOCK', 'UNLOCK', 'SHARE', 'EXCLUSIVE',

        # 索引管理
        'INDEX', 'UNIQUE', 'FULLTEXT', 'BTREE', 'HASH',

        # 查询优化
        'EXPLAIN', 'ANALYZE', 'PLAN',

        # 访问控制
        'GRANT', 'REVOKE', 'ROLE', 'USER', 'PRIVILEGES'
    }

    OPERATORS = {
        '=', '!=', '<', '<=', '>', '>=', '+', '-', '*', '/', '%'
    }

    PUNCTUATIONS = {
        '(', ')', ',', ';', '.'
    }

    def __init__(self, sql: str):
        self.sql = sql
        self.pos = 0
        self.line = 1
        self.column = 1
        self.tokens: List[Token] = []

    def tokenize(self) -> List[Token]:
        """将SQL字符串转换为token序列"""
        while self.pos < len(self.sql):
            if self._skip_whitespace():
                continue

            if self._skip_comments():
                continue

            token = (self._match_string()
                     or self._match_number()
                     or self._match_operator()
                     or self._match_punctuation()
                     or self._match_identifier_or_keyword())

            if token:
                self.tokens.append(token)
            else:
                char = self.sql[self.pos]
                raise SyntaxError(f"[Lexer Error] 无法识别的字符 '{char}' (line {self.line}, column {self.column})")

        self.tokens.append(Token(TokenType.EOF, '', self.line, self.column))
        return self.tokens

    def _skip_whitespace(self) -> bool:
        if self.pos >= len(self.sql):
            return False

        if self.sql[self.pos].isspace():
            if self.sql[self.pos] == '\n':
                self.line += 1
                self.column = 1
            else:
                self.column += 1
            self.pos += 1
            return True
        return False

    def _skip_comments(self) -> bool:
        """支持 '--' 单行注释 和 '/* ... */' 多行注释"""
        if self.pos >= len(self.sql):
            return False

        # 单行注释
        if self.sql[self.pos:self.pos + 2] == '--':
            while self.pos < len(self.sql) and self.sql[self.pos] != '\n':
                self.pos += 1
                self.column += 1
            return True

        # 多行注释
        if self.sql[self.pos:self.pos + 2] == '/*':
            self.pos += 2
            self.column += 2
            while self.pos < len(self.sql) - 1 and self.sql[self.pos:self.pos + 2] != '*/':
                if self.sql[self.pos] == '\n':
                    self.line += 1
                    self.column = 1
                else:
                    self.column += 1
                self.pos += 1
            if self.sql[self.pos:self.pos + 2] == '*/':
                self.pos += 2
                self.column += 2
            else:
                raise SyntaxError("未闭合的多行注释")
            return True

        return False

    def _match_string(self) -> Optional[Token]:
        if self.pos >= len(self.sql) or self.sql[self.pos] != "'":
            return None
        start_line, start_column = self.line, self.column
        self.pos += 1
        self.column += 1
        start_pos = self.pos

        while self.pos < len(self.sql) and self.sql[self.pos] != "'":
            if self.sql[self.pos] == '\n':
                self.line += 1
                self.column = 1
            else:
                self.column += 1
            self.pos += 1

        if self.pos >= len(self.sql):
            raise SyntaxError("未闭合的字符串字面量")

        value = self.sql[start_pos:self.pos]
        self.pos += 1
        self.column += 1
        return Token(TokenType.STRING, value, start_line, start_column)

    def _match_number(self) -> Optional[Token]:
        if self.pos >= len(self.sql) or not self.sql[self.pos].isdigit():
            return None
        start_line, start_column = self.line, self.column
        start_pos = self.pos
        while self.pos < len(self.sql) and self.sql[self.pos].isdigit():
            self.pos += 1
            self.column += 1
        if self.pos < len(self.sql) and self.sql[self.pos] == '.':
            self.pos += 1
            self.column += 1
            while self.pos < len(self.sql) and self.sql[self.pos].isdigit():
                self.pos += 1
                self.column += 1
        value = self.sql[start_pos:self.pos]
        return Token(TokenType.NUMBER, value, start_line, start_column)

    def _match_operator(self) -> Optional[Token]:
        if self.pos >= len(self.sql):
            return None
        # 支持多字符运算符
        for op_len in (2, 1):
            candidate = self.sql[self.pos:self.pos + op_len]
            if candidate in self.OPERATORS:
                token = Token(TokenType.OPERATOR, candidate, self.line, self.column)
                self.pos += op_len
                self.column += op_len
                return token
        return None

    def _match_punctuation(self) -> Optional[Token]:
        if self.pos >= len(self.sql) or self.sql[self.pos] not in self.PUNCTUATIONS:
            return None
        char = self.sql[self.pos]
        token = Token(TokenType.PUNCTUATION, char, self.line, self.column)
        self.pos += 1
        self.column += 1
        return token

    def _match_identifier_or_keyword(self) -> Optional[Token]:
        if self.pos >= len(self.sql) or not (self.sql[self.pos].isalpha() or self.sql[self.pos] == '_'):
            return None
        start_line, start_column = self.line, self.column
        start_pos = self.pos
        while self.pos < len(self.sql) and (self.sql[self.pos].isalnum() or self.sql[self.pos] == '_'):
            self.pos += 1
            self.column += 1
        value = self.sql[start_pos:self.pos]
        upper_value = value.upper()
        if upper_value in self.KEYWORDS:
            return Token(TokenType.KEYWORD, upper_value, start_line, start_column)
        return Token(TokenType.IDENTIFIER, value, start_line, start_column)
