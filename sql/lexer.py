"""
SQL词法分析器
将SQL字符串分解为token序列
"""

import re
from typing import List, Tuple, Optional
from enum import Enum


class TokenType(Enum):
    """Token类型枚举"""
    KEYWORD = 'KEYWORD'  # 关键字
    IDENTIFIER = 'IDENTIFIER'  # 标识符
    NUMBER = 'NUMBER'  # 数字
    STRING = 'STRING'  # 字符串
    OPERATOR = 'OPERATOR'  # 运算符
    PUNCTUATION = 'PUNCTUATION'  # 标点符号
    EOF = 'EOF'  # 结束标记


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
    """词法分析器"""

    # SQL关键字列表
    KEYWORDS = {
        'CREATE', 'TABLE', 'INSERT', 'INTO', 'SELECT', 'FROM', 'WHERE',
        'UPDATE', 'SET', 'DELETE', 'VALUES', 'INT', 'FLOAT', 'STRING',
        'BOOL', 'PRIMARY', 'KEY', 'AND', 'OR', 'NOT', 'ORDER', 'BY'
    }

    # 运算符
    OPERATORS = {
        '=', '!=', '<', '<=', '>', '>=', '+', '-', '*', '/'
    }

    # 标点符号
    PUNCTUATIONS = {
        '(', ')', ',', ';'
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
            # 跳过空白字符
            if self._skip_whitespace():
                continue

            # 处理注释
            if self._skip_comments():
                continue

            # 尝试匹配不同模式的token
            token = self._match_string() or \
                    self._match_number() or \
                    self._match_operator() or \
                    self._match_punctuation() or \
                    self._match_identifier_or_keyword()

            if token:
                self.tokens.append(token)
            else:
                # 无法识别的字符
                char = self.sql[self.pos]
                raise SyntaxError(f"无法识别的字符: '{char}' at line {self.line}, column {self.column}")

        # 添加EOF token
        self.tokens.append(Token(TokenType.EOF, '', self.line, self.column))
        return self.tokens

    def _skip_whitespace(self) -> bool:
        """跳过空白字符，返回是否跳过了任何字符"""
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
        """跳过注释，返回是否跳过了注释"""
        if self.pos >= len(self.sql) or self.sql[self.pos] != '-':
            return False

        # 检查是否为单行注释 '--'
        if self.pos + 1 < len(self.sql) and self.sql[self.pos + 1] == '-':
            # 跳过注释直到行尾
            while self.pos < len(self.sql) and self.sql[self.pos] != '\n':
                self.pos += 1
                self.column += 1
            return True

        return False

    def _match_string(self) -> Optional[Token]:
        """匹配字符串字面量"""
        if self.pos >= len(self.sql) or self.sql[self.pos] != "'":
            return None

        start_pos = self.pos
        start_line = self.line
        start_column = self.column

        # 跳过开头的单引号
        self.pos += 1
        self.column += 1

        # 查找结束的单引号
        while self.pos < len(self.sql) and self.sql[self.pos] != "'":
            if self.sql[self.pos] == '\n':
                self.line += 1
                self.column = 1
            else:
                self.column += 1
            self.pos += 1

        # 检查是否找到结束引号
        if self.pos >= len(self.sql):
            raise SyntaxError("未闭合的字符串字面量")

        # 提取字符串内容
        value = self.sql[start_pos + 1:self.pos]

        # 跳过结束的单引号
        self.pos += 1
        self.column += 1

        return Token(TokenType.STRING, value, start_line, start_column)

    def _match_number(self) -> Optional[Token]:
        """匹配数字字面量"""
        if self.pos >= len(self.sql) or not self.sql[self.pos].isdigit():
            return None

        start_pos = self.pos
        start_line = self.line
        start_column = self.column

        # 匹配整数部分
        while self.pos < len(self.sql) and self.sql[self.pos].isdigit():
            self.pos += 1
            self.column += 1

        # 匹配小数部分
        if self.pos < len(self.sql) and self.sql[self.pos] == '.':
            self.pos += 1
            self.column += 1

            while self.pos < len(self.sql) and self.sql[self.pos].isdigit():
                self.pos += 1
                self.column += 1

        value = self.sql[start_pos:self.pos]
        return Token(TokenType.NUMBER, value, start_line, start_column)

    def _match_operator(self) -> Optional[Token]:
        """匹配运算符"""
        if self.pos >= len(self.sql) or self.sql[self.pos] not in self.OPERATORS:
            return None

        start_pos = self.pos
        start_line = self.line
        start_column = self.column

        # 检查多字符运算符
        if self.pos + 1 < len(self.sql):
            two_char_op = self.sql[self.pos:self.pos + 2]
            if two_char_op in self.OPERATORS:
                self.pos += 2
                self.column += 2
                return Token(TokenType.OPERATOR, two_char_op, start_line, start_column)

        # 单字符运算符
        char = self.sql[self.pos]
        self.pos += 1
        self.column += 1
        return Token(TokenType.OPERATOR, char, start_line, start_column)

    def _match_punctuation(self) -> Optional[Token]:
        """匹配标点符号"""
        if self.pos >= len(self.sql) or self.sql[self.pos] not in self.PUNCTUATIONS:
            return None

        char = self.sql[self.pos]
        token = Token(TokenType.PUNCTUATION, char, self.line, self.column)

        self.pos += 1
        self.column += 1

        return token

    def _match_identifier_or_keyword(self) -> Optional[Token]:
        """匹配标识符或关键字"""
        if self.pos >= len(self.sql) or not (self.sql[self.pos].isalpha() or self.sql[self.pos] == '_'):
            return None

        start_pos = self.pos
        start_line = self.line
        start_column = self.column

        # 读取完整的标识符
        while self.pos < len(self.sql) and (self.sql[self.pos].isalnum() or self.sql[self.pos] == '_'):
            self.pos += 1
            self.column += 1

        value = self.sql[start_pos:self.pos]

        # 检查是否为关键字
        if value.upper() in self.KEYWORDS:
            return Token(TokenType.KEYWORD, value.upper(), start_line, start_column)

        return Token(TokenType.IDENTIFIER, value, start_line, start_column)