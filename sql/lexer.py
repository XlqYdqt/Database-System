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
    WILDCARD = 'WILDCARD'  # 新增通配符
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
        'BOOL', 'PRIMARY', 'KEY', 'AND', 'OR', 'NOT', 'ORDER', 'BY','IN','ASC','DESC',

        # 事务控制关键字
        'BEGIN', 'COMMIT', 'ROLLBACK', 'SAVEPOINT', 'SET', 'TRANSACTION',

        # 并发控制关键字
        'LOCK', 'UNLOCK', 'SHARE', 'EXCLUSIVE', 'CONCURRENTLY',

        # 索引关键字
        'INDEX', 'USING', 'BTREE', 'HASH', 'DROP','UNIQUE',

        # 查询优化关键字
        'EXPLAIN', 'ANALYZE',

        # 权限管理关键字
        'GRANT', 'REVOKE', 'ROLE', 'USER', 'PRIVILEGES', 'ON', 'TO',

        # 通用关键字
        'IF', 'EXISTS', 'NO', 'WAIT',
        # 聚合与分组
        'GROUP', 'HAVING', 'DISTINCT',

        # JOIN 相关
        'JOIN', 'INNER', 'LEFT', 'RIGHT', 'FULL', 'OUTER', 'ON',

        # NULL 判断
        'IS', 'NULL',

        # 其他查询
        'LIMIT', 'OFFSET', 'AS', 'LIKE', 'BETWEEN',
        'UNION', 'ALL', 'EXCEPT', 'INTERSECT',

        # 布尔字面量
        'TRUE', 'FALSE',

        # CASE 表达式
        'CASE', 'WHEN', 'THEN', 'ELSE', 'END',


    }

    OPERATORS = {
        '!=', '=', '<>', '<', '<=', '>', '>=', '+', '-', '*', '/', '%'
    }

    PUNCTUATIONS = {
        '(', ')', ',', ';', '.', '*'  # '*' 作为通配符处理

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
                self._raise_lexer_error(f"无法识别的字符 '{char}'")

        self.tokens.append(Token(TokenType.EOF, '', self.line, self.column))
        return self.tokens

    def _raise_lexer_error(self, message: str):
        """统一错误处理"""
        raise SyntaxError(f"[Lexer Error] {message} (line {self.line}, column {self.column})")

    def _skip_whitespace(self) -> bool:
        """跳过空白字符"""
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
        """支持 '--' 单行注释和 '/* ... */' 多行注释"""
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
                self._raise_lexer_error("未闭合的多行注释")
            return True

        return False

    def _match_string(self) -> Optional[Token]:
        """匹配字符串字面量"""
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
            self._raise_lexer_error("未闭合的字符串字面量")

        value = self.sql[start_pos:self.pos]
        self.pos += 1
        self.column += 1
        return Token(TokenType.STRING, value, start_line, start_column)

    def _match_number(self) -> Optional[Token]:
        """匹配数字"""
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
        """匹配运算符"""
        if self.pos >= len(self.sql):
            return None
        for op_len in (2, 1):  # 支持多字符运算符，如 `!=`
            candidate = self.sql[self.pos:self.pos + op_len]
            if candidate in self.OPERATORS:
                token = Token(TokenType.OPERATOR, candidate, self.line, self.column)
                self.pos += op_len
                self.column += op_len
                return token
        return None

    def _match_punctuation(self) -> Optional[Token]:
        """匹配标点符号"""
        if self.pos >= len(self.sql) or self.sql[self.pos] not in self.PUNCTUATIONS:
            return None
        char = self.sql[self.pos]
        token_type = TokenType.WILDCARD if char == '*' else TokenType.PUNCTUATION
        token = Token(token_type, char, self.line, self.column)
        self.pos += 1
        self.column += 1
        return token

    def _match_identifier_or_keyword(self) -> Optional[Token]:
        """匹配标识符或关键字"""
        if self.pos >= len(self.sql) or not (self.sql[self.pos].isalpha() or self.sql[self.pos] == '_'):
            return None
        start_line, start_column = self.line, self.column
        start_pos = self.pos
        while self.pos < len(self.sql) and (self.sql[self.pos].isalnum() or self.sql[self.pos] == '_'):
            self.pos += 1
            self.column += 1
        value = self.sql[start_pos:self.pos]
        upper_value = value.upper()
        token_type = TokenType.KEYWORD if upper_value in self.KEYWORDS else TokenType.IDENTIFIER
        return Token(token_type, value, start_line, start_column)
