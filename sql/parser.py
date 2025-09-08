"""
SQL语法分析器
使用递归下降法将token序列解析为抽象语法树
"""

from typing import List, Optional
from .lexer import Token, TokenType, Lexer
from .ast import *


class Parser:
    """语法分析器"""

    def __init__(self, tokens: List[Token]):
        self.tokens = tokens
        self.pos = 0
        self.current_token = self.tokens[0] if tokens else None

    def parse(self) -> Statement:
        """解析SQL语句"""
        if not self.current_token:
            raise SyntaxError("空语句")

        # 根据第一个关键字决定解析哪种语句
        if self.current_token.type == TokenType.KEYWORD:
            if self.current_token.value == 'CREATE':
                return self.parse_create_table()
            elif self.current_token.value == 'INSERT':
                return self.parse_insert()
            elif self.current_token.value == 'SELECT':
                return self.parse_select()
            elif self.current_token.value == 'UPDATE':
                return self.parse_update()
            elif self.current_token.value == 'DELETE':
                return self.parse_delete()

        raise SyntaxError(f"不支持的语句类型: {self.current_token.value}")

    def _advance(self, expected_type: Optional[TokenType] = None, expected_value: Optional[str] = None):
        """前进到下一个token，可选的类型和值检查"""
        if expected_type and self.current_token.type != expected_type:
            raise SyntaxError(f"期望{expected_type.value}，但得到{self.current_token.type.value}")

        if expected_value and self.current_token.value != expected_value:
            raise SyntaxError(f"期望'{expected_value}'，但得到'{self.current_token.value}'")

        self.pos += 1
        if self.pos < len(self.tokens):
            self.current_token = self.tokens[self.pos]
        else:
            self.current_token = None

    def _expect(self, token_type: TokenType, token_value: Optional[str] = None) -> Token:
        """期望下一个token是指定类型和值，并返回它"""
        if not self.current_token or self.current_token.type != token_type:
            raise SyntaxError(
                f"期望{token_type.value}，但得到{self.current_token.type if self.current_token else 'EOF'}")

        if token_value and self.current_token.value != token_value:
            raise SyntaxError(f"期望'{token_value}'，但得到'{self.current_token.value}'")

        token = self.current_token
        self._advance()
        return token

    def parse_create_table(self) -> CreateTableStatement:
        """解析CREATE TABLE语句"""
        # 消耗CREATE TABLE关键字
        self._expect(TokenType.KEYWORD, 'CREATE')
        self._expect(TokenType.KEYWORD, 'TABLE')

        # 解析表名
        table_name = self._expect(TokenType.IDENTIFIER).value

        # 解析列定义列表
        self._expect(TokenType.PUNCTUATION, '(')
        columns = self.parse_column_definitions()
        self._expect(TokenType.PUNCTUATION, ')')

        # 可选的分号
        if self.current_token and self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == ';':
            self._advance()

        return CreateTableStatement(table_name, columns)

    def parse_column_definitions(self) -> List[ColumnDefinition]:
        """解析列定义列表"""
        columns = []

        while self.current_token and self.current_token.type != TokenType.PUNCTUATION and self.current_token.value != ')':
            # 解析列名
            column_name = self._expect(TokenType.IDENTIFIER).value

            # 解析数据类型
            data_type_str = self._expect(TokenType.KEYWORD).value
            try:
                data_type = DataType(data_type_str)
            except ValueError:
                raise SyntaxError(f"不支持的数据类型: {data_type_str}")

            # 检查是否有PRIMARY KEY约束
            is_primary = False
            if self.current_token and self.current_token.type == TokenType.KEYWORD and self.current_token.value == 'PRIMARY':
                self._advance()
                self._expect(TokenType.KEYWORD, 'KEY')
                is_primary = True

            columns.append(ColumnDefinition(column_name, data_type, is_primary))

            # 检查是否有逗号（更多列）
            if self.current_token and self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == ',':
                self._advance()

        return columns

    def parse_insert(self) -> InsertStatement:
        """解析INSERT语句"""
        # 消耗INSERT INTO关键字
        self._expect(TokenType.KEYWORD, 'INSERT')
        self._expect(TokenType.KEYWORD, 'INTO')

        # 解析表名
        table_name = self._expect(TokenType.IDENTIFIER).value

        # 解析列名列表（可选）
        columns = []
        if self.current_token and self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == '(':
            self._advance()
            columns = self.parse_column_list()
            self._expect(TokenType.PUNCTUATION, ')')

        # 解析VALUES关键字
        self._expect(TokenType.KEYWORD, 'VALUES')
        self._expect(TokenType.PUNCTUATION, '(')

        # 解析值列表
        values = self.parse_value_list()
        self._expect(TokenType.PUNCTUATION, ')')

        # 可选的分号
        if self.current_token and self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == ';':
            self._advance()

        return InsertStatement(table_name, columns, values)

    def parse_column_list(self) -> List[str]:
        """解析列名列表"""
        columns = []

        while self.current_token and self.current_token.type != TokenType.PUNCTUATION and self.current_token.value != ')':
            column_name = self._expect(TokenType.IDENTIFIER).value
            columns.append(column_name)

            # 检查是否有逗号
            if self.current_token and self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == ',':
                self._advance()

        return columns

    def parse_value_list(self) -> List[Expression]:
        """解析值列表"""
        values = []

        while self.current_token and self.current_token.type != TokenType.PUNCTUATION and self.current_token.value != ')':
            value = self.parse_expression()
            values.append(value)

            # 检查是否有逗号
            if self.current_token and self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == ',':
                self._advance()

        return values

    def parse_select(self) -> SelectStatement:
        """解析SELECT语句"""
        # 消耗SELECT关键字
        self._expect(TokenType.KEYWORD, 'SELECT')

        # 解析选择列列表
        columns = self.parse_select_columns()

        # 解析FROM子句
        self._expect(TokenType.KEYWORD, 'FROM')
        table_name = self._expect(TokenType.IDENTIFIER).value

        # 解析WHERE子句（可选）
        where_clause = None
        if self.current_token and self.current_token.type == TokenType.KEYWORD and self.current_token.value == 'WHERE':
            self._advance()
            where_clause = self.parse_expression()

        # 解析ORDER BY子句（可选）
        order_by = []
        if self.current_token and self.current_token.type == TokenType.KEYWORD and self.current_token.value == 'ORDER':
            self._advance()
            self._expect(TokenType.KEYWORD, 'BY')
            order_by = self.parse_order_by()

        # 可选的分号
        if self.current_token and self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == ';':
            self._advance()

        return SelectStatement(columns, table_name, where_clause, order_by)

    def parse_select_columns(self) -> List[Expression]:
        """解析选择列列表"""
        columns = []

        while self.current_token and self.current_token.type != TokenType.KEYWORD and self.current_token.value != 'FROM':
            # 解析列表达式
            if self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == '*':
                # 通配符*
                columns.append(Column('*'))
                self._advance()
            else:
                columns.append(self.parse_expression())

            # 检查是否有逗号
            if self.current_token and self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == ',':
                self._advance()

        return columns

    def parse_order_by(self) -> List[Column]:
        """解析ORDER BY子句"""
        columns = []

        while self.current_token and self.current_token.type != TokenType.KEYWORD and self.current_token.value != ';':
            column = self.parse_column_reference()
            columns.append(column)

            # 检查是否有逗号
            if self.current_token and self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == ',':
                self._advance()

        return columns

    def parse_update(self) -> UpdateStatement:
        """解析UPDATE语句"""
        # 消耗UPDATE关键字
        self._expect(TokenType.KEYWORD, 'UPDATE')

        # 解析表名
        table_name = self._expect(TokenType.IDENTIFIER).value

        # 解析SET子句
        self._expect(TokenType.KEYWORD, 'SET')
        assignments = self.parse_assignments()

        # 解析WHERE子句（可选）
        where_clause = None
        if self.current_token and self.current_token.type == TokenType.KEYWORD and self.current_token.value == 'WHERE':
            self._advance()
            where_clause = self.parse_expression()

        # 可选的分号
        if self.current_token and self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == ';':
            self._advance()

        return UpdateStatement(table_name, assignments, where_clause)

    def parse_assignments(self) -> Dict[str, Expression]:
        """解析赋值列表"""
        assignments = {}

        while self.current_token and self.current_token.type != TokenType.KEYWORD and self.current_token.value != 'WHERE':
            # 解析列名
            column_name = self._expect(TokenType.IDENTIFIER).value

            # 解析等号
            self._expect(TokenType.OPERATOR, '=')

            # 解析表达式
            expression = self.parse_expression()
            assignments[column_name] = expression

            # 检查是否有逗号
            if self.current_token and self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == ',':
                self._advance()

        return assignments

    def parse_delete(self) -> DeleteStatement:
        """解析DELETE语句"""
        # 消耗DELETE FROM关键字
        self._expect(TokenType.KEYWORD, 'DELETE')
        self._expect(TokenType.KEYWORD, 'FROM')

        # 解析表名
        table_name = self._expect(TokenType.IDENTIFIER).value

        # 解析WHERE子句（可选）
        where_clause = None
        if self.current_token and self.current_token.type == TokenType.KEYWORD and self.current_token.value == 'WHERE':
            self._advance()
            where_clause = self.parse_expression()

        # 可选的分号
        if self.current_token and self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == ';':
            self._advance()

        return DeleteStatement(table_name, where_clause)

    def parse_expression(self) -> Expression:
        """解析表达式"""
        return self.parse_logical_expression()

    def parse_logical_expression(self) -> Expression:
        """解析逻辑表达式（AND/OR）"""
        left = self.parse_comparison_expression()

        while self.current_token and self.current_token.type == TokenType.KEYWORD and self.current_token.value in (
                'AND', 'OR'):
            op = Operator(self.current_token.value)
            self._advance()
            right = self.parse_comparison_expression()
            left = BinaryExpression(left, op, right)

        return left

    def parse_comparison_expression(self) -> Expression:
        """解析比较表达式"""
        left = self.parse_primary_expression()

        if self.current_token and self.current_token.type == TokenType.OPERATOR and self.current_token.value in ('=',
                                                                                                                 '!=',
                                                                                                                 '<',
                                                                                                                 '<=',
                                                                                                                 '>',
                                                                                                                 '>='):
            op = Operator(self.current_token.value)
            self._advance()
            right = self.parse_primary_expression()
            return BinaryExpression(left, op, right)

        return left

    def parse_primary_expression(self) -> Expression:
        """解析基本表达式（列引用或字面量）"""
        if self.current_token.type == TokenType.IDENTIFIER:
            # 列引用
            return self.parse_column_reference()
        elif self.current_token.type in (TokenType.NUMBER, TokenType.STRING):
            # 字面量
            return self.parse_literal()
        elif self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == '(':
            # 括号表达式
            self._advance()
            expression = self.parse_expression()
            self._expect(TokenType.PUNCTUATION, ')')
            return expression
        elif self.current_token.type == TokenType.KEYWORD and self.current_token.value == 'NOT':
            # NOT表达式
            self._advance()
            expression = self.parse_primary_expression()
            return BinaryExpression(Literal(True, DataType.BOOL), Operator.NOT, expression)

        raise SyntaxError(f"意外的token: {self.current_token.value}")

    def parse_column_reference(self) -> Column:
        """解析列引用"""
        column_name = self._expect(TokenType.IDENTIFIER).value

        # 检查是否有表名前缀
        table_name = None
        if self.current_token and self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == '.':
            self._advance()
            table_name = column_name
            column_name = self._expect(TokenType.IDENTIFIER).value

        return Column(column_name, table_name)

    def parse_literal(self) -> Literal:
        """解析字面量"""
        if self.current_token.type == TokenType.NUMBER:
            value = self.current_token.value
            # 判断是整数还是浮点数
            if '.' in value:
                data_type = DataType.FLOAT
                value = float(value)
            else:
                data_type = DataType.INT
                value = int(value)
            self._advance()
            return Literal(value, data_type)
        elif self.current_token.type == TokenType.STRING:
            value = self.current_token.value
            self._advance()
            return Literal(value, DataType.STRING)

        raise SyntaxError(f"期望字面量，但得到{self.current_token.type.value}")