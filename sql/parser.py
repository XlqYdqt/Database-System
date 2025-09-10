from typing import List, Optional, Dict
from .lexer import Token, TokenType, Lexer
from .ast import *

class Parser:
    """SQL语法分析器"""

    def __init__(self, tokens: List[Token]):
        self.tokens = tokens
        self.pos = 0
        self.current_token = self.tokens[0] if tokens else None

    def parse(self) -> Statement:
        """解析SQL语句"""
        if not self.current_token:
            raise SyntaxError("空语句")

        if self.current_token.type == TokenType.KEYWORD:
            if self.current_token.value == 'CREATE':
                if self.next_token_is(TokenType.KEYWORD, 'TABLE'):
                    return self.parse_create_table()
                elif self.next_token_is(TokenType.KEYWORD, 'INDEX'):
                    return self.parse_create_index()
                elif self.next_token_is(TokenType.KEYWORD, 'VIEW'):
                    return self.parse_create_view()
                elif self.next_token_is(TokenType.KEYWORD, 'ROLE'):
                    return self.parse_create_role()
            elif self.current_token.value == 'INSERT':
                return self.parse_insert()
            elif self.current_token.value == 'SELECT':
                return self.parse_select()
            elif self.current_token.value == 'UPDATE':
                return self.parse_update()
            elif self.current_token.value == 'DELETE':
                return self.parse_delete()
            elif self.current_token.value == 'GRANT':
                return self.parse_grant()
            elif self.current_token.value == 'REVOKE':
                return self.parse_revoke()
            elif self.current_token.value == 'LOCK':
                return self.parse_lock()
            elif self.current_token.value in {'BEGIN', 'COMMIT', 'ROLLBACK', 'SAVEPOINT', 'SET TRANSACTION'}:
                return self.parse_transaction()
            elif self.current_token.value == 'EXPLAIN':
                return self.parse_explain()

        raise SyntaxError(f"不支持的语句类型: {self.current_token.value}")

    def next_token_is(self, token_type: TokenType, token_value: str) -> bool:
        """检查下一个Token是否为期望的类型和值"""
        return (self.pos < len(self.tokens) - 1 and
                self.tokens[self.pos + 1].type == token_type and
                self.tokens[self.pos + 1].value == token_value)

    def _advance(self, expected_type: Optional[TokenType] = None, expected_value: Optional[str] = None):
        """前进到下一个Token，可选的类型和值检查"""
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
        """期望下一个Token是指定类型和值，并返回它"""
        if not self.current_token or self.current_token.type != token_type:
            raise SyntaxError(f"期望{token_type.value}，但得到{self.current_token.type if self.current_token else 'EOF'}")
        if token_value and self.current_token.value != token_value:
            raise SyntaxError(f"期望'{token_value}'，但得到'{self.current_token.value}'")
        token = self.current_token
        self._advance()
        return token

    def parse_create_table(self) -> CreateTableStatement:
        """解析CREATE TABLE语句"""
        self._expect(TokenType.KEYWORD, 'CREATE')
        self._expect(TokenType.KEYWORD, 'TABLE')

        table_name = self._expect(TokenType.IDENTIFIER).value

        self._expect(TokenType.PUNCTUATION, '(')
        columns = self.parse_column_definitions()
        self._expect(TokenType.PUNCTUATION, ')')

        if self.current_token and self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == ';':
            self._advance()

        return CreateTableStatement(table_name, columns)

    def parse_column_definitions(self) -> List[ColumnDefinition]:
        """解析列定义列表"""
        columns = []

        while self.current_token and self.current_token.type != TokenType.PUNCTUATION and self.current_token.value != ')':
            column_name = self._expect(TokenType.IDENTIFIER).value

            data_type_str = self._expect(TokenType.KEYWORD).value
            try:
                data_type = DataType(data_type_str)
            except ValueError:
                raise SyntaxError(f"不支持的数据类型: {data_type_str}")

            # 初始化constraints
            constraints = []
            if self.current_token and self.current_token.type == TokenType.KEYWORD and self.current_token.value == 'PRIMARY':
                self._advance()
                self._expect(TokenType.KEYWORD, 'KEY')
                constraints.append((ColumnConstraint.PRIMARY_KEY, None))

            columns.append(ColumnDefinition(column_name, data_type, constraints))

            if self.current_token and self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == ',':
                self._advance()

        return columns

    def parse_insert(self) -> InsertStatement:
        """解析INSERT语句"""
        self._expect(TokenType.KEYWORD, 'INSERT')
        self._expect(TokenType.KEYWORD, 'INTO')

        table_name = self._expect(TokenType.IDENTIFIER).value

        columns = []
        if self.current_token and self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == '(':
            self._advance()
            columns = self.parse_column_list()
            self._expect(TokenType.PUNCTUATION, ')')

        self._expect(TokenType.KEYWORD, 'VALUES')
        self._expect(TokenType.PUNCTUATION, '(')

        values = self.parse_value_list()
        self._expect(TokenType.PUNCTUATION, ')')

        if self.current_token and self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == ';':
            self._advance()

        return InsertStatement(table_name, columns, values)

    def parse_column_list(self) -> List[str]:
        """解析列名列表"""
        columns = []

        while self.current_token and self.current_token.type != TokenType.PUNCTUATION and self.current_token.value != ')':
            column_name = self._expect(TokenType.IDENTIFIER).value
            columns.append(column_name)

            if self.current_token and self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == ',':
                self._advance()

        return columns

    def parse_value_list(self) -> List[Expression]:
        """解析值列表"""
        values = []

        while self.current_token and self.current_token.type != TokenType.PUNCTUATION and self.current_token.value != ')':
            value = self.parse_expression()
            values.append(value)

            if self.current_token and self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == ',':
                self._advance()

        return values

    def parse_select(self) -> SelectStatement:
        """解析SELECT语句"""
        self._expect(TokenType.KEYWORD, 'SELECT')

        columns = self.parse_select_columns()

        self._expect(TokenType.KEYWORD, 'FROM')
        table_name = self._expect(TokenType.IDENTIFIER).value

        where_clause = None
        if self.current_token and self.current_token.type == TokenType.KEYWORD and self.current_token.value == 'WHERE':
            self._advance()
            where_clause = self.parse_expression()

        order_by = []
        if self.current_token and self.current_token.type == TokenType.KEYWORD and self.current_token.value == 'ORDER':
            self._advance()
            self._expect(TokenType.KEYWORD, 'BY')
            order_by = self.parse_order_by()

        if self.current_token and self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == ';':
            self._advance()

        return SelectStatement(columns, table_name, where=where_clause, order_by=order_by)

    def parse_select_columns(self) -> List[Expression]:
        """解析选择列列表"""
        columns = []

        while self.current_token and self.current_token.type != TokenType.KEYWORD and self.current_token.value != 'FROM':
            # 检查通配符 *
            if self.current_token.type == TokenType.OPERATOR and self.current_token.value == '*':  # 此处区别点在 `TokenType.OPERATOR` 而不是 `TokenType.WILDCARD`
                columns.append(Column('*'))
                self._advance()
            else:
                columns.append(self.parse_expression())

            if self.current_token and self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == ',':
                self._advance()

        return columns

    def parse_order_by(self) -> List[Column]:
        """解析ORDER BY子句"""
        order_by_columns = []

        while self.current_token and self.current_token.type != TokenType.KEYWORD and self.current_token.value != ';':
            order_by_columns.append(self.parse_column_reference())

            if self.current_token and self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == ',':
                self._advance()

        return order_by_columns

    def parse_update(self) -> UpdateStatement:
        """解析UPDATE语句"""
        self._expect(TokenType.KEYWORD, 'UPDATE')

        table_name = self._expect(TokenType.IDENTIFIER).value

        self._expect(TokenType.KEYWORD, 'SET')
        assignments = self.parse_assignments()

        where_clause = None
        if self.current_token and self.current_token.type == TokenType.KEYWORD and self.current_token.value == 'WHERE':
            self._advance()
            where_clause = self.parse_expression()

        if self.current_token and self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == ';':
            self._advance()

        return UpdateStatement(table_name, assignments, where=where_clause)

    def parse_assignments(self) -> Dict[str, Expression]:
        """解析赋值列表"""
        assignments = {}

        while self.current_token and self.current_token.type != TokenType.KEYWORD and self.current_token.value != 'WHERE':
            column_name = self._expect(TokenType.IDENTIFIER).value

            self._expect(TokenType.OPERATOR, '=')

            expression = self.parse_expression()
            assignments[column_name] = expression

            if self.current_token and self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == ',':
                self._advance()

        return assignments

    def parse_delete(self) -> DeleteStatement:
        """解析DELETE语句"""
        self._expect(TokenType.KEYWORD, 'DELETE')
        self._expect(TokenType.KEYWORD, 'FROM')

        table_name = self._expect(TokenType.IDENTIFIER).value

        where_clause = None
        if self.current_token and self.current_token.type == TokenType.KEYWORD and self.current_token.value == 'WHERE':
            self._advance()
            where_clause = self.parse_expression()

        if self.current_token and self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == ';':
            self._advance()

        return DeleteStatement(table_name, where=where_clause)

    def parse_expression(self) -> Expression:
        """解析表达式"""
        return self.parse_logical_expression()

    def parse_logical_expression(self) -> Expression:
        """解析逻辑表达式（AND/OR）"""
        left = self.parse_comparison_expression()

        while self.current_token and self.current_token.type == TokenType.KEYWORD and self.current_token.value in ('AND', 'OR'):
            op = Operator(self.current_token.value)
            self._advance()
            right = self.parse_comparison_expression()
            left = BinaryExpression(left, op, right)

        return left

    def parse_comparison_expression(self) -> Expression:
        """解析比较表达式"""
        left = self.parse_primary_expression()

        if self.current_token and self.current_token.type == TokenType.OPERATOR and self.current_token.value in ('=', '!=', '<', '<=', '>', '>='):
            op = Operator(self.current_token.value)
            self._advance()
            right = self.parse_primary_expression()
            return BinaryExpression(left, op, right)

        return left

    def parse_primary_expression(self) -> Expression:
        """解析基本表达式（列引用或字面量）"""
        if self.current_token.type == TokenType.IDENTIFIER:
            return self.parse_column_reference()
        elif self.current_token.type in (TokenType.NUMBER, TokenType.STRING):
            return self.parse_literal()
        elif self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == '(':
            self._advance()
            expression = self.parse_expression()
            self._expect(TokenType.PUNCTUATION, ')')
            return expression
        elif self.current_token.type == TokenType.KEYWORD and self.current_token.value == 'NOT':
            self._advance()
            expression = self.parse_primary_expression()
            return UnaryExpression(Operator.NOT, expression)

        raise SyntaxError(f"意外的Token: {self.current_token.value}")

    def parse_column_reference(self) -> Column:
        """解析列引用"""
        column_name = self._expect(TokenType.IDENTIFIER).value

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

    # 新增的解析方法

    def parse_create_index(self) -> CreateIndexStatement:
        """解析CREATE INDEX语句"""
        self._expect(TokenType.KEYWORD, 'CREATE')
        self._expect(TokenType.KEYWORD, 'INDEX')

        index_name = self._expect(TokenType.IDENTIFIER).value
        self._expect(TokenType.KEYWORD, 'ON')

        table_name = self._expect(TokenType.IDENTIFIER).value
        self._expect(TokenType.PUNCTUATION, '(')

        columns = self.parse_column_list()
        self._expect(TokenType.PUNCTUATION, ')')

        index_type = None
        if self.current_token and self.current_token.type == TokenType.KEYWORD and self.current_token.value == 'USING':
            self._advance()
            index_type_str = self._expect(TokenType.KEYWORD).value
            try:
                index_type = IndexType(index_type_str)
            except ValueError:
                raise SyntaxError(f"不支持的索引类型: {index_type_str}")

        concurrently = False
        if self.current_token and self.current_token.type == TokenType.KEYWORD and self.current_token.value == 'CONCURRENTLY':
            concurrently = True
            self._advance()

        if_not_exists = False
        if self.current_token and self.current_token.type == TokenType.KEYWORD and self.current_token.value == 'IF':
            self._advance()
            self._expect(TokenType.KEYWORD, 'NOT')
            self._expect(TokenType.KEYWORD, 'EXISTS')
            if_not_exists = True

        where_clause = None
        if self.current_token and self.current_token.type == TokenType.KEYWORD and self.current_token.value == 'WHERE':
            self._advance()
            where_clause = self.parse_expression()

        if self.current_token and self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == ';':
            self._advance()

        return CreateIndexStatement(index_name, table_name, columns, index_type=index_type, where_clause=where_clause, concurrently=concurrently, if_not_exists=if_not_exists)

    def parse_transaction(self) -> TransactionStatement:
        """解析事务控制语句"""
        command_str = self._expect(TokenType.KEYWORD).value
        try:
            command = TransactionCommand(command_str)
        except ValueError:
            raise SyntaxError(f"不支持的事务命令: {command_str}")

        isolation_level = None
        if command == TransactionCommand.SET_TRANSACTION:
            if self.current_token and self.current_token.type == TokenType.KEYWORD and self.current_token.value == 'ISOLATION':
                self._advance()
                self._expect(TokenType.KEYWORD, 'LEVEL')
                isolation_level_str = self._expect(TokenType.KEYWORD).value
                try:
                    isolation_level = IsolationLevel(isolation_level_str)
                except ValueError:
                    raise SyntaxError(f"不支持的隔离级别: {isolation_level_str}")

        savepoint_name = None
        if command in {TransactionCommand.SAVEPOINT, TransactionCommand.ROLLBACK_TO}:
            savepoint_name = self._expect(TokenType.IDENTIFIER).value

        if self.current_token and self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == ';':
            self._advance()

        return TransactionStatement(command, isolation_level=isolation_level, savepoint_name=savepoint_name)

    def parse_grant(self) -> GrantStatement:
        """解析GRANT权限授予语句"""
        self._expect(TokenType.KEYWORD, 'GRANT')

        # 解析权限列表
        privileges = [Privilege(self._expect(TokenType.KEYWORD).value)]
        while self.current_token and self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == ',':
            self._advance()
            privileges.append(Privilege(self._expect(TokenType.KEYWORD).value))

        self._expect(TokenType.KEYWORD, 'ON')

        # 尝试解析 object_type，如果未显式提供，则默认为 TABLE
        object_type = None
        if self.current_token.type == TokenType.KEYWORD:
            object_type_str = self._expect(TokenType.KEYWORD).value
            try:
                object_type = ObjectType(object_type_str.upper())
            except ValueError:
                raise SyntaxError(f"不支持的对象类型: {object_type_str}")
        else:
            object_type = ObjectType.TABLE  # 默认为 TABLE

        # 解析对象名称
        object_name = self._expect(TokenType.IDENTIFIER).value

        self._expect(TokenType.KEYWORD, 'TO')

        # 解析 grantees 列表
        grantees = [self._expect(TokenType.IDENTIFIER).value]
        while self.current_token and self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == ',':
            self._advance()
            grantees.append(self._expect(TokenType.IDENTIFIER).value)

        # 检查是否包含 WITH GRANT OPTION
        with_grant_option = False
        if self.current_token and self.current_token.type == TokenType.KEYWORD and self.current_token.value == 'WITH':
            self._advance()
            self._expect(TokenType.KEYWORD, 'GRANT')
            self._expect(TokenType.KEYWORD, 'OPTION')
            with_grant_option = True

        # 解析结束符号
        if self.current_token and self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == ';':
            self._advance()

        # 传递所有参数创建 GrantStatement
        return GrantStatement(privileges=privileges, object_type=object_type, object_name=object_name,
                              grantees=grantees, with_grant_option=with_grant_option)

    def parse_lock(self) -> LockStatement:
        """解析LOCK语句"""
        self._expect(TokenType.KEYWORD, 'LOCK')
        self._expect(TokenType.KEYWORD, 'TABLE')

        table_names = [self._expect(TokenType.IDENTIFIER).value]
        while self.current_token and self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == ',':
            self._advance()
            table_names.append(self._expect(TokenType.IDENTIFIER).value)

        self._expect(TokenType.KEYWORD, 'IN')
        mode_str = self._expect(TokenType.KEYWORD).value
        try:
            mode = LockMode(mode_str)
        except ValueError:
            raise SyntaxError(f"不支持的锁模式: {mode_str}")

        nowait = False
        if self.current_token and self.current_token.type == TokenType.KEYWORD and self.current_token.value == 'NOWAIT':
            self._advance()
            nowait = True

        if self.current_token and self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == ';':
            self._advance()

        return LockStatement(table_names, mode, nowait=nowait)

    def parse_explain(self) -> ExplainStatement:
        """解析EXPLAIN语句"""
        self._expect(TokenType.KEYWORD, 'EXPLAIN')

        options = set()
        if self.current_token and self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == '(':
            self._advance()
            while self.current_token and self.current_token.type == TokenType.KEYWORD:
                option_str = self._expect(TokenType.KEYWORD).value
                try:
                    option = ExplainOption(option_str)
                    options.add(option)
                except ValueError:
                    raise SyntaxError(f"不支持的EXPLAIN选项: {option_str}")

                if self.current_token and self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == ',':
                    self._advance()
            self._expect(TokenType.PUNCTUATION, ')')

        format = None
        if self.current_token and self.current_token.type == TokenType.KEYWORD and self.current_token.value == 'FORMAT':
            self._advance()
            format = self._expect(TokenType.KEYWORD).value

        statement = self.parse()

        if self.current_token and self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == ';':
            self._advance()

        return ExplainStatement(statement, options=options, format=format)
