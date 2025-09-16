from typing import List, Dict, Optional, Union
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
            elif self.current_token.value == 'DROP':
                # 不要消耗 DROP token，让具体的解析方法处理
                if self.next_token_is(TokenType.KEYWORD, 'INDEX'):
                    return self.parse_drop_index()
                elif self.next_token_is(TokenType.KEYWORD, 'TABLE'):
                    return self.parse_drop_table()
                elif self.next_token_is(TokenType.KEYWORD, 'VIEW'):
                    return self.parse_drop_view()
            elif self.current_token.value == 'INSERT':
                return self.parse_insert()
            elif self.current_token.value == 'SELECT':
                return self.parse_select()
            elif self.current_token.value == 'UPDATE':
                return self.parse_update()
            elif self.current_token.value == 'DELETE':
                return self.parse_delete()
            elif self.current_token.value == 'GRANT':
                return self.parse_grant()  # 现在可能返回 GrantStatement 或 GrantRoleStatement
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
            raise SyntaxError(
                f"[DEBUG] 期望 TokenType.{token_type.name}, 但得到 {self.current_token.type if self.current_token else 'EOF'} at position {self.pos}")
        if token_value and self.current_token.value != token_value:
            raise SyntaxError(
                f"[DEBUG] 期望 token '{token_value}'，但得到 '{self.current_token.value}' at position {self.pos}")
        token = self.current_token
        self._advance()
        return token

    def parse_create_table(self) -> CreateTableStatement:
        """解析CREATE TABLE语句"""
        self._expect(TokenType.KEYWORD, 'CREATE')
        self._expect(TokenType.KEYWORD, 'TABLE')

        # 处理 IF NOT EXISTS
        if_not_exists = False
        if (self.current_token and self.current_token.type == TokenType.KEYWORD and
                self.current_token.value == 'IF'):
            self._advance()
            self._expect(TokenType.KEYWORD, 'NOT')
            self._expect(TokenType.KEYWORD, 'EXISTS')
            if_not_exists = True

        table_name = self._expect(TokenType.IDENTIFIER).value

        self._expect(TokenType.PUNCTUATION, '(')
        columns = self.parse_column_definitions()
        self._expect(TokenType.PUNCTUATION, ')')

        if self.current_token and self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == ';':
            self._advance()

        return CreateTableStatement(table_name, columns, if_not_exists=if_not_exists)

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

            # 处理类型参数 (如 VARCHAR(255))
            length = None
            precision = None
            scale = None
            if self.current_token and self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == '(':
                self._advance()
                # 解析类型参数
                if data_type in (DataType.VARCHAR, DataType.CHAR):
                    length = int(self._expect(TokenType.NUMBER).value)
                elif data_type == DataType.DECIMAL:
                    precision = int(self._expect(TokenType.NUMBER).value)
                    if self.current_token and self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == ',':
                        self._advance()
                        scale = int(self._expect(TokenType.NUMBER).value)
                self._expect(TokenType.PUNCTUATION, ')')

            # 初始化constraints
            constraints = []
            default_value = None

            # 解析列约束
            while (self.current_token and
                   self.current_token.type == TokenType.KEYWORD and
                   self.current_token.value in ('PRIMARY', 'NOT', 'UNIQUE', 'DEFAULT', 'CHECK', 'REFERENCES')):

                if self.current_token.value == 'PRIMARY':
                    self._advance()
                    self._expect(TokenType.KEYWORD, 'KEY')
                    constraints.append((ColumnConstraint.PRIMARY_KEY, None))

                elif self.current_token.value == 'NOT':
                    self._advance()
                    self._expect(TokenType.KEYWORD, 'NULL')
                    constraints.append((ColumnConstraint.NOT_NULL, None))

                elif self.current_token.value == 'UNIQUE':
                    self._advance()
                    constraints.append((ColumnConstraint.UNIQUE, None))

                elif self.current_token.value == 'DEFAULT':
                    self._advance()
                    default_value = self.parse_expression()

                elif self.current_token.value == 'CHECK':
                    self._advance()
                    self._expect(TokenType.PUNCTUATION, '(')
                    check_expr = self.parse_expression()
                    self._expect(TokenType.PUNCTUATION, ')')
                    constraints.append((ColumnConstraint.CHECK, check_expr))

                elif self.current_token.value == 'REFERENCES':
                    self._advance()
                    ref_table = self._expect(TokenType.IDENTIFIER).value
                    self._expect(TokenType.PUNCTUATION, '(')
                    ref_column = self._expect(TokenType.IDENTIFIER).value
                    self._expect(TokenType.PUNCTUATION, ')')
                    constraints.append((ColumnConstraint.REFERENCES, (ref_table, ref_column)))

            columns.append(ColumnDefinition(
                column_name, data_type, constraints,
                default_value=default_value,
                length=length, precision=precision, scale=scale
            ))

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

        # 支持 INSERT ... SELECT 语法
        if self.current_token and self.current_token.type == TokenType.KEYWORD and self.current_token.value == 'SELECT':
            select_stmt = self.parse_select()
            return InsertStatement(table_name, columns, select_stmt=select_stmt)

        self._expect(TokenType.KEYWORD, 'VALUES')

        values_list = []
        while self.current_token and self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == '(':
            self._advance()
            values = self.parse_value_list()
            self._expect(TokenType.PUNCTUATION, ')')
            values_list.append(values)

            if self.current_token and self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == ',':
                self._advance()

        if self.current_token and self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == ';':
            self._advance()

        return InsertStatement(table_name, columns, values=values_list)  # 使用 values 而不是 values_list

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
        """解析值列表，如 (1, 'Alice', 25)"""
        values = []

        while self.current_token and not (
                self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == ')'):
            # 解析表达式
            expr = self.parse_expression()
            values.append(expr)

            # 如果有逗号，继续解析下一个值
            if self.current_token and self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == ',':
                self._advance()

            else:
                # 如果没有逗号，但也不是右括号，可能是语法错误
                if self.current_token and not (
                        self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == ')'):
                    raise SyntaxError(f"Expected comma or closing parenthesis, got {self.current_token}")

        return values

    def parse_select(self) -> SelectStatement:
        """解析SELECT语句"""
        self._expect(TokenType.KEYWORD, 'SELECT')

        # 处理 DISTINCT
        distinct = False
        if self.current_token and self.current_token.type == TokenType.KEYWORD and self.current_token.value == 'DISTINCT':
            distinct = True
            self._advance()

        columns = self.parse_select_columns()

        self._expect(TokenType.KEYWORD, 'FROM')
        table_name = self._expect(TokenType.IDENTIFIER).value

        # 处理 JOIN
        joins = []
        while (self.current_token and
               self.current_token.type == TokenType.KEYWORD and
               self.current_token.value in ('INNER', 'LEFT', 'RIGHT', 'FULL', 'CROSS')):
            join_type = self.current_token.value
            self._advance()
            self._expect(TokenType.KEYWORD, 'JOIN')
            join_table = self._expect(TokenType.IDENTIFIER).value
            self._expect(TokenType.KEYWORD, 'ON')
            join_condition = self.parse_expression()
            joins.append(Join(join_type, join_table, join_condition))

        where_clause = None
        if self.current_token and self.current_token.type == TokenType.KEYWORD and self.current_token.value == 'WHERE':
            self._advance()
            where_clause = self.parse_expression()

        group_by = []
        if self.current_token and self.current_token.type == TokenType.KEYWORD and self.current_token.value == 'GROUP':
            self._advance()
            self._expect(TokenType.KEYWORD, 'BY')
            group_by = self.parse_column_list()

        having_clause = None
        if self.current_token and self.current_token.type == TokenType.KEYWORD and self.current_token.value == 'HAVING':
            self._advance()
            having_clause = self.parse_expression()

        order_by = []
        if self.current_token and self.current_token.type == TokenType.KEYWORD and self.current_token.value == 'ORDER':
            self._advance()
            self._expect(TokenType.KEYWORD, 'BY')
            order_by = self.parse_order_by()

        limit = None
        if self.current_token and self.current_token.type == TokenType.KEYWORD and self.current_token.value == 'LIMIT':
            self._advance()
            limit = int(self._expect(TokenType.NUMBER).value)

        offset = None
        if self.current_token and self.current_token.type == TokenType.KEYWORD and self.current_token.value == 'OFFSET':
            self._advance()
            offset = int(self._expect(TokenType.NUMBER).value)

        if self.current_token and self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == ';':
            self._advance()

        return SelectStatement(
            columns, table_name,
            where=where_clause,
            order_by=order_by,
            group_by=group_by,
            having=having_clause,
            limit=limit,
            offset=offset,
            distinct=distinct,
            joins=joins
        )

    def parse_select_columns(self) -> List[Expression]:
        """解析选择列列表"""
        columns = []

        while self.current_token and self.current_token.type != TokenType.KEYWORD and self.current_token.value != 'FROM':
            # 检查通配符 *
            if self.current_token.type == TokenType.OPERATOR and self.current_token.value == '*':
                columns.append(Column('*'))
                self._advance()
            else:
                expr = self.parse_expression()

                # 处理列别名
                alias = None
                if (self.current_token and self.current_token.type == TokenType.KEYWORD and
                        self.current_token.value == 'AS'):
                    self._advance()
                    alias = self._expect(TokenType.IDENTIFIER).value

                if alias:
                    expr = AliasExpression(expr, alias)

                columns.append(expr)

            if self.current_token and self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == ',':
                self._advance()

        return columns

    def parse_order_by(self) -> List[OrderByClause]:
        """解析ORDER BY子句"""
        order_by_columns = []

        while self.current_token and self.current_token.type != TokenType.KEYWORD and self.current_token.value != ';':
            column = self.parse_column_reference()

            # 处理排序方向
            direction = 'ASC'  # 默认升序
            if (self.current_token and self.current_token.type == TokenType.KEYWORD and
                    self.current_token.value in ('ASC', 'DESC')):
                direction = self.current_token.value
                self._advance()

            order_by_columns.append(OrderByClause(column, direction))

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

    # 在 parse_logical_expression 和 parse_comparison_expression 方法中
    # 修改运算符处理代码

    def parse_logical_expression(self) -> Expression:
        """解析逻辑表达式（AND/OR）"""
        left = self.parse_comparison_expression()

        while (self.current_token and
               self.current_token.type == TokenType.KEYWORD and
               self.current_token.value in ('AND', 'OR')):
            # 运算符始终作为 Operator 实例处理
            op = Operator(self.current_token.value)
            self._advance()
            right = self.parse_comparison_expression()
            left = BinaryExpression(left, op, right)

        return left

    def parse_comparison_expression(self) -> Expression:
        """解析比较表达式"""
        left = self.parse_additive_expression()

        # # 添加调试信息
        # if self.current_token:
        #     print(f"[DEBUG] 当前token: {self.current_token.value} (类型: {self.current_token.type})")

        # 检查是否是运算符或关键字（包括 IN）
        if (self.current_token and
                (self.current_token.type == TokenType.OPERATOR or
                 (self.current_token.type == TokenType.KEYWORD and
                  self.current_token.value.upper() in ('LIKE', 'ILIKE', 'IN', 'IS', 'IS NOT', 'BETWEEN')))):

            # 使用 Operator 类初始化
            op = Operator(self.current_token.value.upper())
            print(f"[DEBUG] 识别到运算符: {op.value}")
            self._advance()

            # 处理特殊操作符
            if op.value == 'BETWEEN':
                # BETWEEN x AND y
                lower = self.parse_additive_expression()
                self._expect_keyword('AND')
                upper = self.parse_additive_expression()
                return BetweenExpression(left, lower, upper)

            elif op.value == 'IN':
                print(f"[DEBUG] 开始解析IN表达式")
                # IN (value1, value2, ...) 或 IN (subquery)
                self._expect(TokenType.PUNCTUATION, '(')
                print(f"[DEBUG] 消耗左括号后，当前token: {self.current_token.value if self.current_token else 'EOF'}")

                # 检查是否是子查询
                if self.current_token and self.current_token.type == TokenType.KEYWORD and self.current_token.value.upper() == 'SELECT':
                    print(f"[DEBUG] 识别到子查询")
                    subquery = self.parse_select()
                    self._expect(TokenType.PUNCTUATION, ')')
                    return InExpression(left, subquery)
                else:
                    print(f"[DEBUG] 识别到值列表")
                    # 值列表
                    values = self.parse_value_list()
                    self._expect(TokenType.PUNCTUATION, ')')
                    return InExpression(left, values)

            else:
                # 处理其他运算符
                right = self.parse_additive_expression()
                return BinaryExpression(left, op, right)

        return left
    def _expect_keyword(self, keyword: str) -> Token:
        """期望下一个Token是指定的关键字"""
        if not self.current_token or self.current_token.type != TokenType.KEYWORD or self.current_token.value.upper() != keyword.upper():
            raise SyntaxError(f"期望关键字 '{keyword}'")
        token = self.current_token
        self._advance()
        return token


    def parse_additive_expression(self) -> Expression:
        """解析加减表达式"""
        left = self.parse_multiplicative_expression()

        while (self.current_token and
               self.current_token.type == TokenType.OPERATOR and
               self.current_token.value in ('+', '-')):
            op = self.current_token.value
            self._advance()
            right = self.parse_multiplicative_expression()
            left = BinaryExpression(left, op, right)

        return left

    def parse_multiplicative_expression(self) -> Expression:
        """解析乘除表达式"""
        left = self.parse_primary_expression()

        while (self.current_token and
               self.current_token.type == TokenType.OPERATOR and
               self.current_token.value in ('*', '/', '%')):
            op = self.current_token.value
            self._advance()
            right = self.parse_primary_expression()
            left = BinaryExpression(left, op, right)

        return left

    def parse_primary_expression(self) -> Expression:
        """解析基本表达式（列引用、字面量、函数调用或子查询）"""
        if self.current_token.type == TokenType.IDENTIFIER:
            # 可能是列引用或函数调用
            identifier = self.current_token.value
            self._advance()

            # 检查是否是函数调用
            if self.current_token and self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == '(':
                self._advance()
                # 解析参数
                args = []
                while self.current_token and self.current_token.type != TokenType.PUNCTUATION and self.current_token.value != ')':
                    if self.current_token.type == TokenType.OPERATOR and self.current_token.value == '*':
                        args.append(Column('*'))
                        self._advance()
                    else:
                        args.append(self.parse_expression())

                    if self.current_token and self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == ',':
                        self._advance()
                self._expect(TokenType.PUNCTUATION, ')')
                return FunctionCall(identifier, args)
            else:
                # 列引用
                table_name = None
                if self.current_token and self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == '.':
                    self._advance()
                    table_name = identifier
                    column_name = self._expect(TokenType.IDENTIFIER).value
                    return Column(column_name, table_name)
                else:
                    return Column(identifier)
        elif self.current_token.type in (TokenType.NUMBER, TokenType.STRING):
            return self.parse_literal()
        elif self.current_token.type == TokenType.OPERATOR and self.current_token.value == '*':
            # 处理通配符*
            self._advance()
            return Column('*')
        elif self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == '(':
            self._advance()
            # 检查是否是子查询
            if self.current_token and self.current_token.type == TokenType.KEYWORD and self.current_token.value == 'SELECT':
                subquery = self.parse_select()
                self._expect(TokenType.PUNCTUATION, ')')
                return SubqueryExpression(subquery)
            else:
                expression = self.parse_expression()
                self._expect(TokenType.PUNCTUATION, ')')
                return expression
        elif self.current_token.type == TokenType.KEYWORD and self.current_token.value == 'NOT':
            self._advance()
            expression = self.parse_primary_expression()
            return UnaryExpression(Operator.NOT, expression)
        elif self.current_token.type == TokenType.KEYWORD and self.current_token.value == 'EXISTS':
            self._advance()
            self._expect(TokenType.PUNCTUATION, '(')
            subquery = self.parse_select()
            self._expect(TokenType.PUNCTUATION, ')')
            return ExistsExpression(subquery)
        elif self.current_token.type == TokenType.KEYWORD and self.current_token.value == 'CASE':
            return self.parse_case_expression()

        raise SyntaxError(f"意外的Token: {self.current_token.value if self.current_token else 'EOF'}")

    def parse_case_expression(self) -> CaseExpression:
        """解析CASE表达式"""
        self._expect(TokenType.KEYWORD, 'CASE')

        # 可选的CASE表达式
        case_operand = None
        if not (
                self.current_token and self.current_token.type == TokenType.KEYWORD and self.current_token.value == 'WHEN'):
            case_operand = self.parse_expression()

        when_clauses = []
        while self.current_token and self.current_token.type == TokenType.KEYWORD and self.current_token.value == 'WHEN':
            self._advance()
            condition = self.parse_expression()
            self._expect(TokenType.KEYWORD, 'THEN')
            result = self.parse_expression()
            when_clauses.append((condition, result))

        else_clause = None
        if self.current_token and self.current_token.type == TokenType.KEYWORD and self.current_token.value == 'ELSE':
            self._advance()
            else_clause = self.parse_expression()

        self._expect(TokenType.KEYWORD, 'END')

        return CaseExpression(when_clauses, else_clause, case_operand)

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
        elif self.current_token.type == TokenType.KEYWORD and self.current_token.value == 'NULL':
            self._advance()
            return Literal(None, DataType.NULL)
        elif self.current_token.type == TokenType.KEYWORD and self.current_token.value == 'TRUE':
            self._advance()
            return Literal(True, DataType.BOOL)
        elif self.current_token.type == TokenType.KEYWORD and self.current_token.value == 'FALSE':
            self._advance()
            return Literal(False, DataType.BOOL)

        raise SyntaxError(f"期望字面量，但得到{self.current_token.type.value if self.current_token else 'EOF'}")

    def parse_create_index(self) -> CreateIndexStatement:
        """解析CREATE INDEX语句"""
        self._expect(TokenType.KEYWORD, 'CREATE')

        # 处理 UNIQUE 索引
        unique = False
        if self.current_token and self.current_token.type == TokenType.KEYWORD and self.current_token.value == 'UNIQUE':
            unique = True
            self._advance()

        self._expect(TokenType.KEYWORD, 'INDEX')

        # 处理 IF NOT EXISTS
        if_not_exists = False
        if (self.current_token and self.current_token.type == TokenType.KEYWORD and
                self.current_token.value == 'IF'):
            self._advance()
            self._expect(TokenType.KEYWORD, 'NOT')
            self._expect(TokenType.KEYWORD, 'EXISTS')
            if_not_exists = True

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

        where_clause = None
        if self.current_token and self.current_token.type == TokenType.KEYWORD and self.current_token.value == 'WHERE':
            self._advance()
            where_clause = self.parse_expression()

        if self.current_token and self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == ';':
            self._advance()

        return CreateIndexStatement(
            index_name, table_name, columns,
            index_type=index_type,
            where_clause=where_clause,
            concurrently=concurrently,
            if_not_exists=if_not_exists,
            unique=unique
        )

    def parse_drop_index(self) -> DropIndexStatement:
        """解析DROP INDEX语句"""
        self._expect(TokenType.KEYWORD, 'DROP')

        # 处理 CONCURRENTLY 选项
        concurrently = False
        if (self.current_token and self.current_token.type == TokenType.KEYWORD and
                self.current_token.value == 'CONCURRENTLY'):
            concurrently = True
            self._advance()

        self._expect(TokenType.KEYWORD, 'INDEX')

        # 处理 IF EXISTS 选项
        if_exists = False
        if (self.current_token and self.current_token.type == TokenType.KEYWORD and
                self.current_token.value == 'IF'):
            self._advance()
            self._expect(TokenType.KEYWORD, 'EXISTS')
            if_exists = True

        # 解析索引名称
        index_name = self._expect(TokenType.IDENTIFIER).value

        # 处理 CASCADE 或 RESTRICT 选项
        cascade = False
        restrict = False
        if self.current_token and self.current_token.type == TokenType.KEYWORD:
            if self.current_token.value == 'CASCADE':
                cascade = True
                self._advance()
            elif self.current_token.value == 'RESTRICT':
                restrict = True
                self._advance()

        # 处理分号（如果有）
        if self.current_token and self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == ';':
            self._advance()

        # 创建并返回 DropIndexStatement 对象
        return DropIndexStatement(
            index_name=index_name,
            if_exists=if_exists,
            concurrently=concurrently,
            cascade=cascade,
            restrict=restrict
        )
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

    def parse_create_role(self) -> CreateRoleStatement:
        self._expect(TokenType.KEYWORD, 'CREATE')
        self._expect(TokenType.KEYWORD, 'ROLE')

        role_name = self._expect(TokenType.IDENTIFIER).value

        if self.current_token and self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == ';':
            self._advance()

        return CreateRoleStatement(role_name)

    def parse_grant(self) -> Union[GrantStatement, GrantRoleStatement]:
        """解析GRANT语句（权限授予或角色授予）"""
        self._expect(TokenType.KEYWORD, 'GRANT')

        # 查看下一个token是权限还是角色名
        next_token = self.current_token
        if next_token and next_token.type == TokenType.KEYWORD and next_token.value.upper() in [p.value for p in
                                                                                                Privilege]:
            # 权限授予
            privileges = [Privilege(self._expect(TokenType.KEYWORD).value.upper())]
            while self.current_token and self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == ',':
                self._advance()
                privileges.append(Privilege(self._expect(TokenType.KEYWORD).value.upper()))

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

            return GrantStatement(
                privileges=privileges,
                object_type=object_type,
                object_name=object_name,
                grantees=grantees,
                with_grant_option=with_grant_option
            )
        else:
            # 角色授予
            roles = [self._expect(TokenType.IDENTIFIER).value]
            while self.current_token and self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == ',':
                self._advance()
                roles.append(self._expect(TokenType.IDENTIFIER).value)

            self._expect(TokenType.KEYWORD, 'TO')

            grantees = [self._expect(TokenType.IDENTIFIER).value]
            while self.current_token and self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == ',':
                self._advance()
                grantees.append(self._expect(TokenType.IDENTIFIER).value)

            # 检查是否包含 WITH ADMIN OPTION
            with_admin_option = False
            if self.current_token and self.current_token.type == TokenType.KEYWORD and self.current_token.value == 'WITH':
                self._advance()
                self._expect(TokenType.KEYWORD, 'ADMIN')
                self._expect(TokenType.KEYWORD, 'OPTION')
                with_admin_option = True

            # 解析结束符号
            if self.current_token and self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == ';':
                self._advance()

            return GrantRoleStatement(
                roles=roles,
                grantees=grantees,
                with_admin_option=with_admin_option
            )

    def parse_revoke(self) -> RevokeStatement:
        """解析REVOKE权限撤销语句"""
        self._expect(TokenType.KEYWORD, 'REVOKE')

        # 检查是否包含 GRANT OPTION
        grant_option = False
        if (self.current_token and self.current_token.type == TokenType.KEYWORD and
                self.current_token.value == 'GRANT' and
                self.next_token_is(TokenType.KEYWORD, 'OPTION')):
            self._advance()
            self._advance()
            grant_option = True

        # 解析权限列表
        privileges = [Privilege(self._expect(TokenType.KEYWORD).value.upper())]
        while self.current_token and self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == ',':
            self._advance()
            privileges.append(Privilege(self._expect(TokenType.KEYWORD).value.upper()))

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

        self._expect(TokenType.KEYWORD, 'FROM')

        # 解析 grantees 列表
        grantees = [self._expect(TokenType.IDENTIFIER).value]
        while self.current_token and self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == ',':
            self._advance()
            grantees.append(self._expect(TokenType.IDENTIFIER).value)

        if self.current_token and self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == ';':
            self._advance()

        # 添加 object_type 参数
        return RevokeStatement(
            privileges=privileges,
            object_type=object_type,
            object_name=object_name,
            grantees=grantees,
            grant_option=grant_option
        )

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

    def parse_create_view(self) -> CreateViewStatement:
        """解析CREATE VIEW语句"""
        self._expect(TokenType.KEYWORD, 'CREATE')
        self._expect(TokenType.KEYWORD, 'VIEW')

        view_name = self._expect(TokenType.IDENTIFIER).value

        # 处理列名列表（可选）
        columns = []
        if self.current_token and self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == '(':
            self._advance()
            columns = self.parse_column_list()
            self._expect(TokenType.PUNCTUATION, ')')

        self._expect(TokenType.KEYWORD, 'AS')
        select_stmt = self.parse_select()

        if self.current_token and self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == ';':
            self._advance()

        return CreateViewStatement(view_name, select_stmt, columns=columns)

    def is_privilege_keyword(self) -> bool:
        """检查当前token是否是权限关键字"""
        if not self.current_token or self.current_token.type != TokenType.KEYWORD:
            return False

        try:
            Privilege(self.current_token.value.upper())
            return True
        except ValueError:
            return False