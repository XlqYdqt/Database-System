from typing import List, Dict, Optional, Union
from .lexer import Token, TokenType, Lexer
from .ast import *

class Parser:
    """SQL 语法分析器 (SQL Syntax Analyzer)"""

    def __init__(self, tokens: List[Token]):
        self.tokens = tokens
        self.pos = 0
        self.current_token = self.tokens[0] if tokens else None

    def parse(self) -> Statement:
        """解析SQL语句 (Parses the SQL statement)"""
        if not self.current_token:
            raise SyntaxError("空语句 (Empty statement)")

        if self.current_token.type == TokenType.KEYWORD:
            keyword = self.current_token.value
            if keyword == 'CREATE':
                # FIX: 重写CREATE分发逻辑以提高清晰度和健壮性
                # FIX: Rewriting CREATE dispatch logic for clarity and robustness.

                # 预读 CREATE 后面的 Token
                second_tok = self.tokens[self.pos + 1] if self.pos + 1 < len(self.tokens) else None
                third_tok = self.tokens[self.pos + 2] if self.pos + 2 < len(self.tokens) else None

                if second_tok:
                    # 检查 CREATE UNIQUE INDEX
                    if second_tok.value == 'UNIQUE' and third_tok and third_tok.value == 'INDEX':
                        return self.parse_create_index()
                    # 检查 CREATE TABLE, CREATE INDEX, etc.
                    elif second_tok.value == 'TABLE':
                        return self.parse_create_table()
                    elif second_tok.value == 'INDEX':
                        return self.parse_create_index()
                    elif second_tok.value == 'VIEW':
                        return self.parse_create_view()
                    elif second_tok.value == 'ROLE':
                        return self.parse_create_role()

            elif keyword == 'DROP':
                if self.next_token_is(TokenType.KEYWORD, 'INDEX'):
                    return self.parse_drop_index()
                # elif self.next_token_is(TokenType.KEYWORD, 'TABLE'):
                #     return self.parse_drop_table()
                # elif self.next_token_is(TokenType.KEYWORD, 'VIEW'):
                #     return self.parse_drop_view()
            elif keyword == 'INSERT':
                return self.parse_insert()
            elif keyword == 'SELECT':
                return self.parse_select()
            elif keyword == 'UPDATE':
                return self.parse_update()
            elif keyword == 'DELETE':
                return self.parse_delete()
            elif keyword == 'GRANT':
                return self.parse_grant()
            elif keyword == 'REVOKE':
                return self.parse_revoke()
            elif keyword == 'LOCK':
                return self.parse_lock()
            elif keyword in {'BEGIN', 'COMMIT', 'ROLLBACK', 'SAVEPOINT', 'SET TRANSACTION'}:
                return self.parse_transaction()
            elif keyword == 'EXPLAIN':
                return self.parse_explain()

        raise SyntaxError(f"不支持的语句类型 (Unsupported statement type): {self.current_token.value}")

    def peek_is_at(self, n: int, token_type: TokenType, token_value: str) -> bool:
        """检查未来第n个Token是否符合预期的类型和值 (Checks the token n positions ahead)"""
        index = self.pos + n
        if index < len(self.tokens):
            token = self.tokens[index]
            return token.type == token_type and token.value == token_value
        return False

    def next_token_is(self, token_type: TokenType, token_value: str) -> bool:
        """检查下一个Token是否为期望的类型和值 (Checks the next Token to see if it's the expected type and value)"""
        return (self.pos < len(self.tokens) - 1 and
                self.tokens[self.pos + 1].type == token_type and
                self.tokens[self.pos + 1].value == token_value)

    def _advance(self, expected_type: Optional[TokenType] = None, expected_value: Optional[str] = None):
        """前进到下一个Token，可选的类型和值检查 (Moves to the next token, with optional type and value checks)"""
        if expected_type and self.current_token.type != expected_type:
            raise SyntaxError(f"期望 {expected_type.value}，但得到 {self.current_token.type.value}")
        if expected_value and self.current_token.value != expected_value:
            raise SyntaxError(f"期望 '{expected_value}'，但得到 '{self.current_token.value}'")
        self.pos += 1
        if self.pos < len(self.tokens):
            self.current_token = self.tokens[self.pos]
        else:
            self.current_token = None

    def _expect(self, token_type: TokenType, token_value: Optional[str] = None) -> Token:
        """期望下一个Token是指定类型和值，并返回它 (Expects the next token to be of a specific type and value, and returns it)"""
        if not self.current_token or self.current_token.type != token_type:
            raise SyntaxError(
                f"[DEBUG] 期望 TokenType.{token_type}, 但得到 {self.current_token.type if self.current_token else 'EOF'} at position {self.pos}")
        if token_value and self.current_token.value != token_value:
            raise SyntaxError(
                f"[DEBUG] 期望 token '{token_value}'，但得到 '{self.current_token.value}' at position {self.pos}")
        token = self.current_token
        self._advance()
        return token

    def parse_create_table(self) -> CreateTableStatement:
        """解析 CREATE TABLE 语句 (Parses a CREATE TABLE statement)"""
        self._expect(TokenType.KEYWORD, 'CREATE')
        self._expect(TokenType.KEYWORD, 'TABLE')

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
        """解析列定义列表 (Parses a list of column definitions)"""
        columns = []

        while self.current_token and self.current_token.type != TokenType.PUNCTUATION and self.current_token.value != ')':
            column_name = self._expect(TokenType.IDENTIFIER).value

            data_type_str = self._expect(TokenType.KEYWORD).value
            try:
                data_type = DataType(data_type_str)
            except ValueError:
                raise SyntaxError(f"不支持的数据类型 (Unsupported data type): {data_type_str}")

            length = None
            precision = None
            scale = None
            if self.current_token and self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == '(':
                self._advance()
                if data_type in (DataType.VARCHAR, DataType.CHAR):
                    length = int(self._expect(TokenType.NUMBER).value)
                elif data_type == DataType.DECIMAL:
                    precision = int(self._expect(TokenType.NUMBER).value)
                    if self.current_token and self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == ',':
                        self._advance()
                        scale = int(self._expect(TokenType.NUMBER).value)
                self._expect(TokenType.PUNCTUATION, ')')

            constraints = []
            default_value = None

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
        """解析 INSERT 语句 (Parses an INSERT statement)"""
        self._expect(TokenType.KEYWORD, 'INSERT')
        self._expect(TokenType.KEYWORD, 'INTO')

        table_name = self._expect(TokenType.IDENTIFIER).value

        columns = []
        if self.current_token and self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == '(':
            self._advance()
            columns = self.parse_column_list()
            self._expect(TokenType.PUNCTUATION, ')')

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

        return InsertStatement(table_name, columns, values=values_list)

    def parse_column_list(self) -> List[str]:
        """解析列名列表 (Parses a list of column names)"""
        columns = []

        while self.current_token and self.current_token.type != TokenType.PUNCTUATION and self.current_token.value != ')':
            column_name = self._expect(TokenType.IDENTIFIER).value
            columns.append(column_name)

            if self.current_token and self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == ',':
                self._advance()

        return columns

    def parse_value_list(self) -> List[Expression]:
        """解析值列表, 例如 (1, 'Alice', 25) (Parses a list of values, e.g., (1, 'Alice', 25))"""
        values = []

        while self.current_token and not (
                self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == ')'):
            expr = self.parse_expression()
            values.append(expr)

            if self.current_token and self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == ',':
                self._advance()
            else:
                if self.current_token and not (
                        self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == ')'):
                    raise SyntaxError(
                        f"期望逗号或右括号, 但得到 {self.current_token} (Expected comma or closing parenthesis, got {self.current_token})")

        return values

    def parse_select(self) -> SelectStatement:
        """解析 SELECT 语句 (Parses a SELECT statement)"""
        self._expect(TokenType.KEYWORD, 'SELECT')

        distinct = False
        if self.current_token and self.current_token.type == TokenType.KEYWORD and self.current_token.value == 'DISTINCT':
            distinct = True
            self._advance()

        columns = self.parse_select_columns()

        self._expect(TokenType.KEYWORD, 'FROM')
        table_name = self._expect(TokenType.IDENTIFIER).value

        joins = []
        # 修改 JOIN 解析逻辑，添加对简单 JOIN 关键字的支持
        while (self.current_token and
               self.current_token.type == TokenType.KEYWORD and
               self.current_token.value in ('INNER', 'LEFT', 'RIGHT', 'FULL', 'CROSS', 'JOIN')):
            # 处理简单的 JOIN 关键字（默认为 INNER JOIN）
            if self.current_token.value == 'JOIN':
                join_type = 'INNER'
                self._advance()
            else:
                join_type = self.current_token.value
                self._advance()
                # 确保消耗 JOIN 关键字
                if self.current_token and self.current_token.type == TokenType.KEYWORD and self.current_token.value == 'JOIN':
                    self._advance()

            join_table = self._expect(TokenType.IDENTIFIER).value

            # 检查是否有 ON 条件
            if self.current_token and self.current_token.type == TokenType.KEYWORD and self.current_token.value == 'ON':
                self._advance()
                join_condition = self.parse_expression()
            else:
                # 对于 CROSS JOIN，可能没有 ON 条件
                join_condition = None

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
        """解析选择列列表 (Parses the list of selected columns)"""
        columns = []

        while self.current_token and self.current_token.type != TokenType.KEYWORD and self.current_token.value != 'FROM':
            if self.current_token.type == TokenType.OPERATOR and self.current_token.value == '*':
                columns.append(Column('*'))
                self._advance()
            else:
                expr = self.parse_expression()
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
        """解析 ORDER BY 子句 (Parses the ORDER BY clause)"""
        order_by_columns = []

        while self.current_token and self.current_token.type != TokenType.KEYWORD and self.current_token.value != ';':
            column = self.parse_column_reference()
            direction = 'ASC'
            if (self.current_token and self.current_token.type == TokenType.KEYWORD and
                    self.current_token.value in ('ASC', 'DESC')):
                direction = self.current_token.value
                self._advance()

            order_by_columns.append(OrderByClause(column, direction))

            if self.current_token and self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == ',':
                self._advance()

        return order_by_columns

    def parse_update(self) -> UpdateStatement:
        """解析 UPDATE 语句 (Parses an UPDATE statement)"""
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
        """解析赋值列表 (Parses a list of assignments)"""
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
        """解析 DELETE 语句 (Parses a DELETE statement)"""
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
        """解析表达式 (Parses an expression)"""
        return self.parse_logical_expression()

    def parse_logical_expression(self) -> Expression:
        """解析逻辑表达式 (AND/OR) (Parses logical expressions (AND/OR))"""
        left = self.parse_comparison_expression()
        while (self.current_token and
               self.current_token.type == TokenType.KEYWORD and
               self.current_token.value in ('AND', 'OR')):
            op = Operator(self.current_token.value)
            self._advance()
            right = self.parse_comparison_expression()
            left = BinaryExpression(left, op, right)
        return left

    def parse_comparison_expression(self) -> Expression:
        """解析比较表达式 (Parses comparison expressions)"""
        left = self.parse_additive_expression()

        # # 添加调试信息
        # if self.current_token:
        #     print(f"[DEBUG] 当前token: {self.current_token.value} (类型: {self.current_token.type})")

        # 检查是否是运算符或关键字（包括 IN）
        if (self.current_token and
                (self.current_token.type == TokenType.OPERATOR or
                 (self.current_token.type == TokenType.KEYWORD and
                  self.current_token.value.upper() in ('LIKE', 'ILIKE', 'IN', 'IS', 'IS NOT', 'BETWEEN')))):

            op = Operator(self.current_token.value.upper())
            self._advance()

            if op.value == 'BETWEEN':
                lower = self.parse_additive_expression()
                self._expect_keyword('AND')
                upper = self.parse_additive_expression()
                return BetweenExpression(left, lower, upper)
            elif op.value == 'IN':
                self._expect(TokenType.PUNCTUATION, '(')
                if self.current_token and self.current_token.type == TokenType.KEYWORD and self.current_token.value.upper() == 'SELECT':
                    subquery = self.parse_select()
                    self._expect(TokenType.PUNCTUATION, ')')
                    return InExpression(left, subquery)
                else:
                    values = self.parse_value_list()
                    self._expect(TokenType.PUNCTUATION, ')')
                    return InExpression(left, values)
            else:
                right = self.parse_additive_expression()
                return BinaryExpression(left, op, right)
        return left

    def _expect_keyword(self, keyword: str) -> Token:
        """期望下一个Token是指定的关键字 (Expects the next token to be a specific keyword)"""
        if not self.current_token or self.current_token.type != TokenType.KEYWORD or self.current_token.value.upper() != keyword.upper():
            raise SyntaxError(f"期望关键字 '{keyword}' (Expected keyword '{keyword}')")
        token = self.current_token
        self._advance()
        return token

    def parse_additive_expression(self) -> Expression:
        """解析加减表达式 (Parses addition/subtraction expressions)"""
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
        """解析乘除表达式 (Parses multiplication/division expressions)"""
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
        """解析基本表达式 (字面量, 列, 函数调用, 子查询) (Parses primary expressions (literals, columns, function calls, subqueries))"""
        if self.current_token.type == TokenType.IDENTIFIER:
            identifier = self.current_token.value
            self._advance()
            if self.current_token and self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == '(':
                self._advance()
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
            self._advance()
            return Column('*')
        elif self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == '(':
            self._advance()
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

        raise SyntaxError(
            f"意外的Token (Unexpected Token): {self.current_token.value if self.current_token else 'EOF'}")

    def parse_case_expression(self) -> CaseExpression:
        """解析 CASE 表达式 (Parses a CASE expression)"""
        self._expect(TokenType.KEYWORD, 'CASE')
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
        """解析列引用 (Parses a column reference)"""
        column_name = self._expect(TokenType.IDENTIFIER).value
        table_name = None
        if self.current_token and self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == '.':
            self._advance()
            table_name = column_name
            column_name = self._expect(TokenType.IDENTIFIER).value
        return Column(column_name, table_name)

    def parse_literal(self) -> Literal:
        """解析字面量 (Parses a literal value)"""
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

        raise SyntaxError(
            f"期望一个字面量, 但得到 {self.current_token.type.value if self.current_token else 'EOF'} (Expected a literal, but got ...)")

    def parse_create_index(self) -> CreateIndexStatement:
        """解析 CREATE INDEX 语句, 包括可选的 UNIQUE (Parses a CREATE INDEX statement, including optional UNIQUE)"""
        self._expect(TokenType.KEYWORD, 'CREATE')

        unique = False
        if self.current_token and self.current_token.type == TokenType.KEYWORD and self.current_token.value == 'UNIQUE':
            unique = True
            self._advance()

        self._expect(TokenType.KEYWORD, 'INDEX')

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
                raise SyntaxError(f"不支持的索引类型 (Unsupported index type): {index_type_str}")

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
        """解析 DROP INDEX 语句 (Parses a DROP INDEX statement)"""
        self._expect(TokenType.KEYWORD, 'DROP')
        concurrently = False
        if (self.current_token and self.current_token.type == TokenType.KEYWORD and
                self.current_token.value == 'CONCURRENTLY'):
            concurrently = True
            self._advance()
        self._expect(TokenType.KEYWORD, 'INDEX')
        if_exists = False
        if (self.current_token and self.current_token.type == TokenType.KEYWORD and
                self.current_token.value == 'IF'):
            self._advance()
            self._expect(TokenType.KEYWORD, 'EXISTS')
            if_exists = True
        index_name = self._expect(TokenType.IDENTIFIER).value
        cascade = False
        restrict = False
        if self.current_token and self.current_token.type == TokenType.KEYWORD:
            if self.current_token.value == 'CASCADE':
                cascade = True
                self._advance()
            elif self.current_token.value == 'RESTRICT':
                restrict = True
                self._advance()
        if self.current_token and self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == ';':
            self._advance()
        return DropIndexStatement(
            index_name=index_name,
            if_exists=if_exists,
            concurrently=concurrently,
            cascade=cascade,
            restrict=restrict
        )

    # def parse_drop_table(self) -> DropTableStatement:
    #     """解析 DROP TABLE 语句 (Parses a DROP TABLE statement)"""
    #     self._expect(TokenType.KEYWORD, 'DROP')
    #     self._expect(TokenType.KEYWORD, 'TABLE')
    #     if_exists = False
    #     if self.current_token and self.current_token.type == TokenType.KEYWORD and self.current_token.value == 'IF':
    #         self._advance()
    #         self._expect(TokenType.KEYWORD, 'EXISTS')
    #         if_exists = True

    #     table_name = self._expect(TokenType.IDENTIFIER).value

    #     if self.current_token and self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == ';':
    #         self._advance()

    #     return DropTableStatement(table_name=table_name, if_exists=if_exists)

    # def parse_drop_view(self) -> DropViewStatement:
    #     """解析 DROP VIEW 语句 (Parses a DROP VIEW statement)"""
    #     self._expect(TokenType.KEYWORD, 'DROP')
    #     self._expect(TokenType.KEYWORD, 'VIEW')
    #     if_exists = False
    #     if self.current_token and self.current_token.type == TokenType.KEYWORD and self.current_token.value == 'IF':
    #         self._advance()
    #         self._expect(TokenType.KEYWORD, 'EXISTS')
    #         if_exists = True

    #     view_name = self._expect(TokenType.IDENTIFIER).value

    #     if self.current_token and self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == ';':
    #         self._advance()

    #     return DropViewStatement(view_name=view_name, if_exists=if_exists)

    def parse_transaction(self) -> TransactionStatement:
        """解析事务控制语句 (Parses transaction control statements)"""
        command_str = self._expect(TokenType.KEYWORD).value
        try:
            command = TransactionCommand(command_str)
        except ValueError:
            raise SyntaxError(f"不支持的事务命令 (Unsupported transaction command): {command_str}")
        isolation_level = None
        if command == TransactionCommand.SET_TRANSACTION:
            if self.current_token and self.current_token.type == TokenType.KEYWORD and self.current_token.value == 'ISOLATION':
                self._advance()
                self._expect(TokenType.KEYWORD, 'LEVEL')
                isolation_level_str = self._expect(TokenType.KEYWORD).value
                try:
                    isolation_level = IsolationLevel(isolation_level_str)
                except ValueError:
                    raise SyntaxError(f"不支持的隔离级别 (Unsupported isolation level): {isolation_level_str}")
        savepoint_name = None
        if command in {TransactionCommand.SAVEPOINT, TransactionCommand.ROLLBACK_TO}:
            savepoint_name = self._expect(TokenType.IDENTIFIER).value
        if self.current_token and self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == ';':
            self._advance()
        return TransactionStatement(command, isolation_level=isolation_level, savepoint_name=savepoint_name)

    def parse_create_role(self) -> CreateRoleStatement:
        """解析 CREATE ROLE 语句 (Parses a CREATE ROLE statement)"""
        self._expect(TokenType.KEYWORD, 'CREATE')
        self._expect(TokenType.KEYWORD, 'ROLE')
        role_name = self._expect(TokenType.IDENTIFIER).value
        if self.current_token and self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == ';':
            self._advance()
        return CreateRoleStatement(role_name)

    def parse_grant(self) -> Union[GrantStatement, GrantRoleStatement]:
        """解析 GRANT 语句 (权限或角色) (Parses a GRANT statement (for privileges or roles))"""
        self._expect(TokenType.KEYWORD, 'GRANT')
        next_token = self.current_token
        if next_token and next_token.type == TokenType.KEYWORD and next_token.value.upper() in [p.value for p in
                                                                                                Privilege]:
            privileges = [Privilege(self._expect(TokenType.KEYWORD).value.upper())]
            while self.current_token and self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == ',':
                self._advance()
                privileges.append(Privilege(self._expect(TokenType.KEYWORD).value.upper()))
            self._expect(TokenType.KEYWORD, 'ON')
            object_type = ObjectType.TABLE
            if self.current_token.type == TokenType.KEYWORD:
                try:
                    object_type_str = self.current_token.value
                    object_type = ObjectType(object_type_str.upper())
                    self._advance()  # Consume object type if valid
                except ValueError:
                    # Not a recognized object type, assume it's part of the object name
                    pass
            object_name = self._expect(TokenType.IDENTIFIER).value
            self._expect(TokenType.KEYWORD, 'TO')
            grantees = [self._expect(TokenType.IDENTIFIER).value]
            while self.current_token and self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == ',':
                self._advance()
                grantees.append(self._expect(TokenType.IDENTIFIER).value)
            with_grant_option = False
            if self.current_token and self.current_token.type == TokenType.KEYWORD and self.current_token.value == 'WITH':
                self._advance()
                self._expect(TokenType.KEYWORD, 'GRANT')
                self._expect(TokenType.KEYWORD, 'OPTION')
                with_grant_option = True
            if self.current_token and self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == ';':
                self._advance()
            return GrantStatement(privileges=privileges, object_type=object_type, object_name=object_name,
                                  grantees=grantees, with_grant_option=with_grant_option)
        else:
            roles = [self._expect(TokenType.IDENTIFIER).value]
            while self.current_token and self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == ',':
                self._advance()
                roles.append(self._expect(TokenType.IDENTIFIER).value)
            self._expect(TokenType.KEYWORD, 'TO')
            grantees = [self._expect(TokenType.IDENTIFIER).value]
            while self.current_token and self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == ',':
                self._advance()
                grantees.append(self._expect(TokenType.IDENTIFIER).value)
            with_admin_option = False
            if self.current_token and self.current_token.type == TokenType.KEYWORD and self.current_token.value == 'WITH':
                self._advance()
                self._expect(TokenType.KEYWORD, 'ADMIN')
                self._expect(TokenType.KEYWORD, 'OPTION')
                with_admin_option = True
            if self.current_token and self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == ';':
                self._advance()
            return GrantRoleStatement(roles=roles, grantees=grantees, with_admin_option=with_admin_option)

    def parse_revoke(self) -> RevokeStatement:
        """解析 REVOKE 语句 (Parses a REVOKE statement)"""
        self._expect(TokenType.KEYWORD, 'REVOKE')
        grant_option = False
        if (self.current_token and self.current_token.type == TokenType.KEYWORD and
                self.current_token.value == 'GRANT' and
                self.next_token_is(TokenType.KEYWORD, 'OPTION')):
            self._advance()
            self._advance()
            grant_option = True
        privileges = [Privilege(self._expect(TokenType.KEYWORD).value.upper())]
        while self.current_token and self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == ',':
            self._advance()
            privileges.append(Privilege(self._expect(TokenType.KEYWORD).value.upper()))
        self._expect(TokenType.KEYWORD, 'ON')
        object_type = ObjectType.TABLE
        if self.current_token.type == TokenType.KEYWORD:
            try:
                object_type_str = self.current_token.value
                object_type = ObjectType(object_type_str.upper())
                self._advance()
            except ValueError:
                pass
        object_name = self._expect(TokenType.IDENTIFIER).value
        self._expect(TokenType.KEYWORD, 'FROM')
        grantees = [self._expect(TokenType.IDENTIFIER).value]
        while self.current_token and self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == ',':
            self._advance()
            grantees.append(self._expect(TokenType.IDENTIFIER).value)
        if self.current_token and self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == ';':
            self._advance()
        return RevokeStatement(privileges=privileges, object_type=object_type, object_name=object_name,
                               grantees=grantees, grant_option=grant_option)

    def parse_lock(self) -> LockStatement:
        """解析 LOCK 语句 (Parses a LOCK statement)"""
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
            raise SyntaxError(f"不支持的锁模式 (Unsupported lock mode): {mode_str}")
        nowait = False
        if self.current_token and self.current_token.type == TokenType.KEYWORD and self.current_token.value == 'NOWAIT':
            self._advance()
            nowait = True
        if self.current_token and self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == ';':
            self._advance()
        return LockStatement(table_names, mode, nowait=nowait)

    def parse_explain(self) -> ExplainStatement:
        """解析 EXPLAIN 语句 (Parses an EXPLAIN statement)"""
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
                    raise SyntaxError(f"不支持的EXPLAIN选项 (Unsupported EXPLAIN option): {option_str}")
                if self.current_token and self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == ',':
                    self._advance()
            self._expect(TokenType.PUNCTUATION, ')')
        format_val = None
        if self.current_token and self.current_token.type == TokenType.KEYWORD and self.current_token.value == 'FORMAT':
            self._advance()
            format_val = self._expect(TokenType.KEYWORD).value

        statement = self.parse()

        if self.current_token and self.current_token.type == TokenType.PUNCTUATION and self.current_token.value == ';':
            self._advance()
        return ExplainStatement(statement, options=options, format=format_val)

    def parse_create_view(self) -> CreateViewStatement:
        """解析 CREATE VIEW 语句 (Parses a CREATE VIEW statement)"""
        self._expect(TokenType.KEYWORD, 'CREATE')
        self._expect(TokenType.KEYWORD, 'VIEW')
        view_name = self._expect(TokenType.IDENTIFIER).value
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
        """检查当前token是否是权限关键字 (Checks if the current token is a privilege keyword)"""
        if not self.current_token or self.current_token.type != TokenType.KEYWORD:
            return False
        try:
            Privilege(self.current_token.value.upper())
            return True
        except ValueError:
            return False

