"""
语义分析器
检查SQL语句的语义正确性
"""

from typing import Dict, List, Optional
from .ast import *
from .parser import Parser


class SemanticError(Exception):
    """语义错误异常"""
    pass


class SemanticAnalyzer:
    """语义分析器"""

    def __init__(self):
        # 存储表元数据：表名 -> 列定义列表
        self.tables: Dict[str, List[ColumnDefinition]] = {}

    def analyze(self, statement: Statement) -> Statement:
        """分析语句的语义正确性"""
        if isinstance(statement, CreateTableStatement):
            return self.analyze_create_table(statement)
        elif isinstance(statement, InsertStatement):
            return self.analyze_insert(statement)
        elif isinstance(statement, SelectStatement):
            return self.analyze_select(statement)
        elif isinstance(statement, UpdateStatement):
            return self.analyze_update(statement)
        elif isinstance(statement, DeleteStatement):
            return self.analyze_delete(statement)
        else:
            raise SemanticError(f"不支持的语句类型: {type(statement).__name__}")

    def analyze_create_table(self, statement: CreateTableStatement) -> CreateTableStatement:
        """分析CREATE TABLE语句"""
        table_name = statement.table_name

        # 检查表是否已存在
        if table_name in self.tables:
            raise SemanticError(f"表 '{table_name}' 已存在")

        # 检查列定义
        column_names = set()
        primary_key_count = 0

        for column in statement.columns:
            # 检查列名是否重复
            if column.name in column_names:
                raise SemanticError(f"列名 '{column.name}' 重复")
            column_names.add(column.name)

            # 统计主键数量
            if column.is_primary:
                primary_key_count += 1

        # 检查主键数量（目前只支持单个主键）
        if primary_key_count > 1:
            raise SemanticError("目前只支持单个主键")

        # 保存表元数据
        self.tables[table_name] = statement.columns

        return statement

    def analyze_insert(self, statement: InsertStatement) -> InsertStatement:
        """分析INSERT语句"""
        table_name = statement.table_name

        # 检查表是否存在
        if table_name not in self.tables:
            raise SemanticError(f"表 '{table_name}' 不存在")

        table_columns = self.tables[table_name]

        # 如果指定了列列表，检查列是否存在
        if statement.columns:
            for column_name in statement.columns:
                if not any(col.name == column_name for col in table_columns):
                    raise SemanticError(f"列 '{column_name}' 在表 '{table_name}' 中不存在")
        else:
            # 如果没有指定列，使用所有列
            statement.columns = [col.name for col in table_columns]

        # 检查值数量是否匹配列数量
        if len(statement.values) != len(statement.columns):
            raise SemanticError(f"值数量({len(statement.values)})与列数量({len(statement.columns)})不匹配")

        # 检查值类型是否匹配列类型
        for i, (value, column_name) in enumerate(zip(statement.values, statement.columns)):
            # 查找列定义
            column_def = next((col for col in table_columns if col.name == column_name), None)
            if not column_def:
                continue  # 这应该不会发生，因为前面已经检查过了

            # 检查类型匹配
            if not self._check_type_compatibility(value, column_def.data_type):
                raise SemanticError(
                    f"值 '{value}' 的类型与列 '{column_name}' 的类型 {column_def.data_type.value} 不匹配")

        return statement

    def analyze_select(self, statement: SelectStatement) -> SelectStatement:
        """分析SELECT语句"""
        table_name = statement.table_name

        # 检查表是否存在
        if table_name not in self.tables:
            raise SemanticError(f"表 '{table_name}' 不存在")

        table_columns = self.tables[table_name]

        # 检查选择列表中的列
        for column_expr in statement.columns:
            self._check_column_expression(column_expr, table_name, table_columns)

        # 检查WHERE子句中的表达式
        if statement.where:
            self._check_expression(statement.where, table_name, table_columns)

        # 检查ORDER BY子句中的列
        for column in statement.order_by:
            self._check_column_reference(column, table_name, table_columns)

        return statement

    def analyze_update(self, statement: UpdateStatement) -> UpdateStatement:
        """分析UPDATE语句"""
        table_name = statement.table_name

        # 检查表是否存在
        if table_name not in self.tables:
            raise SemanticError(f"表 '{table_name}' 不存在")

        table_columns = self.tables[table_name]

        # 检查赋值语句中的列
        for column_name, expression in statement.assignments.items():
            # 检查列是否存在
            if not any(col.name == column_name for col in table_columns):
                raise SemanticError(f"列 '{column_name}' 在表 '{table_name}' 中不存在")

            # 检查表达式
            self._check_expression(expression, table_name, table_columns)

            # 检查类型兼容性
            column_def = next(col for col in table_columns if col.name == column_name)
            if not self._check_type_compatibility(expression, column_def.data_type):
                raise SemanticError(f"表达式类型与列 '{column_name}' 的类型 {column_def.data_type.value} 不匹配")

        # 检查WHERE子句中的表达式
        if statement.where:
            self._check_expression(statement.where, table_name, table_columns)

        return statement

    def analyze_delete(self, statement: DeleteStatement) -> DeleteStatement:
        """分析DELETE语句"""
        table_name = statement.table_name

        # 检查表是否存在
        if table_name not in self.tables:
            raise SemanticError(f"表 '{table_name}' 不存在")

        table_columns = self.tables[table_name]

        # 检查WHERE子句中的表达式
        if statement.where:
            self._check_expression(statement.where, table_name, table_columns)

        return statement

    def _check_column_expression(self, expr: Expression, table_name: str, table_columns: List[ColumnDefinition]):
        """检查列表达式"""
        if isinstance(expr, Column):
            self._check_column_reference(expr, table_name, table_columns)
        elif isinstance(expr, BinaryExpression):
            self._check_expression(expr, table_name, table_columns)
        # 其他类型的表达式暂时不支持

    def _check_expression(self, expr: Expression, table_name: str, table_columns: List[ColumnDefinition]):
        """检查表达式"""
        if isinstance(expr, Column):
            self._check_column_reference(expr, table_name, table_columns)
        elif isinstance(expr, Literal):
            pass  # 字面量不需要检查
        elif isinstance(expr, BinaryExpression):
            self._check_expression(expr.left, table_name, table_columns)
            self._check_expression(expr.right, table_name, table_columns)

            # 检查运算符的语义（例如，不能比较不兼容的类型）
            if not self._check_operator_compatibility(expr.op, expr.left, expr.right):
                raise SemanticError(f"运算符 '{expr.op.value}' 两边的表达式类型不兼容")

    def _check_column_reference(self, column: Column, table_name: str, table_columns: List[ColumnDefinition]):
        """检查列引用"""
        if column.table and column.table != table_name:
            raise SemanticError(f"表 '{column.table}' 不在FROM子句中")

        if column.name != '*' and not any(col.name == column.name for col in table_columns):
            raise SemanticError(f"列 '{column.name}' 在表 '{table_name}' 中不存在")

    def _check_type_compatibility(self, expr: Expression, expected_type: DataType) -> bool:
        """检查表达式类型是否与期望类型兼容"""
        if isinstance(expr, Literal):
            # 字面量的类型是已知的
            return expr.data_type == expected_type

        elif isinstance(expr, Column):
            # 列的类型需要从表元数据中获取
            # 这里简化处理，假设总是兼容
            return True

        elif isinstance(expr, BinaryExpression):
            # 二元表达式的类型取决于运算符和操作数
            # 这里简化处理，假设总是兼容
            return True

        return False

    def _check_operator_compatibility(self, op: Operator, left: Expression, right: Expression) -> bool:
        """检查运算符两边的表达式是否兼容"""
        # 这里简化处理，只检查一些基本情况
        if op in (Operator.AND, Operator.OR):
            # AND和OR要求两边都是布尔类型
            left_type = self._get_expression_type(left)
            right_type = self._get_expression_type(right)
            return left_type == DataType.BOOL and right_type == DataType.BOOL

        # 比较运算符要求两边类型兼容
        left_type = self._get_expression_type(left)
        right_type = self._get_expression_type(right)

        # 数字类型可以相互比较
        if left_type in (DataType.INT, DataType.FLOAT) and right_type in (DataType.INT, DataType.FLOAT):
            return True

        # 相同类型可以比较
        return left_type == right_type

    def _get_expression_type(self, expr: Expression) -> DataType:
        """获取表达式的类型"""
        if isinstance(expr, Literal):
            return expr.data_type

        elif isinstance(expr, Column):
            # 这里简化处理，返回一个默认类型
            return DataType.INT

        elif isinstance(expr, BinaryExpression):
            # 根据运算符决定类型
            if expr.op in (Operator.AND, Operator.OR, Operator.NOT):
                return DataType.BOOL
            else:
                # 比较运算符返回布尔类型
                if expr.op in (Operator.EQ, Operator.NEQ, Operator.LT, Operator.LTE, Operator.GT, Operator.GTE):
                    return DataType.BOOL
                # 算术运算符返回数字类型
                else:
                    return DataType.INT

        return DataType.INT