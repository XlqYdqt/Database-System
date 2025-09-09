"""
抽象语法树（AST）节点定义
用于表示SQL语句的结构化信息
"""

from enum import Enum
from typing import List, Optional, Union, Dict, Any


class DataType(Enum):
    """支持的SQL数据类型"""
    INT = 'INT'
    FLOAT = 'FLOAT'
    STRING = 'STRING'
    BOOL = 'BOOL'


class Operator(Enum):
    """支持的运算符"""
    EQ = '='  # 等于
    NEQ = '!='  # 不等于
    LT = '<'  # 小于
    LTE = '<='  # 小于等于
    GT = '>'  # 大于
    GTE = '>='  # 大于等于
    AND = 'AND'  # 与
    OR = 'OR'  # 或
    NOT = 'NOT'  # 非


class ColumnDefinition:
    """列定义"""

    def __init__(self, name: str, data_type: DataType, is_primary: bool = False):
        self.name = name
        self.data_type = data_type
        self.is_primary = is_primary

    def __repr__(self):
        return f"ColumnDefinition(name={self.name}, type={self.data_type.value}, primary={self.is_primary})"


class Expression:
    """表达式基类"""
    pass


class Column(Expression):
    """列引用表达式"""

    def __init__(self, name: str, table: Optional[str] = None):
        self.name = name
        self.table = table  # 表名前缀（可选）

    def __repr__(self):
        if self.table:
            return f"Column(table={self.table}, name={self.name})"
        return f"Column(name={self.name})"


class Literal(Expression):
    """字面量表达式"""

    def __init__(self, value: Any, data_type: DataType):
        self.value = value
        self.data_type = data_type

    def __repr__(self):
        return f"Literal(value={self.value}, type={self.data_type.value})"


class BinaryExpression(Expression):
    """二元表达式（如比较运算）"""

    def __init__(self, left: Expression, op: Operator, right: Expression):
        self.left = left
        self.op = op
        self.right = right

    def __repr__(self):
        return f"BinaryExpression(left={self.left}, op={self.op.value}, right={self.right})"


class Statement:
    """SQL语句基类"""
    pass


class CreateTableStatement(Statement):
    """CREATE TABLE语句"""

    def __init__(self, table_name: str, columns: List[ColumnDefinition]):
        self.table_name = table_name
        self.columns = columns

    def __repr__(self):
        return f"CreateTableStatement(table={self.table_name}, columns={self.columns})"


class InsertStatement(Statement):
    """INSERT语句"""

    def __init__(self, table_name: str, columns: List[str], values: List[Expression]):
        self.table_name = table_name
        self.columns = columns
        self.values = values

    def __repr__(self):
        return f"InsertStatement(table={self.table_name}, columns={self.columns}, values={self.values})"


class SelectStatement(Statement):
    """SELECT语句"""

    def __init__(self,
                 columns: List[Expression],
                 table_name: str,
                 where: Optional[Expression] = None,
                 order_by: Optional[List[Column]] = None):
        self.columns = columns
        self.table_name = table_name
        self.where = where
        self.order_by = order_by if order_by else []

    def __repr__(self):
        return f"SelectStatement(table={self.table_name}, columns={self.columns}, where={self.where})"


class UpdateStatement(Statement):
    """UPDATE语句"""

    def __init__(self,
                 table_name: str,
                 assignments: Dict[str, Expression],
                 where: Optional[Expression] = None):
        self.table_name = table_name
        self.assignments = assignments  # 列名到表达式的映射
        self.where = where

    def __repr__(self):
        return f"UpdateStatement(table={self.table_name}, assignments={self.assignments}, where={self.where})"


class DeleteStatement(Statement):
    """DELETE语句"""

    def __init__(self, table_name: str, where: Optional[Expression] = None):
        self.table_name = table_name
        self.where = where

    def __repr__(self):
        return f"DeleteStatement(table={self.table_name}, where={self.where})"