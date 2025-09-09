"""
逻辑计划生成器
将AST转换为逻辑算子树
"""

from typing import List, Dict, Any
from .ast import *


class LogicalPlan:
    """逻辑计划基类"""
    pass


class SeqScan(LogicalPlan):
    """顺序扫描算子"""

    def __init__(self, table_name: str):
        self.table_name = table_name
        self.filter = None  # 可选的过滤条件

    def __repr__(self):
        if self.filter:
            return f"SeqScan(table={self.table_name}, filter={self.filter})"
        return f"SeqScan(table={self.table_name})"


class Filter(LogicalPlan):
    """过滤算子"""

    def __init__(self, condition: Expression, child: LogicalPlan):
        self.condition = condition
        self.child = child

    def __repr__(self):
        return f"Filter(condition={self.condition}) -> {self.child}"


class Project(LogicalPlan):
    """投影算子"""

    def __init__(self, columns: List[Expression], child: LogicalPlan):
        self.columns = columns
        self.child = child

    def __repr__(self):
        return f"Project(columns={self.columns}) -> {self.child}"


class Insert(LogicalPlan):
    """插入算子"""

    def __init__(self, table_name: str, columns: List[str], values: List[Expression]):
        self.table_name = table_name
        self.columns = columns
        self.values = values

    def __repr__(self):
        return f"Insert(table={self.table_name}, columns={self.columns}, values={self.values})"


class Update(LogicalPlan):
    """更新算子"""

    def __init__(self, table_name: str, assignments: Dict[str, Expression], filter_condition: Expression = None):
        self.table_name = table_name
        self.assignments = assignments
        self.filter_condition = filter_condition

    def __repr__(self):
        if self.filter_condition:
            return f"Update(table={self.table_name}, assignments={self.assignments}, filter={self.filter_condition})"
        return f"Update(table={self.table_name}, assignments={self.assignments})"


class Delete(LogicalPlan):
    """删除算子"""

    def __init__(self, table_name: str, filter_condition: Expression = None):
        self.table_name = table_name
        self.filter_condition = filter_condition

    def __repr__(self):
        if self.filter_condition:
            return f"Delete(table={self.table_name}, filter={self.filter_condition})"
        return f"Delete(table={self.table_name})"


class CreateTable(LogicalPlan):
    """创建表算子"""

    def __init__(self, table_name: str, columns: List[ColumnDefinition]):
        self.table_name = table_name
        self.columns = columns

    def __repr__(self):
        return f"CreateTable(table={self.table_name}, columns={self.columns})"


class Planner:
    """逻辑计划生成器"""

    def plan(self, statement: Statement) -> LogicalPlan:
        """将AST转换为逻辑计划"""
        if isinstance(statement, CreateTableStatement):
            return self.plan_create_table(statement)
        elif isinstance(statement, InsertStatement):
            return self.plan_insert(statement)
        elif isinstance(statement, SelectStatement):
            return self.plan_select(statement)
        elif isinstance(statement, UpdateStatement):
            return self.plan_update(statement)
        elif isinstance(statement, DeleteStatement):
            return self.plan_delete(statement)
        else:
            raise ValueError(f"不支持的语句类型: {type(statement).__name__}")

    def plan_create_table(self, statement: CreateTableStatement) -> CreateTable:
        """生成创建表的逻辑计划"""
        return CreateTable(statement.table_name, statement.columns)

    def plan_insert(self, statement: InsertStatement) -> Insert:
        """生成插入的逻辑计划"""
        return Insert(statement.table_name, statement.columns, statement.values)

    def plan_select(self, statement: SelectStatement) -> LogicalPlan:
        """生成查询的逻辑计划"""
        # 从顺序扫描开始
        plan = SeqScan(statement.table_name)

        # 添加过滤条件（如果有）
        if statement.where:
            plan = Filter(statement.where, plan)

        # 添加投影（选择列）
        plan = Project(statement.columns, plan)

        return plan

    def plan_update(self, statement: UpdateStatement) -> Update:
        """生成更新的逻辑计划"""
        return Update(statement.table_name, statement.assignments, statement.where)

    def plan_delete(self, statement: DeleteStatement) -> Delete:
        """生成删除的逻辑计划"""
        return Delete(statement.table_name, statement.where)