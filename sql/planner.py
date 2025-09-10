"""
增强版逻辑计划生成器
支持事务、索引、权限、EXPLAIN
"""

from typing import List, Dict, Any
from .ast import *


class LogicalPlan:
    """逻辑计划基类"""
    pass


# ===== 基本算子 =====

class SeqScan(LogicalPlan):
    def __init__(self, table_name: str):
        self.table_name = table_name
        self.filter = None

    def __repr__(self):
        return f"SeqScan(table={self.table_name})"


class Filter(LogicalPlan):
    def __init__(self, condition: Expression, child: LogicalPlan):
        self.condition = condition
        self.child = child

    def __repr__(self):
        return f"Filter(condition={self.condition}) -> {self.child}"


class Project(LogicalPlan):
    def __init__(self, columns: List[Expression], child: LogicalPlan):
        self.columns = columns
        self.child = child

    def __repr__(self):
        return f"Project(columns={self.columns}) -> {self.child}"


class Insert(LogicalPlan):
    def __init__(self, table_name: str, columns: List[str], values: List[Expression]):
        self.table_name = table_name
        self.columns = columns
        self.values = values

    def __repr__(self):
        return f"Insert(table={self.table_name}, columns={self.columns}, values={self.values})"


class Update(LogicalPlan):
    def __init__(self, table_name: str, assignments: Dict[str, Expression], filter_condition: Expression = None):
        self.table_name = table_name
        self.assignments = assignments
        self.filter_condition = filter_condition

    def __repr__(self):
        return f"Update(table={self.table_name}, assignments={self.assignments}, filter={self.filter_condition})"


class Delete(LogicalPlan):
    def __init__(self, table_name: str, filter_condition: Expression = None):
        self.table_name = table_name
        self.filter_condition = filter_condition

    def __repr__(self):
        return f"Delete(table={self.table_name}, filter={self.filter_condition})"


class CreateTable(LogicalPlan):
    def __init__(self, table_name: str, columns: List[ColumnDefinition]):
        self.table_name = table_name
        self.columns = columns

    def __repr__(self):
        return f"CreateTable(table={self.table_name}, columns={self.columns})"


# ===== 新增算子：事务、索引、权限、EXPLAIN =====

class Begin(LogicalPlan):
    def __repr__(self):
        return "BeginTransaction()"


class Commit(LogicalPlan):
    def __repr__(self):
        return "CommitTransaction()"


class Rollback(LogicalPlan):
    def __repr__(self):
        return "RollbackTransaction()"


class CreateIndex(LogicalPlan):
    def __init__(self, table_name: str, index_name: str, columns: List[str]):
        self.table_name = table_name
        self.index_name = index_name
        self.columns = columns

    def __repr__(self):
        return f"CreateIndex(table={self.table_name}, index={self.index_name}, columns={self.columns})"


class DropIndex(LogicalPlan):
    def __init__(self, index_name: str):
        self.index_name = index_name

    def __repr__(self):
        return f"DropIndex(index={self.index_name})"


class Grant(LogicalPlan):
    def __init__(self, privileges: List[str], table_name: str, grantee: str):
        self.privileges = privileges
        self.table_name = table_name
        self.grantee = grantee

    def __repr__(self):
        return f"Grant(privileges={self.privileges}, table={self.table_name}, to={self.grantee})"


class Revoke(LogicalPlan):
    def __init__(self, privileges: List[str], table_name: str, grantee: str):
        self.privileges = privileges
        self.table_name = table_name
        self.grantee = grantee

    def __repr__(self):
        return f"Revoke(privileges={self.privileges}, table={self.table_name}, from={self.grantee})"


class Explain(LogicalPlan):
    def __init__(self, child: LogicalPlan):
        self.child = child

    def __repr__(self):
        return f"Explain({self.child})"


class Planner:
    """增强版逻辑计划生成器"""

    def plan(self, statement: Statement) -> LogicalPlan:
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
        elif isinstance(statement, BeginStatement):
            return Begin()
        elif isinstance(statement, CommitStatement):
            return Commit()
        elif isinstance(statement, RollbackStatement):
            return Rollback()
        elif isinstance(statement, CreateIndexStatement):
            return CreateIndex(statement.table_name, statement.index_name, statement.columns)
        elif isinstance(statement, DropIndexStatement):
            return DropIndex(statement.index_name)
        elif isinstance(statement, GrantStatement):
            return Grant(statement.privileges, statement.table_name, statement.grantee)
        elif isinstance(statement, RevokeStatement):
            return Revoke(statement.privileges, statement.table_name, statement.grantee)
        elif isinstance(statement, ExplainStatement):
            return Explain(self.plan(statement.inner_statement))
        else:
            raise ValueError(f"不支持的语句类型: {type(statement).__name__}")

    def plan_create_table(self, statement: CreateTableStatement) -> CreateTable:
        return CreateTable(statement.table_name, statement.columns)

    def plan_insert(self, statement: InsertStatement) -> Insert:
        return Insert(statement.table_name, statement.columns, statement.values)

    def plan_select(self, statement: SelectStatement) -> LogicalPlan:
        plan = SeqScan(statement.table_name)
        if statement.where:
            plan = Filter(statement.where, plan)
        plan = Project(statement.columns, plan)
        return plan

    def plan_update(self, statement: UpdateStatement) -> Update:
        return Update(statement.table_name, statement.assignments, statement.where)

    def plan_delete(self, statement: DeleteStatement) -> Delete:
        return Delete(statement.table_name, statement.where)
