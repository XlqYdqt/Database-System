"""
增强版逻辑计划生成器
支持事务、索引、权限、EXPLAIN
"""

from typing import List, Dict, Any, Optional, Set
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


class CreateRole(LogicalPlan):
    def __init__(self, role_name: str):
        self.role_name = role_name

    def __repr__(self):
        return f"CreateRole(role={self.role_name})"


class GrantRole(LogicalPlan):
    def __init__(self, roles: List[str], grantees: List[str]):
        self.roles = roles
        self.grantees = grantees

    def __repr__(self):
        return f"GrantRole(roles={self.roles}, grantees={self.grantees})"


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
    def __init__(self, table_name: str, index_name: str, columns: List[str], index_type: Optional[IndexType] = None):
        self.table_name = table_name
        self.index_name = index_name
        self.columns = columns
        self.index_type = index_type

    def __repr__(self):
        return (f"CreateIndex(table={self.table_name}, index={self.index_name}, columns={self.columns}, "
                f"index_type={self.index_type})")


class DropIndex(LogicalPlan):
    def __init__(self, index_name: str):
        self.index_name = index_name

    def __repr__(self):
        return f"DropIndex(index={self.index_name})"


class Grant(LogicalPlan):
    def __init__(self, privileges: List[str], grantees: List[str], object_type: ObjectType, object_name: str):
        self.privileges = privileges
        self.grantees = grantees
        self.object_type = object_type
        self.object_name = object_name

    def __repr__(self):
        return (f"Grant(privileges={self.privileges}, object_type={self.object_type}, "
                f"object={self.object_name}, to={self.grantees})")


class Revoke(LogicalPlan):
    def __init__(self, privileges: List[str], grantees: List[str], object_type: ObjectType, object_name: str):
        self.privileges = privileges
        self.grantees = grantees
        self.object_type = object_type
        self.object_name = object_name

    def __repr__(self):
        return (f"Revoke(privileges={self.privileges}, object_type={self.object_type}, "
                f"object={self.object_name}, from={self.grantees})")


class Explain(LogicalPlan):
    def __init__(self, child: LogicalPlan, options: Optional[Set[ExplainOption]] = None):
        self.child = child
        self.options = options

    def __repr__(self):
        return f"Explain({self.child}, options={self.options})"


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
        elif isinstance(statement, TransactionStatement):
            return self.plan_transaction(statement)
        elif isinstance(statement, CreateIndexStatement):
            return self.plan_create_index(statement)
        elif isinstance(statement, DropIndexStatement):
            return self.plan_drop_index(statement)
        elif isinstance(statement, GrantStatement):
            return self.plan_grant(statement)
        elif isinstance(statement, RevokeStatement):
            return self.plan_revoke(statement)
        elif isinstance(statement, ExplainStatement):
            return self.plan_explain(statement)
        elif isinstance(statement, CreateRoleStatement):
            return self.plan_create_role(statement)
        elif isinstance(statement, GrantRoleStatement):  # 新增分支
            return self.plan_grant_role(statement)
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

    def plan_transaction(self, statement: TransactionStatement) -> LogicalPlan:
        if statement.command == TransactionCommand.BEGIN:
            return Begin()
        elif statement.command == TransactionCommand.COMMIT:
            return Commit()
        elif statement.command == TransactionCommand.ROLLBACK:
            return Rollback()
        else:
            raise ValueError(f"不支持的事务命令: {statement.command}")

    def plan_create_index(self, statement: CreateIndexStatement) -> CreateIndex:
        return CreateIndex(statement.table_name, statement.index_name, statement.columns, index_type=statement.index_type)

    def plan_drop_index(self, statement: DropIndexStatement) -> DropIndex:
        return DropIndex(statement.index_name)

    def plan_grant(self, statement: GrantStatement) -> Grant:
        return Grant(statement.privileges, statement.grantees, statement.object_type, statement.object_name)

    def plan_revoke(self, statement: RevokeStatement) -> Revoke:
        return Revoke(statement.privileges, statement.grantees, statement.object_type, statement.object_name)

    def plan_explain(self, statement: ExplainStatement) -> Explain:
        return Explain(self.plan(statement.statement), options=statement.options)

    def plan_create_role(self, statement: CreateRoleStatement) -> CreateRole:
        return CreateRole(statement.role_name)

    def plan_grant_role(self, statement: GrantRoleStatement) -> GrantRole:  # 新增方法
        return GrantRole(statement.roles, statement.grantees)