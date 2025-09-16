"""
增强版逻辑计划生成器
支持事务、索引、权限、EXPLAIN，以及 JOIN、IN、ORDER BY 等
"""

from typing import List, Dict, Any, Optional, Set, Tuple
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
        # 自动继承 child 的 table_name，兼容旧代码
        self.table_name = getattr(child, "table_name", None)

    def __repr__(self):
        return f"Filter(condition={self.condition}) -> {self.child}"



class Project(LogicalPlan):
    def __init__(self, columns: List[Expression], child: LogicalPlan):
        self.columns = columns
        self.child = child

    def __repr__(self):
        return f"Project(columns={self.columns}) -> {self.child}"


class Insert(LogicalPlan):
    def __init__(self, table_name: str, columns: List[str], values: List[List[Expression]]):
        self.table_name = table_name
        self.columns = columns
        self.values = values

    def __repr__(self):
        return f"Insert(table={self.table_name}, columns={self.columns}, values={self.values})"


class Update(LogicalPlan):
    def __init__(self, table_name: str, assignments: Dict[str, Expression], filter_condition: Expression = None, child: Optional[LogicalPlan] = None):
        self.table_name = table_name
        self.assignments = assignments
        self.filter_condition = filter_condition
        self.child = child  # 新增 child 参数，用来传递子操作符

    def __repr__(self):
        if self.child:
            return f"Update(table={self.table_name}, assignments={self.assignments}, filter={self.filter_condition}) -> {self.child}"
        else:
            return f"Update(table={self.table_name}, assignments={self.assignments}, filter={self.filter_condition})"


class Delete(LogicalPlan):
    def __init__(self, table_name: str, filter_condition: Expression = None, child: Optional[LogicalPlan] = None):
        self.table_name = table_name
        self.filter_condition = filter_condition
        self.child = child  # 新增 child 参数，用来传递子操作符

    def __repr__(self):
        if self.child:
            return f"Delete(table={self.table_name}, filter={self.filter_condition}) -> {self.child}"
        else:
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
    # [FIX] 增加 unique 属性以接收来自 Planner 的信息
    def __init__(self, table_name: str, index_name: str, columns: List[str], unique: bool = False, index_type: Optional[IndexType] = None):
        self.table_name = table_name
        self.index_name = index_name
        self.columns = columns
        self.unique = unique
        self.index_type = index_type

    def __repr__(self):
        unique_str = "UNIQUE " if self.unique else ""
        return (f"CreateIndex({unique_str}table={self.table_name}, index={self.index_name}, columns={self.columns}, "
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


# ===== 新增：Join / Sort / InSubquery 表达式包装 =====

class Join(LogicalPlan):
    def __init__(self, left: LogicalPlan, right: LogicalPlan, condition: Optional[Expression] = None, join_type: str = "INNER"):
        self.left = left
        self.right = right
        self.condition = condition
        self.join_type = join_type  # "INNER", "LEFT", "RIGHT", "FULL"

    def __repr__(self):
        return f"Join(type={self.join_type}, condition={self.condition}) -> ({self.left}, {self.right})"


class Sort(LogicalPlan):
    def __init__(self, sort_keys: List[Expression], orders: List[str], child: LogicalPlan):
        self.sort_keys = sort_keys
        self.orders = orders
        self.child = child

    def __repr__(self):
        return f"Sort(keys={self.sort_keys}, orders={self.orders}) -> {self.child}"


# 用于把 IN (SELECT ...) 的子查询绑定到一个可被执行器识别的对象上
class InSubquery(Expression):
    def __init__(self, left: Expression, subplan: LogicalPlan):
        self.left = left
        self.subplan = subplan

    def __repr__(self):
        return f"{self.left} IN ({self.subplan})"


# ===== Planner 实现 =====

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
        elif isinstance(statement, GrantRoleStatement):
            return self.plan_grant_role(statement)
        else:
            raise ValueError(f"不支持的语句类型: {type(statement).__name__}")

    def plan_create_table(self, statement: CreateTableStatement) -> CreateTable:
        return CreateTable(statement.table_name, statement.columns)

    def plan_insert(self, statement: InsertStatement) -> Insert:
        # 假设解析器总是生成 List[List[Expression]]
        return Insert(statement.table_name, statement.columns, statement.values)

    def _make_base_from(self, statement: SelectStatement) -> LogicalPlan:
        """
        构造 FROM 部分的初始计划
        """
        if hasattr(statement, 'table_name') and statement.table_name:
            return SeqScan(statement.table_name)
        else:
            raise ValueError("SELECT 语句缺少 FROM 子句或表名信息")

    def _bind_in_subqueries(self, expr: Expression) -> Expression:
        """
        如果表达式是 IN 且右侧是子查询（SelectStatement），把右侧替换为 InSubquery 包含子计划。
        递归处理二元表达式的左右子表达式。
        """
        if expr is None: return None

        if isinstance(expr, InExpression):
            if isinstance(expr.values, SelectStatement):
                subplan = self.plan(expr.values)
                expr.values = subplan
            return expr

        if isinstance(expr, BinaryExpression):
            expr.left = self._bind_in_subqueries(expr.left)
            expr.right = self._bind_in_subqueries(expr.right)

        if isinstance(expr, UnaryExpression):
            expr.expression = self._bind_in_subqueries(expr.expression)

        return expr

    def plan_select(self, statement: SelectStatement) -> LogicalPlan:
        """
        处理 SELECT：
        FROM (SeqScan / Join) -> WHERE (Filter) -> PROJECT -> SORT（Order By）
        """
        # 1. FROM 部分
        plan = self._make_base_from(statement)

        # 2. Joins
        if statement.joins:
            for join_clause in statement.joins:
                right_plan = SeqScan(join_clause.table)
                plan = Join(plan, right_plan, condition=join_clause.condition, join_type=join_clause.join_type)

        # 3. WHERE 条件
        if statement.where:
            plan = Filter(statement.where, plan)

        # 4. 投影
        plan = Project(statement.columns, plan)

        # 5. ORDER BY / SORT
        if statement.order_by:
            keys = [ob.expression for ob in statement.order_by]
            orders = [ob.direction for ob in statement.order_by]
            plan = Sort(keys, orders, plan)

        return plan

    def plan_update(self, statement: UpdateStatement) -> Update:
        """生成更新的逻辑计划，自动构造 child（SeqScan + 可选 Filter）"""
        child = SeqScan(statement.table_name)
        if statement.where:
            child = Filter(statement.where, child)
        return Update(statement.table_name, statement.assignments, statement.where, child)

    def plan_delete(self, statement: DeleteStatement) -> Delete:
        """生成删除的逻辑计划，自动构造 child（SeqScan + 可选 Filter）"""
        child = SeqScan(statement.table_name)
        if statement.where:
            child = Filter(statement.where, child)
        return Delete(statement.table_name, statement.where, child)

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
        # [FIX] 确保将 statement.unique 属性从AST节点传递给逻辑计划节点
        return CreateIndex(
            table_name=statement.table_name,
            index_name=statement.index_name,
            columns=statement.columns,
            unique=statement.unique,
            index_type=statement.index_type
        )

    def plan_drop_index(self, statement: DropIndexStatement) -> DropIndex:
        return DropIndex(statement.index_name)

    def plan_grant(self, statement: GrantStatement) -> Grant:
        privileges_str = [p.value for p in statement.privileges]
        return Grant(privileges_str, statement.grantees, statement.object_type, statement.object_name)

    def plan_revoke(self, statement: RevokeStatement) -> Revoke:
        privileges_str = [p.value for p in statement.privileges]
        return Revoke(privileges_str, statement.grantees, statement.object_type, statement.object_name)

    def plan_explain(self, statement: ExplainStatement) -> Explain:
        return Explain(self.plan(statement.statement), options=statement.options)

    def plan_create_role(self, statement: CreateRoleStatement) -> CreateRole:
        return CreateRole(statement.role_name)

    def plan_grant_role(self, statement: GrantRoleStatement) -> GrantRole:
        return GrantRole(statement.roles, statement.grantees)

