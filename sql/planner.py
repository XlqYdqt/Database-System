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
        return Insert(statement.table_name, statement.columns, statement.values)

    def _make_base_from(self, statement: SelectStatement) -> LogicalPlan:
        """
        构造 FROM 部分的初始计划：
        - 支持单表：statement.table_name 或 statement.from_tables（单元素）
        - 支持多表 join 的简单合并（如果 AST 提供 join 信息，会在 plan_select 中处理）
        """
        # 支持两种 AST 风格：from_tables 或 table_name
        if hasattr(statement, 'from_tables') and statement.from_tables:
            # 假设 from_tables 是 table 名称列表或 TableRef 列表（可为字符串或具有 .name）
            tables = statement.from_tables
            # 将第一个表构造成 SeqScan
            first = tables[0]
            table0 = first.name if hasattr(first, 'name') else first
            plan: LogicalPlan = SeqScan(table0)
            # 如果有多个表但没有显式 join 条件，先顺序将它们交叉 join（笛卡尔），但通常 AST 会有 joins
            for tbl in tables[1:]:
                tbl_name = tbl.name if hasattr(tbl, 'name') else tbl
                right = SeqScan(tbl_name)
                plan = Join(plan, right, condition=None, join_type="CROSS")
            return plan
        elif hasattr(statement, 'table_name') and statement.table_name:
            return SeqScan(statement.table_name)
        else:
            raise ValueError("SELECT 语句缺少 FROM 子句或表名信息")

    def _bind_in_subqueries(self, expr: Expression) -> Expression:
        """
        如果表达式是 IN 且右侧是子查询（SelectStatement），把右侧替换为 InSubquery 包含子计划。
        递归处理二元表达式的左右子表达式。
        """
        # 如果是直接 IN AST 节点（假设 AST 中存在 InExpression）
        # 因为 ast 的实现可能不同，我们使用属性检测
        if expr is None:
            return None

        # 1) 检查二元表达式类型（有 left/op/right）
        if hasattr(expr, 'op') and getattr(expr, 'op', None) and expr.op.upper() == 'IN':
            right = expr.right
            left = expr.left
            # 如果右侧是 SelectStatement（子查询），为其生成子计划
            if isinstance(right, SelectStatement):
                subplan = self.plan(right)
                return InSubquery(left, subplan)
            # 如果右侧是列表（IN (1,2,3)），保持原样
            return expr

        # 2) 递归处理常见复合表达式（BinaryExpression 或类似）
        if hasattr(expr, 'left') and hasattr(expr, 'right'):
            new_left = self._bind_in_subqueries(expr.left)
            new_right = self._bind_in_subqueries(expr.right)
            # 尝试创建一个新实例或就地修改（以兼容不同 AST 实现）
            try:
                expr.left = new_left
                expr.right = new_right
                return expr
            except Exception:
                # 无法原地修改则返回新的轻量包装（保守处理）
                return expr

        # 其他情况直接返回
        return expr

    def plan_select(self, statement: SelectStatement) -> LogicalPlan:
        """
        处理 SELECT：
        - 构造 FROM（SeqScan / Join） -> WHERE (Filter) -> PROJECT -> SORT（Order By）
        - 兼容多种 AST 风格（from_tables / joins / order_by）
        """
        # 1. FROM 部分（基础 plan）
        plan = self._make_base_from(statement)

        # 2. 如果 AST 提供 joins（包含 join 条件），将其应用（常见 AST 会有 joins 或 join_clauses）
        joins = getattr(statement, 'joins', None) or getattr(statement, 'join_clauses', None)
        if joins:
            # 期望 joins 是类似 [(right_table, condition, type), ...] 或有属性的对象列表
            for j in joins:
                # 尝试提取右 表名 / plan
                right_tbl = getattr(j, 'right', None) or (j[0] if isinstance(j, (list, tuple)) else None)
                cond = getattr(j, 'condition', None) or (j[1] if isinstance(j, (list, tuple)) and len(j) > 1 else None)
                jtype = getattr(j, 'type', None) or (j[2] if isinstance(j, (list, tuple)) and len(j) > 2 else "INNER")

                right_name = right_tbl.name if hasattr(right_tbl, 'name') else right_tbl
                right_plan = SeqScan(right_name)
                plan = Join(plan, right_plan, condition=cond, join_type=(jtype or "INNER"))

        # 3. WHERE 条件
        where_expr = getattr(statement, 'where', None)
        if where_expr:
            # 绑定 IN (SELECT ...) 的子查询
            where_expr = self._bind_in_subqueries(where_expr)
            plan = Filter(where_expr, plan)

        # 4. 投影
        proj_cols = getattr(statement, 'columns', None)
        if not proj_cols:
            # 可能 AST 用 select_list 或 projections 命名
            proj_cols = getattr(statement, 'select_list', None) or getattr(statement, 'projections', None) or ['*']
        plan = Project(proj_cols, plan)

        # 5. ORDER BY / SORT
        order_by = getattr(statement, 'order_by', None) or getattr(statement, 'order', None)
        if order_by:
            # 支持多种 order_by 表示方式：列表 of (expr, order) 或有 keys/orders 属性
            if isinstance(order_by, list) and order_by and isinstance(order_by[0], tuple):
                keys = [k for k, _ in order_by]
                orders = [o for _, o in order_by]
            else:
                # 尝试读取属性 keys / orders
                keys = getattr(order_by, 'keys', None) or getattr(order_by, 'columns', None) or [order_by]
                orders = getattr(order_by, 'orders', None) or ['ASC'] * len(keys)
            plan = Sort(keys, orders, plan)

        return plan

    def plan_update(self, statement: UpdateStatement) -> Update:
        """生成更新的逻辑计划，自动构造 child（SeqScan + 可选 Filter）"""
        # 生成顺序扫描算子作为子操作符
        child = SeqScan(statement.table_name)

        # 如果有WHERE条件，加入过滤算子
        if getattr(statement, 'where', None):
            child = Filter(statement.where, child)

        # 生成 Update 操作符，传递 child 操作符
        return Update(statement.table_name, statement.assignments, getattr(statement, 'where', None), child)

    def plan_delete(self, statement: DeleteStatement) -> Delete:
        """生成删除的逻辑计划，自动构造 child（SeqScan + 可选 Filter）"""
        # 生成顺序扫描算子作为子操作符
        child = SeqScan(statement.table_name)

        # 如果有WHERE条件，加入过滤算子
        if getattr(statement, 'where', None):
            child = Filter(statement.where, child)

        # 生成 Delete 操作符，传递 child 操作符
        return Delete(statement.table_name, getattr(statement, 'where', None), child)

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
