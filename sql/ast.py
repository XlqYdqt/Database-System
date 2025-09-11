"""
抽象语法树（AST）节点定义
用于表示SQL语句的结构化信息
支持事务、并发控制、复杂索引、查询优化、访问控制等高级功能
"""

from enum import Enum
from typing import List, Optional, Union, Dict, Any, Tuple, Set


class DataType(Enum):
    """支持的SQL数据类型"""
    INT = 'INT'
    FLOAT = 'FLOAT'
    STRING = 'STRING'
    BOOL = 'BOOL'
    TEXT = 'TEXT'
    DATETIME = 'DATETIME'
    DATE = 'DATE'
    BLOB = 'BLOB'
    DECIMAL = 'DECIMAL'
    # 添加更多数据库支持的数据类型
    VARCHAR = 'VARCHAR'
    CHAR = 'CHAR'
    TIMESTAMP = 'TIMESTAMP'
    JSON = 'JSON'
    UUID = 'UUID'


class Operator:
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
    LIKE = 'LIKE'  # 模糊匹配
    ILIKE = 'ILIKE'  # 不区分大小写的模糊匹配
    IN = 'IN'  # 包含
    IS = 'IS'  # 是
    IS_NOT = 'IS NOT'  # 不是
    BETWEEN = 'BETWEEN'
    EXISTS = 'EXISTS'
    # 添加数学运算符
    ADD = '+'
    SUB = '-'
    MUL = '*'
    DIV = '/'
    MOD = '%'
    # 位运算符
    BIT_AND = '&'
    BIT_OR = '|'
    BIT_XOR = '^'
    BIT_NOT = '~'
    SHIFT_LEFT = '<<'
    SHIFT_RIGHT = '>>'


class JoinType(Enum):
    """连接类型"""
    INNER = 'INNER'
    LEFT = 'LEFT'
    RIGHT = 'RIGHT'
    FULL = 'FULL'
    CROSS = 'CROSS'
    # 添加更多连接类型
    NATURAL = 'NATURAL'
    SELF = 'SELF'


class TransactionCommand(Enum):
    """事务命令"""
    BEGIN = 'BEGIN'
    COMMIT = 'COMMIT'
    ROLLBACK = 'ROLLBACK'
    SAVEPOINT = 'SAVEPOINT'
    ROLLBACK_TO = 'ROLLBACK TO'
    # 添加事务隔离级别
    SET_TRANSACTION = 'SET TRANSACTION'


class IsolationLevel(Enum):
    """事务隔离级别"""
    READ_UNCOMMITTED = 'READ UNCOMMITTED'
    READ_COMMITTED = 'READ COMMITTED'
    REPEATABLE_READ = 'REPEATABLE READ'
    SERIALIZABLE = 'SERIALIZABLE'


class LockMode(Enum):
    """锁模式"""
    SHARE = 'SHARE'
    EXCLUSIVE = 'EXCLUSIVE'
    # 添加更多锁模式
    UPDATE = 'UPDATE'
    NO_KEY_UPDATE = 'NO KEY UPDATE'
    KEY_SHARE = 'KEY SHARE'


class Privilege(Enum):
    """权限类型"""
    SELECT = 'SELECT'
    INSERT = 'INSERT'
    UPDATE = 'UPDATE'
    DELETE = 'DELETE'
    ALL = 'ALL'
    CREATE = 'CREATE'
    DROP = 'DROP'
    ALTER = 'ALTER'
    EXECUTE = 'EXECUTE'
    # 添加更多权限
    REFERENCES = 'REFERENCES'
    TRIGGER = 'TRIGGER'
    TRUNCATE = 'TRUNCATE'
    USAGE = 'USAGE'
    CONNECT = 'CONNECT'
    TEMPORARY = 'TEMPORARY'


class ColumnConstraint(Enum):
    """列约束类型"""
    PRIMARY_KEY = 'PRIMARY KEY'
    UNIQUE = 'UNIQUE'
    NOT_NULL = 'NOT NULL'
    NULL = 'NULL'
    DEFAULT = 'DEFAULT'
    FOREIGN_KEY = 'FOREIGN KEY'
    CHECK = 'CHECK'
    # 添加更多约束
    GENERATED = 'GENERATED'
    IDENTITY = 'IDENTITY'


class IndexType(Enum):
    """索引类型"""
    BTREE = 'BTREE'
    HASH = 'HASH'
    FULLTEXT = 'FULLTEXT'
    SPATIAL = 'SPATIAL'
    # 添加更多索引类型
    GIN = 'GIN'
    GIST = 'GIST'
    BRIN = 'BRIN'


class ExplainOption(Enum):
    """EXPLAIN选项"""
    ANALYZE = 'ANALYZE'
    VERBOSE = 'VERBOSE'
    COSTS = 'COSTS'
    BUFFERS = 'BUFFERS'
    TIMING = 'TIMING'
    SUMMARY = 'SUMMARY'
    FORMAT = 'FORMAT'


class ObjectType(Enum):
    """数据库对象类型"""
    TABLE = 'TABLE'
    DATABASE = 'DATABASE'
    SCHEMA = 'SCHEMA'
    VIEW = 'VIEW'
    INDEX = 'INDEX'
    SEQUENCE = 'SEQUENCE'
    FUNCTION = 'FUNCTION'
    PROCEDURE = 'PROCEDURE'


class ColumnDefinition:
    """列定义"""

    def __init__(self,
                 name: str,
                 data_type: DataType,
                 constraints: Optional[List[Tuple[ColumnConstraint, Any]]] = None,
                 default_value: Optional[Any] = None,
                 length: Optional[int] = None,
                 precision: Optional[int] = None,
                 scale: Optional[int] = None):
        self.name = name
        self.data_type = data_type
        self.constraints = constraints if constraints else []
        self.default_value = default_value
        self.length = length  # 对于VARCHAR等类型
        self.precision = precision  # 对于DECIMAL等类型
        self.scale = scale  # 对于DECIMAL等类型

    def __repr__(self):
        constraints_str = f", constraints={self.constraints}" if self.constraints else ""
        default_str = f", default={self.default_value}" if self.default_value else ""
        length_str = f", length={self.length}" if self.length else ""
        precision_str = f", precision={self.precision}" if self.precision else ""
        scale_str = f", scale={self.scale}" if self.scale else ""
        return f"ColumnDefinition(name={self.name}, type={self.data_type.value}{length_str}{precision_str}{scale_str}{constraints_str}{default_str})"


class Expression:
    """表达式基类"""
    pass


class Column(Expression):
    """列引用表达式"""

    def __init__(self, name: str, table: Optional[str] = None, alias: Optional[str] = None):
        self.name = name
        self.table = table  # 表名前缀（可选）
        self.alias = alias  # 别名（可选）

    def __repr__(self):
        parts = []
        if self.table:
            parts.append(f"table={self.table}")
        parts.append(f"name={self.name}")
        if self.alias:
            parts.append(f"alias={self.alias}")
        return f"Column({', '.join(parts)})"


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


class UnaryExpression(Expression):
    """一元表达式"""

    def __init__(self, op: Operator, expression: Expression):
        self.op = op
        self.expression = expression

    def __repr__(self):
        return f"UnaryExpression(op={self.op.value}, expression={self.expression})"


class FunctionCall(Expression):
    """函数调用表达式"""

    def __init__(self, function_name: str, arguments: List[Expression], distinct: bool = False,
                 window: Optional['WindowClause'] = None):
        self.function_name = function_name
        self.arguments = arguments
        self.distinct = distinct
        self.window = window  # 窗口函数支持

    def __repr__(self):
        distinct_str = "DISTINCT " if self.distinct else ""
        window_str = f" OVER {self.window}" if self.window else ""
        return f"FunctionCall(function={self.function_name}, {distinct_str}args={self.arguments}{window_str})"


class CaseExpression(Expression):
    """CASE表达式"""

    def __init__(self, base_expression: Optional[Expression] = None,
                 when_then_pairs: List[Tuple[Expression, Expression]] = None,
                 else_expression: Optional[Expression] = None):
        self.base_expression = base_expression
        self.when_then_pairs = when_then_pairs if when_then_pairs else []
        self.else_expression = else_expression

    def __repr__(self):
        base_str = f"{self.base_expression} " if self.base_expression else ""
        else_str = f" ELSE {self.else_expression}" if self.else_expression else ""
        return f"CaseExpression({base_str}{self.when_then_pairs}{else_str})"


class SubqueryExpression(Expression):
    """子查询表达式"""

    def __init__(self, select_statement: 'SelectStatement'):
        self.select_statement = select_statement

    def __repr__(self):
        return f"SubqueryExpression({self.select_statement})"


class WindowClause:
    """窗口子句"""

    def __init__(self, partition_by: Optional[List[Expression]] = None,
                 order_by: Optional[List[Tuple[Expression, str]]] = None,
                 frame: Optional[Tuple[str, Optional[str], Optional[str]]] = None):
        self.partition_by = partition_by if partition_by else []
        self.order_by = order_by if order_by else []
        self.frame = frame  # (类型, 开始, 结束)

    def __repr__(self):
        parts = []
        if self.partition_by:
            parts.append(f"PARTITION BY {self.partition_by}")
        if self.order_by:
            parts.append(f"ORDER BY {self.order_by}")
        if self.frame:
            parts.append(f"FRAME {self.frame}")
        return f"WindowClause({' '.join(parts)})"


class JoinClause:
    """连接子句"""

    def __init__(self, table: Union[str, 'SelectStatement'], join_type: JoinType, condition: Optional[Expression] = None,
                 alias: Optional[str] = None):
        self.table = table
        self.join_type = join_type
        self.condition = condition
        self.alias = alias

    def __repr__(self):
        alias_str = f" AS {self.alias}" if self.alias else ""
        return f"JoinClause(table={self.table}{alias_str}, type={self.join_type.value}, condition={self.condition})"


class TableConstraint:
    """表级约束"""

    def __init__(self, constraint_type: ColumnConstraint, columns: List[str],
                 name: Optional[str] = None, reference_table: Optional[str] = None,
                 reference_columns: Optional[List[str]] = None):
        self.name = name
        self.constraint_type = constraint_type
        self.columns = columns
        self.reference_table = reference_table
        self.reference_columns = reference_columns if reference_columns else []

    def __repr__(self):
        ref_str = f" REFERENCES {self.reference_table}({', '.join(self.reference_columns)})" if self.reference_table else ""
        name_str = f"CONSTRAINT {self.name} " if self.name else ""
        return f"TableConstraint({name_str}{self.constraint_type.value} ({', '.join(self.columns)}){ref_str})"


class Statement:
    """SQL语句基类"""
    pass


class CreateTableStatement(Statement):
    """CREATE TABLE语句"""

    def __init__(self,
                 table_name: str,
                 columns: List[ColumnDefinition],
                 constraints: Optional[List[TableConstraint]] = None,
                 if_not_exists: bool = False,
                 temporary: bool = False):
        self.table_name = table_name
        self.columns = columns
        self.constraints = constraints if constraints else []
        self.if_not_exists = if_not_exists
        self.temporary = temporary

    def __repr__(self):
        exists_str = " IF NOT EXISTS" if self.if_not_exists else ""
        temp_str = " TEMPORARY" if self.temporary else ""
        constraints_str = f", constraints={self.constraints}" if self.constraints else ""
        return f"CreateTableStatement({temp_str}table={self.table_name}{exists_str}, columns={self.columns}{constraints_str})"


class CreateIndexStatement(Statement):
    """CREATE INDEX语句"""

    def __init__(self,
                 index_name: str,
                 table_name: str,
                 columns: List[str],
                 unique: bool = False,
                 index_type: Optional[IndexType] = None,
                 where_clause: Optional[Expression] = None,
                 concurrently: bool = False,
                 if_not_exists: bool = False):
        self.index_name = index_name
        self.table_name = table_name
        self.columns = columns
        self.unique = unique
        self.index_type = index_type
        self.where_clause = where_clause
        self.concurrently = concurrently  # 并发创建索引
        self.if_not_exists = if_not_exists

    def __repr__(self):
        unique_str = "UNIQUE " if self.unique else ""
        concurrent_str = "CONCURRENTLY " if self.concurrently else ""
        exists_str = "IF NOT EXISTS " if self.if_not_exists else ""
        type_str = f" USING {self.index_type.value}" if self.index_type else ""
        where_str = f" WHERE {self.where_clause}" if self.where_clause else ""
        return f"CreateIndexStatement({concurrent_str}{unique_str}{exists_str}index={self.index_name}, table={self.table_name}, columns={self.columns}{type_str}{where_str})"

class DropIndexStatement(Statement):
    """DROP INDEX语句"""

    def __init__(self, index_name: str, if_exists: bool = False, concurrently: bool = False):
        """
        :param index_name: 要删除索引的名称
        :param if_exists: 如果索引不存在，是否忽略错误
        :param concurrently: 并发删除索引的标志
        """
        self.index_name = index_name
        self.if_exists = if_exists
        self.concurrently = concurrently

    def __repr__(self):
        exists_str = " IF EXISTS" if self.if_exists else ""
        concurrent_str = " CONCURRENTLY" if self.concurrently else ""
        return f"DropIndexStatement(index={self.index_name}{exists_str}{concurrent_str})"

class InsertStatement(Statement):
    """INSERT语句"""

    def __init__(self,
                 table_name: str,
                 columns: List[str],
                 values: List[Expression],
                 on_conflict: Optional[Tuple[List[str], str]] = None,  # (冲突列, 解决动作)
                 returning: Optional[List[Expression]] = None,
                 with_clause: Optional['WithClause'] = None):
        self.table_name = table_name
        self.columns = columns
        self.values = values
        self.on_conflict = on_conflict  # 冲突处理策略
        self.returning = returning if returning else []  # RETURNING子句
        self.with_clause = with_clause  # CTE支持

    def __repr__(self):
        conflict_str = f", on_conflict={self.on_conflict}" if self.on_conflict else ""
        returning_str = f", returning={self.returning}" if self.returning else ""
        with_str = f", with={self.with_clause}" if self.with_clause else ""
        return f"InsertStatement(table={self.table_name}, columns={self.columns}, values={self.values}{conflict_str}{returning_str}{with_str})"


class WithClause:
    """WITH子句（公共表表达式）"""

    def __init__(self, queries: List[Tuple[str, 'SelectStatement']], recursive: bool = False):
        self.queries = queries  # (名称, 查询)
        self.recursive = recursive

    def __repr__(self):
        recursive_str = "RECURSIVE " if self.recursive else ""
        return f"WithClause({recursive_str}{self.queries})"


class SelectStatement(Statement):
    """SELECT语句"""

    def __init__(self,
                 columns: List[Expression],
                 table_name: Optional[str] = None,
                 joins: Optional[List[JoinClause]] = None,
                 where: Optional[Expression] = None,
                 group_by: Optional[List[Expression]] = None,
                 having: Optional[Expression] = None,
                 order_by: Optional[List[Tuple[Expression, str]]] = None,  # (expression, direction)
                 limit: Optional[Union[int, Expression]] = None,
                 offset: Optional[Union[int, Expression]] = None,
                 distinct: bool = False,
                 for_update: Optional[LockMode] = None,
                 with_clause: Optional[WithClause] = None,
                 hint: Optional[Dict[str, Any]] = None):  # 查询优化提示
        self.columns = columns
        self.table_name = table_name
        self.joins = joins if joins else []
        self.where = where
        self.group_by = group_by if group_by else []
        self.having = having
        self.order_by = order_by if order_by else []
        self.limit = limit
        self.offset = offset
        self.distinct = distinct
        self.for_update = for_update  # 用于并发控制
        self.with_clause = with_clause  # CTE支持
        self.hint = hint if hint else {}  # 查询优化提示

    def __repr__(self):
        distinct_str = "DISTINCT " if self.distinct else ""
        join_str = f", joins={self.joins}" if self.joins else ""
        group_str = f", group_by={self.group_by}" if self.group_by else ""
        having_str = f", having={self.having}" if self.having else ""
        order_str = f", order_by={self.order_by}" if self.order_by else ""
        limit_str = f", limit={self.limit}" if self.limit else ""
        offset_str = f", offset={self.offset}" if self.offset else ""
        for_update_str = f" FOR {self.for_update.value}" if self.for_update else ""
        with_str = f", with={self.with_clause}" if self.with_clause else ""
        hint_str = f", hint={self.hint}" if self.hint else ""
        return f"SelectStatement({distinct_str}columns={self.columns}, table={self.table_name}{join_str}, where={self.where}{group_str}{having_str}{order_str}{limit_str}{offset_str}{for_update_str}{with_str}{hint_str})"


class UpdateStatement(Statement):
    """UPDATE语句"""

    def __init__(self,
                 table_name: str,
                 assignments: Dict[str, Expression],
                 where: Optional[Expression] = None,
                 from_clause: Optional[List[Union[str, JoinClause]]] = None,
                 returning: Optional[List[Expression]] = None,
                 with_clause: Optional[WithClause] = None):
        self.table_name = table_name
        self.assignments = assignments  # 列名到表达式的映射
        self.where = where
        self.from_clause = from_clause if from_clause else []  # 支持多表更新
        self.returning = returning if returning else []  # RETURNING子句
        self.with_clause = with_clause  # CTE支持

    def __repr__(self):
        from_str = f", from={self.from_clause}" if self.from_clause else ""
        returning_str = f", returning={self.returning}" if self.returning else ""
        with_str = f", with={self.with_clause}" if self.with_clause else ""
        return f"UpdateStatement(table={self.table_name}, assignments={self.assignments}, where={self.where}{from_str}{returning_str}{with_str})"


class DeleteStatement(Statement):
    """DELETE语句"""

    def __init__(self,
                 table_name: str,
                 where: Optional[Expression] = None,
                 using: Optional[List[Union[str, JoinClause]]] = None,
                 returning: Optional[List[Expression]] = None,
                 with_clause: Optional[WithClause] = None):
        self.table_name = table_name
        self.where = where
        self.using = using if using else []  # 支持多表删除
        self.returning = returning if returning else []  # RETURNING子句
        self.with_clause = with_clause  # CTE支持

    def __repr__(self):
        using_str = f", using={self.using}" if self.using else ""
        returning_str = f", returning={self.returning}" if self.returning else ""
        with_str = f", with={self.with_clause}" if self.with_clause else ""
        return f"DeleteStatement(table={self.table_name}, where={self.where}{using_str}{returning_str}{with_str})"


class TransactionStatement(Statement):
    """事务控制语句"""

    def __init__(self, command: TransactionCommand,
                 isolation_level: Optional[IsolationLevel] = None,
                 read_only: Optional[bool] = None,
                 deferrable: Optional[bool] = None,
                 savepoint_name: Optional[str] = None):
        self.command = command
        self.isolation_level = isolation_level
        self.read_only = read_only
        self.deferrable = deferrable
        self.savepoint_name = savepoint_name

    def __repr__(self):
        options = []
        if self.isolation_level:
            options.append(f"ISOLATION LEVEL {self.isolation_level.value}")
        if self.read_only is not None:
            options.append("READ ONLY" if self.read_only else "READ WRITE")
        if self.deferrable is not None:
            options.append("DEFERRABLE" if self.deferrable else "NOT DEFERRABLE")

        options_str = f" {', '.join(options)}" if options else ""

        if self.command == TransactionCommand.SAVEPOINT:
            return f"TransactionStatement(command={self.command.value} {self.savepoint_name})"
        elif self.command == TransactionCommand.ROLLBACK_TO:
            return f"TransactionStatement(command={self.command.value} {self.savepoint_name})"
        elif self.command == TransactionCommand.SET_TRANSACTION:
            return f"TransactionStatement(command={self.command.value}{options_str})"
        return f"TransactionStatement(command={self.command.value}{options_str})"


class LockStatement(Statement):
    """显式锁语句"""

    def __init__(self, table_names: List[str], mode: LockMode, nowait: bool = False):
        self.table_names = table_names
        self.mode = mode
        self.nowait = nowait

    def __repr__(self):
        nowait_str = " NOWAIT" if self.nowait else ""
        return f"LockStatement(tables={self.table_names}, mode={self.mode.value}{nowait_str})"


class GrantStatement(Statement):
    """权限授予语句"""

    def __init__(self,
                 privileges: List[Privilege],
                 object_type: ObjectType,
                 object_name: str,
                 grantees: List[str],
                 with_grant_option: bool = False,
                 columns: Optional[List[str]] = None):  # 列级权限
        self.privileges = privileges
        self.object_type = object_type
        self.object_name = object_name
        self.grantees = grantees
        self.with_grant_option = with_grant_option
        self.columns = columns if columns else []  # 列级权限

    def __repr__(self):
        option_str = " WITH GRANT OPTION" if self.with_grant_option else ""
        columns_str = f"({', '.join(self.columns)})" if self.columns else ""
        return f"GrantStatement(privileges={[p.value for p in self.privileges]}, object={self.object_type.value} {self.object_name}{columns_str}, grantees={self.grantees}{option_str})"


class RevokeStatement(Statement):
    """权限回收语句"""

    def __init__(self,
                 privileges: List[Privilege],
                 object_type: ObjectType,
                 object_name: str,
                 grantees: List[str],
                 grant_option: bool = False,
                 columns: Optional[List[str]] = None):
        self.privileges = privileges
        self.object_type = object_type
        self.object_name = object_name
        self.grantees = grantees
        self.grant_option = grant_option
        self.columns = columns if columns else []

    def __repr__(self):
        option_str = " GRANT OPTION FOR" if self.grant_option else ""
        columns_str = f"({', '.join(self.columns)})" if self.columns else ""
        return f"RevokeStatement({option_str}privileges={[p.value for p in self.privileges]}, object={self.object_type.value} {self.object_name}{columns_str}, grantees={self.grantees})"


class CreateRoleStatement(Statement):
    """创建角色语句"""

    def __init__(self, role_name: str, if_not_exists: bool = False,
                 options: Optional[Dict[str, Any]] = None):
        self.role_name = role_name
        self.if_not_exists = if_not_exists
        self.options = options if options else {}

    def __repr__(self):
        exists_str = " IF NOT EXISTS" if self.if_not_exists else ""
        options_str = f" WITH {self.options}" if self.options else ""
        return f"CreateRoleStatement(role={self.role_name}{exists_str}{options_str})"


class AlterRoleStatement(Statement):
    """修改角色语句"""

    def __init__(self, role_name: str, options: Dict[str, Any]):
        self.role_name = role_name
        self.options = options

    def __repr__(self):
        return f"AlterRoleStatement(role={self.role_name}, options={self.options})"


class GrantRoleStatement(Statement):
    """角色授予语句"""

    def __init__(self, roles: List[str], grantees: List[str],
                 with_admin_option: bool = False):
        self.roles = roles
        self.grantees = grantees
        self.with_admin_option = with_admin_option

    def __repr__(self):
        option_str = " WITH ADMIN OPTION" if self.with_admin_option else ""
        return f"GrantRoleStatement(roles={self.roles}, grantees={self.grantees}{option_str})"


class ExplainStatement(Statement):
    """EXPLAIN语句"""

    def __init__(self, statement: Statement, options: Optional[Set[ExplainOption]] = None,
                 format: Optional[str] = None):
        self.statement = statement
        self.options = options if options else set()
        self.format = format

    def __repr__(self):
        options_str = f" ({', '.join([o.value for o in self.options])})" if self.options else ""
        format_str = f" FORMAT {self.format}" if self.format else ""
        return f"ExplainStatement({options_str}{format_str}, statement={self.statement})"


class PrepareStatement(Statement):
    """预处理语句"""

    def __init__(self, name: str, statement: Statement, parameters: Optional[List[DataType]] = None):
        self.name = name
        self.statement = statement
        self.parameters = parameters if parameters else []

    def __repr__(self):
        params_str = f"({', '.join([p.value for p in self.parameters])})" if self.parameters else ""
        return f"PrepareStatement(name={self.name}{params_str}, statement={self.statement})"


class ExecuteStatement(Statement):
    """执行预处理语句"""

    def __init__(self, name: str, parameters: Optional[List[Expression]] = None):
        self.name = name
        self.parameters = parameters if parameters else []

    def __repr__(self):
        params_str = f"({', '.join([str(p) for p in self.parameters])})" if self.parameters else ""
        return f"ExecuteStatement(name={self.name}{params_str})"


class DeallocateStatement(Statement):
    """释放预处理语句"""

    def __init__(self, name: str):
        self.name = name

    def __repr__(self):
        return f"DeallocateStatement(name={self.name})"


# 添加视图支持
class CreateViewStatement(Statement):
    """创建视图语句"""

    def __init__(self, view_name: str, select_statement: SelectStatement,
                 if_not_exists: bool = False, temporary: bool = False,
                 columns: Optional[List[str]] = None):
        self.view_name = view_name
        self.select_statement = select_statement
        self.if_not_exists = if_not_exists
        self.temporary = temporary
        self.columns = columns if columns else []

    def __repr__(self):
        exists_str = " IF NOT EXISTS" if self.if_not_exists else ""
        temp_str = " TEMPORARY" if self.temporary else ""
        columns_str = f"({', '.join(self.columns)})" if self.columns else ""
        return f"CreateViewStatement({temp_str}view={self.view_name}{exists_str}{columns_str}, as={self.select_statement})"