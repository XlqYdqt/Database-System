from typing import Dict, List, Optional
from .ast import *
from .parser import Parser

class SemanticError(Exception):
    """语义错误异常"""
    pass


class SemanticAnalyzer:
    """语义分析器，用于分析SQL语句的语义正确性"""

    def __init__(self):
        # 存储表的元数据：表名 -> 列定义列表
        self.tables: Dict[str, List[ColumnDefinition]] = {}
        # 存储索引的元数据：表名 -> 索引定义列表
        self.indexes: Dict[str, List[CreateIndexStatement]] = {}
        # 存储角色的权限信息：角色名 -> 授权信息列表
        self.roles: Dict[str, List[GrantStatement]] = {}

    def analyze(self, statement: Statement) -> Statement:
        """分析单条SQL语句的语义正确性"""
        # 根据SQL语句类型调用对应的分析方法
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
        elif isinstance(statement, CreateIndexStatement):
            return self.analyze_create_index(statement)
        elif isinstance(statement, GrantStatement):
            return self.analyze_grant(statement)
        elif isinstance(statement, GrantRoleStatement):
            return self.analyze_grant_role(statement)
        elif isinstance(statement, RevokeStatement):
            return self.analyze_revoke(statement)
        elif isinstance(statement, TransactionStatement):
            return self.analyze_transaction(statement)
        elif isinstance(statement, ExplainStatement):
            return self.analyze_explain(statement)
        elif isinstance(statement, LockStatement):
            return self.analyze_lock(statement)
        elif isinstance(statement, CreateRoleStatement):
            return self.analyze_create_role(statement)
        else:
            raise SemanticError(f"不支持的语句类型: {type(statement).__name__}")

    def analyze_create_table(self, statement: CreateTableStatement) -> CreateTableStatement:
        """分析CREATE TABLE语句"""
        table_name = statement.table_name

        # # 检查表是否已存在
        # if table_name in self.tables:
        #     raise SemanticError(f"表 '{table_name}' 已存在")

        # 检查列定义
        column_names = set()
        primary_key_count = 0

        for column in statement.columns:
            # 检查列名是否重复
            if column.name in column_names:
                raise SemanticError(f"列名 '{column.name}' 重复")
            column_names.add(column.name)

            # 检查主键约束数量
            for constraint, _ in column.constraints:
                if constraint == ColumnConstraint.PRIMARY_KEY:
                    primary_key_count += 1

        # 检查主键约束数量是否符合规则
        if primary_key_count > 1:
            raise SemanticError("目前只支持单个主键")

        # 保存表的元数据
        self.tables[table_name] = statement.columns

        return statement

    def analyze_insert(self, statement: InsertStatement) -> InsertStatement:
        """分析INSERT语句"""
        table_name = statement.table_name
        #
        # # 检查目标表是否存在
        # if table_name not in self.tables:
        #     raise SemanticError(f"表 '{table_name}' 不存在")
        #
        # table_columns = self.tables[table_name]
        #
        # # 检查列名是否存在
        # if statement.columns:
        #     for column_name in statement.columns:
        #         if not any(col.name == column_name for col in table_columns):
        #             raise SemanticError(f"列 '{column_name}' 在表 '{table_name}' 中不存在")
        # else:
        #     # 没有指定列名时，默认使用所有列
        #     statement.columns = [col.name for col in table_columns]
        #
        # # 检查插入值数量是否与列数量一致
        # if len(statement.values) != len(statement.columns):
        #     raise SemanticError(f"值数量({len(statement.values)})与列数量({len(statement.columns)})不匹配")
        #
        # # 检查值类型是否匹配列类型
        # for value, column_name in zip(statement.values, statement.columns):
        #     column_def = next((col for col in table_columns if col.name == column_name), None)
        #     if not column_def:
        #         raise SemanticError(f"列 '{column_name}' 不存在")
        #
        #     if not self._check_type_compatibility(value, column_def.data_type):
        #         raise SemanticError(f"值 '{value}' 的类型与列 '{column_name}' 的类型不匹配")

        return statement

    def analyze_select(self, statement: SelectStatement) -> SelectStatement:
        """分析SELECT语句"""
        table_name = statement.table_name

        # 检查表是否存在
        if table_name not in self.tables:
            raise SemanticError(f"表 '{table_name}' 不存在")

        table_columns = self.tables[table_name]

        # 检查选择的列是否存在
        for column_expr in statement.columns:
            self._check_column_expression(column_expr, table_name, table_columns)

        # 检查WHERE条件中的表达式
        if statement.where:
            self._check_expression(statement.where, table_name, table_columns)

        # 检查ORDER BY中的列是否存在
        for column in statement.order_by:
            self._check_column_reference(column, table_name, table_columns)

        return statement

    def analyze_update(self, statement: UpdateStatement) -> UpdateStatement:
        """分析UPDATE语句"""
        table_name = statement.table_name

        # 检查目标表是否存在
        if table_name not in self.tables:
            raise SemanticError(f"表 '{table_name}' 不存在")

        table_columns = self.tables[table_name]

        # 检查赋值操作是否合法
        for column_name, expression in statement.assignments.items():
            if not any(col.name == column_name for col in table_columns):
                raise SemanticError(f"列 '{column_name}' 在表 '{table_name}' 中不存在")

            column_def = next(col for col in table_columns if col.name == column_name)
            if not self._check_type_compatibility(expression, column_def.data_type):
                raise SemanticError(f"表达式类型与列 '{column_name}' 的类型不匹配")

        # 检查WHERE条件中的表达式
        if statement.where:
            self._check_expression(statement.where, table_name, table_columns)

        return statement

    def analyze_delete(self, statement: DeleteStatement) -> DeleteStatement:
        """分析DELETE语句"""
        table_name = statement.table_name

        # 检查目标表是否存在
        if table_name not in self.tables:
            raise SemanticError(f"表 '{table_name}' 不存在")

        table_columns = self.tables[table_name]

        # 检查WHERE条件是否正确
        if statement.where:
            self._check_expression(statement.where, table_name, table_columns)

        return statement

    def analyze_create_index(self, statement: CreateIndexStatement) -> CreateIndexStatement:
        """分析CREATE INDEX语句"""
        table_name = statement.table_name

        # 检查目标表是否存在
        if table_name not in self.tables:
            raise SemanticError(f"表 '{table_name}' 不存在")

        table_columns = self.tables[table_name]

        # 检查索引的列是否存在
        for column_name in statement.columns:
            if not any(col.name == column_name for col in table_columns):
                raise SemanticError(f"列 '{column_name}' 不存在于表 '{table_name}' 中")

        # 检查索引是否重复定义
        if table_name in self.indexes and any(idx.index_name == statement.index_name for idx in self.indexes[table_name]):
            raise SemanticError(f"索引 '{statement.index_name}' 已存在")

        # 保存索引元数据
        if table_name not in self.indexes:
            self.indexes[table_name] = []
        self.indexes[table_name].append(statement)

        return statement

    def analyze_grant(self, statement: GrantStatement) -> GrantStatement:
        """分析GRANT语句"""
        # 检查权限的合法性
        for privilege in statement.privileges:
            if privilege not in Privilege:
                raise SemanticError(f"无效的权限类型: {privilege.name}")

        # 更新角色权限信息
        for grantee in statement.grantees:
            self.roles.setdefault(grantee, []).append(statement)

        return statement

    def analyze_grant_role(self, statement: GrantRoleStatement) -> GrantRoleStatement:
        """分析GRANT ROLE语句"""
        # 检查要授予的角色是否存在
        for role in statement.roles:
            if role not in self.roles:
                raise SemanticError(f"角色 '{role}' 不存在")

        # 检查被授予者是否存在（这里假设被授予者也需要是已存在的角色）
        for grantee in statement.grantees:
            if grantee not in self.roles:
                raise SemanticError(f"被授予者 '{grantee}' 不存在")

        return statement
    def analyze_revoke(self, statement: RevokeStatement) -> RevokeStatement:
        """分析REVOKE语句"""
        # 检查被撤销权限的有效性
        for grantee in statement.grantees:
            if grantee not in self.roles:
                raise SemanticError(f"角色 '{grantee}' 不存在或没有相关权限")

        return statement

    def analyze_transaction(self, statement: TransactionStatement) -> TransactionStatement:
        """分析事务控制语句"""
        if statement.command not in TransactionCommand:
            raise SemanticError(f"不支持的事务操作: {statement.command}")

        return statement

    def analyze_explain(self, statement: ExplainStatement) -> ExplainStatement:
        """分析EXPLAIN语句"""
        # 检查要解释的语句的类型是否支持
        if not isinstance(statement.statement, (SelectStatement, InsertStatement, UpdateStatement, DeleteStatement)):
            raise SemanticError("EXPLAIN命令只支持SELECT、INSERT、UPDATE、DELETE等语句")

        # 执行进一步的分析
        self.analyze(statement.statement)
        return statement

    def analyze_lock(self, statement: LockStatement) -> LockStatement:
        """分析LOCK语句"""
        # 检查表是否存在
        for table_name in statement.table_names:
            if table_name not in self.tables:
                raise SemanticError(f"表 '{table_name}' 不存在")

        # 检查锁模式
        if statement.mode not in LockMode:
            raise SemanticError(f"无效的锁模式: {statement.mode}")

        return statement

    def analyze_create_role(self, statement: CreateRoleStatement) -> CreateRoleStatement:
        role_name = statement.role_name
        # 检查角色是否已存在
        if role_name in self.roles:
            raise SemanticError(f"角色 '{role_name}' 已存在")

        # 添加角色
        self.roles[role_name] = []

        return statement
    def _check_column_expression(self, expr: Expression, table_name: str, table_columns: List[ColumnDefinition]):
        """检查选择列表达式是否正确"""
        if isinstance(expr, Column):
            self._check_column_reference(expr, table_name, table_columns)

    def _check_expression(self, expr: Expression, table_name: str, table_columns: List[ColumnDefinition]):
        """检查表达式是否合法"""
        if isinstance(expr, Column):
            self._check_column_reference(expr, table_name, table_columns)

    def _check_column_reference(self, column: Column, table_name: str, table_columns: List[ColumnDefinition]):
        """检查列引用是否存在"""
        if column.name != '*' and not any(col.name == column.name for col in table_columns):
            raise SemanticError(f"列 '{column.name}' 不存在于表 '{table_name}' 中")

    def _check_type_compatibility(self, expr: Expression, expected_type: DataType) -> bool:
        """检查表达式类型是否与预期类型兼容"""
        if isinstance(expr, Literal):
            return expr.data_type == expected_type
        return True
