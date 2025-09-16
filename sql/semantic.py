from typing import Dict, List, Optional
from .ast import *
from .parser import Parser
from engine.catalog_page import CatalogPage  # 导入CatalogPage

class SemanticError(Exception):
    """语义错误异常"""
    pass


class SemanticAnalyzer:
    """语义分析器，用于分析SQL语句的语义正确性"""

    def __init__(self, catalog: CatalogPage):
        # 使用传入的CatalogPage实例作为元数据源
        self.catalog = catalog
        self.current_transaction = False  # 当前是否在事务中
        self.savepoints = set()  # 当前事务中的保存点
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
        elif isinstance(statement, DropIndexStatement):  # 添加对DROP INDEX的支持
            return self.analyze_drop_index(statement)
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
        elif isinstance(statement, CreateViewStatement):#尚未完成
            return self.analyze_create_view(statement)
        else:
            raise SemanticError(f"语义不支持的语句类型: {type(statement).__name__}")

    def analyze_create_table(self, statement: CreateTableStatement) -> CreateTableStatement:
        """分析CREATE TABLE语句"""
        table_name = statement.table_name

        # 检查表是否已存在（使用CatalogPage）
        if table_name in self.catalog.tables:
            raise SemanticError(f"表 '{table_name}' 已存在")

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

        # 不再在语义分析器中保存表的元数据，这将在执行阶段由CatalogPage处理
        return statement

    def analyze_insert(self, statement: InsertStatement) -> InsertStatement:
        """分析INSERT语句"""
        table_name = statement.table_name

        # 检查目标表是否存在（使用CatalogPage）
        if table_name not in self.catalog.tables:
            raise SemanticError(f"表 '{table_name}' 不存在")

        # 从CatalogPage获取表结构
        table_metadata = self.catalog.get_table_metadata(table_name)
        table_columns = list(table_metadata['schema'].values())

        # 检查列名是否存在
        if statement.columns:
            for column_name in statement.columns:
                if column_name not in table_metadata['schema']:
                    raise SemanticError(f"列 '{column_name}' 在表 '{table_name}' 中不存在")
        else:
            # 没有指定列名时，默认使用所有列
            statement.columns = [col.name for col in table_columns]

        # 检查是VALUES插入还是SELECT插入
        if statement.values:
            # 检查每组插入值数量是否与列数量一致
            for values in statement.values:
                if len(values) != len(statement.columns):
                    raise SemanticError(f"值数量({len(values)})与列数量({len(statement.columns)})不匹配")

                # 检查值类型是否匹配列类型
                for value, column_name in zip(values, statement.columns):
                    column_def = table_metadata['schema'].get(column_name)
                    if not column_def:
                        raise SemanticError(f"列 '{column_name}' 不存在")

                    if not self._check_type_compatibility(value, column_def.data_type):
                        raise SemanticError(f"值 '{value}' 的类型与列 '{column_name}' 的类型不匹配")
        elif statement.select_stmt:
            # 分析INSERT...SELECT语句
            self.analyze_select(statement.select_stmt)
        else:
            raise SemanticError("INSERT语句必须包含VALUES或SELECT子句")

        return statement

    def analyze_select(self, statement: SelectStatement,
                       outer_tables: Optional[Dict[str, List[ColumnDefinition]]] = None) -> SelectStatement:
        """分析SELECT语句，支持外部查询表引用"""

        # 如果指定了表名，检查表是否存在（使用CatalogPage）
        if statement.table_name and statement.table_name not in self.catalog.tables:
            if not outer_tables or statement.table_name not in outer_tables:
                raise SemanticError(f"表 '{statement.table_name}' 不存在")
            table_columns = outer_tables[statement.table_name]
        else:
            table_metadata = self.catalog.get_table_metadata(statement.table_name) if statement.table_name else None
            table_columns = list(table_metadata['schema'].values()) if table_metadata else []

        # 合并外部表和当前查询的表
        all_tables = {}
        if outer_tables:
            all_tables.update(outer_tables)
        if statement.table_name:
            all_tables[statement.table_name] = table_columns

        # 检查选择的列是否存在
        for column_expr in statement.columns:
            self._check_column_expression(column_expr, statement.table_name, table_columns, all_tables)

        # 检查 WHERE 条件中的表达式
        if statement.where:
            self._check_expression(statement.where, statement.table_name, table_columns, all_tables)

        # 检查 ORDER BY 子句有效性
        if statement.order_by:
            for order_by_clause in statement.order_by:
                self._check_expression(order_by_clause.expression, statement.table_name, table_columns, all_tables)
                if order_by_clause.direction.upper() not in {'ASC', 'DESC'}:
                    raise SemanticError(f"ORDER BY 子句中的方向非法: {order_by_clause.direction}")

        # 检查 GROUP BY 子句
        if statement.group_by:
            for group_expr in statement.group_by:
                self._check_expression(group_expr, statement.table_name, table_columns, all_tables)

        # 检查 HAVING 子句
        if statement.having:
            self._check_expression(statement.having, statement.table_name, table_columns, all_tables)

        # ✅ 对于每个 JOIN 子句，检查表和条件，并加入 all_tables
        for join_clause in statement.joins:
            if isinstance(join_clause.table, SelectStatement):
                self.analyze_select(join_clause.table, all_tables)
            else:
                if join_clause.table not in self.catalog.tables:
                    raise SemanticError(f"JOIN表 '{join_clause.table}' 不存在")

                join_table_metadata = self.catalog.get_table_metadata(join_clause.table)
                join_table_columns = list(join_table_metadata['schema'].values())
                all_tables[join_clause.table] = join_table_columns  # ✅ 加入 JOIN 表

                if join_clause.condition:
                    self._check_expression(
                        join_clause.condition,
                        statement.table_name,
                        table_columns + join_table_columns,
                        all_tables
                    )

        return statement

    def analyze_update(self, statement: UpdateStatement) -> UpdateStatement:
        """分析UPDATE语句"""
        table_name = statement.table_name

        # 检查目标表是否存在（使用CatalogPage）
        if table_name not in self.catalog.tables:
            raise SemanticError(f"表 '{table_name}' 不存在")

        # 从CatalogPage获取表结构
        table_metadata = self.catalog.get_table_metadata(table_name)
        table_columns = list(table_metadata['schema'].values())

        # 检查赋值操作是否合法
        for column_name, expression in statement.assignments.items():
            if column_name not in table_metadata['schema']:
                raise SemanticError(f"列 '{column_name}' 在表 '{table_name}' 中不存在")

            column_def = table_metadata['schema'][column_name]
            if not self._check_type_compatibility(expression, column_def.data_type):
                raise SemanticError(f"表达式类型与列 '{column_name}' 的类型不匹配")

        # 检查WHERE条件中的表达式
        if statement.where:
            self._check_expression(statement.where, table_name, table_columns)

        return statement

    def analyze_delete(self, statement: DeleteStatement) -> DeleteStatement:
        """分析DELETE语句"""
        table_name = statement.table_name

        # 检查目标表是否存在（使用CatalogPage）
        if table_name not in self.catalog.tables:
            raise SemanticError(f"表 '{table_name}' 不存在")

        # 从CatalogPage获取表结构
        table_metadata = self.catalog.get_table_metadata(table_name)
        table_columns = list(table_metadata['schema'].values())

        # 检查WHERE条件是否正确
        if statement.where:
            self._check_expression(statement.where, table_name, table_columns)

        return statement

    def analyze_create_index(self, statement: CreateIndexStatement) -> CreateIndexStatement:
        """分析CREATE INDEX语句"""
        table_name = statement.table_name

        # 检查目标表是否存在（使用CatalogPage）
        if table_name not in self.catalog.tables:
            raise SemanticError(f"表 '{table_name}' 不存在")

        # 从CatalogPage获取表结构
        table_metadata = self.catalog.get_table_metadata(table_name)
        table_columns = list(table_metadata['schema'].values())

        # 检查索引的列是否存在
        for column_name in statement.columns:
            if column_name not in table_metadata['schema']:
                raise SemanticError(f"列 '{column_name}' 不存在于表 '{table_name}' 中")

        # 注意：CatalogPage目前不支持索引元数据存储
        # 这里暂时保留原有的索引检查逻辑，但需要调整实现方式
        # 或者可以将索引信息也存储在CatalogPage中

        return statement

    def analyze_drop_index(self, statement: DropIndexStatement) -> DropIndexStatement:
        """分析DROP INDEX语句"""
        # 在实际实现中，您可能需要检查索引是否存在
        # 但由于CatalogPage目前不支持索引元数据存储，这里暂时不做检查
        return statement

    def analyze_create_view(self, statement: CreateViewStatement) -> CreateViewStatement:
        """分析CREATE VIEW语句"""
        # 检查视图是否已存在
        # 注意：您可能需要扩展CatalogPage以支持视图存储
        # 这里暂时不做检查

        # 分析视图的SELECT语句
        self.analyze_select(statement.select_statement)

        # 检查视图列名是否与SELECT语句的列匹配
        if statement.columns and len(statement.columns) != len(statement.select_statement.columns):
            raise SemanticError("视图列数量与SELECT语句列数量不匹配")

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

    # def begin_transaction(self):
    #     """开始一个新事务"""
    #     if self.current_transaction:
    #         raise SemanticError("不能嵌套事务")
    #     self.current_transaction = True
    #     self.savepoints.clear()
    #
    # def commit_transaction(self):
    #     """提交当前事务"""
    #     if not self.current_transaction:
    #         raise SemanticError("没有活动的事务可以提交")
    #     self.current_transaction = False
    #     self.savepoints.clear()
    #
    # def rollback_transaction(self, savepoint_name=None):
    #     """回滚事务或到指定保存点"""
    #     if savepoint_name and savepoint_name not in self.savepoints:
    #         raise SemanticError(f"保存点 '{savepoint_name}' 不存在")
    #
    #     if not savepoint_name and not self.current_transaction:
    #         raise SemanticError("没有活动的事务可以回滚")
    #
    #     # 如果回滚到保存点，只移除该保存点之后的所有保存点
    #     if savepoint_name:
    #         # 找到保存点位置并移除之后的所有保存点
    #         savepoints_list = sorted(self.savepoints)  # 假设保存点有顺序
    #         index = savepoints_list.index(savepoint_name)
    #         for sp in savepoints_list[index + 1:]:
    #             self.savepoints.remove(sp)
    #     else:
    #         # 完全回滚事务
    #         self.current_transaction = False
    #         self.savepoints.clear()
    #
    # def create_savepoint(self, savepoint_name):
    #     """创建保存点"""
    #     if not self.current_transaction:
    #         raise SemanticError("只能在事务中创建保存点")
    #
    #     if savepoint_name in self.savepoints:
    #         raise SemanticError(f"保存点 '{savepoint_name}' 已存在")
    #
    #     self.savepoints.add(savepoint_name)

    def analyze_explain(self, statement: ExplainStatement) -> ExplainStatement:
        """分析EXPLAIN语句"""
        if not isinstance(statement.statement, (SelectStatement, InsertStatement, UpdateStatement, DeleteStatement)):
            raise SemanticError("EXPLAIN命令只支持SELECT、INSERT、UPDATE、DELETE等语句")

        # 验证 statement 是否为正确的类型
        if isinstance(statement.statement, str):
            raise SemanticError(f"EXPLAIN中的语句类型无效: {statement.statement}")

        self.analyze(statement.statement)  # 分析目标语句
        return statement

    def analyze_lock(self, statement: LockStatement) -> LockStatement:
        """分析LOCK语句"""
        # 检查表是否存在（使用CatalogPage）
        for table_name in statement.table_names:
            if table_name not in self.catalog.tables:
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

    def _check_column_expression(self, expr: Expression, table_name: str, table_columns: List[ColumnDefinition], all_tables: Dict[str, List[ColumnDefinition]]):
        """检查选择列表达式是否正确"""
        if isinstance(expr, Column):
            self._check_column_reference(expr, table_name, table_columns,all_tables)

    def _check_expression(self, expr: Expression, table_name: str, table_columns: List[ColumnDefinition],
                          all_tables: Optional[Dict[str, List[ColumnDefinition]]] = None):
        """检查表达式是否合法，支持外部表引用"""
        if all_tables is None:
            all_tables = {table_name: table_columns} if table_name else {}

        if isinstance(expr, Column):
            self._check_column_reference(expr, table_name, table_columns, all_tables)
        elif isinstance(expr, SubqueryExpression):
            # 递归分析子查询，传递所有表信息
            self.analyze_select(expr.select_statement, all_tables)
        elif isinstance(expr, BinaryExpression):
            # 处理运算符（可能是字符串或Operator对象）
            if isinstance(expr.op, Operator):
                op_value = expr.op.value
            else:
                op_value = expr.op

            if op_value not in ['=', '!=', '<', '<=', '>', '>=', 'LIKE', 'ILIKE', 'IN', 'IS', 'IS NOT', 'BETWEEN',
                                'AND', 'OR']:
                raise SemanticError(f"不支持的运算符: {op_value}")

            self._check_expression(expr.left, table_name, table_columns, all_tables)
            self._check_expression(expr.right, table_name, table_columns, all_tables)
        elif isinstance(expr, UnaryExpression):
            # 处理一元运算符
            if isinstance(expr.op, Operator):
                op_value = expr.op.value
            else:
                op_value = expr.op

            if op_value not in ['NOT']:
                raise SemanticError(f"不支持的一元运算符: {op_value}")

            self._check_expression(expr.expression, table_name, table_columns, all_tables)
        elif isinstance(expr, FunctionCall):
            for arg in expr.arguments:
                self._check_expression(arg, table_name, table_columns, all_tables)
        elif isinstance(expr, CaseExpression):
            if expr.base_expression:
                self._check_expression(expr.base_expression, table_name, table_columns, all_tables)
            for condition, result in expr.when_then_pairs:
                self._check_expression(condition, table_name, table_columns, all_tables)
                self._check_expression(result, table_name, table_columns, all_tables)
            if expr.else_expression:
                self._check_expression(expr.else_expression, table_name, table_columns, all_tables)
        elif isinstance(expr, BetweenExpression):
            self._check_expression(expr.expression, table_name, table_columns, all_tables)
            self._check_expression(expr.lower, table_name, table_columns, all_tables)
            self._check_expression(expr.upper, table_name, table_columns, all_tables)
        elif isinstance(expr, InExpression):
            self._check_expression(expr.expression, table_name, table_columns, all_tables)
            if isinstance(expr.values, list):
                for value in expr.values:
                    self._check_expression(value, table_name, table_columns, all_tables)
            else:  # 子查询
                self.analyze_select(expr.values, all_tables)
        elif isinstance(expr, ExistsExpression):
            self.analyze_select(expr.subquery, all_tables)
        elif isinstance(expr, AliasExpression):
            self._check_expression(expr.expression, table_name, table_columns, all_tables)
        elif not isinstance(expr, Literal):  # Literal 不需要进一步检查
            raise SemanticError(f"不支持的表达式类型: {type(expr).__name__}")

    def _check_column_reference(self, column: Column, table_name: str, table_columns: List[ColumnDefinition],
                                all_tables: Dict[str, List[ColumnDefinition]]):
        """检查列引用是否存在，支持外部表引用"""
        if column.name == '*':
            return  # 通配符不需要进一步检查

        # 如果指定了表名，检查该表
        if column.table:
            if column.table not in all_tables:
                raise SemanticError(f"表 '{column.table}' 不存在")
            if not any(col.name == column.name for col in all_tables[column.table]):
                raise SemanticError(f"列 '{column.name}' 不存在于表 '{column.table}' 中")
        else:
            # 如果没有指定表名，检查所有表
            found = False
            for table_cols in all_tables.values():
                if any(col.name == column.name for col in table_cols):
                    found = True
                    break
            if not found:
                raise SemanticError(f"列 '{column.name}' 不存在于任何已知表中")
    def _check_type_compatibility(self, expr: Expression, expected_type: DataType) -> bool:
        """检查表达式类型是否与预期类型兼容"""
        if isinstance(expr, Literal):
            # 简单的类型兼容性检查
            if expr.data_type == expected_type:
                return True

            # 允许某些类型之间的隐式转换
            compatible_types = {
                DataType.INT: {DataType.FLOAT, DataType.DECIMAL},
                DataType.FLOAT: {DataType.DECIMAL},
                DataType.VARCHAR: {DataType.TEXT, DataType.CHAR},
                DataType.CHAR: {DataType.VARCHAR, DataType.TEXT},
            }

            if expected_type in compatible_types.get(expr.data_type, set()):
                return True

            return False
        return True  # 对于非字面量表达式，暂时假定类型兼容
