from typing import Dict, Any, List, Tuple, Optional

from engine.operators import FilterOperator
from engine.storage_engine import StorageEngine
from sql.ast import Operator, Expression, Column, Literal, BinaryExpression, DataType, ColumnConstraint, \
    ColumnDefinition


class UpdateOperator(Operator):
    """UPDATE 操作的执行算子"""

    def __init__(self, table_name: str, child: Operator, updates: List[Tuple[str, Expression]],
                 storage_engine: StorageEngine, executor: Any, bplus_tree: Optional[Any] = None):
        self.table_name = table_name
        self.child = child  # 子算子（通常是 FilterOperator 或 SeqScanOperator）
        self.updates = updates  # 要更新的列和新值的表达式列表, e.g., [('age', Literal(30))]
        self.storage_engine = storage_engine
        self.executor = executor
        self.bplus_tree = bplus_tree  # 主键的B+树索引

    def execute(self) -> List[Any]:
        """
        [ATOMICITY FIX] 修复了更新操作的原子性问题。
        现在，如果更新主键时发生索引冲突，会执行回滚操作，
        撤销对数据页的修改，从而保证数据的一致性。
        """
        updated_count = 0

        try:
            schema = self.storage_engine.catalog_page.get_table_metadata(self.table_name)['schema']
        except (TypeError, KeyError):
            raise TableNotFoundError(self.table_name)

        rows_to_update: List[Tuple[Tuple[int, int], Dict[str, Any]]] = self.executor.execute(self.child)

        for original_rid, original_row_dict in rows_to_update:
            try:
                # 步骤 1: 计算更新后的新行数据
                new_row_dict = dict(original_row_dict)
                for col_name, expr in self.updates:
                    new_row_dict[col_name] = self._eval_expr(expr, original_row_dict)

                # 步骤 2: 检查主键是否被修改
                pk_col_def, _ = self.storage_engine._get_pk_info(schema)
                pk_col_name = pk_col_def.name
                old_pk_value = original_row_dict[pk_col_name]
                new_pk_value = new_row_dict[pk_col_name]
                pk_changed = (old_pk_value != new_pk_value)

                # --- [核心修复] 预检查与回滚逻辑 ---

                # 步骤 3: 如果主键改变，进行预检查
                if pk_changed and self.bplus_tree:
                    new_pk_bytes = self.storage_engine._prepare_key_for_b_tree(new_pk_value, pk_col_def.data_type)
                    # 检查新主键是否已存在
                    if self.bplus_tree.search(new_pk_bytes) is not None:
                        raise PrimaryKeyViolationError(new_pk_value)

                # 步骤 4: 执行数据页更新
                new_row_data_bytes = self._serialize_row_data(new_row_dict, schema)
                new_rid = self.storage_engine.update_row_by_rid(self.table_name, original_rid, new_row_data_bytes)
                if new_rid is None:
                    print(f"警告: 更新 RID {original_rid} 失败，已跳过。")
                    continue

                row_moved = (original_rid != new_rid)

                # 步骤 5: 更新索引（如果需要）
                if self.bplus_tree and (pk_changed or row_moved):
                    old_pk_bytes = self.storage_engine._prepare_key_for_b_tree(old_pk_value, pk_col_def.data_type)
                    new_pk_bytes = self.storage_engine._prepare_key_for_b_tree(new_pk_value, pk_col_def.data_type)

                    try:
                        # 先删除旧索引，再插入新索引
                        self.bplus_tree.delete(old_pk_bytes)
                        insert_result = self.bplus_tree.insert(new_pk_bytes, new_rid)

                        # 如果插入失败（例如，在并发环境下刚巧被其他事务插入），则执行回滚
                        if insert_result is None:
                            raise PrimaryKeyViolationError(new_pk_value)

                        if insert_result:  # root_changed
                            self.storage_engine.update_index_root(self.table_name, self.bplus_tree.root_page_id)

                    except Exception as e:
                        # [回滚逻辑] 如果索引更新失败，必须回滚所有操作
                        # 1. 尝试将旧的索引条目重新插回
                        self.bplus_tree.insert(old_pk_bytes, original_rid)
                        # 2. 将数据页上的行数据也回滚到原始状态
                        original_row_data_bytes = self._serialize_row_data(original_row_dict, schema)
                        self.storage_engine.update_row_by_rid(new_rid, original_row_data_bytes)
                        # 3. 将原始错误重新抛出
                        raise e

                updated_count += 1
            except Exception as e:
                print(f"警告：更新行 {original_row_dict} 时发生错误并已跳过: {e}")
                continue

        return [updated_count]

    def _eval_expr(self, expr: Expression, row: Dict[str, Any]) -> Any:
        """递归地对表达式求值。"""
        if isinstance(expr, Literal): return expr.value
        if isinstance(expr, Column): return row[expr.name]
        if isinstance(expr, BinaryExpression):
            left_val = self._eval_expr(expr.left, row)
            right_val = self._eval_expr(expr.right, row)
            op_map = {"=": lambda a, b: a == b, ">": lambda a, b: a > b, "<": lambda a, b: a < b,
                      ">=": lambda a, b: a >= b, "<=": lambda a, b: a <= b, "!=": lambda a, b: a != b,
                      "<>": lambda a, b: a != b}
            op_val = getattr(expr, "op", getattr(expr, "operator", None)).value
            if op_val in op_map:
                return op_map[op_val](left_val, right_val)
            raise NotImplementedError(f"不支持的二元运算符: {op_val}")
        return expr  # 对于无法解析的表达式，直接返回

    def _can_use_index(self) -> bool:
        """判断 WHERE 条件是否是 `主键 = 常量` 的形式，从而决定能否使用索引。"""
        if isinstance(self.child, FilterOperator) and isinstance(self.child.condition, BinaryExpression):
            condition = self.child.condition
            schema = self.storage_engine.catalog_page.get_table_metadata(self.table_name)['schema']
            pk_col_name = self.storage_engine._get_pk_info(schema)[0].name

            # 检查是否是 `Column = Literal` 且该 Column 是主键
            if (isinstance(condition.left, Column) and condition.left.name == pk_col_name and
                    isinstance(condition.right, Literal) and condition.op.value == '='):
                return True
        return False

    def _extract_pk_value(self) -> Any:
        """从 WHERE 条件中提取主键的值。"""
        if self._can_use_index():
            return self.child.condition.right.value
        return None

    # 辅助函数：行数据的序列化和反序列化
    def _serialize_row_data(self, row_dict: Dict[str, Any], schema: Dict[str, ColumnDefinition]) -> bytes:
        """将字典形式的行数据序列化为字节流。"""
        row_data = bytearray()
        for col_def in schema.values():
            val = row_dict[col_def.name]
            if col_def.data_type == DataType.INT:
                row_data.extend(int(val).to_bytes(4, "little", signed=True))
            elif col_def.data_type in (DataType.TEXT, DataType.STRING):
                encoded = str(val).encode("utf-8")
                row_data.extend(len(encoded).to_bytes(4, "little"))
                row_data.extend(encoded)
        return bytes(row_data)

    def _deserialize_row_data(self, row_bytes: bytes, schema: Dict[str, ColumnDefinition]) -> Dict[str, Any]:
        """将字节流反序列化为字典形式的行数据。"""
        row_dict = {}
        offset = 0
        for col_def in schema.values():
            value, offset = self.storage_engine._decode_value(row_bytes, offset, col_def.data_type)
            row_dict[col_def.name] = value
        return row_dict