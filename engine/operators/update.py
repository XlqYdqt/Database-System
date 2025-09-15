from typing import Dict, Any, List, Tuple, Optional
import struct

# [FIX] 移除了未使用的 FilterOperator 导入
# from engine.operators import FilterOperator
from engine.storage_engine import StorageEngine
# [FIX] 导入了逻辑计划节点 Filter，用于正确的类型检查
from sql.ast import Operator, Expression, Column, Literal, BinaryExpression, DataType, ColumnConstraint, \
    ColumnDefinition
from engine.exceptions import TableNotFoundError, PrimaryKeyViolationError
from sql.planner import Filter


class UpdateOperator(Operator):
    """UPDATE 操作的执行算子"""

    def __init__(self, table_name: str, child: Operator, updates: List[Tuple[str, Expression]],
                 storage_engine: StorageEngine, executor: Any, bplus_tree: Optional[Any] = None,
                 txn_id: Optional[int] = None):
        self.table_name = table_name
        self.child = child
        self.updates = updates
        self.storage_engine = storage_engine
        self.executor = executor
        self.bplus_tree = bplus_tree
        self.txn_id = txn_id

    def execute(self) -> List[Any]:
        """
        支持事务的 UPDATE：
        - 非事务模式 (txn_id is None)：立即更新数据和索引
        - 事务模式 (txn_id is not None)：只记录日志，延迟到 COMMIT 时应用
        """
        updated_count = 0

        try:
            schema = self.storage_engine.catalog_page.get_table_metadata(self.table_name)['schema']
        except (TypeError, KeyError):
            raise TableNotFoundError(self.table_name)

        rows_to_update: List[Tuple[Tuple[int, int], Dict[str, Any]]] = []

        # ============= 1. 优先用索引，否则全表扫描 =============
        if self.bplus_tree and self._can_use_index():
            pk_value = self._extract_pk_value()
            if pk_value is not None:
                pk_col_def, _ = self.storage_engine._get_pk_info(schema)
                pk_bytes = self.storage_engine._prepare_key_for_b_tree(pk_value, pk_col_def.data_type)

                rid = self.bplus_tree.search(pk_bytes)
                if rid:
                    row_data_bytes = self.storage_engine.read_row(self.table_name, rid)
                    if row_data_bytes:
                        row_data_dict = self._deserialize_row_data(row_data_bytes, schema)
                        rows_to_update.append((rid, row_data_dict))
        else:
            rows_to_update = self.executor.execute([self.child])

        # ============= 2. 逐行更新 =============
        for original_rid, original_row_dict in rows_to_update:
            try:
                # 构造新行
                new_row_dict = dict(original_row_dict)
                for col_name, expr in self.updates:
                    new_row_dict[col_name] = self._eval_expr(expr, original_row_dict)

                # 主键相关
                pk_col_def, _ = self.storage_engine._get_pk_info(schema)
                pk_col_name = pk_col_def.name
                old_pk_value = original_row_dict[pk_col_name]
                new_pk_value = new_row_dict[pk_col_name]
                pk_changed = (old_pk_value != new_pk_value)

                # 序列化新数据
                new_row_data_bytes = self._serialize_row_data(new_row_dict, schema)

                # 🚩 分事务模式 & 非事务模式处理
                if self.txn_id is not None:
                    # 事务模式：只写入日志，不实际更新
                    old_row_data_bytes = self._serialize_row_data(original_row_dict, schema)
                    self.storage_engine.txn_manager.add_write_set(
                        self.txn_id,
                        self.table_name,
                        original_rid,
                        old_data=old_row_data_bytes,
                        new_data=new_row_data_bytes,
                    )
                else:
                    # 非事务模式：立即更新数据
                    new_rid = self.storage_engine.update_row_by_rid(
                        self.table_name, original_rid, new_row_data_bytes
                    )
                    if new_rid is None:
                        print(f"警告: 更新 RID {original_rid} 失败，已跳过。")
                        continue

                    row_moved = (original_rid != new_rid)

                    # 处理 B+ 树索引
                    if self.bplus_tree and (pk_changed or row_moved):
                        old_pk_bytes = self.storage_engine._prepare_key_for_b_tree(old_pk_value, pk_col_def.data_type)
                        new_pk_bytes = self.storage_engine._prepare_key_for_b_tree(new_pk_value, pk_col_def.data_type)

                        try:
                            self.bplus_tree.delete(old_pk_bytes)
                            insert_result = self.bplus_tree.insert(new_pk_bytes, new_rid)

                            if insert_result is None:
                                raise PrimaryKeyViolationError(new_pk_value)

                            if insert_result:  # root_changed
                                self.storage_engine.update_index_root(self.table_name, self.bplus_tree.root_page_id)

                        except Exception as e:
                            # 回滚索引和行
                            self.bplus_tree.insert(old_pk_bytes, original_rid)
                            original_row_data_bytes = self._serialize_row_data(original_row_dict, schema)
                            self.storage_engine.update_row_by_rid(self.table_name, new_rid, original_row_data_bytes)
                            raise e

                updated_count += 1
            except Exception as e:
                print(f"警告：更新行 {original_row_dict} 时发生错误并已跳过: {e}")
                continue

        return [updated_count]

    def _eval_expr(self, expr: Expression, row: Dict[str, Any]) -> Any:
        """递归地对表达式求值。"""
        if isinstance(expr, Literal): return expr.value
        if isinstance(expr, Column): return row.get(expr.name)
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
        return expr

    def _can_use_index(self) -> bool:
        """判断 WHERE 条件是否是 `主键 = 常量` 的形式，从而决定能否使用索引。"""
        # [FIX] 修复了类型检查的 bug。
        # 之前错误地检查了物理算子 FilterOperator，现在正确地检查逻辑计划节点 Filter。
        if isinstance(self.child, Filter) and isinstance(self.child.condition, BinaryExpression):
            condition = self.child.condition
            try:
                schema = self.storage_engine.catalog_page.get_table_metadata(self.table_name)['schema']
                pk_col_name = self.storage_engine._get_pk_info(schema)[0].name
            except (TypeError, KeyError, ValueError):
                return False

            if (isinstance(condition.left, Column) and condition.left.name == pk_col_name and
                    isinstance(condition.right, Literal) and condition.op.value == '='):
                return True
        return False

    def _extract_pk_value(self) -> Any:
        """从 WHERE 条件中提取主键的值。"""
        if self._can_use_index():
            return self.child.condition.right.value
        return None

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
            # --- [BUG FIX] ---
            # 添加了对 FLOAT 类型的处理，之前缺失导致数据序列化不完整。
            elif col_def.data_type == DataType.FLOAT:
                row_data.extend(struct.pack("<f", float(val)))
            # --- [END FIX] ---
        return bytes(row_data)

    def _deserialize_row_data(self, row_bytes: bytes, schema: Dict[str, ColumnDefinition]) -> Dict[str, Any]:
        """将字节流反序列化为字典形式的行数据。"""
        row_dict = {}
        offset = 0
        for col_def in schema.values():
            value, offset = self.storage_engine._decode_value(row_bytes, offset, col_def.data_type)
            row_dict[col_def.name] = value
        return row_dict
