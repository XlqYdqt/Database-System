import struct
from typing import Any, List, Tuple, Dict, Optional

from engine.storage_engine import StorageEngine
from sql.ast import Operator, Expression, Column, Literal, BinaryExpression, DataType, ColumnConstraint, \
    ColumnDefinition
from engine.exceptions import TableNotFoundError
from sql.planner import Filter


class DeleteOperator(Operator):
    """DELETE 操作的执行算子"""

    def __init__(self, table_name: str, child: Operator, storage_engine: StorageEngine, executor: Any,
                 bplus_tree: Optional[Any] = None, txn_id: Optional[int] = None):
        self.table_name = table_name
        self.child = child
        self.storage_engine = storage_engine
        self.executor = executor
        self.bplus_tree = bplus_tree
        self.txn_id = txn_id

    def execute(self) -> List[Any]:
        """
        [INDEX-SEEK FIX] 修复了 DELETE 未使用索引的问题。
        现在会优先检查是否能使用 B+ 树索引进行点查询，
        如果不行，再回退到全表扫描。
        """
        deleted_count = 0

        try:
            schema = self.storage_engine.catalog_page.get_table_metadata(self.table_name)['schema']
        except (TypeError, KeyError):
            raise TableNotFoundError(self.table_name)

        rows_to_delete: List[Tuple[Tuple[int, int], Dict[str, Any]]] = []

        # --- [核心修复] 索引扫描优化路径 ---
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
                        rows_to_delete.append((rid, row_data_dict))
        else:
            # --- 回退路径：全表扫描 ---
            rows_to_delete = self.executor.execute([self.child])

        # 后续的删除逻辑
        for rid, row_data_dict in rows_to_delete:
            pk_col_def, _ = self.storage_engine._get_pk_info(schema)
            pk_value = row_data_dict[pk_col_def.name]
            pk_bytes_to_delete = self.storage_engine._prepare_key_for_b_tree(pk_value, pk_col_def.data_type)
            index_deleted = False

            try:
                if self.txn_id is not None:
                    # 🚩 事务模式：只写日志，不立即修改索引和数据
                    old_row_data_bytes = self._serialize_row_data(row_data_dict, schema)
                    self.storage_engine.txn_manager.add_write_set(
                        self.txn_id,
                        self.table_name,
                        rid,
                        old_data=old_row_data_bytes,
                        new_data=None
                    )
                else:
                # 🚩 非事务模式：立即删除索引 + 数据
                # 1. 先删除索引
                    if self.bplus_tree:
                        root_changed = self.bplus_tree.delete(pk_bytes_to_delete)
                        index_deleted = True
                        if root_changed:
                            self.storage_engine.update_index_root(self.table_name, self.bplus_tree.root_page_id)

                # 2. 再删除数据行
                    success = self.storage_engine.delete_row_by_rid(self.table_name, rid, self.txn_id)

                    if success:
                        deleted_count += 1
            except Exception as e:
                # 【ATOMICITY FIX】如果第2步（删除数据）失败，但第1步（删除索引）已成功
                # 那么我们需要回滚第1步的操作，以保证数据一致性。
                if index_deleted:
                    # 尝试将刚才删除的索引重新插回去
                    self.bplus_tree.insert(pk_bytes_to_delete, rid)

                print(f"警告：删除行 {rid} 时发生错误并已跳过（已尝试回滚索引）: {e}")
                continue

        return [deleted_count]

    def _can_use_index(self) -> bool:
        """判断 WHERE 条件是否是 `主键 = 常量` 的形式。"""
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
