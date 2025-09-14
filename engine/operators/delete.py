from typing import Any, List, Tuple, Dict, Optional

from engine.exceptions import TableNotFoundError
from engine.operators import FilterOperator
from engine.storage_engine import StorageEngine
from sql.ast import Operator, Expression, Column, Literal, BinaryExpression, DataType, ColumnConstraint, ColumnDefinition


class DeleteOperator(Operator):
    """DELETE 操作的执行算子"""

    def __init__(self, table_name: str, child: Operator, storage_engine: StorageEngine, executor: Any,
                 bplus_tree: Optional[Any] = None):
        self.table_name = table_name
        self.child = child  # 子算子（通常是 FilterOperator 或 SeqScanOperator）
        self.storage_engine = storage_engine
        self.executor = executor
        self.bplus_tree = bplus_tree  # 主键的B+树索引

    def execute(self) -> List[Any]:
        """
        [ATOMICITY FIX] 修复了删除操作的原子性问题。
        现在的操作顺序是：先删除索引，再删除数据。
        这避免了在索引删除失败时，留下一个指向已删除数据的“悬空指针”。
        """
        deleted_count = 0

        try:
            schema = self.storage_engine.catalog_page.get_table_metadata(self.table_name)['schema']
        except (TypeError, KeyError):
            raise TableNotFoundError(self.table_name)

        rows_to_delete: List[Tuple[Tuple[int, int], Dict[str, Any]]] = self.executor.execute(self.child)

        for rid, row_data_dict in rows_to_delete:
            try:
                # --- [核心修复] 操作重排：先处理索引，再处理数据 ---

                # 步骤 1: 如果存在索引，先从索引中删除条目
                if self.bplus_tree:
                    pk_col_def, _ = self.storage_engine._get_pk_info(schema)
                    pk_value = row_data_dict[pk_col_def.name]
                    pk_bytes_to_delete = self.storage_engine._prepare_key_for_b_tree(pk_value, pk_col_def.data_type)

                    root_changed = self.bplus_tree.delete(pk_bytes_to_delete)
                    if root_changed:
                        self.storage_engine.update_index_root(self.table_name, self.bplus_tree.root_page_id)

                # 步骤 2: 在索引成功删除后，再从数据页中删除记录
                success = self.storage_engine.delete_row_by_rid(self.table_name, rid)

                if success:
                    deleted_count += 1
            except Exception as e:
                # 如果在处理单行时发生错误（例如，B+树操作失败），
                # 记录错误并继续处理下一行，而不是让整个DELETE语句崩溃。
                # 一个更完整的系统会有更复杂的事务处理。
                print(f"警告：删除行 {rid} 时发生错误并已跳过: {e}")
                continue

        return [deleted_count]

    # --- 辅助方法 ---

    def _can_use_index(self) -> bool:
        """
        [逻辑修正] 判断 WHERE 条件是否是 `主键 = 常量` 的形式。
        正确的检查对象应该是子算子 FilterOperator 的 condition 属性。
        """
        # 检查子节点是否是 FilterOperator，并且其过滤条件是二元表达式
        if isinstance(self.child, FilterOperator) and isinstance(self.child.condition, BinaryExpression):
            condition = self.child.condition
            schema = self.storage_engine.catalog_page.get_table_metadata(self.table_name)['schema']
            pk_col_def, _ = self.storage_engine._get_pk_info(schema)

            # 检查是否是 `Column = Literal` 且该 Column 是主键
            if (isinstance(condition.left, Column) and condition.left.name == pk_col_def.name and
                    isinstance(condition.right, Literal) and condition.op.value == '='):
                return True
        return False

    def _extract_pk_value(self) -> Any:
        """从 WHERE 条件中提取主键的值。"""
        if self._can_use_index():
            # 值位于 FilterOperator 的 condition 表达式的右侧
            return self.child.condition.right.value
        return None

    def _deserialize_row_data(self, row_bytes: bytes, schema: Dict[str, ColumnDefinition]) -> Dict[str, Any]:
        """将字节流反序列化为字典形式的行数据。"""
        row_dict = {}
        offset = 0
        for col_def in schema.values():
            value, offset = self.storage_engine._decode_value(row_bytes, offset, col_def.data_type)
            row_dict[col_def.name] = value
        return row_dict
