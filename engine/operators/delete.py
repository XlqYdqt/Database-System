from typing import Any, List, Tuple, Dict, Optional

from engine.storage_engine import StorageEngine
from sql.ast import Operator, Expression, Column, Literal, BinaryExpression, DataType, ColumnConstraint, ColumnDefinition


class DeleteOperator(Operator):
    def __init__(self, table_name: str, child: Operator, storage_engine: StorageEngine, executor: Any, bplus_tree=None):
        self.table_name = table_name
        self.child = child               # 子算子（全表扫描/过滤）
        self.storage_engine = storage_engine
        self.executor = executor
        self.bplus_tree = bplus_tree     # 可选：主键索引

    def execute(self) -> List[Any]:
        deleted_count = 0
        rid_list: List[Tuple[Tuple[int, int], Dict[str, Any]]] = []

        # 如果有 B+ 树索引，尝试优化
        if self.bplus_tree and self._can_use_index():
            key = self._extract_pk_value()
            if key is not None:
                pk_col_type = self._get_pk_col_type()
                if pk_col_type == DataType.INT:
                    pk_bytes = key.to_bytes(4, "little", signed=True)
                elif pk_col_type in (DataType.TEXT, DataType.STRING):
                    pk_bytes = key.encode("utf-8")
                else:
                    raise NotImplementedError(f"Unsupported primary key type for indexing: {pk_col_type.name}")

                rid_bytes = self.bplus_tree.search(pk_bytes)
                if rid_bytes is not None:
                    page_id = int.from_bytes(rid_bytes[0:4], "little")
                    row_id = int.from_bytes(rid_bytes[4:8], "little")
                    rid = (page_id, row_id)

                    row_data_bytes = self.storage_engine.read_row(self.table_name, rid)
                    if row_data_bytes:
                        schema = self.storage_engine.catalog_page.get_table_metadata(self.table_name)["schema"]
                        row_data = self._deserialize_row_data(row_data_bytes, schema)
                        rid_list.append((rid, row_data))
        else:
            # 回退到子算子扫描
            scanned_rows = self.executor.execute(self.child)  # [(rid, row_dict)]
            for rid, row_data_dict in scanned_rows:
                rid_list.append((rid, row_data_dict))

        # 实际删除
        for rid, row_data in rid_list:
            # 删除数据页里的记录
            self.storage_engine.delete_row_by_rid(self.table_name, rid)
            deleted_count += 1

            # 同步删除索引
            if self.bplus_tree:
                pk_value = self._get_pk_value_from_row(row_data)
                if pk_value is not None:
                    if isinstance(pk_value, int):
                        pk_bytes = pk_value.to_bytes(4, "little", signed=True)
                    else:
                        pk_bytes = str(pk_value).encode("utf-8")
                    self.bplus_tree.delete(pk_bytes)

        return [deleted_count]

    # === 以下与 UpdateOperator 保持一致的辅助方法 ===

    def _can_use_index(self) -> bool:
        """简化逻辑：如果 WHERE 条件是主键等值查询 (id = xxx)，就能用索引"""
        if isinstance(self.child, BinaryExpression):
            pk_col_name = self._get_pk_col_name()
            if pk_col_name and isinstance(self.child.left, Column) and self.child.left.name == pk_col_name:
                op = getattr(self.child, "op", None) or getattr(self.child, "operator", None)
                op_val = op.value if hasattr(op, "value") else op
                return op_val == "="
        return False

    def _extract_pk_value(self):
        """提取主键等值查询的值"""
        if isinstance(self.child, BinaryExpression) and isinstance(self.child.right, Literal):
            return self.child.right.value
        return None

    def _get_pk_col_name(self) -> Optional[str]:
        schema = self.storage_engine.catalog_page.get_table_metadata(self.table_name)["schema"]
        for col_name, col_def in schema.items():
            if (ColumnConstraint.PRIMARY_KEY, None) in col_def.constraints:
                return col_name
        return None

    def _get_pk_col_type(self) -> Optional[DataType]:
        schema = self.storage_engine.catalog_page.get_table_metadata(self.table_name)["schema"]
        for _, col_def in schema.items():
            if (ColumnConstraint.PRIMARY_KEY, None) in col_def.constraints:
                return col_def.data_type
        return None

    def _get_pk_value_from_row(self, row: Dict[str, Any]) -> Any:
        pk_col_name = self._get_pk_col_name()
        if pk_col_name and pk_col_name in row:
            return row[pk_col_name]
        return None

    def _deserialize_row_data(self, row_data_bytes: bytes, schema: Dict[str, ColumnDefinition]) -> Dict[str, Any]:
        deserialized_data = {}
        offset = 0
        for col_name, col_def in schema.items():
            if col_def.data_type in (DataType.TEXT, DataType.STRING):
                value, new_offset = self.storage_engine._decode_value(row_data_bytes, offset, "TEXT")
            else:
                value, new_offset = self.storage_engine._decode_value(row_data_bytes, offset, col_def.data_type)
            deserialized_data[col_name] = value
            offset = new_offset
        return deserialized_data
