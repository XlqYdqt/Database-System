from typing import Dict, Any, List, Tuple, Optional


from engine.storage_engine import StorageEngine
from sql.ast import Operator, Expression, Column, Literal, BinaryExpression, DataType, ColumnConstraint, \
    ColumnDefinition


class UpdateOperator(Operator):
    def __init__(self, table_name: str, child: Operator, updates, storage_engine: StorageEngine, executor:Any ,bplus_tree=None,):
        self.table_name = table_name
        self.child = child              # 子算子（全表扫描/过滤）
        self.updates = updates          # [(列名, 表达式), ...]
        self.storage_engine = storage_engine
        self.executor = executor
        self.bplus_tree = bplus_tree    # 可选：主键索引

    def execute(self) -> List[Any]:
        updated_count = 0

        # 如果有B+树，尝试走索引优化
        rid_list: List[Tuple[Tuple[int, int], Dict[str, Any]]] = [] # (rid, row_data_dict)
        if self.bplus_tree and self._can_use_index():
            key = self._extract_pk_value()
            if key is not None:
                pk_col_type = self._get_pk_col_type()
                if pk_col_type == DataType.INT:
                    pk_bytes = key.to_bytes(4, 'little', signed=True)
                elif pk_col_type == DataType.TEXT:
                    pk_bytes = key.encode('utf-8')
                else:
                    raise NotImplementedError(f"Unsupported primary key type for indexing: {pk_col_type.name}")

                rid_bytes = self.bplus_tree.search(pk_bytes)
                if rid_bytes is not None:
                    page_id = int.from_bytes(rid_bytes[0:4], 'little')
                    row_id = int.from_bytes(rid_bytes[4:8], 'little')
                    rid = (page_id, row_id)
                    row_data_bytes = self.storage_engine.read_row(self.table_name, rid)
                    if row_data_bytes:
                        # Deserialize row_data_bytes to dict
                        schema = self.storage_engine.catalog_page.get_table_metadata(self.table_name)['schema']
                        row_data = self._deserialize_row_data(row_data_bytes, schema)
                        rid_list.append((rid, row_data))
        else:
            # 回退到子算子扫描
            # 子算子返回的是 (rid, row_data_dict) 列表
            scanned_rows = self.executor.execute(self.child)
            for rid, row_data_dict in scanned_rows:
                rid_list.append((rid, row_data_dict))


        for rid, row_data in rid_list:
            new_row_data = dict(row_data)
            for col_name, expr in self.updates:
                new_row_data[col_name] = self._eval_expr(expr, row_data)

            # 获取主键值
            pk_value = self._get_pk_value_from_row(new_row_data)
            if pk_value is None:
                raise RuntimeError("Primary key value not found in row data during update.")

            # Serialize new_row_data to bytes
            schema = self.storage_engine.catalog_page.get_table_metadata(self.table_name)['schema']
            new_row_data_bytes = self._serialize_row_data(new_row_data, schema)

            self.storage_engine.update_row(self.table_name, pk_value, new_row_data_bytes)
            updated_count += 1

        return [updated_count]

    def _eval_expr(self, expr: Expression, row: Dict[str, Any]) -> Any:
        """递归解析表达式"""
        if isinstance(expr, Column):
            return row[expr.name]
        elif isinstance(expr, Literal):
            return expr.value
        elif isinstance(expr, BinaryExpression):
            left_val = self._eval_expr(expr.left, row)
            right_val = self._eval_expr(expr.right, row)
            op = getattr(expr, "op", None) or getattr(expr, "operator", None)
            op_val = op.value if hasattr(op, "value") else op

            if op_val == "=":
                return left_val == right_val
            elif op_val == ">":
                return left_val > right_val
            elif op_val == "<":
                return left_val < right_val
            elif op_val == ">=":
                return left_val >= right_val
            elif op_val == "<=":
                return left_val <= right_val
            elif op_val in ("!=", "<>"):
                return left_val != right_val
            else:
                raise NotImplementedError(f"Unsupported operator: {op_val}")
        else:
            return expr

    def _can_use_index(self) -> bool:
        """
        简化逻辑：如果 WHERE 条件是主键等值查询 (id = xxx)，就能用索引
        """
        if isinstance(self.child, BinaryExpression):
            # 确保左侧是主键列
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
        """获取主键列的名称"""
        schema = self.storage_engine.catalog_page.get_table_metadata(self.table_name)['schema']
        for col_name, col_def in schema.items():
            if (ColumnConstraint.PRIMARY_KEY, None) in col_def.constraints:
                return col_name
        return None

    def _get_pk_col_type(self) -> Optional[DataType]:
        """获取主键列的类型"""
        schema = self.storage_engine.catalog_page.get_table_metadata(self.table_name)['schema']
        for col_name, col_def in schema.items():
            if (ColumnConstraint.PRIMARY_KEY, None) in col_def.constraints:
                return col_def.data_type
        return None

    def _get_pk_value_from_row(self, row: Dict[str, Any]) -> Any:
        """从行数据中提取主键值"""
        pk_col_name = self._get_pk_col_name()
        if pk_col_name and pk_col_name in row:
            return row[pk_col_name]
        return None

    def _serialize_row_data(self, row_data: Dict[str, Any], schema: Dict[str, ColumnDefinition]) -> bytes:
        """将字典形式的行数据序列化为字节"""
        serialized_data = b''
        for col_name, col_def in schema.items():
            value = row_data.get(col_name)
            if value is None:
                # Handle NULL values if necessary, for now, assume not null
                raise ValueError(f"Column '{col_name}' cannot be null.")

            if col_def.data_type == DataType.INT:
                serialized_data += value.to_bytes(4, 'little', signed=True)
            elif col_def.data_type == DataType.TEXT:
                encoded_value = value.encode('utf-8')
                serialized_data += len(encoded_value).to_bytes(4, 'little') + encoded_value
            elif col_def.data_type == DataType.FLOAT:
                import struct
                serialized_data += struct.pack("<f", value)
            else:
                raise NotImplementedError(f"Unsupported data type for serialization: {col_def.data_type}")
        return serialized_data

    def _deserialize_row_data(self, row_data_bytes: bytes, schema: Dict[str, ColumnDefinition]) -> Dict[str, Any]:
        """将字节形式的行数据反序列化为字典"""
        deserialized_data = {}
        offset = 0
        for col_name, col_def in schema.items():
            value, new_offset = self.storage_engine._decode_value(row_data_bytes, offset, col_def.data_type)
            deserialized_data[col_name] = value
            offset = new_offset
        return deserialized_data

