import json
from typing import Dict, Any, Tuple, Optional
from sql.ast import DataType, ColumnConstraint, ColumnDefinition

from engine.constants import PAGE_SIZE


class CatalogPage:
    """
    目录页（CatalogPage），固定存储在 page_id=0。
    作为所有表元数据的中央目录，管理每个表的堆（数据）根页面ID和索引根页面ID。
    """

    def __init__(self):
        # 存储结构: { table_name: {'heap_root_page_id': int, 'index_root_page_id': int, 'schema': Dict} }
        self.tables: Dict[str, Dict[str, Any]] = {}

    def _serialize_schema(self, schema: Dict[str, ColumnDefinition]) -> Dict[str, Any]:
        """将 schema 对象（包含 ColumnDefinition 实例）序列化为可转为 JSON 的字典。"""
        serialized_schema = {}
        for col_name, col_def_obj in schema.items():
            serialized_col_def = {
                'name': col_def_obj.name,
                'data_type': col_def_obj.data_type.value if hasattr(col_def_obj.data_type,
                                                                    'value') else col_def_obj.data_type,
                'constraints': [(c.value, val) if hasattr(c, 'value') else (c, val) for c, val in
                                col_def_obj.constraints],
                'default_value': col_def_obj.default_value,
                'length': col_def_obj.length,
                'precision': col_def_obj.precision,
                'scale': col_def_obj.scale
            }
            serialized_schema[col_name] = serialized_col_def
        return serialized_schema

    def add_table(self, table_name: str, heap_root_page_id: int, index_root_page_id: int,
                  schema: Dict[str, ColumnDefinition]):
        """添加一个新表的元数据。"""
        if table_name in self.tables:
            raise RuntimeError(f"表 '{table_name}' 已存在于目录中。")
        self.tables[table_name] = {
            'heap_root_page_id': heap_root_page_id,
            'index_root_page_id': index_root_page_id,
            'schema': schema
        }

    def get_table_metadata(self, table_name: str) -> Optional[Dict[str, Any]]:
        """获取指定表的元数据。"""
        return self.tables.get(table_name)

    def serialize(self) -> bytes:
        """将整个 CatalogPage 对象序列化为字节，以便写入磁盘。"""
        data_to_serialize = {
            'tables': {
                name: {
                    'heap_root_page_id': data['heap_root_page_id'],
                    'index_root_page_id': data['index_root_page_id'],
                    'schema': self._serialize_schema(data['schema'])
                }
                for name, data in self.tables.items()
            },
        }
        serialized_data = json.dumps(data_to_serialize).encode('utf-8')
        # 使用空字节填充至页面大小
        padding_size = PAGE_SIZE - len(serialized_data)
        if padding_size < 0:
            raise RuntimeError(f"序列化后的目录页大小 ({len(serialized_data)}) 超出页面限制 ({PAGE_SIZE})")
        return serialized_data + b'\0' * padding_size

    @staticmethod
    def deserialize(data: bytes):
        """从字节反序列化出 CatalogPage 对象。"""
        catalog_page = CatalogPage()
        try:
            # 找到填充的空字节前的有效JSON数据
            null_byte_index = data.find(b'\0')
            json_data_bytes = data[:null_byte_index] if null_byte_index != -1 else data
            if not json_data_bytes:
                return catalog_page

            loaded_data = json.loads(json_data_bytes.decode('utf-8'))

            deserialized_tables = {}
            for table_name, table_data in loaded_data.get('tables', {}).items():
                deserialized_tables[table_name] = {
                    'heap_root_page_id': table_data['heap_root_page_id'],
                    'index_root_page_id': table_data['index_root_page_id'],
                    'schema': CatalogPage._deserialize_schema(table_data.get('schema', {}))
                }
            catalog_page.tables = deserialized_tables
        except (json.JSONDecodeError, UnicodeDecodeError):
            # 如果数据为空或格式损坏，返回一个空的目录对象
            catalog_page.tables = {}
        return catalog_page

    @staticmethod
    def _deserialize_schema(schema: Dict[str, Any]) -> Dict[str, ColumnDefinition]:
        """将 JSON 字典反序列化为 schema 对象。"""
        deserialized_schema = {}
        for col_name, col_def_dict in schema.items():
            dt_val = col_def_dict.get('data_type')
            data_type = DataType(dt_val) if dt_val else None

            constraints = []
            for c, val in col_def_dict.get('constraints', []):
                try:
                    constraint_enum = ColumnConstraint(c)  # 尝试转换为枚举
                    constraints.append((constraint_enum, val))
                except ValueError:
                    constraints.append((c, val))  # 转换失败则保留原样

            deserialized_schema[col_name] = ColumnDefinition(
                name=col_def_dict['name'], data_type=data_type, constraints=constraints,
                default_value=col_def_dict.get('default_value'), length=col_def_dict.get('length'),
                precision=col_def_dict.get('precision'), scale=col_def_dict.get('scale')
            )
        return deserialized_schema