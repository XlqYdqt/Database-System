import json
from typing import Dict, Any, Tuple, Optional
from sql.ast import DataType, ColumnConstraint, ColumnDefinition

from engine.constants import PAGE_SIZE


class CatalogPage:
    """
    CatalogPage 存储在 page_id=0，作为所有表的元数据目录。
    它负责管理表的堆根页面ID和索引根页面ID。
    """
    def __init__(self):
        # {table_name: {'heap_root_page_id': int, 'index_root_page_id': int, 'schema': Dict[str, str]}}
        self.tables: Dict[str, Dict[str, Any]] = {}
        self.next_available_byte = 0 # Initialize next_available_byte

    def _serialize_schema(self, schema: Dict[str, ColumnDefinition]) -> Dict[str, Any]:
        serialized_schema = {}
        for col_name, col_def_obj in schema.items():
            serialized_col_def = {
                'name': col_def_obj.name,
                'data_type': col_def_obj.data_type.value if col_def_obj.data_type else None,
                'constraints': [
                    (c.value, val) if isinstance(c, ColumnConstraint) else (c, val)
                    for c, val in col_def_obj.constraints
                ],
                'default_value': col_def_obj.default_value,
                'length': col_def_obj.length,
                'precision': col_def_obj.precision,
                'scale': col_def_obj.scale
            }
            serialized_schema[col_name] = serialized_col_def
        return serialized_schema

    def add_table(self, table_name: str, heap_root_page_id: int, index_root_page_id: int, schema: Dict[str, ColumnDefinition]):
        """添加一个新表的元数据"""
        if table_name in self.tables:
            raise RuntimeError(f"Table '{table_name}' already exists in CatalogPage")
        self.tables[table_name] = {
            'heap_root_page_id': heap_root_page_id,
            'index_root_page_id': index_root_page_id,
            'schema': schema
        }

    def get_table_metadata(self, table_name: str) -> Dict[str, Any]:
        """获取表的元数据"""
        if table_name not in self.tables:
            raise RuntimeError(f"Table '{table_name}' not found in CatalogPage")
        return self.tables[table_name]

    def drop_table(self, table_name: str):
        """从目录中删除表的元数据"""
        if table_name not in self.tables:
            raise RuntimeError(f"Table '{table_name}' not found in CatalogPage")
        del self.tables[table_name]

    def list_tables(self) -> Dict[str, Dict[str, int]]:
        """返回所有表的元数据"""
        return self.tables

    def serialize(self) -> bytes:
        """将 CatalogPage 序列化为字节，以便写入磁盘"""
        # Serialize tables and next_available_byte
        data_to_serialize = {
            'tables': {
                table_name: {
                    'heap_root_page_id': data['heap_root_page_id'],
                    'index_root_page_id': data['index_root_page_id'],
                    'schema': self._serialize_schema(data['schema'])
                }
                for table_name, data in self.tables.items()
            },
            'next_available_byte': self.next_available_byte
        }
        serialized_data = json.dumps(data_to_serialize).encode('utf-8')

        # Calculate padding
        padding_size = PAGE_SIZE - len(serialized_data)
        if padding_size < 0:
            raise RuntimeError(f"Serialized CatalogPage size ({len(serialized_data)}) exceeds PAGE_SIZE ({PAGE_SIZE})")

        # Add padding
        padded_data = serialized_data + b'\0' * padding_size
        return padded_data

    @staticmethod
    def deserialize(data: bytes):
        """从字节反序列化 CatalogPage"""
        catalog_page = CatalogPage()
        try:
            # Find the actual end of JSON data by looking for the first null byte
            # This assumes padding is always null bytes at the end
            null_byte_index = data.find(b'\0')
            if null_byte_index != -1:
                json_data_bytes = data[:null_byte_index]
            else:
                json_data_bytes = data  # No padding found, assume full data is JSON

            decoded_data = json_data_bytes.decode('utf-8')
            loaded_data = json.loads(decoded_data)
            
            deserialized_tables = {}
            for table_name, table_data in loaded_data.get('tables', {}).items():
                deserialized_tables[table_name] = {
                    'heap_root_page_id': table_data['heap_root_page_id'],
                    'index_root_page_id': table_data['index_root_page_id'],
                    'schema': CatalogPage._deserialize_schema(table_data.get('schema', {}))
                }
            catalog_page.tables = deserialized_tables
            catalog_page.next_available_byte = loaded_data.get('next_available_byte', 0)
        except json.JSONDecodeError:
            # If data is empty or invalid JSON, initialize with an empty catalog and 0 next_available_byte
            catalog_page.tables = {}
            catalog_page.next_available_byte = 0
        return catalog_page

    @staticmethod
    def _deserialize_schema(schema: Dict[str, Any]) -> Dict[str, ColumnDefinition]:
        deserialized_schema = {}
        for col_name, col_def_dict in schema.items():
            data_type = None
            if 'data_type' in col_def_dict:
                if isinstance(col_def_dict['data_type'], str):
                    data_type = DataType(col_def_dict['data_type'])
                elif isinstance(col_def_dict['data_type'], DataType):
                    data_type = col_def_dict['data_type']
            constraints = [
                (ColumnConstraint(c), val) if isinstance(c, str) else (c, val)
                for c, val in col_def_dict.get('constraints', [])
            ]
            deserialized_col_def_obj = ColumnDefinition(
                name=col_def_dict['name'],
                data_type=data_type,
                constraints=constraints,
                default_value=col_def_dict.get('default_value'),
                length=col_def_dict.get('length'),
                precision=col_def_dict.get('precision'),
                scale=col_def_dict.get('scale')
            )
            deserialized_schema[col_name] = deserialized_col_def_obj
        return deserialized_schema