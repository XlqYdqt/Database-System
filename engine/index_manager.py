from typing import Dict, Any, Optional, List, Tuple, TYPE_CHECKING

from engine.b_plus_tree import BPlusTree, INVALID_PAGE_ID
from engine.exceptions import PrimaryKeyViolationError, UniquenessViolationError
from sql.ast import ColumnDefinition, ColumnConstraint

if TYPE_CHECKING:
    from engine.storage_engine import StorageEngine


class IndexManager:
    """
    索引管理器 (IndexManager)
    为单个表管理所有的B+树索引，包括主键索引和二级索引。
    它封装了索引的创建、加载和维护逻辑。
    """

    def __init__(self, table_name: str, storage_engine: 'StorageEngine'):
        self.table_name = table_name
        self.storage_engine = storage_engine
        self.bpm = storage_engine.bpm
        self.indexes: Dict[str, BPlusTree] = {}
        # 注意：当前设计一个索引只对应一列，未来可扩展为多列
        self.column_to_index: Dict[str, str] = {}
        self.unique_indexes: Dict[str, bool] = {}
        self._load_indexes()

    def _load_indexes(self):
        """从目录中加载该表的所有索引信息。"""
        table_meta = self.storage_engine.catalog_page.get_table_metadata(self.table_name)
        if not table_meta or 'indexes' not in table_meta:
            return

        for index_name, index_meta in table_meta['indexes'].items():
            root_page_id = index_meta['root_page_id']
            # 兼容旧的单列'column'和新的多列'columns'
            columns = index_meta.get('columns', [index_meta.get('column')])
            is_unique = index_meta.get('is_unique', False)

            if not columns or columns[0] is None:
                continue

            self.indexes[index_name] = BPlusTree(self.bpm, root_page_id)
            # 当前只处理单列索引的映射
            self.column_to_index[columns[0]] = index_name
            self.unique_indexes[index_name] = is_unique

    def create_index(self, index_name: str, columns: Optional[List[str]] = None, is_unique: bool = False) -> BPlusTree:
        """
        为指定列创建一个新的B+树索引。
        [FIX] 兼容旧的调用方式（来自CREATE TABLE），其中第一个参数是列名，且'columns'参数缺失。
        """
        # 如果 columns 未提供，说明是来自 CREATE TABLE 的旧式调用
        if columns is None:
            column_name = index_name  # 此时第一个参数实际上是列名
            columns = [column_name]   # 将其包装成列表
            # 为主键或旧调用生成默认索引名
            index_name = f"idx_{self.table_name}_{column_name}"
        else:
            # 来自 CREATE INDEX 的新式调用
            if not columns:
                raise ValueError("创建索引必须至少指定一列。")
            column_name = columns[0]

        if index_name in self.indexes:
            raise ValueError(f"索引 '{index_name}' 已存在。")

        new_b_tree = BPlusTree(self.bpm, INVALID_PAGE_ID)
        self.indexes[index_name] = new_b_tree
        self.column_to_index[column_name] = index_name
        self.unique_indexes[index_name] = is_unique

        table_meta = self.storage_engine.catalog_page.get_table_metadata(self.table_name)
        if 'indexes' not in table_meta:
            table_meta['indexes'] = {}

        # 使用新的、更灵活的元数据结构
        table_meta['indexes'][index_name] = {
            'root_page_id': new_b_tree.root_page_id,
            'columns': columns,
            'is_unique': is_unique
        }

        self._populate_index(new_b_tree, column_name, index_name)
        self.storage_engine._flush_catalog_page()
        return new_b_tree

    def drop_index(self, index_name: str):
        """
        删除一个索引。
        """
        if index_name not in self.indexes:
            raise ValueError(f"索引 '{index_name}' 在表 '{self.table_name}' 中不存在。")

        # 1. 从元数据中移除
        table_meta = self.storage_engine.catalog_page.get_table_metadata(self.table_name)
        if table_meta and 'indexes' in table_meta and index_name in table_meta['indexes']:
            del table_meta['indexes'][index_name]
            self.storage_engine._flush_catalog_page()

        # 2. 从内存中移除
        self.indexes.pop(index_name)
        self.unique_indexes.pop(index_name, None)

        # 反向查找并删除 column_to_index 中的条目
        col_to_remove = None
        for col_name, idx_name in self.column_to_index.items():
            if idx_name == index_name:
                col_to_remove = col_name
                break
        if col_to_remove:
            del self.column_to_index[col_to_remove]

    def _populate_index(self, b_tree: BPlusTree, column_name: str, index_name: str):
        """将表中的现有数据填充到新创建的索引中。"""
        table_meta = self.storage_engine.catalog_page.get_table_metadata(self.table_name)
        schema = table_meta['schema']

        col_def_to_index: Optional[ColumnDefinition] = schema.get(column_name)
        if not col_def_to_index:
            raise ValueError(f"列 '{column_name}' 在表 '{self.table_name}' 中不存在。")
        col_index = list(schema.keys()).index(column_name)

        all_rows = self.storage_engine.scan_table(self.table_name)
        for rid, row_data_bytes in all_rows:
            value, _ = self.storage_engine._decode_value_from_row(row_data_bytes, col_index, schema)
            key_bytes = self.storage_engine._prepare_key_for_b_tree(value, col_def_to_index.data_type)
            insert_result = b_tree.insert(key_bytes, rid)

            if insert_result is None and self.unique_indexes.get(index_name, False):
                raise UniquenessViolationError(column_name, value)

        if b_tree.root_page_id != table_meta['indexes'][index_name]['root_page_id']:
            self.update_index_root(column_name, b_tree.root_page_id)

    def get_index_for_column(self, column_name: str) -> Optional[BPlusTree]:
        """根据列名获取对应的B+树索引实例。"""
        index_name = self.column_to_index.get(column_name)
        return self.indexes.get(index_name) if index_name else None

    def insert_entry(self, row_dict: Dict[str, Any], rid: Tuple[int, int]):
        """在新行插入后，更新所有索引，并对唯一索引进行冲突检查。"""
        for col_name, index_name in self.column_to_index.items():
            b_tree = self.indexes[index_name]
            value = row_dict.get(col_name)
            if value is None: continue

            col_def = self.storage_engine.catalog_page.get_table_metadata(self.table_name)['schema'][col_name]
            key_bytes = self.storage_engine._prepare_key_for_b_tree(value, col_def.data_type)
            insert_result = b_tree.insert(key_bytes, rid)

            if insert_result is None:
                is_pk = any(c[0] == ColumnConstraint.PRIMARY_KEY for c in col_def.constraints)
                if is_pk:
                    raise PrimaryKeyViolationError(value)
                elif self.unique_indexes.get(index_name, False):
                    raise UniquenessViolationError(col_name, value)

            if insert_result: self.update_index_root(col_name, b_tree.root_page_id)

    def delete_entry(self, row_dict: Dict[str, Any], rid: Tuple[int, int]):
        """在行删除后，从所有索引中删除对应条目。"""
        for col_name, index_name in self.column_to_index.items():
            b_tree = self.indexes[index_name]
            value = row_dict.get(col_name)
            if value is None: continue

            col_def = self.storage_engine.catalog_page.get_table_metadata(self.table_name)['schema'][col_name]
            key_bytes = self.storage_engine._prepare_key_for_b_tree(value, col_def.data_type)

            if b_tree.delete(key_bytes): self.update_index_root(col_name, b_tree.root_page_id)

    def check_uniqueness_for_update(self, old_row_dict: Dict[str, Any], new_row_dict: Dict[str, Any],
                                    old_rid: Tuple[int, int]):
        """在更新操作前，检查新值是否会违反唯一性约束。"""
        for col_name, index_name in self.column_to_index.items():
            if not self.unique_indexes.get(index_name): continue
            old_value, new_value = old_row_dict.get(col_name), new_row_dict.get(col_name)
            if old_value == new_value: continue

            b_tree = self.indexes[index_name]
            col_def = self.storage_engine.catalog_page.get_table_metadata(self.table_name)['schema'][col_name]
            key_bytes = self.storage_engine._prepare_key_for_b_tree(new_value, col_def.data_type)
            existing_rid = b_tree.search(key_bytes)

            if existing_rid is not None and existing_rid != old_rid:
                is_pk = any(c[0] == ColumnConstraint.PRIMARY_KEY for c in col_def.constraints)
                if is_pk:
                    raise PrimaryKeyViolationError(new_value)
                else:
                    raise UniquenessViolationError(col_name, new_value)

    def update_index_root(self, column_name: str, new_root_id: int):
        """更新并持久化指定索引的根页面ID。"""
        table_meta = self.storage_engine.catalog_page.get_table_metadata(self.table_name)
        index_name = self.column_to_index.get(column_name)
        if table_meta and index_name and index_name in table_meta.get('indexes', {}):
            table_meta['indexes'][index_name]['root_page_id'] = new_root_id
            self.storage_engine._flush_catalog_page()

