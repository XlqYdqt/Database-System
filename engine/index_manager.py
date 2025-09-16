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
        self.column_to_index: Dict[str, str] = {}
        # [NEW] 增加一个属性，用于快速判断索引是否是唯一的
        self.unique_indexes: Dict[str, bool] = {}
        self._load_indexes()

    def _load_indexes(self):
        """从目录中加载该表的所有索引信息。"""
        table_meta = self.storage_engine.catalog_page.get_table_metadata(self.table_name)
        if not table_meta or 'indexes' not in table_meta:
            return

        for index_name, index_meta in table_meta['indexes'].items():
            root_page_id = index_meta['root_page_id']
            column_name = index_meta['column']
            is_unique = index_meta.get('is_unique', False)  # 兼容旧格式

            self.indexes[index_name] = BPlusTree(self.bpm, root_page_id)
            self.column_to_index[column_name] = index_name
            self.unique_indexes[index_name] = is_unique

    def create_index(self, column_name: str, is_unique: bool = False) -> BPlusTree:
        """
        为指定列创建一个新的B+树索引。
        """
        index_name = f"idx_{self.table_name}_{column_name}"
        if index_name in self.indexes:
            raise ValueError(f"索引 '{index_name}' 已存在。")

        new_b_tree = BPlusTree(self.bpm, INVALID_PAGE_ID)
        self.indexes[index_name] = new_b_tree
        self.column_to_index[column_name] = index_name
        self.unique_indexes[index_name] = is_unique

        table_meta = self.storage_engine.catalog_page.get_table_metadata(self.table_name)
        if 'indexes' not in table_meta:
            table_meta['indexes'] = {}

        # [MODIFIED] 在元数据中存储 is_unique 标志
        table_meta['indexes'][index_name] = {
            'root_page_id': new_b_tree.root_page_id,
            'column': column_name,
            'is_unique': is_unique
        }
        self.storage_engine._flush_catalog_page()

        # [关键] 创建索引后，需要扫描全表，将现有数据填充到新索引中
        self._populate_index(new_b_tree, column_name)

        return new_b_tree

    def _populate_index(self, b_tree: BPlusTree, column_name: str):
        """将表中的现有数据填充到新创建的索引中。"""
        table_meta = self.storage_engine.catalog_page.get_table_metadata(self.table_name)
        schema = table_meta['schema']

        col_def_to_index: Optional[ColumnDefinition] = None
        col_index = -1

        # 保证迭代顺序
        for i, (c_name, c_def) in enumerate(schema.items()):
            if c_name == column_name:
                col_def_to_index = c_def
                col_index = i
                break

        if not col_def_to_index:
            raise ValueError(f"列 '{column_name}' 在表 '{self.table_name}' 中不存在。")

        all_rows = self.storage_engine.scan_table(self.table_name)
        for rid, row_data_bytes in all_rows:
            # 【修复】调用 StorageEngine 中新增的辅助函数
            value, _ = self.storage_engine._decode_value_from_row(row_data_bytes, col_index, schema)
            key_bytes = self.storage_engine._prepare_key_for_b_tree(value, col_def_to_index.data_type)
            insert_result = b_tree.insert(key_bytes, rid)

            # 在填充过程中检查唯一性约束
            if insert_result is None and self.unique_indexes.get(f"idx_{self.table_name}_{column_name}", False):
                raise UniquenessViolationError(column_name, value)

        # 填充后，B+树的根节点ID可能已改变，需要更新目录
        if b_tree.root_page_id != table_meta['indexes'][f"idx_{self.table_name}_{column_name}"]['root_page_id']:
            self.update_index_root(column_name, b_tree.root_page_id)

    def get_index_for_column(self, column_name: str) -> Optional[BPlusTree]:
        """根据列名获取对应的B+树索引实例。"""
        index_name = self.column_to_index.get(column_name)
        if index_name:
            return self.indexes.get(index_name)
        return None

    def insert_entry(self, row_dict: Dict[str, Any], rid: Tuple[int, int]):
        """在新行插入后，更新所有索引，并对唯一索引进行冲突检查。"""
        for col_name, index_name in self.column_to_index.items():
            b_tree = self.indexes[index_name]
            value = row_dict.get(col_name)

            if value is None:  # 通常不为 NULL 值创建索引条目
                continue

            col_def = self.storage_engine.catalog_page.get_table_metadata(self.table_name)['schema'][col_name]
            key_bytes = self.storage_engine._prepare_key_for_b_tree(value, col_def.data_type)

            insert_result = b_tree.insert(key_bytes, rid)

            # B+树返回 None 表示键已存在，检查唯一性约束冲突
            if insert_result is None:
                is_pk = any(c[0] == ColumnConstraint.PRIMARY_KEY for c in col_def.constraints)
                if is_pk:
                    raise PrimaryKeyViolationError(value)
                elif self.unique_indexes.get(index_name, False):
                    raise UniquenessViolationError(col_name, value)

            # 如果插入导致根节点分裂，则更新根页面ID
            if insert_result:
                self.update_index_root(col_name, b_tree.root_page_id)

    def delete_entry(self, row_dict: Dict[str, Any], rid: Tuple[int, int]):
        """在行删除后，从所有索引中删除对应条目。"""
        for col_name, index_name in self.column_to_index.items():
            b_tree = self.indexes[index_name]
            value = row_dict.get(col_name)

            if value is None:
                continue

            col_def = self.storage_engine.catalog_page.get_table_metadata(self.table_name)['schema'][col_name]
            key_bytes = self.storage_engine._prepare_key_for_b_tree(value, col_def.data_type)

            root_changed = b_tree.delete(key_bytes)
            if root_changed:
                self.update_index_root(col_name, b_tree.root_page_id)

    # 【新增函数】为 storage_engine.update_row 提供支持
    def check_uniqueness_for_update(self, old_row_dict: Dict[str, Any], new_row_dict: Dict[str, Any],
                                    old_rid: Tuple[int, int]):
        """在更新操作前，检查新值是否会违反唯一性约束。"""
        for col_name, index_name in self.column_to_index.items():
            # 只检查唯一索引
            if not self.unique_indexes.get(index_name):
                continue

            old_value = old_row_dict.get(col_name)
            new_value = new_row_dict.get(col_name)

            # 如果唯一键的值没有改变，则无需检查
            if old_value == new_value:
                continue

            b_tree = self.indexes[index_name]
            col_def = self.storage_engine.catalog_page.get_table_metadata(self.table_name)['schema'][col_name]
            key_bytes = self.storage_engine._prepare_key_for_b_tree(new_value, col_def.data_type)

            # 检查新值是否已存在于B+树中
            existing_rid = b_tree.search(key_bytes)

            # 如果存在，并且它不属于我们正在更新的这一行，那么就构成了冲突
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
