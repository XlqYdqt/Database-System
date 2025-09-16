from __future__ import annotations
from typing import List, Dict, Optional, Any, Tuple, TYPE_CHECKING
import struct

from sql.ast import ColumnDefinition, DataType, ColumnConstraint
from engine.constants import ROW_LENGTH_PREFIX_SIZE
from engine.catalog_page import CatalogPage
from engine.table_heap_page import TableHeapPage
from engine.data_page import DataPage
from engine.exceptions import TableAlreadyExistsError, PrimaryKeyViolationError, TableNotFoundError, \
    UniquenessViolationError
from storage.buffer_pool_manager import BufferPoolManager
from engine.transaction_manager import TransactionManager

if TYPE_CHECKING:
    from engine.index_manager import IndexManager
    from engine.b_plus_tree import BPlusTree


class StorageEngine:
    """
    存储引擎 (重构完善版)。
    将所有数据和索引的修改操作封装起来，保证原子性和一致性。
    """
    B_PLUS_TREE_KEY_SIZE = 16

    def __init__(self, buffer_pool_manager: BufferPoolManager):
        from engine.index_manager import IndexManager
        self.bpm = buffer_pool_manager
        self.index_managers: Dict[str, IndexManager] = {}
        # self.indexes: Dict[str, BPlusTree] = {} # 此属性已移至 IndexManager
        self.txn_manager = TransactionManager(self)

        is_dirty = False
        catalog_page_raw = self.bpm.fetch_page(0)
        if catalog_page_raw is None:
            catalog_page_raw = self.bpm.new_page()
            if catalog_page_raw is None or catalog_page_raw.page_id != 0:
                raise RuntimeError("关键错误：缓冲池无法分配或获取 page_id=0 作为目录页。")

        try:
            if any(b != 0 for b in catalog_page_raw.data):
                self.catalog_page = CatalogPage.deserialize(catalog_page_raw.data)
                is_dirty = False
            else:
                self.catalog_page = CatalogPage()
                catalog_page_raw.data = bytearray(self.catalog_page.serialize())
                is_dirty = True
        finally:
            self.bpm.unpin_page(0, is_dirty)

        self._load_all_indexes()

    def _load_all_indexes(self) -> None:
        """在系统启动时，为每个表创建一个 IndexManager 实例来加载其所有索引。"""
        from engine.index_manager import IndexManager
        for table_name in self.catalog_page.tables.keys():
            self.index_managers[table_name] = IndexManager(table_name, self)

    def _prepare_key_for_b_tree(self, value: Any, col_type: DataType) -> bytes:
        """将Python值转换为B+树期望的、固定长度、可比较的字节键。"""
        if value is None:
            raise ValueError("索引键不能为 None。")
        if col_type == DataType.INT:
            key_bytes = value.to_bytes(8, 'big', signed=True)
        elif col_type in (DataType.TEXT, DataType.STRING):
            key_bytes = str(value).encode('utf-8')
        else:
            raise NotImplementedError(f"不支持的主键类型用于索引: {col_type.name}")

        if len(key_bytes) > self.B_PLUS_TREE_KEY_SIZE:
            key_bytes = key_bytes[:self.B_PLUS_TREE_KEY_SIZE]

        return key_bytes.ljust(self.B_PLUS_TREE_KEY_SIZE, b'\x00')

    def _flush_catalog_page(self) -> None:
        """将目录页（CatalogPage）的内容序列化并强制写回磁盘。"""
        catalog_page_raw = self.bpm.fetch_page(0)
        if catalog_page_raw:
            try:
                catalog_page_raw.data = bytearray(self.catalog_page.serialize())
                self.bpm.unpin_page(0, True)
            except Exception as e:
                self.bpm.unpin_page(0, False)
                raise e
        else:
            raise RuntimeError("在缓冲池中找不到目录页，无法刷新。")

    def get_index_manager(self, table_name: str) -> Optional[IndexManager]:
        """获取指定表的索引管理器。"""
        return self.index_managers.get(table_name)

    def create_table(self, table_name: str, columns: List[ColumnDefinition]) -> bool:
        """创建新表，并自动为 PRIMARY KEY 和 UNIQUE 约束的列创建索引。"""
        from engine.index_manager import IndexManager
        if table_name in self.catalog_page.tables:
            raise TableAlreadyExistsError(f"表 '{table_name}' 已存在。")

        table_heap_page = self.bpm.new_page()
        if not table_heap_page:
            raise MemoryError("缓冲池已满，无法为表创建新的堆页面。")

        try:
            schema_dict = {col.name: col for col in columns}
            self.catalog_page.add_table(table_name, table_heap_page.page_id, schema_dict)
            self._flush_catalog_page()

            self.index_managers[table_name] = IndexManager(table_name, self)

            for col in columns:
                is_pk = any(c[0] == ColumnConstraint.PRIMARY_KEY for c in col.constraints)
                is_unique = any(c[0] == ColumnConstraint.UNIQUE for c in col.constraints)
                if is_pk or is_unique:
                    self.index_managers[table_name].create_index(col.name, is_unique=True)

            empty_heap = TableHeapPage()
            table_heap_page.data = bytearray(empty_heap.serialize())
        finally:
            self.bpm.unpin_page(table_heap_page.page_id, True)

        return True

    # --- 数据修改核心公共方法 (事务兼容) ---

    def insert_row(self, table_name: str, row_data: bytes, row_dict: Dict[str, Any],
                   txn_id: Optional[int] = None) -> bool:
        """
        插入一行数据。
        - 如果 txn_id is None：立即写入（非事务模式）。
        - 如果 txn_id 不为 None：延迟写入（事务模式）。
        """
        if txn_id is not None:
            self.txn_manager.add_write_record(
                txn_id,
                op_type='INSERT',
                table_name=table_name,
                new_data=row_data,
                new_dict=row_dict
            )
            return True
        else:
            return self._do_insert_immediate(table_name, row_data, row_dict)

    def delete_row(self, table_name: str, rid: Tuple[int, int], txn_id: Optional[int] = None) -> bool:
        """
        删除一行数据。
        - 如果 txn_id is None：立即删除（非事务模式）。
        - 如果 txn_id 不为 None：延迟删除（事务模式）。
        """
        old_row_data = self.read_row(table_name, rid)
        if not old_row_data:
            return False

        old_row_dict = self._decode_row(table_name, old_row_data)

        if txn_id is not None:
            self.txn_manager.add_write_record(
                txn_id,
                op_type='DELETE',
                table_name=table_name,
                rid=rid,
                old_dict=old_row_dict
            )
            return True
        else:
            return self._do_delete_immediate(table_name, rid, old_row_dict)

    def update_row(self, table_name: str, old_rid: Tuple[int, int], new_row_dict: Dict[str, Any],
                   txn_id: Optional[int] = None) -> bool:
        """
        更新一行数据。
        - 如果 txn_id is None：立即更新（非事务模式）。
        - 如果 txn_id 不为 None：延迟更新（事务模式）。
        """
        old_row_data = self.read_row(table_name, old_rid)
        if not old_row_data:
            return False

        old_row_dict = self._decode_row(table_name, old_row_data)
        new_row_data = self._serialize_row(table_name, new_row_dict)

        if txn_id is not None:
            self.txn_manager.add_write_record(
                txn_id,
                op_type='UPDATE',
                table_name=table_name,
                rid=old_rid,
                old_dict=old_row_dict,
                new_data=new_row_data,
                new_dict=new_row_dict
            )
            return True
        else:
            return self._do_update_immediate(table_name, old_rid, old_row_dict, new_row_data, new_row_dict)

    # --- 内部原子执行方法 (_do_*_immediate) ---

    def _do_insert_immediate(self, table_name: str, row_data: bytes, row_dict: Dict[str, Any]) -> bool:
        """原子性地插入数据并更新所有索引。"""
        table_metadata = self.catalog_page.get_table_metadata(table_name)
        if not table_metadata:
            raise TableNotFoundError(table_name)

        heap_page_id = table_metadata['heap_root_page_id']
        heap_page_raw = self.bpm.fetch_page(heap_page_id)
        if not heap_page_raw:
            raise IOError(f"无法为表 '{table_name}' 获取堆页面 {heap_page_id}。")

        target_page_raw = None
        heap_page_is_dirty = False
        try:
            table_heap = TableHeapPage.deserialize(heap_page_raw.data)
            record_to_insert = (len(row_data) + ROW_LENGTH_PREFIX_SIZE).to_bytes(ROW_LENGTH_PREFIX_SIZE,
                                                                                 "little") + row_data

            for page_id in reversed(table_heap.get_page_ids()):
                page_raw = self.bpm.fetch_page(page_id)
                if page_raw:
                    try:
                        data_page = DataPage(page_raw.page_id, page_raw.data)
                        if data_page.get_free_space() >= len(record_to_insert):
                            target_page_raw = page_raw
                            break
                    finally:
                        if target_page_raw is None or page_raw.page_id != target_page_raw.page_id:
                            self.bpm.unpin_page(page_id, False)

            if not target_page_raw:
                target_page_raw = self.bpm.new_page()
                if not target_page_raw:
                    raise MemoryError("缓冲池已满，无法为插入创建新的数据页。")
                table_heap.add_page_id(target_page_raw.page_id)
                heap_page_raw.data = bytearray(table_heap.serialize())
                heap_page_is_dirty = True

            target_data_page = DataPage(target_page_raw.page_id, target_page_raw.data)
            row_offset = target_data_page.insert_record(record_to_insert)
            target_page_raw.data = bytearray(target_data_page.get_data())
            rid = (target_page_raw.page_id, row_offset)

            index_manager = self.get_index_manager(table_name)
            if index_manager:
                try:
                    index_manager.insert_entry(row_dict, rid)
                except (PrimaryKeyViolationError, UniquenessViolationError) as e:
                    target_data_page.delete_record(row_offset)  # 回滚数据插入
                    target_page_raw.data = bytearray(target_data_page.get_data())
                    raise e
            return True
        finally:
            self.bpm.unpin_page(heap_page_id, heap_page_is_dirty)
            if target_page_raw:
                self.bpm.unpin_page(target_page_raw.page_id, True)

    def _do_delete_immediate(self, table_name: str, rid: Tuple[int, int], old_row_dict: Dict[str, Any]) -> bool:
        """原子性地删除数据并更新所有索引。"""
        # 1. 先删除所有索引条目
        index_manager = self.get_index_manager(table_name)
        if index_manager:
            index_manager.delete_entry(old_row_dict, rid)

        # 2. 再删除数据页上的行
        page_id, offset = rid
        page = self.bpm.fetch_page(page_id)
        if not page:
            # 如果索引已删但数据页找不到，这是一个严重问题，但对于操作本身算“完成”
            return False
        try:
            data_page = DataPage(page.page_id, page.data)
            deleted = data_page.delete_record(offset)
            if deleted:
                page.data = bytearray(data_page.get_data())
            return deleted
        finally:
            self.bpm.unpin_page(page_id, True)

    def _do_update_immediate(self, table_name: str, old_rid: Tuple[int, int], old_row_dict: Dict[str, Any],
                             new_row_data: bytes, new_row_dict: Dict[str, Any]) -> bool:
        """原子性地更新数据并更新所有索引。"""
        index_manager = self.get_index_manager(table_name)

        # 1. 更新前的预检查
        if index_manager:
            index_manager.check_uniqueness_for_update(old_row_dict, new_row_dict, old_rid)

        # 2. 更新数据页
        new_rid = self._update_data_page_record(old_rid, new_row_data)
        if new_rid is None:
            return False

        # 3. 更新索引
        if index_manager:
            try:
                index_manager.delete_entry(old_row_dict, old_rid)
                index_manager.insert_entry(new_row_dict, new_rid)
            except Exception as e:
                # 尝试回滚数据页的修改
                self._update_data_page_record(new_rid, self._serialize_row(table_name, old_row_dict))
                raise RuntimeError(f"索引更新失败，数据修改已尝试回滚: {e}") from e

        return True

    def _update_data_page_record(self, rid: Tuple[int, int], new_row_data: bytes) -> Optional[Tuple[int, int]]:
        """仅更新数据页上的一行，如果行移动会返回新的RID。"""
        page_id, old_offset = rid
        page = self.bpm.fetch_page(page_id)
        if not page:
            return None
        try:
            data_page = DataPage(page.page_id, page.data)
            new_record = (len(new_row_data) + ROW_LENGTH_PREFIX_SIZE).to_bytes(ROW_LENGTH_PREFIX_SIZE,
                                                                               'little') + new_row_data
            new_offset, _ = data_page.update_record(old_offset, new_record)
            page.data = bytearray(data_page.get_data())
            return (page_id, new_offset)
        except (ValueError, IndexError):
            return None
        finally:
            self.bpm.unpin_page(page_id, True)

    # --- 数据读取和序列化辅助方法 ---

    def scan_table(self, table_name: str) -> List[Tuple[Tuple[int, int], bytes]]:
        """扫描全表，返回所有行数据及其RID。"""
        table_metadata = self.catalog_page.get_table_metadata(table_name)
        if not table_metadata:
            raise TableNotFoundError(table_name)

        heap_page_id = table_metadata['heap_root_page_id']
        heap_page_raw = self.bpm.fetch_page(heap_page_id)
        if not heap_page_raw:
            raise IOError(f"无法为表 '{table_name}' 获取堆页面 {heap_page_id}。")

        results = []
        try:
            table_heap = TableHeapPage.deserialize(heap_page_raw.data)
            for data_page_id in table_heap.get_page_ids():
                page_raw = self.bpm.fetch_page(data_page_id)
                if not page_raw: continue
                try:
                    data_page = DataPage(page_raw.page_id, page_raw.data)
                    for offset, record in data_page.get_all_records():
                        results.append(((data_page_id, offset), record[ROW_LENGTH_PREFIX_SIZE:]))
                finally:
                    self.bpm.unpin_page(data_page_id, False)
        finally:
            self.bpm.unpin_page(heap_page_id, False)

        return results

    def read_row(self, table_name: str, rid: Tuple[int, int]) -> Optional[bytes]:
        """根据RID（记录ID）读取单行数据。"""
        page_id, offset = rid
        page = self.bpm.fetch_page(page_id)
        if not page: return None
        try:
            data_page = DataPage(page.page_id, page.data)
            record = data_page.get_record(offset)
            return record[ROW_LENGTH_PREFIX_SIZE:] if record else None
        finally:
            self.bpm.unpin_page(page_id, False)

    def _serialize_row(self, table_name: str, row_dict: Dict[str, Any]) -> bytes:
        metadata = self.catalog_page.get_table_metadata(table_name)
        if not metadata: raise TableNotFoundError(table_name)
        schema = metadata['schema']
        row_data = bytearray()

        for col_name in schema.keys():
            val = row_dict[col_name]
            col_type = schema[col_name].data_type
            if col_type == DataType.INT:
                row_data.extend(int(val).to_bytes(4, "little", signed=True))
            elif col_type in (DataType.TEXT, DataType.STRING):
                encoded_str = str(val).encode("utf-8")
                row_data.extend(len(encoded_str).to_bytes(4, "little"))
                row_data.extend(encoded_str)
            elif col_type == DataType.FLOAT:
                row_data.extend(struct.pack("<f", float(val)))
            else:
                raise NotImplementedError(f"不支持的数据类型: {col_type}")
        return bytes(row_data)

    def _decode_row(self, table_name: str, row_data: bytes) -> Dict[str, Any]:
        metadata = self.catalog_page.get_table_metadata(table_name)
        if not metadata: raise TableNotFoundError(table_name)
        schema = metadata['schema']
        row_dict = {}
        offset = 0
        for col_name, col_def in schema.items():
            value, new_offset = self._decode_value(row_data, offset, col_def.data_type)
            row_dict[col_name] = value
            offset = new_offset
        return row_dict

    def _decode_value_from_row(self, row_data: bytes, col_index: int, schema: Dict[str, Any]) -> Tuple[Any, int]:
        offset = 0
        current_col_idx = 0
        for col_def in schema.values():
            if current_col_idx == col_index:
                return self._decode_value(row_data, offset, col_def.data_type)
            _, offset = self._decode_value(row_data, offset, col_def.data_type)
            current_col_idx += 1
        raise ValueError(f"列索引 {col_index} 越界。")

    def _decode_value(self, row_data: bytes, offset: int, col_type: DataType) -> Tuple[Any, int]:
        try:
            if col_type == DataType.INT:
                value = int.from_bytes(row_data[offset: offset + 4], "little", signed=True)
                offset += 4
            elif col_type in (DataType.TEXT, DataType.STRING):
                length = int.from_bytes(row_data[offset: offset + 4], "little")
                offset += 4
                value = row_data[offset: offset + length].decode("utf-8")
                offset += length
            elif col_type == DataType.FLOAT:
                value, = struct.unpack("<f", row_data[offset: offset + 4])
                offset += 4
            else:
                raise NotImplementedError(f"不支持的解码类型: {col_type.name}")
            return value, offset
        except (struct.error, IndexError, UnicodeDecodeError) as e:
            raise ValueError(f"从偏移量 {offset} 解码类型 {col_type.name} 失败: {e}")
