
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

class TransactionManager:
    def __init__(self, storage_engine):
        self.storage_engine = storage_engine
        self.transactions = {}
        self.next_txn_id = 0

    def begin_transaction(self):
        txn_id = self.next_txn_id
        self.next_txn_id += 1
        self.transactions[txn_id] = {'state': 'active', 'writes': []}
        print(f"Transaction {txn_id} started.")
        return txn_id

    # def commit_transaction(self, txn_id):
    #     if txn_id not in self.transactions:
    #         raise ValueError(f"Transaction {txn_id} not found.")
    #     if self.transactions[txn_id]['state'] != 'active':
    #         raise ValueError(f"Transaction {txn_id} is not active.")

        # 提交逻辑：目前，所有写入操作在 StorageEngine 中已直接持久化。
        # 因此，这里只需标记事务为已提交并清理。
        print(f"Transaction {txn_id} committed.")
        self.transactions[txn_id]['state'] = 'committed'
        del self.transactions[txn_id]

    def abort_transaction(self, txn_id):
        if txn_id not in self.transactions:
            raise ValueError(f"Transaction {txn_id} not found.")
        if self.transactions[txn_id]['state'] != 'active':
            raise ValueError(f"Transaction {txn_id} is not active.")

        # 回滚逻辑：遍历写入集，撤销所有操作
        print(f"Transaction {txn_id} aborting...")
        for write in reversed(self.transactions[txn_id]['writes']):
            table_name = write['table_name']
            rid = write['rid']
            old_data = write['old_data']
            new_data = write['new_data']

            if old_data is None and new_data is not None:  # 插入操作的回滚
                print(f"Rollback: Deleting inserted record at RID {rid}")
                self.storage_engine.delete_row_by_rid(table_name, rid, txn_id=None)
            elif old_data is not None and new_data is None:  # 删除操作的回滚
                print(f"Rollback: Re-inserting deleted record at RID {rid}")
                self.storage_engine.insert_row(table_name, old_data, txn_id=None)
            elif old_data is not None and new_data is not None:  # 更新操作的回滚
                print(f"Rollback: Restoring record at RID {rid} with old data")
                self.storage_engine.update_row_by_rid(table_name, rid, old_data, txn_id=None)

        self.transactions[txn_id]['state'] = 'aborted'
        del self.transactions[txn_id]
        print(f"Transaction {txn_id} aborted.")

    def add_write_set(self, txn_id, table_name, rid, old_data, new_data):
        if txn_id not in self.transactions:
            raise ValueError(f"Transaction {txn_id} not found.")
        self.transactions[txn_id]['writes'].append({'table_name': table_name, 'rid': rid, 'old_data': old_data, 'new_data': new_data})

    def get_transaction_state(self, txn_id):
        return self.transactions.get(txn_id, {}).get('state')

    def commit_transaction(self, txn_id):
        if txn_id not in self.transactions:
            raise ValueError(f"Transaction {txn_id} not found.")
        if self.transactions[txn_id]['state'] != 'active':
            raise ValueError(f"Transaction {txn_id} is not active.")

        print(f"Transaction {txn_id} committing...")

        # 🚩真正执行缓存的写操作
        for write in self.transactions[txn_id]['writes']:
            table_name = write['table_name']
            new_data = write['new_data']
            old_data = write['old_data']
            rid = write['rid']

            if old_data is None and new_data is not None:  # 插入操作
                self.storage_engine._do_insert_immediate(table_name, new_data)

            elif old_data is not None and new_data is None:  # 删除操作
                self.storage_engine._do_delete_immediate(table_name, rid)


            elif old_data is not None and new_data is not None:  # UPDATE

                # 先执行真正的数据更新
                self.storage_engine._do_update_immediate(table_name, rid, new_data)

                # new_rid = self.storage_engine._do_update_immediate(table_name, rid, new_data)

                # # 如果表有索引，检查主键是否变化
                #
                # schema = self.storage_engine.catalog_page.get_table_metadata(table_name)['schema']
                #
                # pk_col_def, pk_col_index = self.storage_engine._get_pk_info(schema)
                #
                # old_pk_value, _ = self.storage_engine._decode_value_from_row(old_data, pk_col_index, schema)
                #
                # new_pk_value, _ = self.storage_engine._decode_value_from_row(new_data, pk_col_index, schema)
                #
                # if old_pk_value != new_pk_value or (new_rid != rid):
                #
                #     old_pk_bytes = self.storage_engine._prepare_key_for_b_tree(old_pk_value, pk_col_def.data_type)
                #
                #     new_pk_bytes = self.storage_engine._prepare_key_for_b_tree(new_pk_value, pk_col_def.data_type)
                #
                #     bplus_tree = self.storage_engine.indexes.get(table_name)
                #
                #     if bplus_tree:
                #
                #         bplus_tree.delete(old_pk_bytes)
                #
                #         insert_result = bplus_tree.insert(new_pk_bytes, new_rid)
                #
                #         if insert_result:
                #             self.storage_engine.update_index_root(table_name, bplus_tree.root_page_id)

        print(f"Transaction {txn_id} committed.")
        self.transactions[txn_id]['state'] = 'committed'
        del self.transactions[txn_id]
