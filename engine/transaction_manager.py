from typing import Dict, Any, Optional, List, Tuple


class TransactionManager:
    def __init__(self, storage_engine):
        self.storage_engine = storage_engine
        self.transactions: Dict[int, Dict[str, Any]] = {}
        self.next_txn_id = 0

    def begin_transaction(self) -> int:
        """开始一个新事务。"""
        txn_id = self.next_txn_id
        self.next_txn_id += 1
        self.transactions[txn_id] = {'state': 'active', 'write_set': []}
        print(f"事务 {txn_id} 已开始。")
        return txn_id

    def commit_transaction(self, txn_id: int):
        """提交一个事务，将所有挂起的写操作应用到数据库。"""
        if txn_id not in self.transactions or self.transactions[txn_id]['state'] != 'active':
            raise ValueError(f"事务 {txn_id} 不存在或不处于活动状态。")

        print(f"事务 {txn_id} 正在提交...")

        write_set = self.transactions[txn_id]['write_set']

        # 按照顺序应用写集合中的所有操作
        for write_record in write_set:
            op_type = write_record['op_type']
            table_name = write_record['table_name']

            if op_type == 'INSERT':
                self.storage_engine._do_insert_immediate(
                    table_name,
                    write_record['new_data'],
                    write_record['new_dict']
                )
            elif op_type == 'DELETE':
                self.storage_engine._do_delete_immediate(
                    table_name,
                    write_record['rid'],
                    write_record['old_dict']
                )
            elif op_type == 'UPDATE':
                self.storage_engine._do_update_immediate(
                    table_name,
                    write_record['rid'],
                    write_record['old_dict'],
                    write_record['new_data'],
                    write_record['new_dict']
                )

        print(f"事务 {txn_id} 已提交。")
        self.transactions[txn_id]['state'] = 'committed'
        del self.transactions[txn_id]

    def abort_transaction(self, txn_id: int):
        """中止一个事务，丢弃所有更改。"""
        if txn_id not in self.transactions or self.transactions[txn_id]['state'] != 'active':
            raise ValueError(f"事务 {txn_id} 不存在或不处于活动状态。")

        # 由于我们采用延迟写入，中止事务只需要丢弃写集合即可
        print(f"事务 {txn_id} 正在中止...")
        self.transactions[txn_id]['state'] = 'aborted'
        del self.transactions[txn_id]
        print(f"事务 {txn_id} 已中止。")

    def add_write_record(self, txn_id: int, **kwargs):
        """向指定事务的写集合中添加一条写记录。"""
        if txn_id not in self.transactions:
            raise ValueError(f"事务 {txn_id} 不存在。")
        self.transactions[txn_id]['write_set'].append(kwargs)