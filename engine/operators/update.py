from typing import Dict, Any, List, Tuple, Optional

from engine.storage_engine import StorageEngine
from sql.ast import Operator, Expression, Column, Literal, BinaryExpression
from engine.exceptions import PrimaryKeyViolationError, UniquenessViolationError


class UpdateOperator(Operator):
    """
    UPDATE 操作的执行算子 (重构版)
    职责分离：本算子只负责找出需要更新的行并计算新值，具体的更新操作
    完全委托给 StorageEngine 的原子方法来完成。
    """

    def __init__(self, table_name: str, child: Operator, updates: List[Tuple[str, Expression]],
                 storage_engine: StorageEngine, executor: Any, txn_id: Optional[int] = None):
        self.table_name = table_name
        self.child = child
        self.updates = updates
        self.storage_engine = storage_engine
        self.executor = executor
        self.txn_id = txn_id

    def execute(self) -> List[Any]:
        """执行UPDATE操作。"""
        # 1. 通过子计划（通常是Filter或SeqScan）获取待更新行的RID和原始数据
        rows_to_update: List[Tuple[Tuple[int, int], Dict[str, Any]]] = self.executor.execute([self.child])

        updated_count = 0
        for original_rid, original_row_dict in rows_to_update:
            try:
                # 2. 基于原始数据和SET子句，计算出更新后的新行字典
                new_row_dict = dict(original_row_dict)
                for col_name, expr in self.updates:
                    new_row_dict[col_name] = self._eval_expr(expr, original_row_dict)

                # 3. 调用 StorageEngine 的统一更新接口，并传入事务ID
                if self.storage_engine.update_row(self.table_name, original_rid, new_row_dict, self.txn_id):
                    updated_count += 1

            except (PrimaryKeyViolationError, UniquenessViolationError) as e:
                print(f"错误: 更新行 {original_row_dict} 失败，违反约束: {e}")
                # 在事务中，一个失败通常会导致整个事务回滚，但这里我们先简单跳过
                continue
            except Exception as e:
                print(f"警告：更新行 {original_row_dict} 时发生未知错误，已跳过: {e}")
                continue

        return [f"{updated_count} 行已更新"]

    def _eval_expr(self, expr: Expression, row: Dict[str, Any]) -> Any:
        """递归地对表达式求值。"""
        if isinstance(expr, Literal): return expr.value
        if isinstance(expr, Column): return row.get(expr.name)
        if isinstance(expr, BinaryExpression):
            left_val = self._eval_expr(expr.left, row)
            right_val = self._eval_expr(expr.right, row)
            op_val = expr.op.value
            if op_val == '=': return left_val == right_val
            if op_val == '>': return left_val > right_val
            if op_val == '<': return left_val < right_val
            if op_val == '>=': return left_val >= right_val
            if op_val == '<=': return left_val <= right_val
            if op_val in ('!=', '<>'): return left_val != right_val
            raise NotImplementedError(f"不支持的二元运算符: {op_val}")
        raise NotImplementedError(f"不支持的表达式类型: {type(expr)}")
