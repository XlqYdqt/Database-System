from typing import Dict, Any, List, Tuple, Optional

from engine.storage_engine import StorageEngine
from sql.ast import Operator, Expression, Column, Literal, BinaryExpression
from engine.exceptions import PrimaryKeyViolationError, UniquenessViolationError


class UpdateOperator(Operator):
    """
    UPDATE 操作的执行算子 (重构版)
    职责分离：本算子只负责找出需要更新的行并计算新值，具体的更新操作（包括数据和所有索引的维护）
    完全委托给 StorageEngine 的原子方法来完成。
    """

    def __init__(self, table_name: str, child: Operator, updates: List[Tuple[str, Expression]],
                 storage_engine: StorageEngine, executor: Any):
        self.table_name = table_name
        self.child = child
        self.updates = updates
        self.storage_engine = storage_engine
        self.executor = executor

    def execute(self) -> List[Any]:
        """
        执行UPDATE操作。
        1. 通过执行子计划获取所有待更新行的原始RID和数据。
        2. 对每一行，根据SET子句计算出新的行数据。
        3. 调用 StorageEngine 中封装好的、保证原子性的 update_row 方法。
        """
        # Step 1: 找出所有需要被更新的行
        rows_to_update: List[Tuple[Tuple[int, int], Dict[str, Any]]] = self.executor.execute(self.child)

        updated_count = 0
        for original_rid, original_row_dict in rows_to_update:
            try:
                # Step 2: 计算更新后的新行数据
                new_row_dict = dict(original_row_dict)
                for col_name, expr in self.updates:
                    new_row_dict[col_name] = self._eval_expr(expr, original_row_dict)

                # Step 3: 调用 StorageEngine 的原子更新方法
                # 该方法会处理唯一性检查、数据页更新、所有相关索引的删除和插入
                if self.storage_engine.update_row(self.table_name, original_rid, new_row_dict):
                    updated_count += 1

            except (PrimaryKeyViolationError, UniquenessViolationError) as e:
                # 如果更新违反了唯一约束，这是一个明确的失败，打印错误并跳过
                print(f"错误: 更新行 {original_row_dict} 失败，违反了唯一性约束: {e}")
                continue
            except Exception as e:
                # 其他未知错误
                print(f"警告：更新行 {original_row_dict} 时发生未知错误，已跳过: {e}")
                continue

        return [updated_count]

    def _eval_expr(self, expr: Expression, row: Dict[str, Any]) -> Any:
        """递归地对表达式求值。"""
        if isinstance(expr, Literal): return expr.value
        if isinstance(expr, Column): return row.get(expr.name)
        if isinstance(expr, BinaryExpression):
            left_val = self._eval_expr(expr.left, row)
            right_val = self._eval_expr(expr.right, row)
            op = getattr(expr, "op", getattr(expr, "operator", None))
            op_val = op.value
            if op_val == '=': return left_val == right_val
            if op_val == '>': return left_val > right_val
            if op_val == '<': return left_val < right_val
            if op_val == '>=': return left_val >= right_val
            if op_val == '<=': return left_val <= right_val
            if op_val in ('!=', '<>'): return left_val != right_val
            # 注意: 此处不处理 AND/OR，因为它们在 FilterOperator 中处理
            raise NotImplementedError(f"不支持的二元运算符: {op_val}")
        raise NotImplementedError(f"不支持的表达式类型: {type(expr)}")

