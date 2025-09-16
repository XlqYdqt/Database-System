from typing import Any, Iterable, List

from sql.ast import SelectStatement
from sql.planner import Planner, LogicalPlan, Operator


class SubqueryOperator:
    """
    执行子查询逻辑计划并返回一维值列表（用于 IN 或标量比较）。
    - plan: 可以是 LogicalPlan / SelectStatement / Operator
    - executor: 主执行器实例
    """

    def __init__(self, plan, executor: Any):
        self.plan = plan
        self.executor = executor
        self._cached_result = None  # 缓存非相关子查询结果，避免重复执行

    def _normalize_rows(self, rows: Iterable) -> List:
        """把 executor 返回的 rows 规范化成一维值列表（取每行的第一个字段）"""
        vals = []
        for item in rows:
            # Case 1: (rid, dict) or (rid, row_list)
            if isinstance(item, tuple) and len(item) == 2:
                _, row = item
                if isinstance(row, dict):
                    first_col_val = next(iter(row.values()))
                    vals.append(first_col_val)
                elif isinstance(row, (list, tuple)):
                    vals.append(row[0])
                else:
                    vals.append(row)
                continue

            # Case 2: dict
            if isinstance(item, dict):
                first_col_val = next(iter(item.values()))
                vals.append(first_col_val)
                continue

            # Case 3: list/tuple (no rid)
            if isinstance(item, (list, tuple)):
                vals.append(item[0])
                continue

            # Case 4: scalar value
            vals.append(item)
        return vals

    def execute(self) -> List:
        """
        执行子查询并返回一个“扁平的”一维值列表。
        """
        if self._cached_result is not None:
            return self._cached_result

        plan_obj = self.plan
        if isinstance(plan_obj, SelectStatement):
            # 假设 Planner 不需要 catalog
            planner = Planner()
            plan_obj = planner.plan(plan_obj)

        plans = plan_obj if isinstance(plan_obj, list) else [plan_obj]

        rows = self.executor.execute(plans)
        if not rows:
            self._cached_result = []
            return self._cached_result

        vals = self._normalize_rows(rows)
        self._cached_result = vals

        return vals
