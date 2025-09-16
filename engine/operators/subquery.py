from typing import Any, Iterable, List

from sql.ast import SelectStatement
from sql.planner import Planner, LogicalPlan


class SubqueryOperator:
    """
    执行子查询逻辑计划并返回一维值列表（用于 IN 或标量比较）。
    """

    def __init__(self, plan, executor: Any):
        self.plan = plan
        self.executor = executor
        self._cached_result = None  # 缓存非相关子查询结果

    def _normalize_rows(self, rows: Iterable) -> List:
        """
        [FIX] 把 executor 返回的 rows 规范化成一维值列表，并严格校验列数。
        """
        vals = []
        for item in rows:
            row_to_check = None
            if isinstance(item, tuple) and len(item) == 2:
                _, row = item
                row_to_check = row
            else:
                row_to_check = item

            # 严格校验子查询结果只能有一列
            if isinstance(row_to_check, dict):
                if len(row_to_check) != 1:
                    raise RuntimeError(f"子查询只能返回一列，但实际返回了 {len(row_to_check)} 列: {list(row_to_check.keys())}")
                vals.append(next(iter(row_to_check.values())))
            elif isinstance(row_to_check, (list, tuple)):
                if len(row_to_check) != 1:
                    raise RuntimeError(f"子查询只能返回一列，但实际返回了 {len(row_to_check)} 列")
                vals.append(row_to_check[0])
            else: # 标量值
                vals.append(row_to_check)
        return vals

    def execute(self) -> List:
        """
        执行子查询并返回一个“扁平的”一维值列表。
        """
        if self._cached_result is not None:
            return self._cached_result

        plan_obj = self.plan
        if isinstance(plan_obj, SelectStatement):
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

