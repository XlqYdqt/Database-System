from typing import List, Any
from sql.ast import Column, Operator

class SortOperator(Operator):
    """可靠多列排序算子，支持字符串/数字混合 ASC/DESC"""

    def __init__(self, child: Operator, keys: List[Any], orders: List[str], executor: Any):
        self.child = child
        self.keys = keys
        self.orders = orders
        self.executor = executor

    def execute(self) -> List[Any]:
        rows = self.executor.execute([self.child])

        # 统一获取行 dict
        def get_row_dict(item):
            if isinstance(item, tuple) and len(item) == 2:
                _, row_dict = item
            elif isinstance(item, dict):
                row_dict = item
            else:
                raise ValueError(f"Cannot sort row: {item}")
            return row_dict

        # 组合 keys 和 orders
        key_order_list = []
        for i, k in enumerate(self.keys):
            order = self.orders[i] if i < len(self.orders) else "ASC"
            if isinstance(k, Column):
                col_name = k.name
            else:
                col_name = str(k)
            key_order_list.append((col_name, order.upper()))

        # 稳定排序：先排次列，后排主列
        for col_name, order in reversed(key_order_list):
            reverse = (order == "DESC")

            def sort_key(item):
                val = get_row_dict(item).get(col_name)
                if val is None:
                    # None 安全处理
                    return "" if reverse else chr(127)*100
                return val

            rows.sort(key=sort_key, reverse=reverse)

        return rows
