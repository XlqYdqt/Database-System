class TableAlreadyExistsError(Exception):
    """当试图创建已存在的表时抛出此异常。"""
    def __init__(self, table_name):
        self.table_name = table_name
        super().__init__(f"表 '{table_name}' 已存在。")

class TableNotFoundError(Exception):
    """当操作一个不存在的表时抛出此异常。"""
    def __init__(self, table_name):
        self.table_name = table_name
        super().__init__(f"表 '{table_name}' 不存在。")

class PrimaryKeyViolationError(Exception):
    """当试图插入重复的主键时抛出此异常。"""
    def __init__(self, key_value):
        self.key_value = key_value
        super().__init__(f"主键约束冲突，键值为 '{key_value}'。")

