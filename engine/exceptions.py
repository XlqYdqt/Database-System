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

class UniquenessViolationError(Exception):
    """当试图在有唯一约束的列上插入重复值时抛出此异常。"""
    def __init__(self, column_name: str, key_value: any):
        self.column_name = column_name
        self.key_value = key_value
        super().__init__(f"唯一约束冲突：列 '{column_name}' 的值 '{key_value}' 已存在。")
