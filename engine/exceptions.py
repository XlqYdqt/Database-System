class TableAlreadyExistsError(Exception):
    """Exception raised when attempting to create a table that already exists."""
    def __init__(self, table_name):
        self.table_name = table_name
        super().__init__(f"Table '{table_name}' already exists.")