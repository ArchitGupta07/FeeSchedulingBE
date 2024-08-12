
from services.table_manager import TableManager


class VersionManager(TableManager):

    def __init__(self, table_name) -> None:
        self.table_name = table_name
        # self.df = super().fetch_table_from_db(table_name, db:Database)
        

    def apply_row_changes(self):
        pass

    def delete_row_changes(self):
        pass

    def apply_column_changes(self):
        pass

    def delete_column_changes(self):
        pass

    def apply_cell_changes(self):
        pass
    
    def delete_cell_changes(self):
        pass