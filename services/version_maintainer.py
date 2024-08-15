
from services.table_manager import TableManager


class VersionManager(TableManager):

    def __init__(self, table_name,db) -> None:
        self.table_name = table_name
        self.db = db
        
        # self.df = super().fetch_table_from_db(table_name, db:Database)
        

    def apply_row_changes(self):
        #print("check")
        pass

    def delete_row_changes(self):
        pass

    def apply_column_changes(self):
        pass

    def delete_column_changes(self):
        pass

    def apply_cell_changes(self):
        import pandas as pd

# Sample existing DataFrame
        # data = {
        #     'hash': ['abc123', 'def456', 'ghi789'],
        #     'name': ['Alice', 'Bob', 'Charlie'],
        #     'age': [25, 30, 35]
        # }

        # df = pd.DataFrame(data).set_index('hash')

        # # Dictionary with new additions/updates
        # new_data = {
        #     'abc123': {'name': 'Alicia', 'age': 26},   # Update existing row
        #     'xyz123': {'name': 'David', 'age': 40},    # Add new row
        #     'def456': {'name': 'Robert'}               # Partial update
        # }

        # # Convert dictionary to DataFrame
        # new_df = pd.DataFrame.from_dict(new_data, orient='index')

        # # Update the existing DataFrame
        # df.update(new_df)
        # df = pd.concat([df, new_df[~new_df.index.isin(df.index)]])

        # print(df)

        pass
    
    def delete_cell_changes(self):
        pass