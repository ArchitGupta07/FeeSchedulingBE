
from sqlalchemy import text
from services.table_manager import TableManager

from sqlalchemy.exc import SQLAlchemyError
from fastapi import HTTPException


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

    def apply_column_changes(self, table_changes):
        new_columns = {}
        deleted_columns = []
        for k,v  in table_changes.items():

            if v["operation"]=="ADD":

                new_columns.update(v["values"])
            elif v["operation"]=="DELETE":
                deleted_columns.extend(list(v["values"].keys()))

        self.add_new_columns(new_columns)
        self.delete_columns(deleted_columns)


        return None

    def delete_column_changes(self):
        pass
     
# {type: 'COLUMN', operation: 'ADD', values: '{"age": "numeric"}'}
# 1
# : 
# {type: 'COLUMN', operation: 'DELETE', values: '{"pa": "numeric"}'}
# 2
# : 
# {type: 'ROW', operation: 'DELETE', values: '{"bfda6d6f68": ""}'}

#         pass


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

    
    def add_new_columns(self,new_columns):
        try:
        # Iterate through the list of new columns and add each to the table
            for col_name in new_columns:
                # Construct the ALTER TABLE query to add a new column with NULL default

                alter_query_parts = []
                for col_name, col_type in new_columns.items():
                    alter_query_parts.append(f"ADD COLUMN {col_name} {col_type} DEFAULT NULL")
                
                alter_query = f"ALTER TABLE {self.table_name} " + ", ".join(alter_query_parts)
                # alter_query = text(f"ALTER TABLE {table_name} ADD COLUMN {col_name} TEXT DEFAULT NULL")
                self.db.execute(alter_query)
            
            # Commit the changes to the database
            self.db.commit()

        except SQLAlchemyError as e:
            # Rollback the transaction in case of an error
            self.db.rollback()
            raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


    def delete_columns(self, deleted_columns):
       
        try:
            # Iterate through the list of columns to delete and drop each from the table
            for col_name in deleted_columns:
                # Construct the ALTER TABLE query to drop the column
                drop_query = text(f"ALTER TABLE {self.table_name} DROP COLUMN {col_name}")
                self.db.execute(drop_query)
            
            # Commit the changes to the database
            self.db.commit()

        except SQLAlchemyError as e:
            # Rollback the transaction in case of an error
            self.db.rollback()
            raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")