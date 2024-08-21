
from sqlalchemy import text


from sqlalchemy.exc import SQLAlchemyError
from fastapi import HTTPException
# 
import pandas as pd
from db import Database


class VersionManager():

    def __init__(self, table_name=None) -> None:
        self.table_name = table_name
        self.db = Database()
        
        # self.df = super().fetch_table_from_db(table_name, db:Database)
        

    def fetch_table_from_db_check(self, table_name) :
        query = text(f"SELECT * FROM {table_name}")
        try :
            result = self.db.execute(query)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
        rows = result.fetchall()

        columns = result.keys()  # Retrieve column names from the result

        df = pd.DataFrame(rows, columns=columns)
        # print(df.to_dict(orient="records"))
        return df.to_dict(orient="records")
    
    def get_table_version(self, table_name ) :
        query = text("SELECT * FROM table_versions WHERE table_name = :table_name")
        try:
            # Execute the query with the table_name as a parameter
            result = self.db.execute(query, {"table_name": table_name})
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
        rows = result.fetchall()

        columns = result.keys()  # Retrieve column names from the result

        df = pd.DataFrame(rows, columns=columns)
        # print(df.to_dict(orient="records"))
        return df.to_dict(orient="records")
    

    def get_table_changes(self, version_id ) :
        query = text("SELECT * FROM table_changes WHERE version_id = :version_id")
        try:
            # Execute the query with the table_name as a parameter
            result = self.db.execute(query, {"version_id": version_id})
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
        rows = result.fetchall()

        columns = result.keys()  # Retrieve column names from the result

        df = pd.DataFrame(rows, columns=columns)
        # print(df.to_dict(orient="records"))
        return df.to_dict(orient="records")
    

    def get_cell_changes(self, version_id ) :
        query = text("SELECT * FROM cell_changes WHERE version_id = :version_id")
        try:
            # Execute the query with the table_name as a parameter
            result = self.db.execute(query, {"version_id": version_id})
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
        rows = result.fetchall()

        columns = result.keys()  # Retrieve column names from the result

        df = pd.DataFrame(rows, columns=columns)
        # print(df.to_dict(orient="records"))
        return df.to_dict(orient="records")

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

    
    def add_new_columns(self,new_columns, table_name):   

        try:
            # Iterate through the dictionary of new columns and their types
            for col_name, col_type in new_columns.items():
                # Check if the column already exists in the table
                check_column_query = text(f"""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = :table_name 
                    AND column_name = :col_name
                """)
                result = self.db.execute(check_column_query, {"table_name": table_name, "col_name": col_name}).fetchone()

                if result is None:  # Column does not exist
                    # Construct the ALTER TABLE query to add the new column with NULL default
                    alter_query = text(f"ALTER TABLE {table_name} ADD COLUMN {col_name} {col_type} DEFAULT NULL")
                    self.db.execute(alter_query)
                
            # Commit the changes to the database
            # self.db.commit()

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
            # self.db.commit()

        except SQLAlchemyError as e:
            # Rollback the transaction in case of an error
            self.db.rollback()
            raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

    def apply_table_operations(self, operations_data, table_name, version_id):
        try:
            for operation in operations_data:
                operation_type = operation['operations'].upper()
                row_hash = operation['row_name']  # This is the unique identifier for the row
                # column_name = operation['column_name']
                column_name = f'"{operation['column_name']}"'
                new_value = operation['new_value']

                if operation_type == "ADD":
                    # Insert a new row or update the column in the table
                    insert_query = text(f"""
                        INSERT INTO {table_name} (hash, {column_name})
                        VALUES (:row_hash, :new_value)
                        ON CONFLICT (hash) DO UPDATE 
                        SET {column_name} = :new_value
                    """)
                    self.db.execute(insert_query, {"row_hash": row_hash, "new_value": new_value})

                elif operation_type == "UPDATE":
                    # Update the existing row in the table
                    update_query = text(f"""
                        UPDATE {table_name}
                        SET {column_name} = :new_value
                        WHERE hash = :row_hash
                    """)
                    self.db.execute(update_query, {"row_hash": row_hash, "new_value": new_value})

                elif operation_type == "DELETE":
                    # Delete the row from the table (if DELETE operation was present)
                    delete_query = text(f"""
                        DELETE FROM {table_name}
                        WHERE hash = :row_hash
                    """)
                    self.db.execute(delete_query, {"row_hash": row_hash})

               
            update_version_status_query = text("""
                UPDATE table_versions
                SET isapproved = :new_status
                WHERE id = :version_id
                """)
            self.db.execute(update_version_status_query, {
                "new_status": True,  
                "version_id": version_id  
            }) 
            # Commit the changes
            # self.db.commit()

        except SQLAlchemyError as e:
            # Rollback in case of an error
            print(e)
            # self.db.rollback()
            raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

    def apply_new_changes(self, tableName):

        self.tableName=tableName

        data  = self.get_table_version(tableName)
        unapproved_entries = [entry for entry in data if not entry['isapproved']]

        # Sort by created_at in descending order
        unapproved_entries.sort(key=lambda x: x['created_at'], reverse=True)

        # Get the UUID of the latest unapproved entry
        if unapproved_entries:
            latest_unapproved_uuid = unapproved_entries[0]['id']
            print(f"The UUID of the latest unapproved entry is: {latest_unapproved_uuid}")
        else:
            print("No unapproved entries found.")


        data = self.fetch_table_from_db_check("table_changes")

        data = self.get_table_changes(latest_unapproved_uuid)
        print(data)

        new_columns = {}
        deleted_cols = {}
        deleted_rows = []

        for item in data:
            if item["type"]=="COLUMN":
                if item["operations"]=="ADD":
                    new_columns.update(item["values"])
                elif item["operations"]=="DELETE":
                    deleted_cols.update(item["values"])
            
            else:
                for hashes in item["values"]:
                    deleted_rows.append(hashes)
            
        # print(new_columns, deleted_cols, deleted_rows)

        self.add_new_columns(new_columns, tableName)

        newData = self.get_cell_changes(latest_unapproved_uuid)



        print(newData)

        self.apply_table_operations(newData, tableName, latest_unapproved_uuid)

            



        return None

# check = VersionManager("cell_changes")

# print(check.fetch_table_from_db_check())