from sqlalchemy import text


from sqlalchemy.exc import SQLAlchemyError
from fastapi import HTTPException
# 
import pandas as pd
from db import Database
from sqlalchemy.sql import text
from services.table_manager import TableManager


class BackTracker():

    def __init__(self, table_name=None) -> None:
        self.table_name = table_name
        self.db = Database()

    def get_newer_version_ids(self, table_name, current_version_id):
        # Step 1: Get the timestamp of the current version
        timestamp_query = text("""
            SELECT created_at
            FROM table_versions 
            WHERE table_name = :table_name AND id = :current_version_id
        """)
        
        try:
            # Execute the query to get the timestamp for the current version
            timestamp_result = self.db.execute(timestamp_query, {"table_name": table_name, "current_version_id": current_version_id})
            current_timestamp_row = timestamp_result.fetchone()
            
            if not current_timestamp_row:
                raise HTTPException(status_code=404, detail="Current version ID not found")
            
            current_timestamp = current_timestamp_row.created_at
            
            # Step 2: Get all version IDs newer than the current timestamp
            newer_versions_query = text("""
                SELECT id
                FROM table_versions 
                WHERE table_name = :table_name AND created_at > :current_timestamp
                AND isapproved = true                     
                ORDER BY created_at DESC
            """)
            
            result = self.db.execute(newer_versions_query, {"table_name": table_name, "current_timestamp": current_timestamp})
            rows = result.fetchall()
            
            # List to hold all version_ids newer than the current one
            newer_version_ids = [row.id for row in rows]

            print(newer_version_ids)
            
            return newer_version_ids
        
        except SQLAlchemyError as e:           
            raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

        
    def get_current_table_cols(self,version_id):

        query = text("""
            SELECT * 
            FROM table_versions 
            WHERE id = :version_id
        """)
        
        try:
            # Execute the query with the provided version_id
            result = self.db.execute(query, {"version_id": version_id})
            row = result.fetchone()
            print("active columns................",row.active_columns)
            return list(row.active_columns)
        except SQLAlchemyError as e:           
            raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
        

    def get_new_hashes(self,versions):

        new_hashes = {}


        for id in versions:
            table_changes_query = text("""
                SELECT values
                FROM table_changes 
                WHERE version_id = :version_id and type = 'ROW' AND operations = 'ADD'   
            """)
            
            result = self.db.execute(table_changes_query, {"version_id": id})
            rows = result.fetchall()
            if rows:
                row = rows[-1]
                new_hashes.update(row[0])

        print("\n\n")
        print(new_hashes)
        print("\n\n")

        return [hash for hash in new_hashes]
    


    def get_updated_cell_changes(self, version_id):

        query = text("SELECT * FROM cell_changes WHERE operations = 'UPDATE' AND version_id = :version_id")
        try:
            # Execute the query with the table_name as a parameter
            result = self.db.execute(query, {"version_id": version_id})
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
        rows = result.fetchall()

        columns = result.keys()  # Retrieve column names from the result

        df = pd.DataFrame(rows, columns=columns)
        print(df.to_dict(orient="records"))
        return df.to_dict(orient="records")
        
        
    

    def revert_cell_changes(self, table_name, version_id):
        pass


    def revert_table_changes(self,table_name,version_id):

        active_columns = self.get_current_table_cols(version_id)
        table_manager_obj = TableManager()
        df = table_manager_obj.fetch_table_from_db(table_name,self.db)

        columns_to_remove = [col for col in df.columns if col not in active_columns]
        df = df.drop(columns=columns_to_remove)


        newer_versions =  self.get_newer_version_ids(table_name, version_id)

        new_row_hashes= self.get_new_hashes(newer_versions)

        df = df[~df['hash'].isin(new_row_hashes)]


        for version in newer_versions:
            for change in self.get_updated_cell_changes(version):
                    row_hash = change['row_name']  # Get the row identifier (hash)
                    column_name = change['column_name']  # Get the column to be reverted
                    old_value = change['old_val']  # Get the old value
                    
                    # Check if the row exists in the DataFrame
                    if row_hash in df['hash'].values:
                        df.loc[df['hash'] == row_hash, column_name] = old_value
        print(df)
        return df





# check = BackTracker()
# check.get_newer_version_ids("demo100_20240821_150037","84cf2789-1799-4884-bfee-9e81bd3e42b2")
    