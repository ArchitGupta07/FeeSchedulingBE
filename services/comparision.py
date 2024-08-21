import json
import traceback

from fastapi import File, HTTPException, UploadFile
import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from db import Database
from enums import Axis, Operations
from services.table_manager import TableManager
from utils.helper import add_hash_col, convert_column_to_numeric, convert_to_python_type, dtype_to_postgres, infer_type, remove_null_values
class Comparision : 
    
    async def create_df_from_excel(self,table_name : str,cmp_file : UploadFile,db : Database) :
        content = await cmp_file.read()
        df = pd.read_excel(content)
        tbl_obj = TableManager()

        df = tbl_obj.find_table_headers(df)
        df = remove_null_values(df)
        df = convert_column_to_numeric(df)
        

        #print("cols......................",df.columns)

        df.columns = [col.lower() for col in df.columns]
        #new df doesnt have hash column we have to retrieve the hash col from the table meta data add apply hash col
        hashable_cols = []
        query = text("SELECT hashable_cols FROM table_details WHERE table_name = :table_name")
        result = db.execute(query,{"table_name": table_name})

        query_data = result.fetchone()

        hashable_cols = query_data[0].split(",")
        #print("cols...........................",hashable_cols)
        # #print("cols......................",df.columns)
        df = add_hash_col(df,hashable_cols)
        return df
    

    async def compare(self,db: Database,table_name: str , cmp_file : UploadFile) :
        #get the latst existing version for this specific table
        # query = f"SELECT varsion_num FROM table_version WHERE table_name = {table_name} ORDER BY ASC version_num LIMIT 1"
        try :
            table_manager_obj = TableManager()
            old_df = table_manager_obj.fetch_table_from_db(table_name ,db) 

            new_df =await self.create_df_from_excel(table_name,cmp_file,db)
            #store availabl cols as a array of string
            old_columns = set(old_df.columns)
            new_columns = set(new_df.columns)

            # Find columns that are in both old_df and new_df
            overlapping_columns = old_columns & new_columns

            # Convert the set to a list of strings
            newly_added_columns = new_columns - old_columns
            overlapping_columns_list = list(overlapping_columns)
            newly_added_columns_list = list(newly_added_columns)
            combined_columns_list = overlapping_columns_list + newly_added_columns_list


            
            query = text("""
                            INSERT INTO table_versions (table_name, isapproved, active_columns)
                            VALUES (:table_name, false, :active_columns)
                            RETURNING id
                        """)
            result = db.execute(query, {
            'table_name': table_name,
            'active_columns': combined_columns_list
            })
            letest_version = result.fetchone()
            if letest_version:
                inserted_id = letest_version[0]  # Assuming the ID is the first column
                print("Inserted ID:", inserted_id)
                changes = []
                table_changes = []

                # Set the unique code column as the index for comparison
                # new_df.columns = [col.lower() for col in new_df.columns]
                new_df.set_index("hash", inplace=True)
                old_df.set_index("hash", inplace=True)
                # Extract unique codes
                old_codes = old_df.index
                new_codes = new_df.index



                # Detect added columns
                added_columns = {
                    col: dtype_to_postgres(new_df[col].dtype) for col in new_df.columns if col not in old_df.columns
                }
                if added_columns:
                    table_changes.append({
                        "type" : Axis.COLUMN.name,
                        "operations": Operations.ADD.name,
                        "values": json.dumps(added_columns)
                    })
                
                # Detect deleted columns
                deleted_columns = {
                    col: dtype_to_postgres(old_df[col].dtype) for col in old_df.columns if col not in new_df.columns
                }
                if deleted_columns:
                    table_changes.append({
                        "type" : Axis.COLUMN.name,
                        "operations": Operations.DELETE.name,
                        "values": json.dumps(deleted_columns)
                    })
                
                # Detect modifications at the cell level
                # old_code = list of hash val of existing table
                for code in old_codes:
                    if code in new_codes:
                        old_row = old_df.loc[code]
                        new_row = new_df.loc[code]
                        
                        # before comparing make a check on the new_df ,if the col is present in  new_df, if not(col is deleted) new_val would be None
                        for col in old_row.index:

                            # print( code)

                            # print("col,,,,",old_row.index)
                            
                            old_value = old_row[col]
                            if col not in new_row.index:
                                #this means col is deleted, 
                                continue 
                            else : 
                                new_value = new_row[col]
                                if str(old_value) != str(new_value):
                                    old_value = convert_to_python_type(old_row[col])
                                    new_value = convert_to_python_type(new_row[col])
                                    changes.append({
                                        "operations": Operations.UPDATE.name,
                                        "row_name": code,
                                        "column_name": col,
                                        "old_value": old_value,
                                        "new_value": new_value
                                    })

                        #this iteration is for the cols that are present in the new df but are not in the old df(cols are added)
                        for col in new_row.index:
                            if col not in old_row.index:
                                new_value = convert_to_python_type(new_row[col])
                                changes.append({
                                    "operations": Operations.UPDATE.name,  #here we can say that the data is updated
                                    "row_name": code,
                                    "column_name": col,
                                    "old_value": None,  # No old value since the column is new
                                    "new_value": new_value
                                })


                # Detect added rows (added codes)
                for code in new_codes:
                    if code not in old_codes:
                        for col in new_df.columns:
                            new_value = convert_to_python_type(new_df.at[code, col])
                            changes.append({
                                "operations": Operations.ADD.name,
                                "row_name": code,
                                "column_name": col,
                                "old_value" : None,
                                "new_value": new_value
                            })

                # Detect deleted rows (deleted codes)
                deleted_rows = {}
                for code in old_codes:
                    if code not in new_codes:
                        #add all the hashes with type as empty and then store in table_changes
                        deleted_rows[code] = ""
                        table_changes.append({
                        "type" : Axis.ROW.name,
                        "operations": Operations.DELETE.name,
                        "values": json.dumps(deleted_rows)
                        }) 
    
                for val in table_changes:
                    query = text("""
                        INSERT INTO table_changes (version_id, type, operations, values)
                        VALUES (:version_id, :type, :operations, :values)
                    """)
                    data = {
                        'version_id': letest_version[0],
                        'type': val["type"],
                        'operations': val["operations"], 
                        'values': val["values"] 
                    }
                    db.execute(query, data)
                    #insert all the cell_changes as well
                    query = text("""
                            INSERT INTO cell_changes (version_id, operations, row_name, column_name, old_val, new_value)
                            VALUES (:version_id, :operations, :row_name, :column_name, :old_value, :new_value)
                        """)
                    data = [
                                {
                                    'version_id': letest_version[0],
                                    'operations': change["operations"],
                                    'row_name': change["row_name"],
                                    'column_name': change["column_name"],
                                    'old_value': change["old_value"],
                                    'new_value': change["new_value"]
                                }
                                for change in changes
                            ]
                    try:
                        with db.db.begin():  # Start a transaction
                            db.db.execute(query, data)
                            db.db.commit()
                        print("Batch insert into cell_changes successful")
                    except Exception as e:
                        db.db.rollback()  # Rollback in case of error
                        print(f"Error occurred while inserting data into cell_changes: {e}")

            else:
                print("No ID re`turned.")



            data = {"cell_changes": changes, "table_changes": table_changes}

            return data
        except Exception as error : 
            traceback.print_exc()
            print(error)
        

        

        
 

    def save_changes() :
        return 
    
    def apply_changes() : 
        return
    
    

    def get_cell_changes(self, version_id: str, db: Database):
    # Define the SQL query to retrieve data from the cell_changes table based on version_id
        query = text("""
            SELECT * 
            FROM cell_changes 
            WHERE version_id = :version_id
        """)
        
        try:
            # Execute the query with the provided version_id
            result = db.execute(query, {"version_id": version_id})
        except SQLAlchemyError as e:
            # Handle any database errors
            raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
        
        # Fetch all rows from the result
        rows = result.fetchall()

        # Retrieve column names from the result
        columns = result.keys()

        # Convert the result to a DataFrame
        df = pd.DataFrame(rows, columns=columns)

        print(df.to_dict(orient="records"))
        
        return df.to_dict(orient="records")

    
    def get_table_data(self,table_name,db : Database) : 
        table_manager_obj = TableManager()
        df = table_manager_obj.fetch_table_from_db(table_name,db)
        json_data=df.to_dict(orient='records')
        return json_data