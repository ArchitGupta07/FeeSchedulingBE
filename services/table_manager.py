import datetime
import re
import tempfile
from fastapi import File, UploadFile, Depends, HTTPException
import pandas as pd
from sqlalchemy.exc import SQLAlchemyError
from db import Database, get_db, engine
from sqlalchemy.sql import text
from utils.helper import add_hash_col, convert_column_to_numeric, infer_type, remove_null_values
import datetime

class TableManager:

    # def __init__(self, db: Database):
    #     self.db = db
        
    def fetch_table_from_db(self,table_name : str,db :Database) :
        query = text(f"SELECT * FROM {table_name}")
        try :
            result = db.execute(query)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
        rows = result.fetchall()

        columns = result.keys()  # Retrieve column names from the result

        df = pd.DataFrame(rows, columns=columns)
        #print(df)
        return df
    
    def extract_code_colname(self, excel_table):
        hashable_col = None
        for col in excel_table.columns:
            if "code" in col.lower():
                hashable_col = col
        return hashable_col
    

    def _is_float_like(self, value):
        try:
            float(value)  # Try converting the value to float
            return True
        except ValueError:
            return False
        
    def calculate_hashable_col(self, df, exclude_col=""):
        max_unique_count = -1
        hashable_col = None
        for col in df.columns:
            if col == exclude_col:
                continue
            if pd.api.types.is_numeric_dtype(df[col]):
                continue

            first_five = df[col].astype(str).head(5)  # Convert to string and take the first 5 values
            can_be_float = any(self._is_float_like(value) for value in first_five)

            if can_be_float:
                continue

            
            unique_count = df[col].nunique()
            if unique_count > max_unique_count:
                max_unique_count = unique_count
                hashable_col = col
        return hashable_col
    
    def generate_table_name(self,base_name):
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            return f"{base_name}_{timestamp}"
    



    def find_table_headers(self, df_temp):       
        #print("Original DataFrame:")
        #print(df_temp)

      
        max_non_null_count = 0
        longest_row_index = None

        for i, row in df_temp.iterrows():
            # Count non-null values
            non_null_count = row.count() - row.isnull().sum()  # Count of non-null values
            total_valid_count = non_null_count  # Only count non-null values

            if total_valid_count > max_non_null_count:
                max_non_null_count = total_valid_count
                longest_row_index = i

        if longest_row_index is not None:
            #print(f"The longest row with non-null values is at index {longest_row_index} with {max_non_null_count} valid entries:")
            #print(df_temp.iloc[longest_row_index])

            # Step 3: Set the header to the identified row and create a new DataFrame
            df_with_header = df_temp.iloc[longest_row_index:]  # Get the DataFrame starting from the header row
            df_with_header.columns = df_with_header.iloc[0]  # Set the header
            df_with_header = df_with_header[1:]  # Remove the header row from the DataFrame
            df_with_header.reset_index(drop=True, inplace=True)  # Reset index

            #print("Full table with the header set:")
            #print(df_with_header)
            
            return df_with_header
        else:
            #print("No valid rows found.")
            return None




    def find_table_headers(self, df_temp):       
        #print("Original DataFrame:")


        # #print(df_temp)

        

        # Step 2: Check if the first row is a valid header
        non_empty_columns = df_temp.columns[df_temp.notna().any()]
        num_non_empty_columns = len(non_empty_columns)

        unnamed_columns = [col for col in df_temp.columns if 'Unnamed:' in col]
        num_unnamed_columns = len(unnamed_columns)

        #print(num_non_empty_columns)

      
        max_non_null_count = num_non_empty_columns-num_unnamed_columns
        longest_row_index = None
        count=0

        for i, row in df_temp.iterrows():
            # Count non-null values
            non_null_count = row.count() - row.isnull().sum()  # Count of non-null values
            total_valid_count = non_null_count  # Only count non-null values
            if count<=2:
                print("woe................",total_valid_count, row)

            count+=1
                
            if total_valid_count > max_non_null_count:
                max_non_null_count = total_valid_count
                longest_row_index = i

        #print("max............",total_valid_count)


        if longest_row_index is not None:
            #print(f"The longest row with non-null values is at index {longest_row_index} with {max_non_null_count} valid entries:")
            #print(df_temp.iloc[longest_row_index])

            # Step 3: Set the header to the identified row and create a new DataFrame
            df_with_header = df_temp.iloc[longest_row_index:]  # Get the DataFrame starting from the header row
            df_with_header.columns = df_with_header.iloc[0]  # Set the header
            df_with_header = df_with_header[1:]  # Remove the header row from the DataFrame
            df_with_header.reset_index(drop=True, inplace=True)  # Reset index

            #print("Full table with the header set:")
            #print(df_with_header)
            
            return df_with_header
        else:
            #print("No valid rows found.")
            return df_temp


 
    
    async def insert_table(self,db : Database, file: UploadFile = File(...)) :
        content = await file.read()

        file_name = file.filename.split(".")[0].lower()
        table_prefix = re.sub(r'[^a-z0-9]', '_', file_name)
        # Example usage:
        table_name = self.generate_table_name(table_prefix)
        
        df = pd.read_excel(content)
       
        df = self.find_table_headers(df)

        

        df = remove_null_values(df)
        df = convert_column_to_numeric(df)
        print("second")
        print(df.dtypes)
        if "NON-FACILITY GLOBAL FEE" in df.columns:
            print(df["NON-FACILITY GLOBAL FEE"])
        # if "FEE" in df.columns:
        #     print(df["FEE"])
        


        hashable_cols = []
        df.columns = [col.lower() for col in df.columns]
        col1 = self.extract_code_colname(df)
        if not col1:
            col1 = self.calculate_hashable_col(df)
        hashable_cols.append(col1)
        hashable_cols.append(self.calculate_hashable_col(df, col1))
        # #print("cols.................................",df.columns)
        # #print("cols.................................",hashable_cols )
        df = add_hash_col(df, hashable_cols)
        send_df = df
        try:
            # with db.begin() as transaction:
                # Create the table
                df.to_sql(table_name, engine, index=False, if_exists='replace')

                #print(df)

                # Insert the metadata
                hashable_cols_str = ",".join(hashable_cols)
                query = text("INSERT INTO table_details (table_name, hashable_cols,file_name) VALUES (:table_name, :hashable_cols,:file_name)")
                db.execute(query, {"table_name": table_name, "hashable_cols": hashable_cols_str,"file_name" : file.filename})
                
                # Commit the transaction
                # transaction.commit()

        except SQLAlchemyError as e:
            # Rollback the transaction in case of an error
            # transaction.rollback()
            raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
        # send_df.set_index("hash", inplace=True)

        #print(send_df)
        return send_df.to_dict(orient='records'),table_name,file.filename
    
    
    
    def get_all_files(self,db : Database) :
        #return all the table data
        query = text(f"SELECT table_name, file_name FROM table_details")
        result = db.execute(query)
        #print("------------------------------------------")
        rows = result.fetchall()
        #print(rows)
        files = [{"table_name": row.table_name, "file_name": row.file_name} for row in rows]

        return files
    
    def download_xls(self,db: Database,table_name : str) :
        df = self.fetch_table_from_db(table_name,db)
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
            file_path = tmp.name
            df.to_excel(file_path, index=False)
        return file_path

        
        
