import hashlib
import pandas as pd
import numpy as np

def infer_type(value):
    try:
        return int(value)
    except ValueError:
        try:
            return float(value)
        except ValueError:
            return value

def dtype_to_postgres(dtype):
        if pd.api.types.is_string_dtype(dtype) or dtype == 'object':
            return 'text'
        elif pd.api.types.is_numeric_dtype(dtype):
            return 'numeric'
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            return 'timestamp'
        else:
            return 'text'
        
def convert_column_to_numeric(df):
    for column_name in df.columns:
        # Convert the column to numeric (float), with coercion to handle non-numeric values
        if pd.to_numeric(df[column_name], errors='coerce').notnull().all():
            df[column_name] = pd.to_numeric(df[column_name], errors='coerce')
            
            # Check if all non-null values in the column are equivalent to their integer form
            if df[column_name].notnull().all() and all(df[column_name].dropna() == df[column_name].dropna().astype(int)):
                df[column_name] = df[column_name].astype(int)
            else:
                df[column_name] = df[column_name].astype(float)
            
    return df

def hash_row(row, columns):
    #print(columns)
    row_str = ''.join([str(row[col]) for col in columns])  
    hash_result = hashlib.md5(row_str.encode()).hexdigest()
    return hash_result[:10]



#funciton to update the df with with the hash values
def add_hash_col(df,hashable_cols) : 
    df['hash'] = df.apply(lambda row: hash_row(row, hashable_cols), axis=1)
    return df



def convert_to_python_type(value):
    if isinstance(value, (np.int64, np.float64)):
        return int(value)  # Convert numpy int/float to Python int
    elif isinstance(value, (np.bool_, np.object_)):
        return value.item()  # Convert numpy objects to Python native types
    return value


def remove_null_values(df):
    for column in df.columns:
        # First, replace empty strings with NaN so they can be filled properly
        df[column].replace(['', ' ', None], np.nan, inplace=True)
        df[column].replace(r'^\s*$', np.nan, regex=True, inplace=True)


        
        if pd.api.types.is_numeric_dtype(df[column]):
            # Fill numeric columns with 0
            df[column].fillna(-1, inplace=True)
        elif pd.api.types.is_string_dtype(df[column]):
            # Fill string columns with '-1.1'
            df[column].fillna('-1', inplace=True)
        elif pd.api.types.is_datetime64_any_dtype(df[column]):
            # Fill datetime columns with a default date
            df[column].fillna(pd.Timestamp('2001-01-01'), inplace=True)
        else:
            # For other data types, fill with '-1.1'
            df[column].fillna('-1', inplace=True)
    
    return df


# import hashlib
from datetime import datetime

def generate_unique_constraint_name(table_name):
    # Create a unique string based on the table name and current timestamp
    unique_string = f"{table_name}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    
    # Hash the unique string to create a unique identifier
    hash_object = hashlib.sha256(unique_string.encode())
    unique_id = hash_object.hexdigest()[:8]  # Use first 8 characters for simplicity
    
    # Format the constraint name
    constraint_name = f"unique_hash_{unique_id}"
    return constraint_name