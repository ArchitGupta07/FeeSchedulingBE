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



def hash_row(row, columns):
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
        if pd.api.types.is_numeric_dtype(df[column]):
            df[column].fillna(0, inplace=True)  # Fill numeric columns with 0
        elif pd.api.types.is_string_dtype(df[column]):
            df[column].fillna('', inplace=True)  # Fill string columns with empty string
        elif pd.api.types.is_datetime64_any_dtype(df[column]):
            df[column].fillna(pd.Timestamp('2001-01-01'), inplace=True)  # Fill datetime columns with a default date
        else:
            # df[column].fillna('unknown', inplace=True)
            df[column].fillna('', inplace=True)
    return df