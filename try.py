import pandas as pd

# Create dummy DataFrames
old_df = pd.DataFrame({
    'id': [1, 2, 3],
    'name': ['Alice', 'Bob', 'Charlie'],
    'age': [25, 30, 35],
    'address': ['123 Elm St', '456 Oak St', '789 Pine St']  # Extra column
})

new_df = pd.DataFrame({
    'id': [1, 2, 3],
    'name': ['Alice', 'Bob', 'Charlie'],
    'age': [26, 31, 36],  # Column 'address' is missing
    'ph' : [4,2,1]
})

# Extract columns from both DataFrames
old_columns = set(old_df.columns)
new_columns = set(new_df.columns)

# Find columns that are in both old_df and new_df
existing_columns = old_columns & new_columns

# Convert the set to a list of strings
existing_columns_list = list(existing_columns)

print("Existing columns:", existing_columns_list)
