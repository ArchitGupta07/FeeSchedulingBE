# table_changes = {0:{"type": 'COLUMN', "operation": 'ADD', "values": {"age": "numeric",
#                                                                      "phn":9897888}},
# 1
# : 
# {"type": 'COLUMN', "operation": 'DELETE', "values": {"pa": "numeric"}},
# 2
# : 
# {"type": 'ROW', "operation": 'DELETE', "values": {"bfda6d6f68": ""}}}


# new_columns = {}
# deleted_columns = []
# for k,v  in table_changes.items():

#     if v["operation"]=="ADD":

#         new_columns.update(v["values"])
#     elif v["operation"]=="DELETE":
#         deleted_columns.extend(list(v["values"].keys()))


# print(new_columns)

import pandas as pd
# print(deleted_columns)

data = {
            'hash': ['abc123', 'def456', 'ghi789'],
            'name': ['Alice', 'Bob', 'Charlie'],
            'age': [25, 30, 35]
        }

df = pd.DataFrame(data).set_index('hash')

print(df)

new_data = {
            'abc123': {'name': 'Alicia', 'age': 26},   # Update existing row
            'xyz123': {'name': 'David', 'age': 40},    # Add new row
            'def456': {'name': 'Robert'}               # Partial update
        }

        # Convert dictionary to DataFrame
new_df = pd.DataFrame.from_dict(new_data, orient='index')

print(new_df)

df.update(new_df)
df = pd.concat([df, new_df[~new_df.index.isin(df.index)]])


print(df)