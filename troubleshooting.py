import pandas as pd
import json

# this script is used for troubleshooting issues with data import

# Issue 1: we are getting the below warning on the appliances metadata:

    # Deduplicating parent CSV data...
    # D:\[09] School\DS 7330 - File Organization & Database Management\ds7330_final_project\csv_importer.py:71: DtypeWarning: Columns (14,15) have mixed types. Specify dtype option on import or set low_memory=False.
    #   metadata_df = pd.read_csv(metadata_csv_path)
    # Creating tables...
    # D:\[09] School\DS 7330 - File Organization & Database Management\ds7330_final_project\csv_table.py:19: DtypeWarning: Columns (14,15) have mixed types. Specify dtype option on import or set low_memory=False.
    #   df = pd.read_csv(self.csv_file)
    # Table 'meta_appliances' created successfully.

# Note that it imports successfully, I just want to investigate the warning and see what the data types are for the columns in question before trying larger datasets.

csv_file = 'dedup_meta_appliances.csv'

df = pd.read_csv(csv_file, nrows=100)

print("Column Details:")

for index, col in enumerate(df.columns):
    print(f"Column {index}: {col} (dtype: {df[col].dtype})")

# columns 14 and 15 are "subtitle" and "author" respectively. There are only a handful of rows with this data,
# so I want to inspect them.

json_file = "data/meta_Appliances.jsonl"

with open(json_file, "r", encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON: {e}")
            continue
# print the JSON string
        if record.get("subtitle") is not None:
            print(line)

# those are books in the "appliances" data. not a big deal for our purposes, but messy data