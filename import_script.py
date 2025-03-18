import mysql.connector

# import internal modules
import json_converter # converts the jsonl files to csv
import csv_importer # imports the csv files into mysql
import db_config # stores db connection details

# This script calls the json_converter and csv_importer scripts to convert JSONL files to CSV
# and import the data into mysql.

# connection needs to have local infile enabled, see readme.md about setting this on the server side
conn = mysql.connector.connect(
    host=db_config.host,
    user=db_config.user,
    password=db_config.password,
    database=db_config.database,
    allow_local_infile=True
)

# path/reference to the JSONL file for metadata
meta_jsonl_path = 'data/meta_Amazon_Fashion.jsonl'

# path/reference to JSONL file for review data
review_jsonl_path = 'data/Amazon_Fashion.jsonl'

# convert JSONL files to CSV
meta_csv_path = json_converter.convert_jsonl_to_csv(meta_jsonl_path)
review_csv_path = json_converter.convert_jsonl_to_csv(review_jsonl_path)
print("JSONL converted to CSV, processing import to MySQL...")

# import csv pair to mysql
importer = csv_importer.CSVImporter(conn)
importer.process_csv_pair(meta_csv_path, review_csv_path)

# close connection
conn.close()
print("Data import complete.")
