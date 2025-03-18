import mysql.connector
import json_to_csv
import csv_importer
import db_config

# This script calls the json_to_csv and csv_importer scripts to convert JSONL files to CSV
# and import the data into mysql.

# connection needs to have local infile enabled, see readme.md about setting this on the server side
conn = mysql.connector.connect(
    host=db_config.host,
    user=db_config.user,
    password=db_config.password,
    database=db_config.database,
    allow_local_infile=True
)

# path/references to the JSONL file for metadata
meta_jsonl_path = 'meta_Subscription_Boxes.jsonl'

# path/reference to JSONL file for review data
review_jsonl_path = 'Subscription_Boxes.jsonl'

# convert JSONL files to CSV
meta_csv_path = json_to_csv.convert_json_to_csv(meta_jsonl_path)
review_csv_path = json_to_csv.convert_json_to_csv(review_jsonl_path)
print("JSONL converted to CSV, processing import to MySQL...")

# import csv pair to mysql
importer = csv_importer.CSVImporter(conn)
importer.process_csv_pair("meta_subscription_boxes.csv", "subscription_boxes.csv")

# close connection
conn.close()
print("Data import complete.")
