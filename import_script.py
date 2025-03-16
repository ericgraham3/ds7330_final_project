import json_to_csv
import csv_to_mysql

# This script calls the json_to_csv and csv_to_mysql scripts to convert JSONL files to CSV and then import the data into mysql.

# path/references to the JSONL file for metadata
meta_jsonl_path = 'meta_Subscription_Boxes.jsonl'

# path/reference to JSONL file for review data
review_jsonl_path = 'Subscription_Boxes.jsonl'

# convert JSONL files to CSV
meta_csv_path = json_to_csv.convert_json_to_csv(meta_jsonl_path)
review_csv_path = json_to_csv.convert_json_to_csv(review_jsonl_path)
print("JSONL converted to CSV, processing import to MySQL...")

# import csv pair to mysql
csv_to_mysql.import_csv_pair(meta_csv_path, review_csv_path)

# close connection
csv_to_mysql.conn.close()
print("Data import complete.")
