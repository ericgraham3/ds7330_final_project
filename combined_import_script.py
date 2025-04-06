import mysql.connector
import psycopg2
from pymongo import MongoClient

# import internal modules
import json_converter # converts the jsonl files to csv
import csv_importer # imports the csv files into mysql
import db_config # stores db connection details

import pg_csv_importer # imports the csv files into postgresql
import pg_db_config # stores db connection details

import mongo_importer # imports the json files into mongodb
import mongo_db_config # stores db connection details

# This script calls the json_converter and csv_importer scripts to convert JSONL files to CSV
# and import the data into mysql.

# MySQL

# connection needs to have local infile enabled, see readme.md about setting this on the server side
conn = mysql.connector.connect(
    host=db_config.host,
    user=db_config.user,
    password=db_config.password,
    database=db_config.database,
    allow_local_infile=True
)

# path/reference to the JSONL file for metadata
meta_jsonl_path = 'meta_Subscription_Boxes.jsonl'

# path/reference to JSONL file for review data
review_jsonl_path = 'Subscription_Boxes.jsonl'

# convert JSONL files to CSV
meta_csv_path = json_converter.convert_jsonl_to_csv(meta_jsonl_path)
review_csv_path = json_converter.convert_jsonl_to_csv(review_jsonl_path)
print("JSONL converted to CSV, processing import to MySQL...")

print("MySQL Processes:")
# import csv pair to mysql
importer = csv_importer.CSVImporter(conn)
importer.process_csv_pair(meta_csv_path, review_csv_path)

# close connection
conn.close()
print("Data import complete.")

# postgreSQL

print("postgreSQL Processes:")
# no infile for postgresql like in mysql
conn2 = psycopg2.connect(
    host=pg_db_config.host,
    port=pg_db_config.port,
    user=pg_db_config.user,
    password=pg_db_config.password,
    dbname=pg_db_config.database,
)

# import csv pair to postgresql
pg_importer = pg_csv_importer.CSVImporter(conn2)
pg_importer.process_csv_pair(meta_csv_path, review_csv_path)

# close connection
conn2.close()
print("Data import complete.")

# mongodb

print("MongoDB Processes:")
# connect to mongodb
client = MongoClient(mongo_db_config.client)
db = client[mongo_db_config.database]

# import json pair to mongodb
m_importer = mongo_importer.JSONImporter(client, db)
m_importer.process_json_pair(meta_jsonl_path, review_jsonl_path)

# close client
client.close()
print("Data import complete.")