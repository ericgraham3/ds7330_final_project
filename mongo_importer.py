import os
import pandas as pd
from pymongo import MongoClient, errors
import json

class JSONImporter:
    def __init__(self, client, db):
        """
        Initialize JSONImporter object with a MongoDB connection.
        """
        self.client = client
        self.db = db

    def process_json_pair(self, metadata_json_path, reviews_json_path):
        """
        Processes a pair of JSON files:
          1. Creates documents based on the JSON files
          2. Bulk loads JSON data into the corresponding collection
        """
        print("Creating metadata collection")
        collection = self.db[os.path.splitext(os.path.basename(metadata_json_path))[0].lower()]

        with open(metadata_json_path, 'r', encoding='utf-8') as f:
            data = []
            for line in f:
                line = line.strip()
                if not line:
                    continue  # Skip blank lines
                try:
                    # Attempt to parse the line as JSON
                    record = json.loads(line)
                    data.append(record)
                except json.JSONDecodeError as e:
                    # handle or log the malformed line
                    print(f"Skipping malformed JSON line: {e}")
        try:
            print("Inserting documents from JSON into collection")
            collection.insert_many(data)
        except errors.ConnectionFailure as e:
            print(f"Connection failed: {e}")
        except errors.DuplicateKeyError as e:
            print(f"Duplicate key error: {e}")
        except errors.PyMongoError as e:
            print(f"Some other PyMongo error occurred: {e}")

        print("Creating review collection")
        collection = self.db[os.path.splitext(os.path.basename(reviews_json_path))[0].lower()]

        with open(reviews_json_path, 'r', encoding='utf-8') as f:
            data = []
            for line in f:
                line = line.strip()
                if not line:
                    continue  # Skip blank lines
                try:
                    # Attempt to parse the line as JSON
                    record = json.loads(line)
                    data.append(record)
                except json.JSONDecodeError as e:
                    # handle or log the malformed line
                    print(f"Skipping malformed JSON line: {e}")
        try:
            print("Inserting documents from JSON into collection")
            collection.insert_many(data)
        except errors.ConnectionFailure as e:
            print(f"Connection failed: {e}")
        except errors.DuplicateKeyError as e:
            print(f"Duplicate key error: {e}")
        except errors.PyMongoError as e:
            print(f"Some other PyMongo error occurred: {e}")
