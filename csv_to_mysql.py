import pandas as pd
import json
import ast
import os
import mysql.connector
import db_config

# connection
conn = mysql.connector.connect(
    host=db_config.host,
    user=db_config.user,
    password=db_config.password,
    database=db_config.database,
)

# parses JSON lines and handle empty values
def safe_parse_json(value):
    if pd.isna(value) or str(value).strip() == '':
        return None
    try:
        parsed = ast.literal_eval(value)
        return json.dumps(parsed)
    except (ValueError, SyntaxError):
        try:
            parsed = json.loads(value)
            return json.dumps(parsed)
        except json.JSONDecodeError:
            return None

# insert into mysql
def insert_data(df, table, json_columns):
    columns_sql = ', '.join(df.columns)
    placeholders = ', '.join(['%s'] * len(df.columns))
    cursor = conn.cursor()

    for _, row in df.iterrows():
        values = []
        for col in df.columns:
            val = row[col]
            if col in json_columns:
                val = safe_parse_json(val)
            elif pd.isna(val):
                val = None
            elif col == 'timestamp':
                try:
                    val = int(float(val))
                except (ValueError, TypeError):
                    val = None
            elif isinstance(val, bool):
                val = int(val)
            values.append(val)

        try:
            cursor.execute(
                f"INSERT INTO {table} ({columns_sql}) VALUES ({placeholders})",
                tuple(values)
            )
        except mysql.connector.errors.DataError as e:
            print(f"Data error: {e}, skipping row: {values}")
            continue

    conn.commit()
    cursor.close()

# take a pair of csv and import
def import_csv_pair(meta_csv_path, reviews_csv_path):
    # derive table names from file names
    meta_table_name = os.path.splitext(os.path.basename(meta_csv_path))[0].lower()
    reviews_table_name = os.path.splitext(os.path.basename(reviews_csv_path))[0].lower()

    # process meta CSV
    meta_df = pd.read_csv(meta_csv_path, encoding='utf-8', on_bad_lines='skip')

    # when the "bought together" column includes a book, that item has extra key:value pairs for subtitle and author
    # so we need to drop those columns, or any others that are not in the expected columns
    expected_columns = [
        'main_category', 'title', 'average_rating', 'rating_number', 'features',
        'description', 'price', 'images', 'videos', 'store', 'categories',
        'details', 'parent_asin', 'bought_together'
    ]

    # impute nulls for any missing expected columns
    for col in expected_columns:
        if col not in meta_df.columns:
            meta_df[col] = None

    # identify unexpected columns not in expected_columns
    unexpected_columns = [col for col in meta_df.columns if col not in expected_columns]
    if unexpected_columns:
        print(f"Warning: Unexpected columns found in meta dataset and will be dropped (see readme): {unexpected_columns}")

    # drop extra columns not in expected_columns
    meta_df = meta_df[[col for col in meta_df.columns if col in expected_columns]]
    meta_df.drop_duplicates(subset=['parent_asin'], inplace=True)
    json_cols_meta = ['features', 'description', 'images', 'videos', 'categories', 'details', 'bought_together']
    meta_df = meta_df.where(pd.notnull(meta_df), None)
    insert_data(meta_df, meta_table_name, json_cols_meta)

    # process reviews CSV
    reviews_df = pd.read_csv(reviews_csv_path, encoding='utf-8', on_bad_lines='skip')
    json_cols_reviews = ['images']
    reviews_df = reviews_df.where(pd.notnull(reviews_df), None)
    insert_data(reviews_df, reviews_table_name, json_cols_meta)

# test:
# import_csv_pair('meta_subscription_boxes.csv', 'subscription_boxes.csv')
