import os
import pandas as pd
import psycopg2
from pg_csv_table import CSVTable

class CSVImporter:
    def __init__(self, connection):
        """
        Initialize CSVImporter object with a PostgreSQL connection.
        """
        self.connection = connection

    def _add_primary_key(self, csv_table, key_column="parent_asin"):
        """
        Makes parent_asin a primary key. This is one reason we do all that pre-processing on the json and csv.
        """
        cursor = self.connection.cursor()
        try:
            sql = f'ALTER TABLE "{csv_table.table_name}" ADD PRIMARY KEY ("{key_column}");'
            cursor.execute(sql)
            self.connection.commit()
            print(f"Primary key on '{key_column}' added for table '{csv_table.table_name}'.")
        except psycopg2.Error as err:
            print(f"Error adding primary key on '{key_column}' for table '{csv_table.table_name}':", err)
        finally:
            cursor.close()

    def _create_foreign_key_relationship(self, parent_csv, child_csv, key_column="parent_asin", on_delete="CASCADE", on_update="CASCADE"):
        """
        Creates a foreign key relationship on parent_asin. This is another reason we do all that pre-processing on the json and csv.
        """
        constraint_name = f"fk_{child_csv.table_name}_{key_column}"
        alter_sql = f"""
        ALTER TABLE "{child_csv.table_name}"
        ADD CONSTRAINT "{constraint_name}"
        FOREIGN KEY ("{key_column}")
        REFERENCES "{parent_csv.table_name}"("{key_column}")
        ON DELETE {on_delete}
        ON UPDATE {on_update};
        """
        cursor = self.connection.cursor()

        try:
            sql = f"""
            DELETE FROM {child_csv.table_name}
            WHERE {key_column} NOT IN (SELECT {key_column} FROM {parent_csv.table_name});
            """
            cursor.execute(sql)
            self.connection.commit()
            print(f"Rows in {child_csv.table_name} where the {key_column} doesn't exist in {parent_csv.table_name} removed.")
        except psycopg2.Error as err:
            print(f"Error removing rows from table '{parent_csv.table_name}':", err)

        try:
            cursor.execute(alter_sql)
            self.connection.commit()
            print(f"Foreign key '{constraint_name}' created: {child_csv.table_name}.{key_column} references {parent_csv.table_name}.{key_column}.")
        except psycopg2.Error as err:
            print("Error creating foreign key:", err)
        finally:
            cursor.close()

    def process_csv_pair(self, metadata_csv_path, reviews_csv_path, key_column="parent_asin", on_delete="CASCADE", on_update="CASCADE"):
        """
        Processes a pair of CSV files:
          1. Deduplicates the parent CSV data to avoid issues with duplicate parent_asin values
          2. Creates tables based on the CSV filenames and structure
          3. Bulk loads CSV data into the corresponding tables using PostgreSQL's COPY command
          4. Adds a primary key to the product metadata table (hardcoded to parent_asin)
          5. Creates a foreign key relationship between the product metadata and review tables
        """
        # Deduplicate metadata's CSV data: sometimes there are duplicate parent_asin, and that prevents us from
        # creating the foreign key relationship between metadata and review data
        print("Preprocessing parent CSV data to remove duplicate and null values from parent_asin...")

        # Read csv and force parent_asin to be a string
        metadata_df = pd.read_csv(metadata_csv_path, dtype={key_column: str})

        # Strip whitespace from parent_asin
        metadata_df[key_column] = metadata_df[key_column].str.strip()

        # Replace empty strings and literal "NULL"/"null" with NaN
        metadata_df[key_column] = metadata_df[key_column].replace({"": pd.NA, "NULL": pd.NA, "null": pd.NA})

        # Debug prints to check cleaning results
        print("Null count in parent_asin after cleaning:", metadata_df[key_column].isna().sum())
        print("Unique parent_asin values:", metadata_df[key_column].unique())

        # Drop duplicates and rows where parent_asin is null
        metadata_df = metadata_df.drop_duplicates(subset=[key_column])
        metadata_df = metadata_df.dropna(subset=[key_column])

        dedup_metadata_csv_path = f"dedup_{os.path.basename(metadata_csv_path)}"

        # Save the deduplicated DataFrame to a new CSV file
        metadata_df.to_csv(dedup_metadata_csv_path, index=False)

        # Create CSVTable objects using deduplicated CSV file for the metadata
        metadata_csv = CSVTable(dedup_metadata_csv_path)
        reviews_csv = CSVTable(reviews_csv_path)

        # Create tables for both CSVs
        print("Creating tables...")
        metadata_csv.create_table(self.connection)
        reviews_csv.create_table(self.connection)

        # Load CSV data into each table using PostgreSQL's COPY command
        print("Bulk loading CSV data into tables...")
        metadata_csv.load_data_bulk(self.connection)
        reviews_csv.load_data_bulk(self.connection)

        # Add primary key to the parent table
        print("Adding primary key to parent table...")
        self._add_primary_key(metadata_csv, key_column)

        # Create the foreign key relationship between the metadata and product data
        print("Creating foreign key relationship...")
        self._create_foreign_key_relationship(metadata_csv, reviews_csv, key_column, on_delete, on_update)
