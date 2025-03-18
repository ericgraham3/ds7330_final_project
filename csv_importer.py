import os
import mysql.connector
import pandas as pd
from csv_table import CSVTable

# this class coordinates the import of a pair of CSV files, one for reviews and one for metadata, it takes a connection
# to instantiate the "CSVImporter" object, then we pass two CSV files to the process_csv_pair function and call
# csv_table to create tables and do insert the data into the db


class CSVImporter:
    def __init__(self, connection):
        """
        initialize csvimporter object with an mysql connection
        """
        self.connection = connection

    def _add_primary_key(self, csv_table, key_column="parent_asin"):
        """
        makes parent_asin a primary key, this is why we de-duplicate
        """
        cursor = self.connection.cursor()
        try:
            sql = f"ALTER TABLE `{csv_table.table_name}` ADD PRIMARY KEY (`{key_column}`);"
            cursor.execute(sql)
            self.connection.commit()
            print(f"Primary key on '{key_column}' added for table '{csv_table.table_name}'.")
        except mysql.connector.Error as err:
            print(f"Error adding primary key on '{key_column}' for table '{csv_table.table_name}':", err)
        finally:
            cursor.close()

    def _create_foreign_key_relationship(self, parent_csv, child_csv, key_column="parent_asin", on_delete="CASCADE",
                                         on_update="CASCADE"):
        """
        creates a foreign key relationship on parent_asin
        """
        constraint_name = f"fk_{child_csv.table_name}_{key_column}"
        alter_sql = f"""
        ALTER TABLE `{child_csv.table_name}`
        ADD CONSTRAINT `{constraint_name}`
        FOREIGN KEY (`{key_column}`)
        REFERENCES `{parent_csv.table_name}`(`{key_column}`)
        ON DELETE {on_delete}
        ON UPDATE {on_update};
        """
        cursor = self.connection.cursor()
        try:
            cursor.execute(alter_sql)
            self.connection.commit()
            print(
                f"Foreign key '{constraint_name}' created: {child_csv.table_name}.{key_column} references {parent_csv.table_name}.{key_column}.")
        except mysql.connector.Error as err:
            print("Error creating foreign key:", err)
        finally:
            cursor.close()

    def process_csv_pair(self, metadata_csv_path, reviews_csv_path, key_column="parent_asin", on_delete="CASCADE",
                         on_update="CASCADE"):
        """
        processes a pair of CSV files:
          1. deduplicates the parent CSV data to avoid issues with duplicate parent_asin values
          2. creates tables based on the CSV filenames and structure
          3. bulk loads CSV data into the corresponding tables using mysql's internal bulk import tool
          4. adds a primary key to the product metadata table
          5. creates a foreign key relationship between the product metadta and review tables
        """
        # deduplicate metadata's CSV data: sometimes there are duplicate parent_asin, and that prevents us from
        # creating the foreign key relationship between metadata and review data
        print("Deduplicating parent CSV data...")
        metadata_df = pd.read_csv(metadata_csv_path)
        metadata_df = metadata_df.drop_duplicates(subset=[key_column])
        dedup_metadata_csv_path = f"dedup_{os.path.basename(metadata_csv_path)}"
        metadata_df.to_csv(dedup_metadata_csv_path, index=False)

        # create CSVTable objects using deduplicated CSV file for the metadata
        metadata_csv = CSVTable(dedup_metadata_csv_path)
        reviews_csv = CSVTable(reviews_csv_path)

        # create tables for both CSVs
        print("Creating tables...")
        metadata_csv.create_table(self.connection)
        reviews_csv.create_table(self.connection)

        # load CSV data into each table using MySQL bulk import
        print("Bulk loading CSV data into tables...")
        metadata_csv.load_data_bulk(self.connection)
        reviews_csv.load_data_bulk(self.connection)

        # primary key to the parent table
        print("Adding primary key to parent table...")
        self._add_primary_key(metadata_csv, key_column)

        # create the foreign key relationship between the metadata and product data
        print("Creating foreign key relationship...")
        self._create_foreign_key_relationship(metadata_csv, reviews_csv, key_column, on_delete, on_update)


# test:
# if __name__ == "__main__":
#     import db_config
#
#     conn = mysql.connector.connect(
#         host=db_config.host,
#         user=db_config.user,
#         password=db_config.password,
#         database=db_config.database,
#         allow_local_infile=True
#     )
#
#     importer = CSVImporter(conn)
#     importer.process_csv_pair("meta_subscription_boxes.csv", "subscription_boxes.csv")
