import os
import pandas as pd
import mysql.connector


class CSVTable:
    def __init__(self, csv_file):
        self.csv_file = csv_file
        base_name = os.path.splitext(os.path.basename(csv_file))[0].lower()
        # Remove the "dedup_" prefix if present
        if base_name.startswith("dedup_"):
            base_name = base_name[len("dedup_"):]
        self.table_name = base_name
        self.headers = None
        self.dtypes = None

    def load_dataframe(self):
        """load the CSV into a df and store headers and data types"""
        df = pd.read_csv(self.csv_file)
        self.headers = df.columns.tolist()
        self.dtypes = df.dtypes
        return df

    def create_table(self, connection):
        """create a table based on CSV structure"""
        df = self.load_dataframe()  # load data to infer structure

        # Map pandas dtypes to SQL types
        def map_dtype(dtype, col_name):
            dtype_str = str(dtype)
            if col_name == "parent_asin":
                # enforce a VARCHAR for parent_asin since it's used in a foreign key, MySQL will not accept "TEXT"
                # field as a foreign key
                return "VARCHAR(255)"
            if 'int' in dtype_str:
                return "INT"
            elif 'float' in dtype_str:
                return "FLOAT"
            elif 'bool' in dtype_str:
                return "BOOLEAN"
            elif 'datetime' in dtype_str:
                return "DATETIME"
            else:
                # generic TEXT type for other object types
                return "TEXT"

        # pass column name and data type to map_dtype
        sql_type_mapping = {col: map_dtype(dtype, col) for col, dtype in df.dtypes.items()}

        columns_definitions = ",\n  ".join([f"`{col}` {sql_type_mapping[col]}" for col in self.headers])
        create_table_sql = f"CREATE TABLE IF NOT EXISTS `{self.table_name}` (\n  {columns_definitions}\n);"

        cursor = connection.cursor()
        try:
            cursor.execute(create_table_sql)
            connection.commit()
            print(f"Table '{self.table_name}' created successfully, no data has been loaded into the table.")
        except mysql.connector.Error as err:
            print(f"Error creating table '{self.table_name}': {err}")
        finally:
            cursor.close()

        return create_table_sql

    def load_data_bulk(self, connection, delimiter=',', enclosed_by='"', lines_terminated='\n'):
        """use MySQL bulk loader to load csv data into its table"""
        csv_file_escaped = self.csv_file.replace("\\", "\\\\")
        load_sql = f"""
        LOAD DATA LOCAL INFILE '{csv_file_escaped}'
        INTO TABLE `{self.table_name}`
        FIELDS TERMINATED BY '{delimiter}' ENCLOSED BY '{enclosed_by}'
        LINES TERMINATED BY '{lines_terminated}'
        IGNORE 1 LINES;
        """

        cursor = connection.cursor()
        try:
            cursor.execute(load_sql)
            connection.commit()
            print(f"CSV file '{self.csv_file}' loaded successfully into table '{self.table_name}'.")
        except mysql.connector.Error as err:
            print("Error loading CSV:", err)
        finally:
            cursor.close()