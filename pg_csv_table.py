import os
import pandas as pd
import psycopg2  # psycopg2 is the PostgreSQL connector

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
        """Load the CSV into a DataFrame and store headers and data types"""
        df = pd.read_csv(self.csv_file)
        self.headers = df.columns.tolist()
        self.dtypes = df.dtypes
        return df

    def create_table(self, connection):
        """Create a table based on CSV structure"""
        df = self.load_dataframe()  # Load data to infer structure

        # Map pandas dtypes to SQL types for PostgreSQL
        def map_dtype(dtype, col_name):
            dtype_str = str(dtype)
            if col_name == "parent_asin":
                # Enforce a VARCHAR for parent_asin since it's used in a foreign key
                return "VARCHAR(255)"
            if 'int' in dtype_str:
                return "BIGINT"
            elif 'float' in dtype_str:
                return "FLOAT"
            elif 'bool' in dtype_str:
                return "BOOLEAN"
            elif 'datetime' in dtype_str:
                return "TIMESTAMP"
            else:
                # Generic TEXT type for other object types
                return "TEXT"

        # Pass column name and data type to map_dtype
        sql_type_mapping = {col: map_dtype(dtype, col) for col, dtype in df.dtypes.items()}

        columns_definitions = ",\n  ".join([f"\"{col}\" {sql_type_mapping[col]}" for col in self.headers])
        create_table_sql = f"CREATE TABLE IF NOT EXISTS \"{self.table_name}\" (\n  {columns_definitions}\n);"

        cursor = connection.cursor()
        try:
            cursor.execute(create_table_sql)
            connection.commit()
            print(f"Table '{self.table_name}' created successfully, no data has been loaded into the table.")
        except psycopg2.Error as err:
            print(f"Error creating table '{self.table_name}': {err}")
        finally:
            cursor.close()

        return create_table_sql

    def load_data_bulk(self, connection, delimiter=',', enclosed_by='"', lines_terminated='\n'):
        """Use PostgreSQL COPY to load CSV data into its table"""
        with open(self.csv_file, 'r', encoding='utf-8') as f:
            # Skip header
            next(f)
            cursor = connection.cursor()
            try:
                # Prepare the COPY SQL query
                copy_sql = f"""
                COPY \"{self.table_name}\" ({', '.join(self.headers)})
                FROM STDIN WITH CSV HEADER DELIMITER '{delimiter}' QUOTE '{enclosed_by}' ESCAPE '\\';
                """
                # Execute the COPY command
                cursor.copy_expert(sql=copy_sql, file=f)
                connection.commit()
                print(f"CSV file '{self.csv_file}' loaded successfully into table '{self.table_name}'.")
            except psycopg2.Error as err:
                print(f"Error loading CSV: {err}")
            finally:
                cursor.close()
