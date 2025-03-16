## Database & Table Setup

Run **create_database_and_tables.sql** in MySQL Workbench to set up your subscription box database and tables. The JSONL files are supposed to share the same structure (see notes below), so we should be able to just rename the tables to match the category. Mind the names of the foreign keys, those need to be changed as well!

### IMPORTANT: Table Names 

The json_to_csv script takes a JSONL file and creates a CSV with the exact same name **in lowercase**. So "meta_Appliances.jsonl" becomes "meta_appliances.csv".

The csv_to_mysql.py script takes two csv files and inserts the data into MySQL tables **with the exact same name as the csv file.** 

So if I name my database tables "Appliances" and "meta-appliances", the import script won't work. I need to name my tables "appliances" and "meta_appliances". This is a shortcoming of manually directly the database tables in SQL.

#### A Python solution could streamline this in the future, I just haven't gotten to it yet.

## Database Connection

I used the db_config.py file to store my database connection information. You can either manually enter your database credentials into the relevant scripts, or create your own db_config.py file (see the db_config_example.py file that is in the repo).

## Data Import

- **csv_to_mysql.py** imports two CSV files (one for main data, one for metadata) per category.
- Update file paths in **import_script.py** (default set for subscription box data) and run the script.

### Handling Unexpected Columns

When the "bought together" column includes a book, that item has extra key:value pairs for subtitle and author. I set the script to drop these until we know more about which product categories we'll be using, and whether they have these (or any other) unexpected columns. 

At that point, we'll know whether any unexpected columns exist in the categories we're using, and we can create them manually or update the import script to create them automatically.

## Query Timing

Modify the query in **query_time.py** and run that script (default: average ratings for subscription_boxes). The script runs the query five times and reports the average execution time. Uncomment the "if results: print..." line to see the output.

## Roadmap

- Develop CSV-to-PostgreSQL import and query testing scripts
- Automate table creation from CSV filenames and column names
- Refactor code for better modularity