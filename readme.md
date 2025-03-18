## Database Setup

You need to create a database on your mysql server and enable LOCAL INFILE. I have added a SQL script called "create_database" to do this. 

If you want to make the database manually then you can enable LOCAL INFILE with the following SQL statement:

SET GLOBAL local_infile = 'ON';

I don't think this can be done via python, I think it has to be done on teh server side.

I think you have to re-enable this if you stop or restart your MySQL server, there is a way to set it to always start with this option enabled but I haven't tried it yet.

## Database Connection

I used the db_config.py file to store my database connection information. You can either manually enter your database credentials into the relevant scripts, or you can create your own db_config.py file (see the db_config_example.py file that is in the repo).

## Data Import

Update file paths in **import_script.py** (default set for subscription box data) and run the script. It will convert the JSONL files to CSV format, then import them to the database. It will create the tables and set up the forein key relationship between the pair of tables.

## Query Timing

Modify the query in **query_time.py** and run that script (default: average ratings for subscription_boxes). The script runs the query five times and reports the average execution time. Uncomment the "if results: print..." line to see the output.

## Updates

3/18/25: removed json_to_csv.py and moved functionality to json_converter.py, which includes additional dating cleaning steps. Also added some extra cleaning functionality (dropping null values in parent_asin) to csv_importer.py. This is based on experiences with the "appliances" and "amazon fashion" categories.

## Roadmap

- Develop CSV-to-PostgreSQL import and query testing scripts
- Refactor code for better modularity (try to consolidate data validation/cleaning steps, create classes for json_conversion, database connector, and query testing)

### Completed

- Automate table creation from CSV filenames and column names