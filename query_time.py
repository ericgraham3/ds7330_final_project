from __future__ import print_function  # make print a function
import mysql.connector  # mysql functionality
import sys  # for misc errors
import time  # for timing the query execution
import pandas as pd
import subprocess

import db_config  # storing my db password locally

# connection, you can either set up a db_config file or replace here with your own values
SERVER = db_config.host
USERNAME = db_config.user
PASSWORD = db_config.password
DATABASE = db_config.database

# test query to list average ratings of all subscription boxes, replace this with your own query against whatever tables you have
QUERY = """
SELECT 
    sb.parent_asin,
    msb.title,
    msb.main_category,
    ROUND(AVG(sb.rating), 2) AS average_rating
FROM subscription_boxes AS sb
JOIN meta_subscription_boxes AS msb
    ON sb.parent_asin = msb.parent_asin
GROUP BY 
    sb.parent_asin,
    msb.title,
    msb.main_category;
"""

if __name__ == "__main__":
    con = None
    cursor = None
    num_runs = num_runs = int(sys.argv[1]) if len(sys.argv) > 1 else 80 # how many times test will run default is 80
    execution_times = []

    try:
        # Initialize db connection
        con = mysql.connector.connect(
            host=SERVER,
            user=USERNAME,
            password=PASSWORD,
            database=DATABASE
        )
        print("Connection established.")

        # initialize cursor
        cursor = con.cursor()
        print("Cursor created.")

        print(QUERY)

        # run query multiple times (see num_runs above) and store execution time
        for run in range(num_runs):
            start_time = time.perf_counter()
            cursor.execute(QUERY)
            # Fetch the results to ensure the query is fully executed
            results = cursor.fetchall()
            end_time = time.perf_counter()

            elapsed_time = end_time - start_time
            execution_times.append(elapsed_time)
            print("Run {0}: Query executed in {1:.4f} seconds".format(run + 1, elapsed_time))

        # average execution time
        avg_time = sum(execution_times) / len(execution_times)
        print("\nAverage execution time over {0} runs: {1:.4f} seconds".format(num_runs, avg_time))

        # make dataframe of all times
        df = pd.DataFrame({
            'run': range(1, len(execution_times) + 1),
            'execution times': execution_times
        })

        # export dataframe as csv
        df.to_csv("mysql_execution_times.csv", index=False)

        # print one set of results, uncomment below if you want to see actual query results

        # if results:
        #     print("\nResults:")
        #     # Print table header
        #     print("".join(["{:<20}".format(col) for col in cursor.column_names]))
        #     print("-" * 100)
        #     # Iterate through results and print each row
        #     for row in results:
        #         print("".join(["{:<20}".format(str(col)) for col in row]))


    except mysql.connector.Error as e:  # catch SQL errors
        print("SQL Error: {0}".format(e.msg))
    except Exception as ex:  # anything else
        print("Unexpected error: {0}".format(sys.exc_info()[0]))
    finally:
        # Cleanup: Close cursor and connection if they were opened
        if cursor:
            cursor.close()
        if con:
            con.close()

        print("\nConnection terminated.", end='')
        subprocess.run(["python", "comparison_plots.py"])
