from __future__ import print_function  # make print a function
from pymongo import MongoClient, errors  # mysql functionality
import sys  # for misc errors
import time  # for timing the query execution
import pandas as pd # for storing all results in a df
import subprocess

import mongo_db_config  # storing my db password locally

# connection, you can either set up a db_config file or replace here with your own values
CLIENT = mongo_db_config.client
DATABASE = mongo_db_config.database

# test query to list average ratings of all subscription boxes, replace this with your own query against whatever tables you have
pipeline = [
    # Perform a left outer join between subscription_boxes and meta_subscription_boxes
    {
        "$lookup": {
            "from": "meta_subscription_boxes",  # The collection to join with
            "localField": "parent_asin",         # Field in subscription_boxes
            "foreignField": "parent_asin",       # Field in meta_subscription_boxes
            "as": "meta_data"                    # The name of the field to store joined data
        }
    },
    {
        "$unwind": "$meta_data"  # Unwind the meta_data array from the $lookup stage
    },
    # Group the results by parent_asin, title, and main_category and calculate the average rating
    {
        "$group": {
            "_id": {
                "parent_asin": "$parent_asin",
                "title": "$meta_data.title",
                "main_category": "$meta_data.main_category"
            },
            "average_rating": {
                "$avg": "$rating"  # Calculate the average rating
            }
        }
    },
    # Round the average_rating to 2 decimal places
    {
        "$project": {
            "_id": 0,
            "parent_asin": "$_id.parent_asin",
            "title": "$_id.title",
            "main_category": "$_id.main_category",
            "average_rating": {
                "$round": ["$average_rating", 2]  # Round the average rating to 2 decimals
            }
        }
    }
]

if __name__ == "__main__":
    client = None
    db = None
    collection = None
    num_runs = num_runs = int(sys.argv[1]) if len(sys.argv) > 1 else 5 # how many times test will run default is 80
    execution_times = []

    try:
        # Initialize db connection
        client = MongoClient(CLIENT)
        db = client[DATABASE]
        collection = db["subscription_boxes"]
        print("Connection established.")

        print(pipeline)

        # run query multiple times (see num_runs above) and store execution time
        for run in range(num_runs):
            start_time = time.perf_counter()
            results = collection.aggregate(pipeline)
            # Fetch the results to ensure the query is fully executed
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
        df.to_csv("mongo_execution_times.csv", index=False)

        # print one set of results, uncomment below if you want to see actual query results

        # if results:
        #     print("\nResults:")
        #     # Print table header
        #     print("".join(["{:<20}".format(col) for col in cursor.column_names]))
        #     print("-" * 100)
        #     # Iterate through results and print each row
        #     for row in results:
        #         print("".join(["{:<20}".format(str(col)) for col in row]))

    except errors.ConnectionFailure as e:
        print(f"Connection failed: {e}")
    except errors.DuplicateKeyError as e:
        print(f"Duplicate key error: {e}")
    except errors.PyMongoError as e:
        print(f"Some other PyMongo error occurred: {e}")
    except Exception as ex:  # anything else
        print("Unexpected error: {0}".format(sys.exc_info()[0]))
    finally:
        # Cleanup: Close cursor and connection if they were opened
        if client:
            client.close()

        print("\nConnection terminated.", end='')
        subprocess.run(["python", "comparison_plots.py"])
