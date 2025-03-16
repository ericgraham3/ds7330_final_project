import pandas as pd
import os

def convert_json_to_csv(jsonl_path):
    # read JSONL file into a df
    df = pd.read_json(jsonl_path, lines=True, encoding='utf-8')

    # extract filename, convert to lowercase, and replace extension with .csv
    base_name = os.path.basename(jsonl_path)
    csv_name = os.path.splitext(base_name)[0].lower() + '.csv'

    # save df to CSV
    df.to_csv(csv_name, encoding='utf-8', index=False)

    # confirmation message
    print(f"Converted {jsonl_path} to {csv_name}.")

    # return the path to the CSV file
    return csv_name

# test:
# convert_to_jsonl('data/meta_Subscription_Boxes.jsonl')
# convert_to_jsonl('data/Subscription_Boxes.jsonl')
