import os
import json
import pandas as pd
import re

# this script is used to convert JSONL files to CSV format, while also cleaning up problematic characters
# there are multiple steps at which the data is validated and cleaned between this script and csv_importer.py
# it would probably be more efficient to take out some of these steps or consolidate, but this was the product of
# on the fly troubleshooting and testing with multiple categories, so it is what it is for now

def remove_problematic_chars(text):
    # this is needed because some JSON records have problematic characters that can cause issues when converting to CSV

    # convert non-string types to string
    text = str(text)

    # removes backslashes, quotes, newlines, and carriage returns
    cleaned = re.sub(r'[\\"\'\r\n]+', '', text)
    return cleaned

def convert_jsonl_to_csv(jsonl_path):
    valid_records = []
    with open(jsonl_path, 'r', encoding='utf-8') as f:
        for i, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue  # Skip blank lines
            try:
                # Attempt to parse the line as JSON
                record = json.loads(line)
                valid_records.append(record)
            except json.JSONDecodeError as e:
                # handle or log the malformed line
                print(f"Skipping malformed JSON at line {i}: {e}")

    # create a df from all valid JSON records
    df = pd.DataFrame(valid_records)

    # apply the cleaning function to all cells in the df
    df = df.applymap(remove_problematic_chars)

    # extract csv filename from jsonl filename
    base_name = os.path.basename(jsonl_path)
    csv_name = os.path.splitext(base_name)[0].lower() + '.csv'

    # save the csv
    df.to_csv(csv_name, index=False, encoding='utf-8')
    print(f"Converted {jsonl_path} to {csv_name} (with malformed lines skipped and problematic characters removed).")

    return csv_name

# test:
# convert_jsonl_to_csv('meta_Subscription_Boxes.jsonl')
# convert_jsonl_to_csv('Subscription_Boxes.jsonl')
