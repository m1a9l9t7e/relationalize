import csv
import json
import os
from typing import Dict

from relationalize import Relationalize, Schema, MssqlDialect, PostgresDialect
from relationalize.utils import create_local_file


def aggregate_json_files(directory):
    """
    Aggregates all JSON arrays from files in the specified directory into a single file.

    Args:
        directory (str): The path to the directory containing the JSON files.
    """
    # Extract the name of the directory
    dir_name = os.path.basename(directory.rstrip("/"))
    output_file = f"{dir_name}.json"
    output_path = os.path.join(os.path.dirname(directory), output_file)

    with open(output_path, 'w') as outfile:
        for filename in os.listdir(directory):
            if filename.endswith(".json"):
                file_path = os.path.join(directory, filename)
                with open(file_path, 'r') as infile:
                    data = json.load(infile)
                    for item in data:
                        outfile.write(json.dumps(item) + "\n")

    return output_file


# This example utilizes the local file system as a temporary storage location.
TEMP_OUTPUT_DIR = "output/temp"
FINAL_OUTPUT_DIR = "output/final"
INPUT_DIR = "example_data"

INPUT_FILENAME = aggregate_json_files(r'example_data\journeys')
OBJECT_NAME = "journeys"

def create_iterator(filename):
    with open(filename, "r") as infile:
        for line in infile:
            yield json.loads(line)


def get_objects_from_dir(directory: str):
    for filename in os.listdir(directory):
        yield filename


# 0. Set up file system
os.makedirs(TEMP_OUTPUT_DIR, exist_ok=True)
os.makedirs(FINAL_OUTPUT_DIR, exist_ok=True)

# 1. Relationalize raw data
with Relationalize(OBJECT_NAME, create_local_file(output_dir=TEMP_OUTPUT_DIR)) as r:
    r.relationalize(create_iterator(os.path.join(INPUT_DIR, INPUT_FILENAME)))

# 2. Generate schemas for each transformed/flattened
schemas: Dict[str, Schema] = {}
for filename in get_objects_from_dir(TEMP_OUTPUT_DIR):
    object_name, _ = os.path.splitext(filename)
    schemas[object_name] = Schema(sql_dialect=MssqlDialect())
    for obj in create_iterator(os.path.join(TEMP_OUTPUT_DIR, filename)):
        schemas[object_name].read_object(obj)

    schemas[object_name].drop_duplicate_columns()
    schemas[object_name].merge_multi_choice_columns()

    if object_name == 'journeys_diff_datatype_values':
        print(schemas[object_name].schema)

# 3. Convert transform/flattened data to prep for database.
#    Generate SQL DDL.
for filename in get_objects_from_dir(TEMP_OUTPUT_DIR):
    object_name, _ = os.path.splitext(filename)

    with open(os.path.join(FINAL_OUTPUT_DIR, f"{object_name}.csv"), "w") as out_file:
        writer = csv.DictWriter(
            out_file, fieldnames=schemas[object_name].generate_output_columns(), lineterminator='\n'
        )
        writer.writeheader()
        for obj in create_iterator(os.path.join(TEMP_OUTPUT_DIR, filename)):
            converted_obj = schemas[object_name].convert_object(obj)
            writer.writerow(converted_obj)

    with open(
            os.path.join(FINAL_OUTPUT_DIR, f"DDL_{object_name}.sql"), "w"
    ) as ddl_file:
        ddl_file.write(
            schemas[object_name].generate_ddl(table=object_name, schema="dbo")
        )
