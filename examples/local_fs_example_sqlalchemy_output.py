import json
import os
from typing import Dict
import pandas as pd
import tqdm
from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import declarative_base

from relationalize import Relationalize, Schema, MssqlDialect
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

# 3. Connect to Database, create & populate tables
connection_string = (
    'mssql+pyodbc://localhost:1433/RELATIONALIZE'
    '?driver=ODBC+Driver+18+for+SQL+Server'
    '&TrustServerCertificate=yes'
    '&authentication=ActiveDirectoryIntegrated'
)

engine = create_engine(connection_string, fast_executemany=True)

Base = declarative_base()
metadata = MetaData()
metadata.reflect(bind=engine)

pbar = tqdm.tqdm(get_objects_from_dir(TEMP_OUTPUT_DIR))
for filename in pbar:
    object_name, _ = os.path.splitext(filename)
    pbar.set_description(f'Load {object_name}')

    # Create Table
    table = schemas[object_name].create_table(object_name, metadata)
    table.drop(engine, checkfirst=True)
    table.create(engine)

    # Populate Table
    rows = []
    for obj in create_iterator(os.path.join(TEMP_OUTPUT_DIR, filename)):
        converted_obj = schemas[object_name].convert_object(obj)
        rows.append(converted_obj)

    df = pd.DataFrame(rows)
    df.to_sql(object_name, con=engine, if_exists='append', index=False, method='multi')
