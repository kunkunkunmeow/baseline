# python script to
# 1. download all data .gz files from GCS
# 2. decompress files and save locally as csv
# 3. read csv files into dataframe and check for errors
# 4. upload csv to GCS
# 5. upload dataframe to Bigquery

from google.cloud import storage
import os
import gzip
import shutil
from pathlib import Path
import pandas_gbq
import pandas as pd
import numpy as np
import logging
import re
from fuzzywuzzy import process, fuzz

# Instantiates a client
storage_client = storage.Client()

project_id = "gum-eroski-dev"
dataset_id = "source_data"
bucket = "erk-data-feed"
blobs = storage_client.list_blobs(bucket, prefix="Working_folder/AT/ETL_test/")

blob_list = [blob.name for blob in blobs]
blob_fname = [blob.split("/")[-1] for blob in blob_list]
print(blob_list)

home = str(Path.home())
local_dir = os.path.abspath(home + "/etl_test/")
# local_directory = os.fsencode("~/etl_test/")


def initialise_logger():
    """Initialise logger settings"""

    # Set logger properties
    logger = logging.getLogger("auto_etl_to_bq")
    logger.setLevel(logging.DEBUG)

    # create file handler which logs even debug messages
    fh = logging.FileHandler("auto_etl_to_bq.log")
    fh.setLevel(logging.DEBUG)

    # create console handler with a higher log level
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)

    # create formatter and add it to the handlers
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)

    # add the handlers to the logger
    logger.addHandler(fh)
    logger.addHandler(ch)

    return logger


def download_blob(bucket_name, source_blob_name, destination_file_name):
    """Downloads a blob from the bucket."""
    # bucket_name = "your-bucket-name"
    # source_blob_name = "storage-object-name"
    # destination_file_name = "local/path/to/file"

    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)

    logger.info("Blob {} downloaded to {}.".format(source_blob_name, destination_file_name))


def gunzip(source_filepath, dest_filepath, block_size=65536):
    """Unzips .gz files and writes to persistent disk"""

    with gzip.open(source_filepath, "rb") as s_file, open(dest_filepath, "wb") as d_file:
        while True:
            block = s_file.read(block_size)
            if not block:
                break
            else:
                d_file.write(block)
        d_file.write(block)


def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket"""
    # bucket_name = "your-bucket-name"
    # source_file_name = "local/path/to/file"
    # destination_blob_name = "storage-object-name"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    logger.info("File {} uploaded to {}.".format(source_file_name, destination_blob_name))


def change_extension(old_extension, new_extension, directory):
    """Change file extensions for files in directory"""

    for file in os.listdir(directory):
        pre, ext = os.path.splitext(file)
        if ext == old_extension:
            os.rename(file, pre + new_extension)
            continue
        else:
            continue


def csv_checks(csv_filename, dataset_schema):
    """Checks format of csv files with Bigquery tables"""

    # read csv file into dataframe
    try:
        csv_data = pd.read_csv(csv_filename, nrows=2)
        # logger.info(csv_data.describe(include="all"))
        # check for matching table in Bigquery
        fn = csv_filename.split("/")[-1]
        table_name_list = dataset_schema.table_name.unique()
        # remove digits and replace underscores from both strings
        fn_str = re.sub(r"\d+", "", fn.split(".")[0]).replace("_", " ").lower()
        table_name_str = [
            re.sub(r"\d+", "", x).replace("_", " ").lower().replace("estructura", "")
            for x in table_name_list
        ]
        # create dictionary of table names with indexes
        table_name_dict = {idx: el for idx, el in enumerate(table_name_str)}
        # find top match Bigquery table
        matched_table = process.extractOne(fn_str, table_name_dict, scorer=fuzz.token_sort_ratio)
        logger.info(
            "csv file name = {} matched with {}".format(fn, table_name_list[matched_table[2]])
        )
        # select subset dataset_schema
        matched_table_schema = dataset_schema.loc[
            dataset_schema.table_name == table_name_list[matched_table[2]]
        ]
        # check if csv header matches Bigquery table
        csv_header = csv_data.head(1).tolist()
        logger.info(csv_header)
        table_columns = matched_table_schema.column_name.tolist()
        logger.info(table_columns)
    except:
        logger.info("csv file: {} did not read properly".format(csv_filename))


def get_bq_schemas(dataset_id):
    """Returns Bigquery dataset information"""

    # get table names and columns
    sql_str = """
    SELECT table_name, column_name, data_type FROM `gum-eroski-dev`.source_data.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS
    """
    # read from Bigquery
    dataset_schema = pandas_gbq.read_gbq(sql_str, project_id=project_id)
    return dataset_schema


if __name__ == "__main__":
    logger = initialise_logger()
    dataset_schema = get_bq_schemas(dataset_id)
    for blob in blob_list:
        blob_fn = blob.split("/")[-1]
        # check if file exists
        if os.path.exists(os.path.abspath(local_dir + "/" + blob_fn)):
            logger.info(
                "Blob {} already exists in {}.".format(
                    blob, os.path.abspath(local_dir + "/" + blob_fn)
                )
            )
        else:
            download_blob(bucket, blob, os.path.abspath(local_dir + "/" + blob_fn))
        if os.path.exists(os.path.abspath(local_dir + "/" + blob_fn.split(".")[0] + ".csv")):
            logger.info(
                "File {} already unzipped".format(os.path.abspath(local_dir + "/" + blob_fn))
            )
            csv_checks(
                os.path.abspath(local_dir + "/" + blob_fn.split(".")[0] + ".csv"), dataset_schema
            )
        else:
            gunzip(
                os.path.abspath(local_dir + "/" + blob_fn),
                os.path.abspath(local_dir + "/" + blob_fn.split(".")[0] + ".csv"),
            )
        # upload_blob(
        #     bucket,
        #     os.path.abspath(local_dir + "/" + blob_fn.split(".")[0] + ".csv"),
        #     "Working_folder/AT/ETL_test_upload/" + blob_fn.split(".")[0] + ".csv",
        # )

        # upload dataframe to Bigquery
        # pandas_gbq.to_gbq(blob_dataframe, )
