# python script to decompress .dat.gz files

from google.cloud import storage
import os
import gzip
import shutil
from pathlib import Path
import pandas_gbq
import pandas as pd
import numpy as np
import logging

# Instantiates a client
storage_client = storage.Client()

bucket = "erk-data-feed"
blobs = storage_client.list_blobs(bucket, prefix="Working_folder/AT/ETL_test/")

blob_list = [blob.name for blob in blobs]
blob_fname = [blob.split("/")[-1] for blob in blob_list]
print(blob_list)

home = str(Path.home())
local_dir = os.path.abspath(home + "/etl_test/")
# local_directory = os.fsencode("~/etl_test/")


def initialise_logger():
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

    # check if file exists
    if os.path.exists(destination_file_name):
        logger.info("Blob {} already exists in {}.".format(source_blob_name, destination_file_name))
    else:
        storage_client = storage.Client()

        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(source_blob_name)
        blob.download_to_filename(destination_file_name)

        logger.info("Blob {} downloaded to {}.".format(source_blob_name, destination_file_name))


def gunzip(source_filepath, dest_filepath, block_size=65536):
    with gzip.open(source_filepath, "rb") as s_file, open(dest_filepath, "wb") as d_file:
        while True:
            block = s_file.read(block_size)
            if not block:
                break
            else:
                d_file.write(block)
        d_file.write(block)


def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    # bucket_name = "your-bucket-name"
    # source_file_name = "local/path/to/file"
    # destination_blob_name = "storage-object-name"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    logger.info("File {} uploaded to {}.".format(source_file_name, destination_blob_name))


def change_extension(old_extension, new_extension, directory):
    for file in os.listdir(directory):
        pre, ext = os.path.splitext(file)
        if ext == old_extension:
            os.rename(file, pre + new_extension)
            continue
        else:
            continue


if __name__ == "__main__":
    initialise_logger()
    for blob in blob_list:
        blob_fn = blob.split("/")[-1]
        download_blob(bucket, blob, os.path.abspath(local_dir + "/" + blob_fn))
        if os.path.exists(os.path.abspath(local_dir + "/" + blob_fn.split(".")[0] + ".csv")):
            logger.info(
                "File {} already unzipped".format(os.path.abspath(local_dir + "/" + blob_fn))
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
