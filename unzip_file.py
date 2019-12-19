# python script to decompress .dat.gz files

from google.cloud import storage
import os
import gzip
import shutil

# Instantiates a client
storage_client = storage.Client()

# The name for the new bucket
# bucket_name = 'my-new-bucket'

# Creates the new bucket
# bucket = storage_client.create_bucket(bucket_name)

bucket = storage_client.get_bucket("erk-data-feed")
print(bucket.exists())
blobs = storage_client.list_blobs(bucket, prefix="ETL_test")

blob_list = [blob.name for blob in blobs]
print(blob_list)

path_to_file = "~/etl_test/" + blob_list[0]

local_directory = os.fsencode("~/etl_test/")


def download_blob(bucket_name, source_blob_name, destination_file_name):
    """Downloads a blob from the bucket."""
    # bucket_name = "your-bucket-name"
    # source_blob_name = "storage-object-name"
    # destination_file_name = "local/path/to/file"

    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)

    print("Blob {} downloaded to {}.".format(source_blob_name, destination_file_name))


def gunzip(source_filepath, dest_filepath, block_size=65536):
    with gzip.open(source_filepath, "rb") as s_file, open(
        dest_filepath, "wb"
    ) as d_file:
        while True:
            block = s_file.read(block_size)
            if not block:
                break
            else:
                d_file.write(block)
        d_file.write(block)


def change_extension(old_extension, new_extension, directory):
    for file in os.listdir(directory):
        pre, ext = os.path.splitext(file)
        if ext == old_extension:
            os.rename(file, pre + new_extension)
            continue
        else:
            continue


if __name__ == "__main__":
    download_blob(bucket, blob_list[0], path_to_file)
    #gunzip(path_to_file, path_to_file)
    #change_extension(".dat", ".csv", local_directory)
