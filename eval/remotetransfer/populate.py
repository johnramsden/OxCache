#!/usr/bin/env python3

import os
import math
import boto3

# Configuration variables
BUCKET_NAME = 'oxcache'
KEY_NAME = 'random_data_2GiB.bin'  # S3 object key name
FILE_NAME = '/tmp/random_data.bin'  # Local temporary file name

# Constants for file generation
TOTAL_SIZE = 2 * 1024 * 1024 * 1024  # 2 GiB in bytes
CHUNK_SIZE = 10 * 1024 * 1024  # 10 MiB per chunk


def generate_random_file(filename, total_size, chunk_size):
    """
    Generate a file with random bytes.

    Args:
        filename (str): The name of the file to create.
        total_size (int): Total file size in bytes.
        chunk_size (int): Size of each chunk to write in bytes.
    """
    iterations = math.ceil(total_size / chunk_size)
    bytes_written = 0

    print(f"Generating file '{filename}' of size {total_size} bytes in {iterations} iterations...")

    with open(filename, 'wb') as f:
        for i in range(iterations):
            # Determine the number of bytes to write in this iteration
            to_write = min(chunk_size, total_size - bytes_written)
            # Generate random bytes
            data = os.urandom(to_write)
            f.write(data)
            bytes_written += to_write
            print(f"  Iteration {i + 1}/{iterations}: Wrote {to_write} bytes (total {bytes_written}/{total_size})")

    print("File generation completed.")


def upload_file_to_s3(filename, bucket_name, key_name):
    """
    Upload a file to an S3 bucket.

    Args:
        filename (str): The path to the file to upload.
        bucket_name (str): The S3 bucket name.
        key_name (str): The S3 key name for the uploaded object.
    """
    s3 = boto3.client('s3')
    print(f"Uploading '{filename}' to S3 bucket '{bucket_name}' with key '{key_name}'...")
    s3.upload_file(filename, bucket_name, key_name)
    print("Upload completed.")


def main():
    # Generate the random data file
    generate_random_file(FILE_NAME, TOTAL_SIZE, CHUNK_SIZE)

    # Upload the file to S3
    upload_file_to_s3(FILE_NAME, BUCKET_NAME, KEY_NAME)

    # Remove the temporary file after upload
    try:
        os.remove(FILE_NAME)
        print(f"Temporary file '{FILE_NAME}' removed.")
    except Exception as e:
        print(f"Could not remove temporary file: {e}")


if __name__ == '__main__':
    main()
