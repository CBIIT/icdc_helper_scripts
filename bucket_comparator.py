# This function takes in two buckets and compares the files in the buckets to check if the objects in the source bucket
# match the Etags in the destination bucket
from boto3.session import Session
import argparse

session = Session()
s3 = session.resource('s3')

# Specifying argument parsing from the command line
parser = argparse.ArgumentParser(description='Script to test Etags')
parser.add_argument("--source_bucket", required=True, type=str, help="Name of  Source Bucket")
parser.add_argument("--dest_bucket", required=True, type=str, help="Name of  Destination Bucket")
args = parser.parse_args()

# Getting the Source and Destination Bucket Objects
orig_bucket = s3.Bucket(args.source_bucket)
dest_bucket = s3.Bucket(args.dest_bucket)

# Initializing the Naughty Files list
naughty_files = []

# Iterate through all the objects in the source bucket
for s3_file in orig_bucket.objects.all():
    object_key_source_bucket = s3_file.key
    dest_file = dest_bucket.Object(object_key_source_bucket)

    # If the object is not found in the destination bucket, add it to the naughty list
    if dest_file is None:
        naughty_files.append(key_source)
        continue

    orig_etag = s3_file.e_tag
    dest_etag = dest_file.e_tag

    # If the Etags do not match append it to the naughty list
    if orig_etag != dest_etag:
        naughty_files.append(key_source)
    else:
        print('Etag matches!')

print(naughty_files)