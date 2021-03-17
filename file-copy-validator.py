#!/usr/bin/env python3

# This script compare two S3 buckets paths, to make sure all files are copied and file ETag/MD5 match
# It will find all files from source bucket/path then try to find same files on destination path
# Then it will compare ETags, if same it record copy succeeded.
# If ETags don't match, it will download both files then calculate and compare MD5s
import argparse
import csv
import os
import boto3
from botocore.exceptions import ClientError

from bento.common.utils import LOG_PREFIX, APP_NAME, get_stream_md5, get_logger, get_time_stamp, removeTrailingSlash, \
                               get_log_file

if LOG_PREFIX not in os.environ:
    os.environ[LOG_PREFIX] = 'File_copy_validator'

os.environ[APP_NAME] = 'File_copy_validator'
log = get_logger('Validator')


# Input like s3://some/path(/)
def split_s3_path(s3_path):
    path_parts = s3_path.replace("s3://", "").split("/")
    bucket = path_parts.pop(0)
    key = "/".join(path_parts)
    return bucket, key

def list_files(s3, bucket, s3_path):
    result = s3.list_objects(Bucket=bucket, Prefix=s3_path)
    return result.get('Contents', [])

def compare_md5(s3, src_bucket, dest_bucket, key):
    try:
        src_obj = s3.get_object(Bucket=src_bucket, Key=key)
        dest_obj = s3.get_object(Bucket=dest_bucket, Key=key)
        src_md5 = get_stream_md5(src_obj['Body'])
        dest_md5 = get_stream_md5(dest_obj['Body'])
        if src_md5 == dest_md5:
            return 'Succeeded', 'MD5s match'
        else:
            return 'Failed', "MD5s don't match"
    except Exception as e:
        log.exception(e)
        return 'Failed', e

def validate_file(s3, file, src_bucket, dest_bucket):
    try:
        target = s3.head_object(Bucket=dest_bucket, Key=file['Key'])
    except ClientError as e:
        return 'Failed', f"{e.response['Error']['Code']}: {e.response['Error']['Message']}"
    except Exception as e:
        return 'Failed', e

    if file['ETag'] == target['ETag']:
        return 'Succeeded', 'ETags match'
    elif file['Size'] == target['ContentLength']:
        return compare_md5(s3, src_bucket, dest_bucket, file['Key'])
    else:
        return 'Failed', "File sizes don't match"



def main():
    parser = argparse.ArgumentParser(description='Script to validate file copying')
    parser.add_argument('-sp', '--src-path', required=True, help='Source S3 bucket name and optional path')
    parser.add_argument('-db', '--dest-bucket', required=True, help='Destination S3 bucket name')
    args = parser.parse_args()

    source_path = removeTrailingSlash(args.src_path)
    dest_bucket = removeTrailingSlash(args.dest_bucket)
    src_bucket, s3_path = split_s3_path(source_path)
    tmp_folder = 'tmp'
    fieldnames = ['src_bucket', 'dest_bucket', 'file_name', 'result', 'reason']

    log.info(f"Source bucket: {src_bucket}")
    log.info(f"Dest   bucket: {dest_bucket}")
    log.info(f"Prefix: {s3_path}")
    os.makedirs(tmp_folder, exist_ok=True)
    output_file = f'{tmp_folder}/copy-file-validation-{get_time_stamp()}.csv'
    with open(output_file, 'w') as of:
        s3 = boto3.client('s3')
        writer = csv.DictWriter(of, fieldnames=fieldnames)
        writer.writeheader()

        file_list = list_files(s3, src_bucket, s3_path)
        log.info(f"There are {len(file_list)} files to compare")
        for file in file_list:
            try:
                log.info(f'Valiating file: {file["Key"]} ...')
                result, message = validate_file(s3, file, src_bucket, dest_bucket)
                log.info(f"result: {result}, message: {message}")
                writer.writerow({
                    'src_bucket': src_bucket,
                    'dest_bucket': dest_bucket,
                    'file_name': file['Key'],
                    'result': result,
                    'reason': message
                })
            except Exception as e:
                log.exception(e)
                log.error(f'Valiating file: {file["Key"]} failed! See errors above.')
        log.info("Comparing finished")
        log.info(f"Output file is at: {output_file}")
        log.info(f"Log file is at: {get_log_file()}")


if __name__ == '__main__':
    main()
