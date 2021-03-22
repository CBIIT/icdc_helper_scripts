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
from timeit import default_timer as timer

from bento.common.utils import LOG_PREFIX, APP_NAME, get_md5, get_logger, get_time_stamp, removeTrailingSlash, \
                               get_log_file, format_bytes

if LOG_PREFIX not in os.environ:
    os.environ[LOG_PREFIX] = 'File_copy_validator'

os.environ[APP_NAME] = 'File_copy_validator'
log = get_logger('Validator')

SUCCEEDED = 'Succeeded'
FAILED = 'Failed'
tmp_folder = 'tmp'
PREVIOUSE_VALIDATED = ': in previous validation'


# Input like s3://some/path(/)
def split_s3_path(s3_path):
    path_parts = s3_path.replace("s3://", "").split("/")
    bucket = path_parts.pop(0)
    key = "/".join(path_parts)
    return bucket, key

def list_files(s3, bucket, s3_path):
    paginator = s3.get_paginator('list_objects')
    pages = paginator.paginate(Bucket=bucket, Prefix=s3_path)

    files = []
    for page in pages:
        for obj in page['Contents']:
            files.append(obj)

    return files


def compare_md5(s3, src_bucket, dest_bucket, key):
    tmp_files = []
    try:
        file_name = key.split('/')[-1]

        log.info('Calculating MD5 for source file')
        src_file_name = 'tmp/src_' + file_name
        log.info(f"Downloading source file to: {src_file_name}")
        with open(src_file_name, 'wb') as data:
            s3.download_fileobj(src_bucket, key, data)
            tmp_files.append(src_file_name)
        src_md5 = get_md5(src_file_name)
        log.info(f'Source file MD5: {src_md5}')

        log.info('Calculating MD5 for destination file')
        dest_file_name = 'tmp/dest_' + file_name
        log.info(f"Downloading destination file to: {dest_file_name}")
        with open(dest_file_name, 'wb') as data:
            s3.download_fileobj(dest_bucket, key, data)
            tmp_files.append(dest_file_name)

        dest_md5 = get_md5(dest_file_name)
        log.info(f'Destination file MD5: {dest_md5}')
    except Exception as e:
        log.exception(e)
        return FAILED, e
    finally:
        for file in tmp_files:
            if os.path.isfile(file):
                os.remove(file)

    if src_md5 == dest_md5:
        return SUCCEEDED, 'MD5s match'
    else:
        return FAILED, "MD5s don't match"


def validate_file(s3, file, src_bucket, dest_bucket):
    try:
        target = s3.head_object(Bucket=dest_bucket, Key=file['Key'])
    except ClientError as e:
        return FAILED, f"{e.response['Error']['Code']}: {e.response['Error']['Message']}"
    except Exception as e:
        return FAILED, e

    if file['ETag'] == target['ETag']:
        return SUCCEEDED, 'ETags match'
    elif file['Size'] == target['ContentLength']:
        log.info("ETags don't match, calculating MD5")
        return compare_md5(s3, src_bucket, dest_bucket, file['Key'])
    else:
        return FAILED, "File sizes don't match"



def main():
    parser = argparse.ArgumentParser(description='Script to validate file copying')
    parser.add_argument('-sp', '--src-path', help='Source S3 bucket name and optional path')
    parser.add_argument('-db', '--dest-bucket', help='Destination S3 bucket name')
    parser.add_argument('-pf', '--previous-file', type=argparse.FileType('r'), help='Previous output CSV file of this script')
    args = parser.parse_args()
    start_time = timer()
    fieldnames = ['src_bucket', 'dest_bucket', 'file_name', 'file_size', 'result', 'reason']
    s3 = boto3.client('s3')

    # Revalidate a previous validation file
    if args.previous_file:
        log.info(f'Previous validation file: {args.previous_file.name}')
        reader = csv.DictReader(args.previous_file)
        file_list = []

        for obj in reader:
            src_bucket = obj['src_bucket']
            dest_bucket = obj['dest_bucket']
            if obj['result'] == SUCCEEDED:
                if not obj['reason'].endswith(PREVIOUSE_VALIDATED):
                    obj['reason'] += PREVIOUSE_VALIDATED
                file_list.append(obj)
            else:
                file = s3.head_object(Bucket=src_bucket, Key=obj['file_name'])
                file['Size'] = file['ContentLength']
                file['Key'] = obj['file_name']
                file_list.append(file)

    else:
        if not args.src_path or not args.dest_bucket:
            log.error('Source S3 path and Destination S3 bucket are required!')
            return
        source_path = removeTrailingSlash(args.src_path)
        dest_bucket = removeTrailingSlash(args.dest_bucket)
        src_bucket, s3_path = split_s3_path(source_path)

        log.info(f"Source bucket: {src_bucket}")
        log.info(f"Dest   bucket: {dest_bucket}")
        log.info(f"Prefix: {s3_path}")

        file_list = list_files(s3, src_bucket, s3_path)

    num_files = len(file_list)
    log.info(f"There are {num_files} files to compare")

    os.makedirs(tmp_folder, exist_ok=True)
    output_file = f'{tmp_folder}/copy-file-validation-{get_time_stamp()}.csv'
    with open(output_file, 'w') as of:
        writer = csv.DictWriter(of, fieldnames=fieldnames)
        writer.writeheader()

        counter = 0
        succeeded = 0
        total_size = 0
        for file in file_list:
            counter += 1

            # These files has been successfully validated last time
            if 'result' in file:
                writer.writerow(file)
                file_size = int(file['file_size'])
                total_size += file_size
                log.info(f"Valiating file {counter}/{num_files} ({format_bytes(file_size)}): {file['file_name']}")
                log.info('Validated in previous run')
                continue

            file_size = file['Size']
            total_size += file_size
            try:
                log.info(f'Valiating file {counter}/{num_files} ({format_bytes(file_size)}): {file["Key"]}')
                result, message = validate_file(s3, file, src_bucket, dest_bucket)
            except Exception as e:
                log.exception(e)
                log.error(f'Valiating file: {file["Key"]} failed! See errors above.')
                result = FAILED
                message = e

            if result == SUCCEEDED:
                log.info(f"{result}: {message}")
                succeeded += 1
            else:
                log.error(f"{result}: {message}")
            log.info(f"Total Verified file size: {format_bytes(total_size)}")
            writer.writerow({
                'src_bucket': src_bucket,
                'dest_bucket': dest_bucket,
                'file_name': file['Key'],
                'file_size': file_size,
                'result': result,
                'reason': message
            })

        end_time = timer()
        log.info(f"Comparing finished! Total files validated: {counter}, total file size: {format_bytes(total_size)}")
        log.info(f"Comparing succeeded: {succeeded} out of {num_files} files")
        log.info(f"Running time: {end_time - start_time:.2f} seconds")
        log.info(f"Output file is at: {output_file}")
        log.info(f"Log file is at: {get_log_file()}")


if __name__ == '__main__':
    main()
