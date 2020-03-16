import argparse
import csv
import os

import requests

from bento.common.utils import get_logger, get_md5, get_stream_md5
from bento.common.s3 import S3Bucket
from bento.common.secrets import get_secret
from bento.common.simple_cipher import SimpleCipher
from bento.common.tokens import get_okta_token
from config import Config

FILE_LOCATION = 'file_locations'
S3_PREFIX = 's3://'
S3_DELIMITER = '/'
BUCKET = 'bucket'
KEY = 'key'
EXISTS = 'File exists'
MD5_CORRECT = 'MD5 correct'
MATCH_MD5_CORRECT = 'MATCH MD5 correct'
PATIENT_CORRECT = 'Patient correct'

FILE_NAME = 'file_name'
MD5 = 'md5sum'
PSN = 'patientSequenceNumber'
FILE_TYPE = 'file_type'



PATIENTS_API_PATH = 'patients'
DOWNLOAD_API_PATH = 'download_url'



class ManifestValidator:
    def __init__(self, file_name):
        assert os.path.isfile(file_name)
        self.file_name = file_name
        self.log = get_logger('Manifest_Validator')
        self.s3_buckets = {}

    def validate(self, match_url, token, cipher_key):
        '''
        Validate manifest return True only if every file passed validation

        :param match_url: NCI-MATCH base URL to validate against
        :param token: Okta token to connect to NCI-MATCH
        :return:
        '''
        assert isinstance(match_url, str) and match_url
        self.match_url = match_url
        assert token
        self.token = token
        assert isinstance(cipher_key, int)
        self.cipher = SimpleCipher(cipher_key)

        self.results = []
        self.patients = {}
        self.patients_info = {}
        with open(self.file_name) as in_file:
            reader = csv.DictReader(in_file, delimiter='\t')
            for obj in reader:
                self.results.append(self.validate_file(obj))

        if not self.validate_patient_file_count():
            self.log.error('Patient file count failed!')

    def validate_file(self, obj):
        result = {}
        # result[EXISTS] = self.validate_file_exists(obj)
        # result[MD5_CORRECT] = self.validate_file_md5(obj)
        # result[PATIENT_CORRECT] = self.validate_file_patient(obj)
        result[MATCH_MD5_CORRECT] = self.validate_match_file_md5(obj)
        return result

    def validate_patient_file_count(self):
        '''
        Validate number of files a patient has, return False if there are duplicated files or missing files
        Same patient should not have two same files, and one patient should have at least 5 files

        :return: boolea
        '''
        return False


    def validate_file_patient(self, obj):
        patient_id = self.cipher.simple_decipher(obj[PSN])
        file_type = obj[FILE_TYPE]
        file_name = obj[FILE_NAME]

        if patient_id not in self.patients:
            self.patients[patient_id] = {}
        if file_type not in self.patients[patient_id]:
            self.patients[patient_id][file_type] = {}

        current_type = self.patients[patient_id][file_type]
        current_type[file_name] = current_type.get(file_name, 0) + 1

        s3_path = self.get_match_file_path(patient_id, file_type, file_name)
        org_name = os.path.basename(s3_path)
        return org_name == file_name

    def get_match_file_path(self, patient_id, file_type, file_name):
        patient_info = self.get_patient_meta_data(patient_id)
        try:
            for biopsy in patient_info['biopsies']:
                if biopsy['biopsyType'] == 'STANDARD':
                    for sequence in biopsy['nextGenerationSequences']:
                        if sequence['status'] == 'CONFIRMED':
                            ion_report = sequence['ionReporterResults']
                            field_name = self.get_field_by_file_type(file_type)
                            s3_path = ion_report.get(field_name, '')
                            org_name = os.path.basename(s3_path)
                            if org_name == file_name:
                                return s3_path
        except:
            return ''

        return ''

    @staticmethod
    def get_field_by_file_type(file_type):
        map = {
            'DNABam': 'dnaBamFilePath',
            'RNABam': 'rnaBamFilePath',
            'VCF': 'vcfFilePath',
            'DNABai': 'dnaBaiFilePath',
            'RNABai': 'rnaBaiFilePath'
        }

        return map.get(file_type)

    def validate_file_md5(self, obj):
        info = self.get_s3_file_info(obj[FILE_LOCATION])
        bucket = self.get_s3_bucket(info[BUCKET])
        tmp_file = os.path.join('tmp', info[FILE_NAME])
        bucket.download_file(info[KEY], tmp_file)

        if os.path.isfile(tmp_file):
            local_md5 = get_md5(tmp_file)
            os.remove(tmp_file)
            result = local_md5 == obj[MD5]
            self.log_validation_result(f'{obj[FILE_LOCATION]} MD5', result)
            return result
        else:
            self.log.error(f'Download {info[KEY]} failed!')
            return False

    def validate_match_file_md5(self, obj):
        url = self.get_signed_url(obj)
        with requests.get(url, stream=True) as r:
            # If Error is found and we are in Prod Print and Exit
            if r.status_code >= 400:
                self.log.error(f'Http Error Code {r.status_code} for file {obj[FILE_NAME]}')
                return False
            match_md5 = get_stream_md5(r.raw)
            return match_md5 == obj[MD5]


    def get_signed_url(self, obj):
        patient_id = self.cipher.simple_decipher(obj[PSN])
        s3_url = self.get_match_file_path(patient_id, obj[FILE_TYPE], obj[FILE_NAME])

        url = f'{self.match_url}/{PATIENTS_API_PATH}/{patient_id}/{DOWNLOAD_API_PATH}'
        query = ({"s3_url": s3_url})
        headers = {
            'Authorization': self.token,
            'Content-Type': 'application/json'
        }
        r = requests.post(url, json=query, headers=headers)
        if r.status_code >= 400:
            raise Exception(f'Can NOT retrieve signed URL for {patient_id}: {s3_url}')
        # Add a dictionary item for the file object
        return r.json()['download_url']

    def validate_file_exists(self, obj):
        info = self.get_s3_file_info(obj[FILE_LOCATION])
        bucket = self.get_s3_bucket(info[BUCKET])
        result = bucket.file_exists_on_s3(info[KEY])
        self.log_validation_result(f'{obj[FILE_LOCATION]} exists', result)
        return result

    def log_validation_result(self, validation, result):
        msg = 'Succeeded!' if result else 'Failed!'
        self.log.info(f'Validating {validation}: {msg}')

    def get_s3_bucket(self, bucket):
        '''
        Get S3Bucket object for given bucket

        :param bucket:  bucket name
        :return: S3Bucket object
        '''

        if bucket not in self.s3_buckets:
            self.s3_buckets[bucket] = S3Bucket(bucket)

        return self.s3_buckets[bucket]

    def get_s3_file_info(self, file_location):
        info = {}
        if not file_location.startswith(S3_PREFIX):
            return None
        localtion = file_location.replace(S3_PREFIX, '')
        location_ls = localtion.split(S3_DELIMITER)
        info[BUCKET] = location_ls[0]
        info[KEY] = S3_DELIMITER.join(location_ls[1:])
        info[FILE_NAME] = os.path.basename(info[KEY])
        return info

    def get_patient_meta_data(self, patient_id):
        assert self.token
        assert self.match_url

        if patient_id in self.patients_info:
            return self.patients_info[patient_id]

        url = f'{self.match_url}/{PATIENTS_API_PATH}/{patient_id}'
        headers = {'Authorization': self.token}
        result = requests.get(url, headers=headers)
        if result and result.ok:
            self.patients_info[patient_id] = result.json()
            return self.patients_info[patient_id]
        else:
            return None


def main():
    parser = argparse.ArgumentParser(description='Validate NCI-MATCH File manifest')
    parser.add_argument("--config-file", help="Name of Configuration File to run the validator", required=True)
    parser.add_argument('manifest', help='Manifest file to be validated')
    args = parser.parse_args()

    config = Config(args.config_file)
    log = get_logger('MATCH-Validator')

    secrets = get_secret(config.region, config.secret_name)
    token = get_okta_token(secrets, config.okta_auth_url)
    if not token:
        raise Exception('Failed to obtain a token!')

    manifest = ManifestValidator(args.manifest)
    manifest.validate(config.match_base_url, token, config.cipher_key)




if __name__ == '__main__':
    main()