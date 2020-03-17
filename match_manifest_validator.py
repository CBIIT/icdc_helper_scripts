import argparse
import csv
import os

import requests

from bento.common.utils import get_logger, get_md5, get_stream_md5, get_time_stamp, LOG_PREFIX
if LOG_PREFIX not in os.environ:
    os.environ[LOG_PREFIX] = 'Match_Manifest_Validator'
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
EXISTS = 'File_Presence'
MD5_CORRECT = 'MD5_Correct'
MATCH_MD5_CORRECT = 'MATCH_MD5_Correct'
PATIENT_CORRECT = 'Patient_File_Association_Correct'


FILE_NAME = 'file_name'
MD5 = 'md5sum'
PSN = 'patientSequenceNumber'
FILE_TYPE = 'file_type'
VERSION = 'version'
NUM_FILE_TYPES = 5

UUID = 'uuid'
SIZE = 'size'
RESULT = 'Validation_result'

PATIENTS_API_PATH = 'patients'
DOWNLOAD_API_PATH = 'download_url'
ARM_API_PATH = 'treatment_arms'


class ManifestValidator:
    def __init__(self, file_name):
        assert os.path.isfile(file_name)
        self.file_name = file_name
        self.log = get_logger('Manifest_Validator')
        self.s3_buckets = {}

    def validate(self, match_url, token, cipher_key, arms):
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
        assert isinstance(arms, list)
        self.arms = arms

        self.patients = {}
        self.patients_info = {}
        field_names = [FILE_NAME, PSN, UUID, SIZE, MD5, RESULT, EXISTS, MD5_CORRECT, MATCH_MD5_CORRECT, PATIENT_CORRECT]
        result_file = f'tmp/Validation_Result_{get_time_stamp()}.csv'
        with open(self.file_name) as in_file:
            reader = csv.DictReader(in_file, delimiter='\t')
            with open(result_file, 'w') as out_file:
                writer = csv.DictWriter(out_file, fieldnames=field_names)
                writer.writeheader()
                for obj in reader:
                    result = self.validate_file(obj)
                    writer.writerow(result)

        if not self.validate_patient_count():
            self.log.error('Validate patient count failed!')

        if not self.validate_patient_file_count():
            self.log.error('Validate patient file count failed!')

    def validate_file(self, obj):
        result = {
            FILE_NAME: obj[FILE_NAME],
            PSN: obj[PSN],
            UUID: obj[UUID],
            SIZE: obj[SIZE],
            MD5: obj[MD5]
        }
        result[PATIENT_CORRECT] = self.validate_file_patient(obj)
        result[EXISTS] = self.validate_file_exists(obj)
        result[MD5_CORRECT] = self.validate_file_md5(obj) if result[EXISTS] else False
        result[MATCH_MD5_CORRECT] = self.validate_match_file_md5(obj)

        result[RESULT] = result[EXISTS] and result[PATIENT_CORRECT] and result[MD5_CORRECT] and result[MATCH_MD5_CORRECT]
        return result

    def validate_patient_file_count(self):
        '''
        Validate number of files a patient has, return False if there are duplicated files or missing files
        Same patient should not have two same files, and one patient should have at least 5 files

        :return: boolean
        '''
        result = True
        for patient, file_types in self.patients.items():
            if len(file_types) != NUM_FILE_TYPES:
                self.log.error(f'Patient {patient} has {len(file_types)} file types instead of {NUM_FILE_TYPES}')
                result = False
            for type, files in file_types.items():
                for file_name, count in files.items():
                    if count != 1:
                        self.log.error(f'There are {count} copy of file "{file_name}" for patient "{patient}"')
                        result = False

        self.log_validation_result('Patient\'s file count', result)
        return result

    def validate_patient_count(self):
        '''
        Validate patients in manifest exactly match given arm's patient list (patients with valid slot)
        Must call after validate_file_patient()!

        :return: boolean
        '''
        result = True
        all_patients = set()
        for arm_id in self.arms:
            patients = self.get_patients_for_arm(arm_id)
            all_patients = all_patients.union(patients)

        for patient in self.patients.keys():
            if patient not in all_patients:
                self.log.error(f'Patient "{patient}" is not a validate patient in given arms: {self.arms}')
                result = False

        for patient in all_patients:
            if patient not in self.patients:
                self.log.error(f'Patient "{patient}" is missing in the manifest!')
                result = False

        if result:
            self.log_validation_result('Patient count', result)
        return result

    def _retrieve_arm_info(self, arm_id):
        '''
        Retrieve arm information from Match API, return latest version of the arm

        :param arm_id:
        :return: dict contains information of latest version of the arm
        '''
        arm_url = f'{self.match_url}/{ARM_API_PATH}/{arm_id}'

        headers = {'Authorization': self.token}
        arm_result = requests.get(arm_url, headers=headers)

        arms = arm_result.json()
        arm_info = None
        for arm in arms:
            current_version = arm.get(VERSION)
            if not arm_info or current_version > arm_info[VERSION]:
                arm_info = arm

        return arm_info

    def get_patients_for_arm(self, arm_id):
        """
        This function gets a set of patient IDs for an arm

        :param arm_id:
        :return: list of IDs (string)
        """
        assert isinstance(arm_id, str)
        patients = set()
        arm = self._retrieve_arm_info(arm_id)
        for patient in arm.get('summaryReport', {}).get('assignmentRecords', []):
            slot = int(patient.get('slot', -1))
            if slot > 0:
                patients.add(patient.get('patientSequenceNumber'))

        return patients

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
        result = org_name == file_name
        if not result:
            self.log.error('File name error!')
        self.log_validation_result(f'Patient "{patient_id}" has file "{file_name}"', result)
        return result


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
        try:
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
        except:
            self.log.error(f'Download file "{info[KEY]}" failed!')
            return False

    def validate_match_file_md5(self, obj):
        url = self.get_signed_url(obj)
        if not url:
            self.log_validation_result(f'MD5 for original MATCH file {obj[FILE_NAME]}', False)
            return False
        with requests.get(url, stream=True) as r:
            # If Error is found and we are in Prod Print and Exit
            if r.status_code >= 400:
                self.log.error(f'Validating MD5 for original MATCH file {obj[FILE_NAME]} Failed: {r.reason}')
                return False
            match_md5 = get_stream_md5(r.raw)
            result = match_md5 == obj[MD5]
            self.log_validation_result(f'MD5 for original MATCH file {obj[FILE_NAME]}', result)
            return result


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
            self.log.error(f'Couldn\'t get signed URL for {patient_id}: "{s3_url}"' )
            return None
        # Add a dictionary item for the file object
        return r.json()['download_url']

    def validate_file_exists(self, obj):
        info = self.get_s3_file_info(obj[FILE_LOCATION])
        bucket = self.get_s3_bucket(info[BUCKET])
        result = bucket.file_exists_on_s3(info[KEY])
        self.log_validation_result(f'{obj[FILE_LOCATION]} exists', result)
        return result

    def log_validation_result(self, validation, result):
        if result:
            self.log.info(f'Validating {validation}: Succeeded!')
        else:
            self.log.error(f'Validating {validation}: Failed!')

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

    secrets = get_secret(config.region, config.secret_name)
    token = get_okta_token(secrets, config.okta_auth_url)
    if not token:
        raise Exception('Failed to obtain a token!')

    manifest = ManifestValidator(args.manifest)
    manifest.validate(config.match_base_url, token, config.cipher_key, [arm.arm_id for arm in config.arms])




if __name__ == '__main__':
    main()