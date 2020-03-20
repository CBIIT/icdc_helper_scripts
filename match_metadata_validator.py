import argparse
import csv
import json
import os
from timeit import default_timer as timer

import requests

from bento.common.utils import get_logger, get_md5, get_stream_md5, get_time_stamp, LOG_PREFIX
if LOG_PREFIX not in os.environ:
    os.environ[LOG_PREFIX] = 'Match_Metadata_Validator'
from bento.common.s3 import S3Bucket
from bento.common.secrets import get_secret
from bento.common.simple_cipher import SimpleCipher
from bento.common.tokens import get_okta_token
from config import Config

EXISTS = 'File_Presence'

PSN = 'patientSequenceNumber'
VERSION = 'version'

CASE_ID = 'case_id'
ARM_ID = 'arm_id'
ARM_PREFIX = 'EAY131-'
CASE_PREFIX = 'CTDC-'

RESULT = 'Validation_result'
TRAIL_CORRECT = 'Trial correct'
ARM_CORRECT = 'Arm correct'
ARM_DRUG_CORRECT = 'Arm drug correct'
DIAGNOSIS_CORRECT = 'Diagnosis correct'
GENDER_CORRECT = 'Gender correct'
RACE_CORRECT = 'Race correct'
ETHNICITY_CORRECT = 'Ethnicity correct'

PATIENTS_API_PATH = 'patients'
ARM_API_PATH = 'treatment_arms'


class MetadataValidator:
    def __init__(self, api_url):
        assert isinstance(api_url, str)
        self.api_url = api_url
        self.log = get_logger('Metadata_Validator')

    def validate(self, match_url, token, cipher_key, arms):
        '''
        Validate CTDC API against MATCH, return True only if every case passes validation

        :param match_url: NCI-MATCH base URL to validate against
        :param token: Okta token to connect to NCI-MATCH
        :return:
        '''
        start_time = timer()
        case_validated = 0
        validation_succeeded = 0


        assert isinstance(match_url, str) and match_url
        self.match_url = match_url
        assert token
        self.token = token
        assert isinstance(cipher_key, int)
        self.cipher = SimpleCipher(cipher_key)
        assert isinstance(arms, list)
        self.arms = arms

        self.arm_patients = {}
        for arm_id in self.arms:
            self.arm_patients[self.get_ctdc_arm_id(arm_id)] = self.get_patients_for_arm(arm_id)

        self.patients_info = {}
        self.cases = set()
        field_names = [CASE_ID, RESULT, TRAIL_CORRECT, ARM_CORRECT, ARM_DRUG_CORRECT, DIAGNOSIS_CORRECT, GENDER_CORRECT,
                        RACE_CORRECT, ETHNICITY_CORRECT]
        result_file = f'tmp/Metadata_Validation_Result_{get_time_stamp()}.csv'
        headers = { "Content-Type": "application/json"}
        query = {
            'query': "{caseOverview{  case_id  clinical_trial_code  clinical_trial_id  arm_id  arm_drug  disease  gender  race  ethnicity}}"
        }

        with requests.post(self.api_url, data=json.dumps(query), headers=headers) as res:
            if res.status_code >= 400:
                self.log.error(f'Could NOT retrieve CTDC data at {self.api_url}')
                end_time = timer()
                self.log.info(f'Running time: {end_time - start_time:.2f} seconds')
                return

            data = res.json()['data']['caseOverview']
            with open(result_file, 'w') as out_file:
                self.log.info(f'Validation result file: {result_file}')
                writer = csv.DictWriter(out_file, fieldnames=field_names)
                writer.writeheader()
                for obj in data:
                    result = self.validate_case(obj)
                    case_validated += 1
                    if result[RESULT]:
                        validation_succeeded += 1
                    writer.writerow(result)

        if not self.validate_patient_count():
            self.log.error('Validate patient count failed!')

        end_time = timer()
        self.log.info(f'Running time: {end_time - start_time:.2f} seconds')
        self.log.info(f'Cases processed: {case_validated}, validation succeeded: {validation_succeeded}')

    def validate_case(self, obj):
        case_id = self.get_psn(obj)
        self.cases.add(case_id)

        patient_info = self.get_patient_meta_data(case_id)

        result = {
            CASE_ID: obj[CASE_ID],
            TRAIL_CORRECT: self.validate_trial(obj),
            ARM_CORRECT: self.validate_arm(obj),
            ARM_DRUG_CORRECT: self.validate_arm_drug(obj, patient_info),
            DIAGNOSIS_CORRECT: self.validate_diagnosis(obj, patient_info),
            GENDER_CORRECT: self.validate_gender(obj, patient_info),
            RACE_CORRECT: self.validate_race(obj, patient_info),
            ETHNICITY_CORRECT: self.validate_ethnicity(obj, patient_info)
        }


        result[RESULT] = True
        for value in result.values():
            if not value:
                result[RESULT] = False
                break

        self.log_validation_result(f'case {obj[CASE_ID]}({case_id})', result[RESULT])
        return result

    def get_ctdc_arm_id(self, arm_id):
        return arm_id.replace(ARM_PREFIX, '')

    def get_psn(self, obj):
        return self.cipher.simple_decipher(obj[CASE_ID].replace(CASE_PREFIX, ''))

    def validate_trial(self, obj):
        return obj['clinical_trial_code'] == 'NCI-MATCH'

    def validate_arm(self, obj):
        case_id = self.get_psn(obj)
        arm_id = obj[ARM_ID]
        return case_id in self.arm_patients[arm_id]

    def validate_arm_drug(self, obj, patient_info):
        arm_id = obj[ARM_ID]
        patient_id = self.get_psn(obj)
        arm_drug = self.arm_patients[arm_id][patient_id]
        return obj['arm_drug'] == arm_drug

    def validate_diagnosis(self, obj, patient_info):
        return obj['disease'] == patient_info['diseases'][0]['ctepTerm']

    def validate_gender(self, obj, patient_info):
        return obj['gender'] == patient_info['gender']

    def validate_race(self, obj, patient_info):
        return obj['race'] == patient_info['races'][0]

    def validate_ethnicity(self, obj, patient_info):
        return obj['ethnicity'] == patient_info['ethnicity']

    def validate_patient_count(self):
        '''
        Validate patients in manifest exactly match given arm's patient list (patients with valid slot)
        Must call after validate_file_patient()!

        :return: boolean
        '''
        result = True
        all_patients = set()
        for arm_id in self.arms:
            patients = self.arm_patients[self.get_ctdc_arm_id(arm_id)]
            all_patients = all_patients.union(patients)

        for patient in self.cases:
            if patient not in all_patients:
                self.log.error(f'Patient "{patient}" is not a validate patient in given arms: {self.arms}')
                result = False

        for patient in all_patients:
            if patient not in self.cases:
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
        patients = {}
        arm = self._retrieve_arm_info(arm_id)
        for patient in arm.get('summaryReport', {}).get('assignmentRecords', []):
            slot = int(patient.get('slot', -1))
            if slot > 0:
                drugs = arm['treatmentArmDrugs']
                if len(drugs) == 1:
                    arm_drug = drugs[0]['name']
                else:
                    self.log.error(f'Arm {arm_id} has {len(drugs)} drugs!')
                    arm_drug = ''
                patients[patient.get('patientSequenceNumber')] = arm_drug

        return patients

    def log_validation_result(self, validation, result):
        if result:
            self.log.info(f'Validating {validation}: Succeeded!')
        else:
            self.log.error(f'Validating {validation}: Failed!')

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
    parser = argparse.ArgumentParser(description='Validate CTDC Meta data against NCI-MATCH')
    parser.add_argument("--config-file", help="Name of Configuration File to run the validator", required=True)
    args = parser.parse_args()

    config = Config(args.config_file)

    secrets = get_secret(config.region, config.secret_name)
    token = get_okta_token(secrets, config.okta_auth_url)
    if not token:
        raise Exception('Failed to obtain a token!')

    manifest = MetadataValidator(config.api_url)
    manifest.validate(config.match_base_url, token, config.cipher_key, [arm.arm_id for arm in config.arms])




if __name__ == '__main__':
    main()