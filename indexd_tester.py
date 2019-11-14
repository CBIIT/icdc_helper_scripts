
import csv,requests,argparse,os
from urllib.parse import urlparse
from datetime import datetime

def validate_file(url, filename, statusFile):
	#Get the File with a Get request and write it to a file
	print('Validating File: '+filename)
	r = requests.get(url)
	with open(filename, 'wb') as file:
		file.write(r.content)
	#Delete the file once it has been processed
	os.remove(filename)
	with open(statusFile ,'a+') as file:
		file.write('\nValidating File: '+filename)	
 
# Specifying argument parsing from the command line
parser = argparse.ArgumentParser(description='Script to test IndexD')
parser.add_argument("--file", required=True, type=str, help="Name of IndexD Manifest File")
args = parser.parse_args()

# This is the base URL for the Staging Environment
BASE_URL = 'https://nci-crdc-staging.datacommons.io/user/data/download/dg.4DFC/'

#File to write the Status
statusFile = 'status.txt'
# datetime object containing current date and time

with open(statusFile ,'a+') as file:
	# dd/mm/YY H:M:S
	now = datetime.now()
	dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
	file.write('\n****Starting File Processing at '+ dt_string+'******')


print ('Starting File Processing...')
status= False
#Reading the input file as a TSV file
with open(args.file) as tsvfile:
	try:
		
		reader = csv.DictReader(tsvfile, dialect='excel-tab')
		for row in reader:
			#Construct the Path to the file by getting the GUID and appending it to base url
			indexd_guid= row['GUID']
			url=BASE_URL+indexd_guid
			#Getting the S3 Path Parse Result
			s3_parser = urlparse(row['url'])
			#Getting the Filename from the parse result
			filename = os.path.basename(s3_parser.path)
			r = requests.get(url = url)
			# extracting data in json format 
			data = r.json()
			#print(data['url'])
			validate_file(data['url'],filename,statusFile)

		status= True
		print ('File Processing Complete!')
		with open(statusFile ,'a+') as file:
			# dd/mm/YY H:M:S
			now = datetime.now()
			dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
			file.write('\n****File Processing Complete at '+ dt_string+'****')


	except Exception as e:
		print ('File Processing Failed. See Status File for details.')
		with open(statusFile ,'a+') as file:
			file.write('\n****GUID: '+indexd_guid+' has an error ****')


	


