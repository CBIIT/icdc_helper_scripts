import csv,argparse,os,boto3, pandas as pd
from datetime import datetime
from pathlib import Path
import hashlib



# This is a function that takes an S3 path and returns the Bucket, Key and Filename
def split_s3_path(s3_path):
    path_parts=s3_path.replace("s3://","").split("/")
    bucket=path_parts.pop(0)
    key="/".join(path_parts)
    # Get the filename from the Key
    file_name_temp = key.split("/")
    file_name = file_name_temp[len(file_name_temp)-1]
    return bucket, key, file_name

def get_time():
    return datetime.now().strftime("%d-%m-%Y-%H-%M-%S")

# This is a function that takes a bucket and key and downloads the file to location specified by filename
def download_s3_file(bucket,key,filename):
    '''
    Download a File Given Bucket, Key and Filename
    '''
    
    print(f'Downloading file: s3://{bucket}/{key} at time {get_time()}')
    s3 = boto3.client('s3')
    s3.download_file(bucket, key, filename)
    print(f'Download Complete file: s3://{bucket}/{key} at time {get_time()}')

# This function returns the md5 on a file
def calculate_md5(path, block_size=4096):
    '''
    Block size directly depends on the block size of your filesystem
    to avoid performances issues
    Here I have blocks of 4096 octets (Default NTFS)
    '''
   
    md5 = hashlib.md5()
    with open(path,'rb') as f: 
        for chunk in iter(lambda: f.read(block_size), b''): 
             md5.update(chunk)
    
    
    return md5.hexdigest()    

# Specifying argument parsing from the command line
parser = argparse.ArgumentParser(description='Script to test file loading')
parser.add_argument("--file", required=True, type=str, help="Name of  Manifest File")
#Start Index of File Processing, defaults to start of file
parser.add_argument("--start_row", nargs='?',type=int, help="Starting (0-indexed) Index of file processing",const=0, default=0)
parser.add_argument("--num_rows", type=int, help="Number of rows to process")
parser.add_argument("--md5_blocksize", type=int, help="Size for calculating Md5 in Bytes")

args = parser.parse_args()
# datetime object containing current date and time
now = datetime.now()
dt_string = now.strftime("%d%m/%Y %H:%M:%S")
message='\n**** Starting File Processing at '+ get_time()+' ******'
print (message)
statusFile = f'status_{get_time()}.txt'

try:
    df = pd.read_csv(args.file,sep='\t',header = 0)
    
    start_row = args.start_row
    # If negative number is provided set to first row
    if(start_row < 0):
        start_row = 0
    end_row = start_row+args.num_rows
    
    # If length goes past last row cap it at last row
    if end_row  > df.shape[0]:
        end_row=df.shape[0]

    total_rows = end_row-start_row    
    #print(f'end_row:{end_row} ')
    # Retrieving the Files of interest
    df_interest = df[start_row:end_row]
    # Initialzing the lists for processing
    list_of_files=[]
    list_md5_expected=[]
    list_md5_calculated=[]
    list_filesize_expected=[]
    list_filesize_calc=[]
    list_pass_fail=[]

    if args.md5_blocksize is not None:
        BLK_SIZE = args.md5_blocksize
    else:
        # Setting Block Size to 1MB
        BLK_SIZE = 1000000
    print(f'Block Size {BLK_SIZE}')    
    fileCount=0
    for index, row in df_interest.iterrows(): 

        fileCount+=1
        #Get the Location, Md5 Sum and Filesize
        file_location= row['file_location']

        # Fake File Location
        #file_location= 's3://amit-gallery/MSN14613/IonXpress_057_rawlib.bam'
        md5sum = row['md5sum']
        file_size_bytes = row['file_size']

        # Get the Bucket, Key and File Name
        bucket,key,file_name = split_s3_path(file_location)
        #if(index==1):
        download_s3_file(bucket,key,file_name)

        
        # Get absolute filepath
        filepath = Path(file_name).resolve()
        # Get file size of file On Disk
        file_size_disk =  os.stat(file_name).st_size 
        md5=calculate_md5(filepath,BLK_SIZE)
        
        # Appending to the list to add to dataframe later

        list_of_files.append(file_location)
        list_md5_expected.append(md5sum)
        list_md5_calculated.append(md5)
        list_filesize_expected.append(file_size_bytes)
        list_filesize_calc.append(file_size_disk)
        


        #print(f' Processed File with MD5: {md5sum}, Filename:{file_name} Calculated MD5: {md5} FileSize on Disk {file_size_disk}')
        if((file_size_disk==file_size_bytes) and (md5==md5sum)):
            message = f'File {file_name} Passed'
            list_pass_fail.append('Pass')
        else:
            message = f'Processing Failed on File {file_name} with Expected MD5: {md5sum},  Calculated MD5: {md5} Expected FileSize {file_size_bytes} FileSize on Disk {file_size_disk} '
            list_pass_fail.append('Fail')
        print(message)
        os.remove(file_name)
        # Writing Debug Message into Status File
        with open(statusFile ,'a+') as file:
            content=f'\nProcessed File {fileCount} of {total_rows}.'
            content+=message
            file.write(content)
except Exception as e:
    print(e)       
    
#print(f'Total Files Processed:  {len(list_of_files)}')
if(len(list_of_files) > 0):
    # Creating a Tuple from the list
    result_tuple = list(zip(list_of_files,list_pass_fail,list_md5_expected,list_md5_calculated,list_filesize_expected,list_filesize_calc))
    # Converting lists of tuples into  pandas Dataframe.  
    result_df = pd.DataFrame(result_tuple, columns = ['Files','Status', 'Md5 Expected', 'Md5 Calculated','File Size Expected','File Size Calculated'])  
    print(result_df.head())
    result_tsv = f'result_{get_time()}.tsv'
    # Write results to TSV file
    with open(result_tsv,'w') as write_tsv:
        write_tsv.write(result_df.to_csv(sep='\t', index=False))

message='\n**** Ended File Processing at '+ get_time()+' ******'
print (message)