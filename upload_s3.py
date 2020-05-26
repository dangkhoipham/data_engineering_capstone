import boto3, os
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import Variable

aws_hook = AwsHook("aws_credentials")
credentials = aws_hook.get_credentials()
aws_access_key = credentials.access_key
aws_secret_key = credentials.secret_key
# Initialize a client

bucket = 'capstone-dend-dangkhoipham'

s3 = boto3.resource(
    's3',
    region_name='us-west-2',
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key,
) 

file_names = ['us-cities-demographics.csv', 'immigration_data_sample.csv','airport-codes_csv.csv',"I94_SAS_Labels_Descriptions.SAS" ]

for file in file_names:
    s3.meta.client.upload_file(Filename = file, Bucket = bucket, Key = "raw/"+file)
    
# upload folder
local_directory= "/home/workspace/airflow/sas_data/"
for root, dirs, files in os.walk(local_directory):

  for filename in files:

    # construct the full local path
    local_path = os.path.join(root, filename)

    # construct the full Dropbox path
    relative_path = os.path.relpath(local_path, local_directory)
    s3_path = os.path.join("raw/sas_data",relative_path)
    # relative_path = os.path.relpath(os.path.join(root, filename))

    s3.meta.client.upload_file(local_path, bucket, s3_path)