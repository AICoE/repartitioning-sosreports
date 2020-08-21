import boto3
from config import *

session = boto3.session.Session()

s3_client = boto3.client(
    service_name='s3',
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    endpoint_url=endpoint_url,
    region_name=DEFAULT_REGION
)

response = s3_client.get_object(
    Bucket='DH-STAGE-INSIGHTS'
)

print('Existing buckets:')
for bucket in response['Buckets']:
    print(f'  {bucket["Name"]}')
