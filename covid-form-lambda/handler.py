import boto3
from botocore.exceptions import ClientError
import io
import json
import logging
import os
import pendulum
import urllib.parse
import uuid


AWS_REGION=os.environ.get('AWS_REGION', None)
AWS_ACCESS_KEY_ID=os.environ.get('AWS_ACCESS_KEY_ID', None)
AWS_SECRET_ACCESS_KEY=os.environ.get('AWS_SECRET_ACCESS_KEY', None)
AWS_S3_URL=os.environ.get('AWS_S3_URL', None)
RESPONSES_BUCKET_NAME=os.environ.get('BUCKET_NAME', None)
RESPONSES_PATH = RESPONSES_BUCKET_NAME + '/responses/'
CSV_BUCKET_NAME=os.environ.get('BUCKET_NAME', None) + '/csv'
API_KEYS=os.environ.get('API_KEYS', None)
CSV_NAME='cases.csv'
ACCEPTED_FIELDS = ['fname', 'lname', 'dob']

def get_s3_client():
    s3_client = boto3.client(
        's3',
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        endpoint_url=AWS_S3_URL
    )
    return s3_client


# accepts form submission
def accept_form_response(event, context):
    body = {
        "message": "Go Serverless v1.0! Your function executed successfully!",
        "input": event
    }
    
    data = urllib.parse.parse_qs(event['body'])
    
    for_saving = {
        _: data[_][0] if _ in data else '' for _ in ACCEPTED_FIELDS
    }
    s3_client = get_s3_client()
    try:
        now = pendulum.now().format('YYYYMMDDHHmmss')
        file_name = now + '-' + str(uuid.uuid1()) + '.json'
        with io.BytesIO(json.dumps(for_saving).encode('utf-8')) as f:
            response = s3_client.upload_fileobj(f, RESPONSES_BUCKET_NAME, RESPONSES_PATH + file_name)
    except ClientError as e:
        logging.error(e)

    response = {
        "statusCode": 200,
        # "body": json.dumps(body)
    }

    return response


# download pre generated csv file
def get_csv(event, context):
    body = {
        "message": "Go Serverless v1.0! Your function executed successfully!",
        "input": event
    }
    # TODO add api key checks
    s3_client = get_s3_client()
    try:
        with open(CSV_NAME, 'rb') as f:
            s3.download_fileobj(CSV_BUCKET_NAME, CSV_NAME, f)
    except ClientError as e:
        logging.error(e)

    response = {
        "statusCode": 200,
        "body": json.dumps(body)
    }

    return response


# generate csv file
# def generate_csv(event, context):
#     s3_client = get_s3_client()
#     try:
#         paginator = s3_client.get_paginator('list_objects')
#         page_iterator = paginator.paginate(Bucket=RESPONSES_BUCKET_NAME)
#         for page in page_iterator:
#             for content in page['Contents']:
#                 file_name = content['Key']
#                 logging.info(file_name)
#                 with open(file_name, 'wb') as f:
#                     s3.download_fileobj(RESPONSES_BUCKET_NAME, file_name, f)
#                     binary_data = f.read()
#                     text = binary_data.decode('utf-8')
        

#         with open(CSV_NAME, "rb") as f:
#             response = s3_client.upload_fileobj(CSV_BUCKET_NAME, CSV_NAME)
#     except ClientError as e:
#         logging.error(e)
