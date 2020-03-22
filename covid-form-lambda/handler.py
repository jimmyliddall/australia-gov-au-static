import boto3
from botocore.exceptions import ClientError
import csv
import io
import json
import logging
import os
import pendulum
import urllib.parse
import uuid


AWS_REGION = os.environ.get('AWS_REGION', None)
AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID', None)
AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY', None)
AWS_S3_URL = os.environ.get('AWS_S3_URL', None)
BUCKET_NAME = os.environ.get('BUCKET_NAME', None)
RESPONSES_PATH = BUCKET_NAME + '/responses/'
PROCESSED_PATH = BUCKET_NAME + '/processed/'
CSV_PATH = BUCKET_NAME + '/csv/cases.csv'
API_KEYS = os.environ.get('API_KEYS', None)
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
            response = s3_client.upload_fileobj(f, BUCKET_NAME, RESPONSES_PATH + file_name)
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
    with io.BytesIO() as csvfile:
        try:
            s3_client.download_fileobj(BUCKET_NAME, CSV_PATH, csvfile)
            binary_data = csvfile.getvalue()
            current_csv_content = binary_data.decode('UTF-8')
        except ClientError as e:
            if e.response['Error']['Code'] != "404":
                logging.error(e)

    response = {
        "statusCode": 200,
        "body": current_csv_content,
        "contentType": "application/csv; charset=utf-8"
    }
    return response


# generate csv file
def generate_csv(event, context):
    s3_client = get_s3_client()
    downloaded_data = []
    processed_list = []
    csv_columns = ['fname', 'lname', 'dob']
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(Bucket=BUCKET_NAME, Prefix=RESPONSES_PATH)
        for page in page_iterator:
            for content in page['Contents']:
                key = content['Key']
                file_name = key.replace(RESPONSES_PATH, '')
                file_found = True

                with io.BytesIO() as exiting_file:
                    try:
                        s3_client.download_fileobj(BUCKET_NAME, PROCESSED_PATH + file_name, exiting_file)
                    except ClientError as e:
                        if e.response['Error']['Code'] == "404":
                            file_found = False
                        else:
                            logging.error(e)
                
                if not file_found:
                    processed_list.append(file_name)
                    with io.BytesIO() as f:
                        s3_client.download_fileobj(BUCKET_NAME, key, f)
                        binary_data = f.getvalue()
                        text = binary_data.decode('UTF-8')
                        downloaded_data.append(json.loads(text))
        
        if processed_list:
            with io.BytesIO() as csvfile:
                try:
                    s3_client.download_fileobj(BUCKET_NAME, CSV_PATH, csvfile)
                    binary_data = csvfile.getvalue()
                    current_csv_content = binary_data.decode('UTF-8')
                except ClientError as e:
                    if e.response['Error']['Code'] != "404":
                        logging.error(e)

                with io.StringIO() as string_io:
                    writer = csv.DictWriter(string_io, fieldnames=csv_columns)
                    for _ in downloaded_data:
                        writer.writerow(_)
                    current_csv_content += string_io.getvalue()

                with io.BytesIO(current_csv_content.encode('utf-8')) as upload:
                    response = s3_client.upload_fileobj(upload, BUCKET_NAME, CSV_PATH)

            for file_name in processed_list:
                s3_client.copy({
                    'Bucket': BUCKET_NAME,
                    'Key': RESPONSES_PATH + file_name
                }, BUCKET_NAME, PROCESSED_PATH + file_name)
    except ClientError as e:
        logging.error(e)

