import boto3
from botocore.exceptions import ClientError
import json
import logging
import pendulum
import uuid


def get_s3_client():
    s3_client = boto3.client(
        's3',
        region_name=getenv('AWS_REGION'),
        aws_access_key_id=getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=getenv('AWS_SECRET_ACCESS_KEY'),
        endpoint_url=getenv('AWS_S3_URL')
    )
    return s3_client

def get_responses_bucket_name():
    bucket = ''

def get_csv_bucket_name():
    bucket = ''

def get_csv_name():
    return 'cases.csv'


# accepts form submission
def accept_form_response(event, context):
    body = {
        "message": "Go Serverless v1.0! Your function executed successfully!",
        "input": event
    }
    
    s3_client = get_s3_client()
    bucket_name = get_responses_bucket_name()
    try:
        now = pendulum.now().format('YYYYMMDDHHmmss')
        file_name = now + str(uuid.uuid1()) + '.json'
        with open(file_name, "rb") as f:
            response = s3_client.upload_fileobj(bucket_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True


    response = {
        "statusCode": 200,
        "body": json.dumps(body)
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
    csv_bucket_name = get_csv_bucket_name()
    try:
        with open(get_csv_name(), 'rb') as f:
            s3.download_fileobj(csv_bucket_name, get_csv_name(), f)
    except ClientError as e:
        logging.error(e)

    response = {
        "statusCode": 200,
        "body": json.dumps(body)
    }

    return response


# generate csv file
def generate_csv(event, context):
    s3_client = get_s3_client()
    responses_bucket_name = get_responses_bucket_name()
    csv_bucket_name = get_csv_bucket_name()
    try:
        paginator = s3_client.get_paginator('list_objects')
        page_iterator = paginator.paginate(Bucket=responses_bucket_name)
        for page in page_iterator:
            for content in page['Contents']:
                file_name = content['Key']
                logging.info(file_name)
                with open(file_name, 'wb') as f:
                    s3.download_fileobj(responses_bucket_name, file_name, f)
                    binary_data = f.read()
                    text = binary_data.decode('utf-8')
        

        with open(get_csv_name(), "rb") as f:
            response = s3_client.upload_fileobj(csv_bucket_name, get_csv_name())
    except ClientError as e:
        logging.error(e)
