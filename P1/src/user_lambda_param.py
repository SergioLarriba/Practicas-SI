#!/bin/env python3

import json
import logging
import os

import boto3

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

ENDPOINT_URL = f"http://{os.getenv('LOCALSTACK_HOSTNAME')}:{os.getenv('EDGE_PORT')}"

BUCKET_NAME = 'si1p1'


def get_s3_client():
    return boto3.client(
        's3',
        aws_access_key_id='',
        aws_secret_access_key='',
        region_name='eu-west-1',
        endpoint_url=ENDPOINT_URL
    )


S3_CLIENT = get_s3_client()

# Nos aseguramos que el bucket existe
S3_CLIENT.create_bucket(Bucket=BUCKET_NAME)


def handler(event, context):
    LOGGER.info('Escribiendo en S3')
    print("Received event: " + json.dumps(event, indent=2))

    # Obtenemos el param username
    username = event['pathParameters']['username']
    # Obtenemos datos del cuerpo del evento
    data = json.loads(event.get('body', '{}'))

    clave_objeto = f"user/{username}/data.json"

    S3_CLIENT.put_object(
        Bucket=BUCKET_NAME,
        Key=clave_objeto,
        Body=json.dumps(data)
    )
    resp_body = f"{clave_objeto} placed into S3"
    # API GW compliant response
    resp = {
        "isBase64Encoded": False,
        "statusCode": 200,
        "headers": {
            "content-type": "application/json"
        },
        "body": json.dumps(resp_body)
    }
    return resp
