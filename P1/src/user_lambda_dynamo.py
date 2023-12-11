import json
import logging
import os

import boto3

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

# Configuración de la conexión a DynamoDB
ENDPOINT_URL = f"http://{os.getenv('LOCALSTACK_HOSTNAME')}:{os.getenv('EDGE_PORT')}"
DYNAMODB = boto3.resource(
    "dynamodb",
    aws_access_key_id='test',
    aws_secret_access_key='test',
    region_name='us-east-1',
    endpoint_url=ENDPOINT_URL
)

TABLE_NAME = 'si1p1'


def handler(event, context):
    LOGGER.info('Insertando datos en DynamoDB')
    print("Evento recibido: " + json.dumps(event, indent=2))
    
    # Extrayendo datos del evento
    username = event['pathParameters']['username']
    email = json.loads(event['body'])['email']
    
    table = DYNAMODB.Table(TABLE_NAME)
    response = table.put_item(
        Item={
            'User': username,
            'Email': email
        }
    )
    
    # Construyendo la respuesta
    resp = {
        "isBase64Encoded": False,
        "statusCode": 200,
        "headers": {
            "content-type": "application/json"
        },
        "body": json.dumps({"message": "¡Datos insertados con éxito!"})
    }
    
    return resp
