import boto3
import json
import logging
import os

LOGGER = logging.getLogger()

LOGGER.setLevel(logging.INFO)
# Configuración de la conexión a DynamoDB
# Usando la configuración de localstack proporcionada anteriormente, pero esto se puede modificar para AWS real
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
    LOGGER.info('Obteniendo datos de DynamoDB')
    print("Evento recibido: " + json.dumps(event, indent=2))
    
    # Extrayendo datos del evento
    username = event['pathParameters']['username']
    
    table = DYNAMODB.Table(TABLE_NAME)
    response = table.get_item(
        Key={
            'User': username
        }
    )
    
    # Si se encuentra el ítem, devuélvelo. De lo contrario, devuelve un objeto vacío
    resp_body = response['Item'] if 'Item' in response else {}
    
    # Construyendo la respuesta
    resp = {
        "isBase64Encoded": False,
        "statusCode": 200,
        "headers": {
            "content-type": "application/json"
        },
        "body": json.dumps(resp_body)
    }
    
    return resp
