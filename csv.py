import boto3
import csv
import os
from io import StringIO

s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

def lambda_handler(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    file_name = event['Records'][0]['s3']['object']['key']
    obj = s3_client.get_object(Bucket=bucket, Key=file_name)
    data = obj['Body'].read().decode('utf-8')
    table = dynamodb.Table(os.environ['DYNAMODB_TABLE'])

    for row in csv.DictReader(StringIO(data)):
        id = int(row['id'])
        name = row['name']
        age = int(row['age'])

        response = table.put_item(
            Item={
                'id': id,
                'name': name,
                'age': age
            }
        )

    return 'Data processed successfully!'
