import sys
import json
import boto3
import datetime
from datetime import timedelta

def lambda_handler(event, context):
    current_time=datetime.datetime.now()
    s3_client = boto3.client('s3')
    response=s3_client.get_object(Bucket='rapid-response-prod',Key='irp_event_timestamp/irp_processing_timestamp_prod.json')
    content=response['Body'].read().decode('utf-8')
    json_file=json.loads(content)
    print(json_file)
    return json_file