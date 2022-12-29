import sys
import json
import boto3
import time


def lambda_handler(event, context):
    email=json.dumps(event)
    print(email)
    sfn_client=boto3.client('stepfunctions')
    response=sfn_client.start_execution(
    stateMachineArn='arn:aws:states:ap-southeast-1:603601803076:stateMachine:advance_commit_batch_process_prod',
    input=email)
    
    return 'Execution Started Prod'   
    