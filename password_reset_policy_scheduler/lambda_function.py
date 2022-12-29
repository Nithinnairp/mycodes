import json
import boto3
import pymysql
import logging
import datetime
from DB_conn import condb,condb_dict
import os

DB_NAME = os.environ['DB_NAME']
COGNITO_USER_POOL = os.environ['COGNITO_USER_POOL']
SENDER = os.environ['SENDER']
RECIPIENT = os.environ['RECIPIENT'].split(",")

def lambda_handler(event, context):
    current_time=datetime.datetime.now() # UTC Timezone
    date_str=current_time.strftime("%Y-%m-%d %H:%M:%S")
    error = []
    
    supplier_user_query = "SELECT * FROM {}.supplier_user WHERE password_last_update_date <= DATE_SUB('{}', INTERVAL 6 MONTH) AND status IN ('APPROVED', 'ACTIVATED')".format(DB_NAME, date_str)
    supplier_users = condb_dict(supplier_user_query)
    client = boto3.client('cognito-idp')
    
    for user in supplier_users:
        username = user['email'].strip().replace("@","_").replace(".","_")
        
        try:
            check_user_response = client.admin_get_user(
                                    UserPoolId=COGNITO_USER_POOL,
                                    Username=username)
            
            print(username, 'is in', check_user_response['UserStatus'], 'status')
            if(check_user_response['UserStatus'] == 'CONFIRMED'):
                print("Resetting password for", username)
                response= client.admin_reset_user_password(
                                UserPoolId=COGNITO_USER_POOL,
                                Username=username
                            )
            print("-------------------------------------------------------------")
        except Exception as ex:
            error_map = {'username': username, 'error': ex.__class__.__name__}
            error.append(error_map)
            print("Exception occurred while trying to reset password for", username)
            print(ex)
            print("---------------------------------------")
            
    
    # Sending email notification to IT team to notify on failure of resetting password
    if len(error) > 0:
        ses_client = boto3.client('ses',"us-east-1")
        response = ses_client.send_email(Source=SENDER,Destination={'ToAddresses':RECIPIENT},
                        Message={
                            'Subject': {
                                'Data': "Exception in reset password schedule"
                            },
                            'Body': {
                                'Text': {
                                    'Data': """Hello,\n\r\nFailure happened when trying to reset password for username(s) below: 
                                    \nList of username - {}
                                    \nPlease check log for error details.
                                    \nThanks\n""".format(error)
                                }
                            }
                        })
    
    
    
    
    
    
