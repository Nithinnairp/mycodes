import json
import pymysql
import datetime
import boto3
import os
from DB_conn import condb,condb_dict

current_date = datetime.datetime.now()

def send_email(user):
    SENDER=os.environ['SENDER']
    RECIPIENT = [user['email']]
    client = boto3.client('ses',"us-east-1")
    BODY_HTML = """<html>
                    <body>
                    <p>Hi, Greetings ! </p>
                    <p>Your account has been de-activated</p>
                    <p>Remarks: No activity for more than 120 days</p>
            		<table  style='padding: 10px; border: 1px solid black; border-collapse: collapse;'>
            		<tr>
            		    <th style='border: 1px solid black;'>Name</td>
            		    <th style='border: 1px solid black;'>Email</td>
            		    <th style='border: 1px solid black;'>Contact</td>
            		    <th style='border: 1px solid black;'>Role</td>
            		<tr>
            	        <td style='border: 1px solid black;'>{} {}</td>
            	        <td style='border: 1px solid black;'>{}</td>
            	        <td style='border: 1px solid black;'>{}</td>
            	        <td style='border: 1px solid black;'>{}</td>
            		</tr>
            		</table>
            		</body>
            		</html>""".format(user['first_name'], user['last_name'], user['email'],user['contact'], user['role_name'])
            		
    response = client.send_email(Source=SENDER,Destination={'ToAddresses':RECIPIENT},
                    Message={
                        'Subject': {
                            'Data': "Account status notification [Keysight B2B CollabPortal]"
                        },
                        'Body': {
                            'Html': {
                                'Data': BODY_HTML
                            }
                        }
                    })
                    
def cognito_deactivate_user(user, user_pool_id):
    client = boto3.client("cognito-idp", region_name="ap-southeast-1")
    username = user['email'].strip().replace("@", "_").replace(".", "_")

    response = client.admin_disable_user(
        UserPoolId = user_pool_id,
        Username = username
    )
                    

def lambda_handler(event, context):
    DB_NAME = os.environ['DB_NAME']
    USER_POOL_ID = os.environ['COGNITO_USER_POOL']

    get_supplier_user_query = "SELECT {db_name}.supplier_user.id, {db_name}.supplier_user.first_name, {db_name}.supplier_user.last_name, {db_name}.supplier_user.email, {db_name}.supplier_user.contact, {db_name}.supplier_user.last_login_date, {db_name}.supplier_user.created_date, {db_name}.roles.role_name, {db_name}.roles.role_type FROM {db_name}.supplier_user INNER JOIN {db_name}.roles ON {db_name}.supplier_user.roles_id = {db_name}.roles.id WHERE deactivated_date IS NULL; "
    supplier_users = condb_dict(get_supplier_user_query.format(db_name = DB_NAME))
    get_keysight_user_query = "SELECT {db_name}.keysight_user.id, {db_name}.keysight_user.first_name, {db_name}.keysight_user.last_name, {db_name}.keysight_user.email, {db_name}.keysight_user.contact, {db_name}.keysight_user.last_login_date, {db_name}.keysight_user.created_date, {db_name}.roles.role_name, {db_name}.roles.role_type FROM {db_name}.keysight_user INNER JOIN {db_name}.roles ON {db_name}.keysight_user.roles_id = {db_name}.roles.id WHERE deactivated_date IS NULL;"
    ks_users = condb_dict(get_keysight_user_query.format(db_name = DB_NAME))
    users = supplier_users + ks_users
    for user in users:
        user_id = user['id']
        role_type = user['role_type']
        date = ""
        if user['last_login_date']:
            date = user['last_login_date']
        else:
            date = user['created_date']
        # Check for user that last_login_date >= 120days || created_date >= 120 days
        if date and current_date - date >= datetime.timedelta(days=120):
            # set deactivated_date = today
            if role_type == "Keysight":
                status = "REJECTED"
            elif role_type == "Supplier":
                status = "DE_ACTIVATED"
            update_user_query = "UPDATE {}.{}_user SET deactivated_date = '{}', remarks = \"No activity for more than 120 days\", status = \"{}\" WHERE id = {};"
            condb(update_user_query.format(DB_NAME, role_type.lower(), current_date.strftime('%Y-%m-%d'), status, user_id))
            if role_type == "Supplier":
                cognito_deactivate_user(user, USER_POOL_ID)
            send_email(user)
            print("Deactivation email sent to " ,user['email'])