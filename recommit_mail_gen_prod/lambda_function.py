
import os
import boto3
from botocore.exceptions import ClientError
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import datetime
from DB_conn import condb

DB_NAME = os.environ['DB_NAME']
SENDER = os.environ['SENDER']

def lambda_handler(event,context):
    current_time=datetime.datetime.now()
    sng_time=(current_time+datetime.timedelta(hours=8)).strftime("%Y-%m-%d %H_%M")
    print(str(event))
    bucket_name=event['Records'][0]['s3']['bucket']['name']
    file=event['Records'][0]['s3']['object']['key'][17:].replace('+',' ').replace('%28','(').replace('%29',')').replace('%3A',':')
    # get supplier short name from file name
    supplier_short_name = file.split("_")[2].split("(")[0]
    print(bucket_name)
    print(file)
    
    def get_recipients(col, supplier_short_name):
        # Get email recipient list by supplier short name
        query="""select {column} from {db_name}.active_suppliers where supplier_name LIKE ('{short_name}%');""".format(column=col, db_name=DB_NAME, short_name=supplier_short_name)
        recipient_result=condb(query)
        recipients = recipient_result[0][0]
        # convert to list type
        if recipients is not None:
            list_of_recipients=recipients.split(',')
        else:
            list_of_recipients = []
        return list_of_recipients
    
    # Get email recipients and remove duplicates using set and convert to list back
    RECIPIENT = list(set(get_recipients('Validations_Escalations', supplier_short_name)) | set(get_recipients('Archival_Validation_common', supplier_short_name)))
    
    BODY_TEXT = """Hello,\n\r\nPlease find the attached file for the Recommit notification Summary pending for your actions.  Please fill up the attached template\nwith your comments and reply all before Friday 5 PM SEA.\n\nThanks & Regards,\nKeysightIT-Team\n\n\n"""
    CHARSET = "utf-8"
    textpart = MIMEText(BODY_TEXT.encode(CHARSET), 'plain', CHARSET)
    # Specify a configuration set. If you do not want to use a configuration
    # set, comment the following variable, and the 
    # ConfigurationSetName=CONFIGURATION_SET argument below.
    # CONFIGURATION_SET = "ConfigSet"
    
    # If necessary, replace us-west-2 with the AWS Region you're using for Amazon SES.
    AWS_REGION = "us-east-1"
    
    # The subject line for the email.
    s3_object = boto3.client('s3', "ap-southeast-1")
    s3_object = s3_object.get_object(Bucket=bucket_name, Key='Recommit_Summary/'+file)
    body = s3_object['Body'].read()
    # print(body) 
    file_name=file
    print(file_name)
    msg = MIMEMultipart('mixed')
    msg['Subject']="Recommit Notification Summary for {} at {}".format(supplier_short_name,sng_time)
    msg['From']=SENDER
    msg['To']=', '.join(RECIPIENT)
    # msg['Cc']=', '.join(COPY)
    msg.attach(textpart)
    part = MIMEApplication(body)
    part.add_header("Content-Disposition", 'attachment', filename=file_name)
    msg.attach(part)
    ses_aws_client = boto3.client('ses', "us-east-1")
    ses_aws_client.send_raw_email(Source=SENDER,Destinations=RECIPIENT,RawMessage={"Data":msg.as_bytes()})
