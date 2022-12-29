import json
import boto3
import datetime


my_date=datetime.datetime.today()
year,week_num,day_of_week=my_date.isocalendar()
current_year=str(year)
current_year=current_year[2:]


def lambda_handler(event, context):
    print(event,type(event))
    invoked_function=context.function_name
    print(context.function_name) 
    if type(event)!=dict:
        mail_id=event[0]['email']
        def send_email(id):
            client = boto3.client('ses',"us-east-1")
            SENDER='pdl-noreply-cmcprod@keysight.com'
            # RECIPIENT=['nithin.p@aspirenxt.com']
            RECIPIENT=[mail_id]
            response = client.send_email(Source=SENDER,Destination={'ToAddresses':RECIPIENT},
                            Message={
                                'Subject': {
                                    'Data': "Bulk Upload processing is completed Successfully"
                                },
                                'Body': {
                                    'Text': { 
                                        'Data': """Hello,\n\r\nThe Imported file for Commit processing has completed successfully. Please validate the KR summary.
                                        \n\nThanks & Regards,\nKeysightIT-Team\n\n\n"""
                                    }
                                }
                            })
        send_email(id)
    else:
        mail_id=event['email']
        cause=event['InputPath']['Cause']
        print(cause)
        def send_email(mail_id):
            client = boto3.client('ses',"us-east-1")
            SENDER='pdl-noreply-cmcprod@keysight.com'
            # RECIPIENT=['nithin.p@aspirenxt.com']
            RECIPIENT=[mail_id]
            response = client.send_email(Source=SENDER,Destination={'ToAddresses':RECIPIENT},
                            Message={
                                'Subject': {
                                    'Data': "Bulk Upload processing is Failed"
                                },
                                'Body': {
                                    'Text': {
                                        'Data': """Hello,\n\r\nThe Imported file for Commit processing has failed.\n\nerror occurred is:- {}
                                        \n\nThanks & Regards,\nKeysightIT-Team\n\n\n""".format(cause)
                                    }
                                }
                            })
        send_email(id)
    return "mail sent"
    