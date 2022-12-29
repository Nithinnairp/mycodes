import json
import boto3
import pymysql
import datetime
from DB_conn import condb,condb_dict

def lambda_handler(event, context):
    current_time=datetime.datetime.now()
    date_str=current_time.strftime("%Y-%m-%d %H_%M")
    print(event)
    #########################################################################
    # making the inactive cm_station active
    #########################################################################
    query1="""select distinct current_station from cm_wip_raw_data where upper(current_station) 
             in  (select distinct upper(cm_station_name) from cm_wip_stn_mapping where active_flag=0);"""
             
    result1=condb_dict(query1)
    inactive=[]
    for i in result1:
        inactive.append(list(i.values())[0])
    print(inactive)
    
    for cm_station in inactive:
        update_query="""update cm_wip_stn_mapping set active_flag=1,updated_by=1,updated_by_user='Automatic Process',last_updated_date='{}' where cm_station_name='{}';""".format(current_time,cm_station)
        condb(update_query)
        
    ##########################################################################
    # inserting a new cm_station 
    ##########################################################################
    
    query="""select distinct current_station from cm_wip_raw_data where upper(current_station) not
             in (select distinct upper(cm_station_name) from cm_wip_stn_mapping);"""
             
    result=condb_dict(query)
    newly_added=[]
    for i in result:
        newly_added.append(list(i.values())[0])
    print(newly_added)
    
    for new_cm_station in newly_added:
        insert_query="""insert into cm_wip_stn_mapping
                        (cm_station_name,kr_wip_breakdown,active_flag,remark,created_by,creted_by_user,
                        creation_date,updated_by,updated_by_user,last_updated_date) 
                        values('{}',null,1,"Added Automatically","1","Automatic Process",'{}',null,null,null);""".format(new_cm_station,current_time)
        condb(insert_query)
    
    ##########################################################################
    
    if len(newly_added)>0 or len(inactive)>0:
        SENDER="pdl-noreply-cmcprod@keysight.com"
        RECIPIENT = ["nithin.p@aspirenxt.com","pallav.sharma@keysight.com","pdl-b2b-superuser@keysight.com"]
        client = boto3.client('ses',"us-east-1")
        response = client.send_email(Source=SENDER,Destination={'ToAddresses':RECIPIENT},
                        Message={
                            'Subject': {
                                'Data': "New CM_WIP_Station added"
                            },
                            'Body': {
                                'Text': {
                                    'Data': """Hello,\n\r\nPlease find the list of newly added cm_wip_current_station added automatically.
                                    \nNewly added current stations are {}.\nCurrent stations which were deactivated and are made active  {}
                                    \n\nThanks & Regards,\nKeysightIT-Team\n\n\n""".format(newly_added,inactive)
                                }
                            }
                        })
                        
    else:
        SENDER="pdl-noreply-cmcprod@keysight.com"
        RECIPIENT = ["nithin.p@aspirenxt.com","pallav.sharma@keysight.com","pdl-b2b-devops@keysight.com"]
        client = boto3.client('ses',"us-east-1")
        response = client.send_email(Source=SENDER,Destination={'ToAddresses':RECIPIENT},
                        Message={
                            'Subject': {
                                'Data': "NO New CM_WIP_Station added"
                            },
                            'Body': {
                                'Text': {
                                    'Data': """Hello,\n\r\nNo stations were added or activated.
                                    \n\nThanks & Regards,\nKeysightIT-Team\n\n\n"""
                                }
                            }
                        })