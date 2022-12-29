import sys
import boto3
import pandas as pd
import datetime
import logging

def lambda_handler(event, context):
    print(str(event))
    current_time=datetime.datetime.now()
    current_date=datetime.datetime.strftime(current_time,"%Y-%m-%d")
    
    key=event['Records'][0]['s3']['object']['key']
    folder_name=key[:14]
    s3=boto3.client("s3")
    bucket_name=event['Records'][0]['s3']['bucket']['name']
    key=event['Records'][0]['s3']['object']['key'].split('/')
    folder_name=key[0]
    uploaded_file_name=key[-1]
    
    
    print(folder_name,"foldername",key,"key",bucket_name,"bucket_name")
    
    
    if folder_name=='iodm-part-list':
        try:
            s3=boto3.client('s3')
            objects = s3.list_objects(
            Bucket='rapid-response-prod',
            Prefix='iodm-part-list/iodm_part_list_csv_files/')
            print("objects",objects)
            files=objects['Contents']
            print("files",files)
            
            for i in files:
                filename=i['Key']
                print("filename",filename) 
                if filename=="iodm-part-list/iodm_part_list_csv_files/":
                    continue
                else:
                    response = s3.delete_object(
                    Bucket='rapid-response-prod',
                    Key=filename)
            
            print("uploaded_file_name",uploaded_file_name)
            o_key=uploaded_file_name.replace('+',' ').replace('%28','(').replace('%29',')')
            print("o_key",o_key)
            glueJobName='iodm_part_list'
            df=pd.read_excel('s3://'+ bucket_name +'/iodm-part-list/' +o_key)
            df.to_csv("s3://rapid-response-prod/iodm-part-list/iodm_part_list_csv_files/"+o_key+".csv",encoding='utf-8',index=False,header=True)
            client=boto3.client("glue",region_name='ap-southeast-1')
            response = client.start_job_run(JobName = glueJobName)
            print('GlueJOb '+ glueJobName + ' has started')
        except Exception as e:
            print(e)
    
    elif folder_name=='backup':
        prefix=key[-2]
        print(uploaded_file_name,prefix)
        df=pd.read_csv('s3://'+ bucket_name +'/backup/raw_files/'+prefix+"/"+uploaded_file_name)
        df.to_csv("s3://rapid-response-prod/backup/"+prefix+"/"+uploaded_file_name+".csv",encoding='utf-8',index=False,header=True)
    
    elif folder_name=='keysight_calendar':
        print('calendar_files',event)
        prefix=key[0]
        file=key[1]
        print("&&&&&&&&&&&&&&&&&&&&",prefix,file)
        o_key=uploaded_file_name.replace('+',' ').replace('%28','(').replace('%29',')')
        df=pd.read_excel('s3://'+ bucket_name +'/keysight_calendar/' +o_key)
        df.to_csv("s3://"+bucket_name+'/keysight_calendar/keysight_calendar_csv_files/converted_csv_file.csv',date_format='%Y-%m-%d',index=False)
        glueJobName='keysight_calendar_load'
        client=boto3.client("glue",region_name='ap-southeast-1')
        reponse = client.start_job_run(JobName=glueJobName)
        print('GlueJOb '+ glueJobName + ' has started')
        
    elif folder_name=='FG_Qty_interface':
        print('FG_Qty_interface',event)
        prefix=key[0]
        file=key[1]
        # print("&&&&&&&&&&&&&&&&&&&&",prefix,file)
        o_key=uploaded_file_name.replace('+',' ').replace('%28','(').replace('%29',')')
        print(o_key)
        df=pd.read_excel('s3://'+ bucket_name +'/FG_Qty_interface/' +o_key)
        df.to_csv("s3://"+bucket_name+'/FG_Qty_interface/FG_Qty_interface_csv/converted_csv_file.csv',date_format='%Y-%m-%d',index=False)
        glueJobName='CM_wip_FG_qty'
        client=boto3.client("glue",region_name='ap-southeast-1')
        reponse = client.start_job_run(JobName=glueJobName)
        print('GlueJOb '+ glueJobName + ' has started')
    
    elif folder_name=="daily_iodm_part_list":
        glueJobName='iodm_part_list_incremental_load'
        print("daily_iodm_part_list is received")
        try:
            s3=boto3.client('s3')
            objects = s3.list_objects(
            Bucket='rapid-response-prod',
            Prefix='daily_iodm_part_list/iodm_part_list_csv_files/')
            print(objects)
            files=objects['Contents']
            
            for i in files:
                filename=i['Key']
                print("filename",filename) 
                if filename=="daily_iodm_part_list/iodm_part_list_csv_files/":
                    continue
                else:
                    response = s3.delete_object(
                    Bucket='rapid-response-prod',
                    Key=filename)

            o_key=uploaded_file_name.replace('+',' ').replace('%28','(').replace('%29',')')
            print(o_key)
            df=pd.read_excel('s3://'+ bucket_name +'/daily_iodm_part_list/' + o_key)
            s3=boto3.client("s3")
            df.to_csv("s3://rapid-response-prod/daily_iodm_part_list/iodm_part_list_csv_files/"+o_key+".csv",encoding='utf-8',index=False,header=True)
            client=boto3.client("glue",region_name='ap-southeast-1')
            
            reponse = client.start_job_run(JobName=glueJobName)
            print('GlueJOb '+ glueJobName + ' has started')
        except Exception as e:
            print(e)
    
    