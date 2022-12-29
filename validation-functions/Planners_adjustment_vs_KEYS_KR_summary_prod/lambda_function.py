import sys
import json
import boto3
import pymysql
import pandas as pd
import logging
import datetime
from DB_conn import condb,condb_dict
import os

my_date=datetime.datetime.today()
year,week_num,day_of_week=my_date.isocalendar()
print(week_num,my_date)
complete_current_year=str(year)
current_year=complete_current_year[2:]
DB_NAME = os.environ['DB_NAME']
SENDER = os.environ['SENDER']
S3_BUCKET = os.environ['S3_BUCKET']

def lambda_handler(event, context):
    current_time=datetime.datetime.now()
    date_str=current_time.strftime("%Y-%m-%d %H:%M")
    sng_time=(current_time+datetime.timedelta(hours=8)).strftime("%Y-%m-%d %H:%M")
    try:
        test=event['test']
    except:
        test=0
        pass
        
    try:
        # Get supplier id from event data
        supplier_id=event['supplier_id']
    except:
        # If no supplier id in event data, assign to 0 as an indicator that no supplier id being passed in. We will get list of suppliers to loop through instead.
        supplier_id=0
        pass

    def get_all_suppliers_prefix():
        # Get all suppliers' id and name from forecast plan to process all available suppliers
        query = """SELECT source_supplier_id, source_supplier FROM {}.forecast_plan group by source_supplier;""".format(DB_NAME)
        suppliers_list=list(condb(query))
        # id = supplier_list[0][0]
        # supplier_name = supplier_list[0][1]
        return suppliers_list
        
    def get_supplier_prefix(supplier_id):
        query = """SELECT distinct(source_supplier) FROM {}.forecast_plan WHERE source_supplier_id={};""".format(DB_NAME,supplier_id)
        supplier_query_res=list(condb(query))
        supplier=supplier_query_res[0][0]
        return supplier
        
    def get_recipients(col, supplier_id):
        query="""select {} from {}.active_suppliers where id = {};""".format(col, DB_NAME, supplier_id)
        result=condb(query)
        recipients = result[0][0]
        # convert to list type
        if recipients is not None:
            list_of_recipients=recipients.split(',')
        else:
            list_of_recipients = []
        return list_of_recipients

    def validate(source_supplier_id, supplier_name):
        Validation_details="""Database validation for first 8 wks:Validate KR Summary Report requirement & Commit Value = curr req qty & commit qty (inc YES/NO adv commit)"""
        sql="""select id,ori_part_number,prod_week,prod_year,process_week,process_year,nongsa_supply,cumulative_nongsa_advance_commit_irp,nongsa_advance_commit_kr,
        gsa_supply,cumulative_gsa_advance_commit_irp,gsa_advance_commit_kr,forecast_supply,cumulative_forecast_advance_commit_irp,forecast_advance_commit_kr,
        system_supply,cumulative_system_advance_commit_irp,system_advance_commit_kr
        from {}.forecast_plan where dmp_orndmp='DMP' and source_supplier_id={} and prod_week<={} and 
        (ifnull(nongsa_supply,0)!=0 or ifnull(cumulative_nongsa_advance_commit_irp,0)!=0 or ifnull(nongsa_advance_commit_kr,0)!=0 or
        ifnull(gsa_supply,0)!=0 or ifnull(cumulative_gsa_advance_commit_irp,0)!=0 or ifnull(gsa_advance_commit_kr,0)!=0 or 
        ifnull(forecast_supply,0)!=0 or ifnull(cumulative_forecast_advance_commit_irp,0)!=0 or ifnull(forecast_advance_commit_kr,0)!=0 or 
        ifnull(system_supply,0)!=0 or ifnull(cumulative_system_advance_commit_irp,0)!=0 or ifnull(system_advance_commit_kr,0)!=0);""".format(DB_NAME, source_supplier_id, week_num+8)
        DB_records=condb_dict(sql)
        
        # Get email recipients and remove duplicates using set and convert to list back
        RECIPIENT = list(set(get_recipients('Validations_Commit_Phase', source_supplier_id)) | set(get_recipients('Archival_Validation_common', source_supplier_id)))
        
        Demand_category=['Non GSA','GSA','Forecast','System']
        error_records=[]
        for i in DB_records:
            record_id=i['id']
            print(record_id)
            prod_week=i['prod_week']
            prod_year=i['prod_year']
            ori_part_number=i['ori_part_number']
            nongsa_supply=i['nongsa_supply'] if i['nongsa_supply']!=None else 0
            cumulative_nongsa_advance_commit_irp=i['cumulative_nongsa_advance_commit_irp'] if i['cumulative_nongsa_advance_commit_irp']!=None else 0
            nongsa_advance_commit_kr=i['nongsa_advance_commit_kr'] if i['nongsa_advance_commit_kr']!=None else 0
            gsa_supply=i['gsa_supply'] if i['gsa_supply']!=None else 0
            cumulative_gsa_advance_commit_irp=i['cumulative_gsa_advance_commit_irp'] if i['cumulative_gsa_advance_commit_irp']!=None else 0
            gsa_advance_commit_kr=i['gsa_advance_commit_kr'] if i['gsa_advance_commit_kr']!=None else 0
            forecast_supply=i['forecast_supply'] if i['forecast_supply']!=None else 0
            cumulative_forecast_advance_commit_irp=i['cumulative_forecast_advance_commit_irp'] if i['cumulative_forecast_advance_commit_irp']!=None else 0
            forecast_advance_commit_kr=i['forecast_advance_commit_kr'] if i['forecast_advance_commit_kr']!=None else 0
            system_supply=i['system_supply'] if i['system_supply']!=None else 0
            cumulative_system_advance_commit_irp=i['cumulative_system_advance_commit_irp'] if i['cumulative_system_advance_commit_irp']!=None else 0
            system_advance_commit_kr=i['system_advance_commit_kr'] if i['system_advance_commit_kr']!=None else 0
            
            
            nongsa_category='Non GSA'
            nongsa_adv_commit_query="""select sum(if(commit1_week={0},ifnull(commit1,0),0)+if(commit2_week={0},ifnull(commit2,0),0)
                            +if(commit3_week={0},ifnull(commit3,0),0)+
                            if(commit4_week={0},ifnull(commit4,0),0)) as adv_commit from {3}.advance_commit 
                            where demand_category='{1}' and ori_part_number=%s and process_week={2};""".format(prod_week,nongsa_category,week_num,DB_NAME)
            nongsa_query_result=condb_dict(nongsa_adv_commit_query,[ori_part_number])[0]
            nongsa_adv_commit_value=nongsa_query_result['adv_commit'] if nongsa_query_result['adv_commit']!=None else 0
            nongsa_curr_commit=nongsa_supply+nongsa_adv_commit_value+cumulative_nongsa_advance_commit_irp
            nongsa_kr_commit=nongsa_supply+nongsa_advance_commit_kr
            
            gsa_category='GSA'
            gsa_adv_commit_query="""select sum(if(commit1_week={0},ifnull(commit1,0),0)+if(commit2_week={0},ifnull(commit2,0),0)
                            +if(commit3_week={0},ifnull(commit3,0),0)+
                            if(commit4_week={0},ifnull(commit4,0),0)) as adv_commit from {3}.advance_commit 
                            where demand_category='{1}' and ori_part_number=%s and process_week={2};""".format(prod_week,gsa_category,week_num,DB_NAME)
            gsa_query_result=condb_dict(gsa_adv_commit_query,[ori_part_number])[0]
            gsa_adv_commit_value=gsa_query_result['adv_commit'] if gsa_query_result['adv_commit']!=None else 0
            gsa_curr_commit=gsa_supply+gsa_adv_commit_value+cumulative_gsa_advance_commit_irp
            gsa_kr_commit=gsa_supply+gsa_advance_commit_kr
            
            forecast_category='Forecast'
            forecast_adv_commit_query="""select sum(if(commit1_week={0},ifnull(commit1,0),0)+if(commit2_week={0},ifnull(commit2,0),0)
                                    +if(commit3_week={0},ifnull(commit3,0),0)+
                                    if(commit4_week={0},ifnull(commit4,0),0)) as adv_commit from {3}.advance_commit 
                                    where demand_category='{1}' and ori_part_number=%s and process_week={2};""".format(prod_week,forecast_category,week_num,DB_NAME)
            forecast_query_result=condb_dict(forecast_adv_commit_query,[ori_part_number])[0]
            forecast_adv_commit_value=forecast_query_result['adv_commit'] if forecast_query_result['adv_commit']!=None else 0
            forecast_curr_commit=forecast_supply+forecast_adv_commit_value+cumulative_forecast_advance_commit_irp
            forecast_kr_commit=forecast_supply+forecast_advance_commit_kr
            
            system_category='System'
            system_adv_commit_query="""select sum(if(commit1_week={0},ifnull(commit1,0),0)+if(commit2_week={0},ifnull(commit2,0),0)
            +if(commit3_week={0},ifnull(commit3,0),0)+
            if(commit4_week={0},ifnull(commit4,0),0)) as adv_commit from {3}.advance_commit 
            where demand_category='{1}' and ori_part_number=%s and process_week={2};""".format(prod_week,system_category,week_num,DB_NAME)
            system_query_result=condb_dict(system_adv_commit_query,[ori_part_number])[0]
            system_adv_commit_value=system_query_result['adv_commit'] if system_query_result['adv_commit']!=None else 0
            system_curr_commit=system_supply+system_adv_commit_value+cumulative_system_advance_commit_irp
            system_kr_commit=system_supply+system_advance_commit_kr
            
            if (nongsa_curr_commit!=nongsa_kr_commit) or (gsa_curr_commit!=gsa_kr_commit) or (forecast_curr_commit!=forecast_kr_commit) or (system_curr_commit!=system_kr_commit):
                print(i)
                error_records.append(i)
                print("not matching for nongsa ",record_id)
                
            else:
                pass
        print('error_records',error_records)             
        df=pd.DataFrame(error_records)
        errors=len(error_records)
        if errors>0:
            test_df=df.to_csv(index=False,header=True)
            s3=boto3.client("s3")
            s3.put_object(Body=test_df,Bucket=S3_BUCKET,Key="Validation_reports/"+"pl_kr_validation_"+supplier_name+str(week_num)+".csv")    
            object_url="https://"+S3_BUCKET+".s3.ap-southeast-1.amazonaws.com/Validation_reports/"+"pl_kr_validation_"+supplier_name+str(week_num)+".csv"
            client = boto3.client('ses',"us-east-1")
            print(RECIPIENT)
            response = client.send_email(Source=SENDER,Destination={'ToAddresses':RECIPIENT},
                            Message={
                                'Subject': {
                                    'Data': "{} - Planner's adjustment + Planner review (NO) vs KEYS KR summary Validation Result for work week-{}".format(supplier_name, week_num)
                                },
                                'Body': {
                                    'Text': {
                                        'Data': """Hello,\n\r\nErrors were found in {} Planner's adjustment + Planner review (NO) vs KEYS KR summary Validation as of {}.
                                        \n\nValidation details: {} \n\n Report URL: {} \n\nThanks & Regards,\nKeysightIT-Team\n\n\n""".format(supplier_name, sng_time,Validation_details,object_url)
                                    }
                                }
                            })
        else:
            s3=boto3.client("s3")   
            client = boto3.client('ses',"us-east-1")
            print(RECIPIENT)
            response = client.send_email(Source=SENDER,Destination={'ToAddresses':RECIPIENT},
                            Message={
                                'Subject': {
                                    'Data': "{} - Planner's adjustment + Planner review (NO) vs KEYS KR summary Validation Result for work week-{}".format(supplier_name, week_num)
                                },
                                'Body': {
                                    'Text': {
                                        'Data': """Hello,\n\r\nNo errors were found in {} Planner's adjustment + Planner review (NO) vs KEYS KR summary Validation as of {}.
                                        \n\nValidation details: {} \n\nThanks & Regards,\nKeysightIT-Team\n\n\n""".format(supplier_name, sng_time,Validation_details)
                                    }
                                }
                            })
        
        
    if supplier_id is not None:
        if supplier_id == 0:
            # supplier id is not provided in event data, we loop through the list of suppliers and process all suppliers available
            suppliers=get_all_suppliers_prefix()
            for supplier in suppliers:
                if supplier:
                    supplier_id = supplier[0]
                    supplier_prefix = supplier[1].split()[0]
                    print(supplier_prefix)
                    validate(supplier_id,supplier_prefix)  
                else:
                    print("Individual supplier list empty")
        else:
            # supplier id provided from event data, only process that supplier
            supplier=get_supplier_prefix(supplier_id)
            supplier_prefix=supplier.split()[0]
            validate(supplier_id,supplier_prefix) 
    else:
        print("No supplier info to run!")