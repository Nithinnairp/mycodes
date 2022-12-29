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
# print(week_num)
complete_current_year=str(year)
current_year=complete_current_year[2:]
DB_NAME = os.environ['DB_NAME']
SENDER = os.environ['SENDER']
S3_BUCKET = os.environ['S3_BUCKET']

def lambda_handler(event, context):
    
    current_time=datetime.datetime.now()
    date_str=current_time.strftime("%Y-%m-%d %H_%M")
    sng_time=(current_time+datetime.timedelta(hours=8)).strftime("%Y-%m-%d %H_%M")
    formated_time=(current_time+datetime.timedelta(hours=8)).strftime("%Y-%m-%d %H_%M").replace(' ','+')
    object_url_list=[]
    
    try:
        # Get supplier id from event data
        supplier_id=event['supplier_id']
    except:
        # If no supplier id in event data, assign to 0 as an indicator that no supplier id being passed in. We will get list of suppliers to loop through instead.
        supplier_id=0
        pass
    
    catogory_name={}
    result=condb_dict("select * from {}.config where jhi_key in ('GSA','NON-GSA','FORECAST','SYSTEM');".format(DB_NAME))
    for i in result:
        print(i)
        if i['jhi_key']=='GSA':
            catogory_name['gsa']=i['value']
        elif i['jhi_key']=='NON-GSA':
            catogory_name['nongsa']=i['value']
        elif i['jhi_key']=='FORECAST':
            catogory_name['forecast']=i['value']
        elif i['jhi_key']=='SYSTEM':
            catogory_name['system']=i['value']
        else:
            None

    
    def get_all_suppliers():
        get_suppliers_query="""select source_supplier_id,source_supplier from {db_name}.forecast_plan group by source_supplier;""".format(db_name=DB_NAME)
        suppliers_list=condb_dict(get_suppliers_query)
        return suppliers_list
        
    def get_supplier_name(supplier_id):
        query = """SELECT distinct(source_supplier) FROM {}.forecast_plan WHERE source_supplier_id={};""".format(DB_NAME,supplier_id)
        supplier_query_res=list(condb(query))
        supplier=supplier_query_res[0][0]
        return supplier
    
    def get_recipients(col, supplier_short_name):
        # Get email recipient list by supplier short name
        query="""select {column} from {db_name}.active_suppliers where supplier_name LIKE ('{short_name}%');""".format(column=col, db_name=DB_NAME, short_name=supplier_short_name)
        recipient_result=condb(query)
        try:
            recipients = recipient_result[0][0]
        except: 
            recipients = None
        # convert to list type
        if recipients is not None:
            list_of_recipients=recipients.split(',')
        else:
            list_of_recipients = []
        return list_of_recipients
    
    
    def validate(source_supplier, source_supplier_id):
        supplier_short_name = source_supplier.split()[0]
        total_req_validation="""select ori_part_number,prod_week,prod_year,nongsa_original,nongsa_adj,
        (ifnull(nongsa_original,0)+ifnull(nongsa_adj,0)) as total_nongsa,gsa_original,gsa_adj,
        (ifnull(gsa_original,0)+ifnull(gsa_adj,0)) as total_gsa,forecast_original,forecast_adj,
        (ifnull(forecast_original,0)+ifnull(forecast_adj,0)) as total_forecast,system_original,system_adj,
        (ifnull(system_original,0)+ifnull(system_adj,0)) as total_system,
        ((ifnull(nongsa_original,0)+ifnull(nongsa_adj,0))+(ifnull(gsa_original,0)+ifnull(gsa_adj,0))+
        (ifnull(forecast_original,0)+ifnull(forecast_adj,0))+(ifnull(system_original,0)+ifnull(system_adj,0))) as total_demand
        from {db_name}.forecast_plan 
        where 
        ((ifnull(nongsa_original,0)+ifnull(nongsa_adj,0))+(ifnull(gsa_original,0)+ifnull(gsa_adj,0))+
        (ifnull(forecast_original,0)+ifnull(forecast_adj,0))+(ifnull(system_original,0)+ifnull(system_adj,0)))<0
        and source_supplier_id={supplier_id};""".format(db_name=DB_NAME, supplier_id=source_supplier_id)
        col=['ori_part_number','prod_week','prod_year','nongsa_original','nongsa_adj','total_nongsa',
        'gsa_original','gsa_adj','total_gsa','forecast_original','forecast_adj','total_forecast',
        'system_original','system_adj','total_system','total_demand']
        total_requirement_data=condb(total_req_validation)
        total_requirement_df1=pd.DataFrame(data=total_requirement_data,columns=col)
        
        total_requirement_df1.columns=list(map(lambda x: x.replace('gsa',catogory_name['gsa']), total_requirement_df1))
        total_requirement_df1.columns=list(map(lambda x: x.replace('non{}'.format(catogory_name['gsa']),catogory_name['nongsa']), total_requirement_df1))
        
        total_requirement_csv=total_requirement_df1.to_csv("s3://"+S3_BUCKET+"/Validation_reports/Total_requirement_validation_records_before_commit_phase_"+supplier_short_name+"("+date_str+").csv",index=False,header=True,encoding='UTF-8')
        RECIPIENT = list(set(get_recipients('Validations_Commit_Phase', supplier_short_name)) | set(get_recipients('Archival_Validation_common', supplier_short_name)))
        
        error_records=total_requirement_df1.count()[0]
        if error_records==0:
            print("No mismatch found in planner adjustment from past week")
            client = boto3.client('ses',"us-east-1")
            response = client.send_email(Source=SENDER,Destination={'ToAddresses':RECIPIENT},
                            Message={
                                'Subject': {
                                    'Data': "Total requirement Validation Result before commit phase for {supplier} work week-{week}".format(supplier=supplier_short_name,week=week_num)
                                },
                                'Body': {
                                    'Text': {
                                        'Data': "Hello,\n\r\nNo records found with Total requirement less than zero as of {}.\n\nThanks & Regards,\nKeysightIT-Team\n\n\n".format(sng_time)
                                    }
                                }
                            })
        else:
            print("records found with total requirement greater than zero")
            formated_time=current_time.strftime("%Y-%m-%d %H_%M").replace(' ','+')
            object_url="""https://{}.s3-ap-southeast-1.amazonaws.com/Validation_reports/Total_requirement_validation_records_before_commit_phase_{}({}).csv""".format(S3_BUCKET,supplier_short_name,formated_time)
            client = boto3.client('ses',"us-east-1")
            response = client.send_email(Source=SENDER,Destination={'ToAddresses':RECIPIENT},
                            Message={
                                'Subject': {
                                    'Data': "Total requirement Validation Result before commit phase for {supplier} work week-{week}".format(supplier=supplier_short_name,week=week_num)
                                },
                                'Body': {
                                    'Text': {
                                        'Data': """Hello,\n\r\nRecords found with Total requirement less than zero as of {} download the file from below url.\nURL- {}
                                        \n\nThanks & Regards,\nKeysightIT-Team\n\n\n""".format(date_str,object_url)
                                    }
                                }
                            })
            
    if supplier_id is not None:
        if supplier_id == 0:
            # supplier id is not provided in event data, we loop through the list of suppliers and process all suppliers available
            suppliers=get_all_suppliers()
            for supplier in suppliers:
                if supplier:
                    supp_id=supplier['source_supplier_id']
                    supp_name=supplier['source_supplier']
                    print(supp_id,"-",supp_name)
                    validate(supp_name, supp_id)
                else:
                    print("Individual supplier list empty") 
        else:
            # supplier id provided from event data, only process that supplier
            supplier=get_supplier_name(supplier_id)
            print(supplier_id,"-",supplier)
            validate(supplier, supplier_id) 
    else:
        print("No supplier info to run!")