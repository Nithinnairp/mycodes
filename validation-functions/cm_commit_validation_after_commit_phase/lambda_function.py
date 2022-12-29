# cm_commit_validation_after_commit_phase
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
complete_current_year=str(year)
current_year=complete_current_year[2:]
SENDER=os.environ['SENDER']
DB_NAME = os.environ['DB_NAME']
S3_BUCKET = os.environ['S3_BUCKET']

def lambda_handler(event, context):
    
    current_time=datetime.datetime.now()
    date_str=current_time.strftime("%Y-%m-%d %H:%M")
    sng_time=(current_time+datetime.timedelta(hours=8)).strftime("%Y-%m-%d %H_%M")
    formated_time=(current_time+datetime.timedelta(hours=8)).strftime("%Y-%m-%d %H_%M").replace(' ','+')
    
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
    
    def cm_commit_validation(supplier_id, supplier_name):
        cm_commit_validation_query="""select source_supplier,ori_part_number,product_family,planner_code,prod_week,prod_year,
        (ifnull(nongsa_original,0)+ifnull(nongsa_adj,0)) as "nongsa_kr",
        (ifnull(nongsa_original,0)+ifnull(nongsa_adj,0))-(ifnull(nongsa_supply,0)+ifnull(nongsa_commit1,0)+ifnull(nongsa_commit2,0)+ifnull(nongsa_commit3,0)+ifnull(nongsa_commit4,0)+ifnull(cumulative_nongsa_advance_commit_irp,0)) as "nongsa_exception",
        (ifnull(gsa_original,0)+ifnull(gsa_adj,0)) as "gsa_kr",
        (ifnull(gsa_original,0)+ifnull(gsa_adj,0))-(ifnull(gsa_supply,0)+ifnull(gsa_commit1,0)+ifnull(gsa_commit2,0)+ifnull(gsa_commit3,0)+ifnull(gsa_commit4,0)+ifnull(cumulative_gsa_advance_commit_irp,0)) as "gsa_exception",
        (ifnull(system_original,0)+ifnull(system_adj,0)) as "system_kr",
        (ifnull(system_original,0)+ifnull(system_adj,0))-(ifnull(system_supply,0)+ifnull(system_commit1,0)+ifnull(system_commit2,0)+ifnull(system_commit3,0)+ifnull(system_commit4,0)+ifnull(cumulative_system_advance_commit_irp,0)) as "system_exception",
        (ifnull(forecast_original,0)+ifnull(forecast_adj,0)) as "forecast_kr",
        (ifnull(forecast_original,0)+ifnull(forecast_adj,0))-(ifnull(forecast_supply,0)+ifnull(forecast_commit1,0)+ifnull(forecast_commit2,0)+ifnull(forecast_commit3,0)+ifnull(forecast_commit4,0)+ifnull(cumulative_forecast_advance_commit_irp,0)) as "forecast_exception" 
         from {}.forecast_plan where
        (((ifnull(nongsa_original,0)+ifnull(nongsa_adj,0))-(ifnull(nongsa_supply,0)+ifnull(nongsa_commit1,0)+ifnull(nongsa_commit2,0)+ifnull(nongsa_commit3,0)+ifnull(nongsa_commit4,0)+ifnull(cumulative_nongsa_advance_commit_irp,0)))!=0
        or
        ((ifnull(gsa_original,0)+ifnull(gsa_adj,0))-(ifnull(gsa_supply,0)+ifnull(gsa_commit1,0)+ifnull(gsa_commit2,0)+ifnull(gsa_commit3,0)+ifnull(gsa_commit4,0)+ifnull(cumulative_gsa_advance_commit_irp,0)))!=0
        or
        ((ifnull(system_original,0)+ifnull(system_adj,0))-(ifnull(system_supply,0)+ifnull(system_commit1,0)+ifnull(system_commit2,0)+ifnull(system_commit3,0)+ifnull(system_commit4,0)+ifnull(cumulative_system_advance_commit_irp,0)))!=0
        or
        ((ifnull(forecast_original,0)+ifnull(forecast_adj,0))-(ifnull(forecast_supply,0)+ifnull(forecast_commit1,0)+ifnull(forecast_commit2,0)+ifnull(forecast_commit3,0)+ifnull(forecast_commit4,0)+ifnull(cumulative_forecast_advance_commit_irp,0)))!=0) 
        and dmp_orndmp='DMP' 
        and source_supplier_id={} 
        order by ori_part_number, prod_year, prod_week;""".format(DB_NAME,supplier_id)
        cm_commit_validation_data=condb_dict(cm_commit_validation_query)
        
        # Get email recipients and remove duplicates using set and convert to list back
        RECIPIENT = list(set(get_recipients('Validations_Commit_Phase', supplier_id)) | set(get_recipients('Archival_Validation_common', supplier_id)))
        
        col=['source_supplier','ori_part_number','product_family','planner_code','prod_week','prod_year','nongsa_kr','nongsa_exception',
        'gsa_kr','gsa_exception','system_kr','system_exception','forecast_kr','forecast_exception']
        cm_commit_df1=pd.DataFrame(data=cm_commit_validation_data,columns=col)
        
        cm_commit_df1.columns=list(map(lambda x: x.replace('gsa',catogory_name['gsa']), cm_commit_df1))
        cm_commit_df1.columns=list(map(lambda x: x.replace('non{}'.format(catogory_name['gsa']),catogory_name['nongsa']), cm_commit_df1))
        
        error_count=cm_commit_df1.count()[0]
        if error_count==0:
            result="""No records found in {} where commit after CM commit phase with not equal to 0""".format(supplier_name)
            print(result)
            client = boto3.client('ses',"us-east-1")
            print(RECIPIENT)
            response = client.send_email(Source=SENDER,Destination={'ToAddresses':RECIPIENT},
                            Message={
                                'Subject': {
                                    'Data': "Validation Result for {} CM commit after CM commit phase for work week-{}".format(supplier_name, week_num)
                                },
                                'Body': {
                                    'Text': {
                                        'Data': "Hello,\n\r\nNo Records found in {} where commit after CM commit phase with 0 as of {}.\n\nThanks & Regards,\nKeysightIT-Team\n\n\n".format(supplier_name, sng_time)
                                    }
                                }
                            })
                            
        else:
            result="""Records found in {} where commit after CM commit phase with not equal to 0""".format(supplier_name)
            print(result)
            cm_commit_df2=cm_commit_df1.applymap(lambda x: x[0] if type(x) is bytes else x)
            
            final_df=cm_commit_df2.to_csv(index=False,header=True)
            s3=boto3.client("s3")
            s3.put_object(ACL='public-read',Body=final_df,Bucket=S3_BUCKET,Key="Validation_reports/cm_commit_validation_after_commit_phase_"+supplier_name+"("+sng_time+").csv")
            object_url="""https://{}.s3-ap-southeast-1.amazonaws.com/Validation_reports/cm_commit_validation_after_commit_phase_{}({}).csv""".format(S3_BUCKET,supplier_name, formated_time)
            print(RECIPIENT)
            client = boto3.client('ses',"us-east-1")
            response = client.send_email(Source=SENDER,Destination={'ToAddresses':RECIPIENT},
                            Message={
                                'Subject': {
                                    'Data': "Validation Result for {} CM commit after CM commit phase for work week-{}".format(supplier_name, week_num)
                                },
                                'Body': {
                                    'Text': {
                                        'Data': """Hello,\n\r\nRecords found in {} where commit after CM commit phase with not equal to 0 as of {}, download the file using below url.
                                        \n\ndownload-URL- {}
                                        \n\nThanks & Regards,\nKeysightIT-Team\n\n\n""".format(supplier_name, sng_time,object_url)
                                    }
                                }
                            })
            
            
        return result
    
    if supplier_id is not None:
        if supplier_id == 0:
            # supplier id is not provided in event data, we loop through the list of suppliers and process all suppliers available
            suppliers=get_all_suppliers_prefix()
            for supplier in suppliers:
                if supplier:
                    supplier_id = supplier[0]
                    supplier_prefix = supplier[1].split()[0]
                    print(supplier_prefix)
                    cm_commit_validation(supplier_id,supplier_prefix)
                else:
                    print("Individual supplier list empty")
        else:
            # supplier id provided from event data, only process that supplier
            supplier=get_supplier_prefix(supplier_id)
            supplier_prefix=supplier.split()[0]
            cm_commit_validation(supplier_id,supplier_prefix) 
    else:
        print("No supplier info to run!")
    




















# query="""select ori_part_number,prod_week,prod_year,
# (ifnull(nongsa_original,0)+ifnull(nongsa_adj,0)) as "nongsa_kr",
# (ifnull(nongsa_supply,0)+ifnull(nongsa_commit1,0)+ifnull(nongsa_commit2,0)+ifnull(nongsa_commit3,0)+ifnull(nongsa_commit4,0)+ifnull(cumulative_nongsa_advance_commit_irp,0)) as "nongsa_commit",
# (ifnull(gsa_original,0)+ifnull(gsa_adj,0)) as "gsa_kr",
# (ifnull(gsa_supply,0)+ifnull(gsa_commit1,0)+ifnull(gsa_commit2,0)+ifnull(gsa_commit3,0)+ifnull(gsa_commit4,0)+ifnull(cumulative_gsa_advance_commit_irp,0)) as "gsa_commit",
# (ifnull(forecast_original,0)+ifnull(forecast_adj,0)) as "forecast_kr",
# (ifnull(forecast_supply,0)+ifnull(forecast_commit1,0)+ifnull(forecast_commit2,0)+ifnull(forecast_commit3,0)+ifnull(forecast_commit4,0)+ifnull(cumulative_forecast_advance_commit_irp,0)) as "forecast_commit",
# (ifnull(system_original,0)+ifnull(system_adj,0)) as "system_kr",
# (ifnull(system_supply,0)+ifnull(system_commit1,0)+ifnull(system_commit2,0)+ifnull(system_commit3,0)+ifnull(system_commit4,0)+ifnull(cumulative_system_advance_commit_irp,0)) as "system_commit" 
#  from User_App.forecast_plan where
# (((ifnull(nongsa_original,0)+ifnull(nongsa_adj,0))-(ifnull(nongsa_supply,0)+ifnull(nongsa_commit1,0)+ifnull(nongsa_commit2,0)+ifnull(nongsa_commit3,0)+ifnull(nongsa_commit4,0)+ifnull(cumulative_nongsa_advance_commit_irp,0)))!=0
# or
# ((ifnull(gsa_original,0)+ifnull(gsa_adj,0))-(ifnull(gsa_supply,0)+ifnull(gsa_commit1,0)+ifnull(gsa_commit2,0)+ifnull(gsa_commit3,0)+ifnull(gsa_commit4,0)+ifnull(cumulative_gsa_advance_commit_irp,0)))!=0
# or
# ((ifnull(forecast_original,0)+ifnull(forecast_adj,0))-(ifnull(forecast_supply,0)+ifnull(forecast_commit1,0)+ifnull(forecast_commit2,0)+ifnull(forecast_commit3,0)+ifnull(forecast_commit4,0)+ifnull(cumulative_forecast_advance_commit_irp,0)))!=0
# or
# ((ifnull(system_original,0)+ifnull(system_adj,0))-(ifnull(system_supply,0)+ifnull(system_commit1,0)+ifnull(system_commit2,0)+ifnull(system_commit3,0)+ifnull(system_commit4,0)+ifnull(cumulative_system_advance_commit_irp,0)))!=0) and dmp_orndmp='DMP' 
# and source_supplier_id=2;"""