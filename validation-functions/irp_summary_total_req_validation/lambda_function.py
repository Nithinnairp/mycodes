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
    date_str=current_time.strftime("%Y-%m-%d %H:%M")
    sng_time=(current_time+datetime.timedelta(hours=8)).strftime("%Y-%m-%d %H_%M")
    formated_time=(current_time+datetime.timedelta(hours=8)).strftime("%Y-%m-%d %H_%M").replace(' ','+')
    object_url_list=[]
    
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
    
    def total_req_validation(source_supplier, source_supplier_id):
        supplier_short_name = source_supplier.split()[0]
        Total_requirement_query="""select source_supplier,ori_part_number,bu,product_line,product_family,planner_name,prod_week,prod_month,prod_year,
            dmp_orndmp,measurable,countable,
            nongsa_original,nongsa_adj,nongsa_supply,nongsa_advance_commit_irp,cumulative_nongsa_advance_commit_irp,
            nongsa_cause_code,nongsa_cause_code_remark,
            (ifnull(nongsa_supply,0)+ifnull(nongsa_advance_commit_irp,0)+ifnull(cumulative_nongsa_advance_commit_irp,0)) as nongsa_commit,
            gsa_original,gsa_adj,gsa_supply,gsa_advance_commit_irp,cumulative_gsa_advance_commit_irp,gsa_cause_code,gsa_cause_code_remark,
            (ifnull(gsa_supply,0)+ifnull(gsa_advance_commit_irp,0)+ifnull(cumulative_gsa_advance_commit_irp,0)) as gsa_commit,
            forecast_original,forecast_adj,forecast_supply,forecast_advance_commit_irp,cumulative_forecast_advance_commit_irp,forecast_cause_code,
            forecast_cause_code_remark,
            (ifnull(forecast_supply,0)+ifnull(forecast_advance_commit_irp,0)+ifnull(cumulative_forecast_advance_commit_irp,0)) as forecast_commit,
            system_original,system_adj,system_supply,system_advance_commit_irp,cumulative_system_advance_commit_irp,system_cause_code,system_cause_code_remark,
            (ifnull(system_supply,0)+ifnull(system_advance_commit_irp,0)+ifnull(cumulative_system_advance_commit_irp,0)) as system_commit,
            ((ifnull(nongsa_original,0)+ifnull(nongsa_adj,0))+
            (ifnull(gsa_original,0)+ifnull(gsa_adj,0))+
            (ifnull(forecast_original,0)+ifnull(forecast_adj,0))+
            (ifnull(system_original,0)+ifnull(system_adj,0))) as total_requrement,
            ((ifnull(nongsa_supply,0)+ifnull(nongsa_advance_commit_irp,0)+ifnull(cumulative_nongsa_advance_commit_irp,0))+
            (ifnull(gsa_supply,0)+ifnull(gsa_advance_commit_irp,0)+ifnull(cumulative_gsa_advance_commit_irp,0))+
            (ifnull(forecast_supply,0)+ifnull(forecast_advance_commit_irp,0)+ifnull(cumulative_forecast_advance_commit_irp,0))+
            (ifnull(system_supply,0)+ifnull(system_advance_commit_irp,0)+ifnull(cumulative_system_advance_commit_irp,0))) as total_commit,
            (ifnull(nongsa_de_commit,0)+ifnull(gsa_de_commit,0)+ifnull(forecast_de_commit,0)+ifnull(system_de_commit,0)) as total_decommit
            from {db_name}.forecast_plan 
            where 
            ((((ifnull(nongsa_original,0)+ifnull(nongsa_adj,0))+
            (ifnull(gsa_original,0)+ifnull(gsa_adj,0))+
            (ifnull(forecast_original,0)+ifnull(forecast_adj,0))+
            (ifnull(system_original,0)+ifnull(system_adj,0)))<0)or
            (((ifnull(nongsa_supply,0)+ifnull(nongsa_advance_commit_irp,0)+ifnull(cumulative_nongsa_advance_commit_irp,0))+
            (ifnull(gsa_supply,0)+ifnull(gsa_advance_commit_irp,0)+ifnull(cumulative_gsa_advance_commit_irp,0))+
            (ifnull(forecast_supply,0)+ifnull(forecast_advance_commit_irp,0)+ifnull(cumulative_forecast_advance_commit_irp,0))+
            (ifnull(system_supply,0)+ifnull(system_advance_commit_irp,0)+ifnull(cumulative_system_advance_commit_irp,0)))<0)or
            (ifnull(nongsa_de_commit,0)+ifnull(gsa_de_commit,0)+ifnull(forecast_de_commit,0)+ifnull(system_de_commit,0))<0)
            and source_supplier_id={supplier_id};""".format(db_name=DB_NAME, supplier_id=source_supplier_id)
        total_req_data=condb_dict(Total_requirement_query)
        col=['source_supplier','ori_part_number','bu','product_line','product_family','planner_name','prod_week','prod_month','prod_year',
            'dmp_orndmp','measurable','countable',
            'nongsa_original','nongsa_adj','nongsa_supply','nongsa_advance_commit_irp','cumulative_nongsa_advance_commit_irp',
            'nongsa_cause_code','nongsa_cause_code_remark','nongsa_commit',
            'gsa_original','gsa_adj','gsa_supply','gsa_advance_commit_irp','cumulative_gsa_advance_commit_irp','gsa_cause_code','gsa_cause_code_remark','gsa_commit',
            'forecast_original','forecast_adj','forecast_supply','forecast_advance_commit_irp','cumulative_forecast_advance_commit_irp','forecast_cause_code',
            'forecast_cause_code_remark','forecast_commit',
            'system_original','system_adj','system_supply','system_advance_commit_irp','cumulative_system_advance_commit_irp','system_cause_code','system_cause_code_remark',
            'system_commit','total_requirement','total_commit','total_decommit']
        total_req_df1=pd.DataFrame(data=total_req_data,columns=col)
        
        total_req_df1.columns=list(map(lambda x: x.replace('gsa',catogory_name['gsa']), total_req_df1))
        total_req_df1.columns=list(map(lambda x: x.replace('non{}'.format(catogory_name['gsa']),catogory_name['nongsa']), total_req_df1))
        
        # Get email recipients and remove duplicates using set and convert to list back
        RECIPIENT = list(set(get_recipients('Archival_Validation_IRP_SUMM', supplier_short_name)) | set(get_recipients('Archival_Validation_common', supplier_short_name))) 
        error_count=total_req_df1.count()[0]
        if error_count==0:
            result="No Records found for {} where total_req<0 or total_commit<0 or total_decommit<0 ".format(supplier_short_name)
            print(result)
            client = boto3.client('ses',"us-east-1")
            response = client.send_email(Source=SENDER,Destination={'ToAddresses':RECIPIENT},
                            Message={
                                'Subject': {
                                    'Data': "IRP Summary Raw Report total requirement,total commit Validation Result for {supplier} work week-{week}".format(supplier=supplier_short_name, week=week_num)
                                },
                                'Body': {
                                    'Text': {
                                        'Data': "Hello,\n\r\nNo Records found for {supplier} where total_req<0 or total_commit<0 or total_decommit<0 as of {time}.\n\nThanks & Regards,\nKeysightIT-Team\n\n\n".format(supplier=supplier_short_name, time=sng_time)
                                    }
                                }
                            })
        else:
            result="Records found for {} where total_req<0 or total_commit<0 or total_decommit<0".format(supplier_short_name)
            print(result)
            total_req_df2=total_req_df1.applymap(lambda x: x[0] if type(x) is bytes else x)
            
            final_df=total_req_df2.to_csv(index=False,header=True)
            # total_req_csv=total_req_df2.to_csv("s3://b2b-irp-analytics-prod/Validation_reports/IRM_summary_Total_req_validation_records("+date_str+").csv",index=False,header=True,encoding='UTF-8')
            s3=boto3.client("s3")
            s3.put_object(ACL='public-read',Body=final_df,Bucket=S3_BUCKET,Key="Validation_reports/IRP_summary_Total_req_validation_records_"+supplier_short_name+"("+sng_time+").csv")
            object_url="""https://{s3_bucket}.s3-ap-southeast-1.amazonaws.com/Validation_reports/IRP_summary_Total_req_validation_records_{supplier}({time}).csv""".format(s3_bucket=S3_BUCKET, supplier=supplier_short_name, time=formated_time)
            client = boto3.client('ses',"us-east-1")
            response = client.send_email(Source=SENDER,Destination={'ToAddresses':RECIPIENT},
                            Message={
                                'Subject': {
                                    'Data': "IRP Summary Raw Report total requirement,total commit Validation Result for {supplier} work week-{week}".format(supplier=supplier_short_name, week=week_num)
                                },
                                'Body': {
                                    'Text': {
                                        'Data': """Hello,\n\r\nRecords found for {supplier} where total_req<0 or total_commit<0 or total_decommit<0 as of {time}, download the file using below url.
                                        \n\ndownload-URL- {url}
                                        \n\nThanks & Regards,\nKeysightIT-Team\n\n\n""".format(supplier=supplier_short_name, time=sng_time,url=object_url)
                                    }
                                }
                            })
            
            
        return result
    
    # Get all suppliers and run through each of them
    suppliers=get_all_suppliers()
    for supplier in suppliers:
        if supplier:
            supp_id=supplier['source_supplier_id']
            supp_name=supplier['source_supplier']
            print(supp_id,"-",supp_name)
            total_req_validation(supp_name, supp_id)
        else:
            print("Individual supplier list empty") 
    
    