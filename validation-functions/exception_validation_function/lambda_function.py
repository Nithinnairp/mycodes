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
S3_BUCKET=os.environ['S3_BUCKET']
SENDER=os.environ['SENDER']

def lambda_handler(event, context):
    
    current_time=datetime.datetime.now()
    date_str=current_time.strftime("%Y-%m-%d %H:%M")
    sng_time=(current_time+datetime.timedelta(hours=8)).strftime("%Y-%m-%d %H_%M")
    formated_time=(current_time+datetime.timedelta(hours=8)).strftime("%Y-%m-%d %H_%M").replace(' ','+')
    
    # print(week_num)
    
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
    
    def error_handling():
        return '{},{} on Line:{}'.format(sys.exc_info()[0],sys.exc_info()[1],sys.exc_info()[2].tb_lineno)

    
    def validate_exception(source_supplier, source_supplier_id):
        supplier_short_name = source_supplier.split()[0]
        validating_the_exception_stamping_query1="""select count(*) as count1 from {db_name}.forecast_plan 
            where active=1 and dmp_orndmp='DMP' and  process_type='Exception' and
            (((ifnull(forecast_original,0)+ifnull(forecast_adj,0))-(ifnull(forecast_supply,0)+ifnull(forecast_advance_commit_irp,0)+ifnull(cumulative_forecast_advance_commit_irp,0)))=0 and
            ((ifnull(system_original,0)+ifnull(system_adj,0))-(ifnull(system_supply,0)+ifnull(system_advance_commit_irp,0)+ifnull(cumulative_system_advance_commit_irp,0)))=0 and 
            ((ifnull(gsa_original,0)+ifnull(gsa_adj,0))-(ifnull(gsa_supply,0)+ifnull(gsa_advance_commit_irp,0)+ifnull(cumulative_gsa_advance_commit_irp,0)))=0 and 
            ((ifnull(nongsa_original,0)+ifnull(nongsa_adj,0))-(ifnull(nongsa_supply,0)+ifnull(nongsa_advance_commit_irp,0)+ifnull(cumulative_nongsa_advance_commit_irp,0)))=0)
            and source_supplier_id={supplier_id};""".format(db_name=DB_NAME, supplier_id=source_supplier_id)
        
        validating_the_exception_stamping_query2="""select count(*) as count2 from {db_name}.forecast_plan  
            where dmp_orndmp='DMP' and active=1 and process_type is null and 
            (((ifnull(forecast_original,0)+ifnull(forecast_adj,0))-(ifnull(forecast_supply,0)+ifnull(forecast_advance_commit_irp,0)+ifnull(cumulative_forecast_advance_commit_irp,0)))!=0 or
            ((ifnull(system_original,0)+ifnull(system_adj,0))-(ifnull(system_supply,0)+ifnull(system_advance_commit_irp,0)+ifnull(cumulative_system_advance_commit_irp,0)))!=0 or 
            ((ifnull(gsa_original,0)+ifnull(gsa_adj,0))-(ifnull(gsa_supply,0)+ifnull(gsa_advance_commit_irp,0)+ifnull(cumulative_gsa_advance_commit_irp,0)))!=0 or 
            ((ifnull(nongsa_original,0)+ifnull(nongsa_adj,0))-(ifnull(nongsa_supply,0)+ifnull(nongsa_advance_commit_irp,0)+ifnull(cumulative_nongsa_advance_commit_irp,0)))!=0)
            and source_supplier_id={supplier_id};""".format(db_name=DB_NAME, supplier_id=source_supplier_id)
        
        df1=condb_dict(validating_the_exception_stamping_query1)[0]
        df2=condb_dict(validating_the_exception_stamping_query2)[0]
        
        RECIPIENT = list(set(get_recipients('Validations_Planning_Phase', supplier_short_name)) | set(get_recipients('Archival_Validation_common', supplier_short_name))) 
        if df1['count1']==0 or df2['count2']==0:
            print("No errors found in exception stamping")
            client = boto3.client('ses',"us-east-1")
            response = client.send_email(Source=SENDER,Destination={'ToAddresses':RECIPIENT},
                            Message={
                                'Subject': {
                                    'Data': "Exception Validation Result for {} work week-{}".format(supplier_short_name,week_num)
                                },
                                'Body': {
                                    'Text': {
                                        'Data': "Hello,\n\r\nNo errors were found in exception stamping as of {}.\n\nThanks & Regards,\nKeysightIT-Team\n\n\n".format(sng_time)
                                    }
                                }
                            })
                            
        else:
            wrongly_stamped_records1="""select 
            source_supplier,ori_part_number,prod_week,prod_year,process_week,process_year,process_type,
            nongsa_original,nongsa_adj,nongsa_supply,nongsa_advance_commit_irp,cumulative_nongsa_advance_commit_irp,
            gsa_original,gsa_adj,gsa_supply,gsa_advance_commit_irp,cumulative_gsa_advance_commit_irp,
            forecast_original,forecast_adj,forecast_supply,forecast_advance_commit_irp,cumulative_forecast_advance_commit_irp,
            system_original,system_adj,system_supply,system_advance_commit_irp,cumulative_system_advance_commit_irp,
            manually_added,via_advance_commit,is_imported,is_processed,created_by,created_at,updated_at,updated_by
            from {db_name}.forecast_plan 
            where active=1 and dmp_orndmp='DMP' and  process_type='Exception' and
            (((ifnull(forecast_original,0)+ifnull(forecast_adj,0))-(ifnull(forecast_supply,0)+ifnull(forecast_advance_commit_irp,0)+ifnull(cumulative_forecast_advance_commit_irp,0)))=0 and
            ((ifnull(system_original,0)+ifnull(system_adj,0))-(ifnull(system_supply,0)+ifnull(system_advance_commit_irp,0)+ifnull(cumulative_system_advance_commit_irp,0)))=0 and 
            ((ifnull(gsa_original,0)+ifnull(gsa_adj,0))-(ifnull(gsa_supply,0)+ifnull(gsa_advance_commit_irp,0)+ifnull(cumulative_gsa_advance_commit_irp,0)))=0 and 
            ((ifnull(nongsa_original,0)+ifnull(nongsa_adj,0))-(ifnull(nongsa_supply,0)+ifnull(nongsa_advance_commit_irp,0)+ifnull(cumulative_nongsa_advance_commit_irp,0)))=0)
            and source_supplier_id={supplier_id};;""".format(db_name=DB_NAME, supplier_id=source_supplier_id)
            wrongly_stamped_records2="""select 
            source_supplier,ori_part_number,prod_week,prod_year,process_week,process_year,process_type,
            nongsa_original,nongsa_adj,nongsa_supply,nongsa_advance_commit_irp,cumulative_nongsa_advance_commit_irp,
            gsa_original,gsa_adj,gsa_supply,gsa_advance_commit_irp,cumulative_gsa_advance_commit_irp,
            forecast_original,forecast_adj,forecast_supply,forecast_advance_commit_irp,cumulative_forecast_advance_commit_irp,
            system_original,system_adj,system_supply,system_advance_commit_irp,cumulative_system_advance_commit_irp,
            manually_added,via_advance_commit,is_imported,is_processed,created_by,created_at,updated_at,updated_by
            from {db_name}.forecast_plan 
            where dmp_orndmp='DMP' and active=1 and process_type is null and 
            (((ifnull(forecast_original,0)+ifnull(forecast_adj,0))-(ifnull(forecast_supply,0)+ifnull(forecast_advance_commit_irp,0)+ifnull(cumulative_forecast_advance_commit_irp,0)))!=0 or
            ((ifnull(system_original,0)+ifnull(system_adj,0))-(ifnull(system_supply,0)+ifnull(system_advance_commit_irp,0)+ifnull(cumulative_system_advance_commit_irp,0)))!=0 or 
            ((ifnull(gsa_original,0)+ifnull(gsa_adj,0))-(ifnull(gsa_supply,0)+ifnull(gsa_advance_commit_irp,0)+ifnull(cumulative_gsa_advance_commit_irp,0)))!=0 or 
            ((ifnull(nongsa_original,0)+ifnull(nongsa_adj,0))-(ifnull(nongsa_supply,0)+ifnull(nongsa_advance_commit_irp,0)+ifnull(cumulative_nongsa_advance_commit_irp,0)))!=0)
            and source_supplier_id={supplier_id};;""".format(db_name=DB_NAME, supplier_id=source_supplier_id)
            
            error_data1=condb(wrongly_stamped_records1)
            error_data2=condb(wrongly_stamped_records2)
            
            col=['source_supplier','ori_part_number','prod_week','prod_year','process_week','process_year','process_type',
            'nongsa_original','nongsa_adj','nongsa_supply','nongsa_advance_commit_irp','cumulative_nongsa_advance_commit_irp',
            'gsa_original','gsa_adj','gsa_supply','gsa_advance_commit_irp','cumulative_gsa_advance_commit_irp',
            'forecast_original','forecast_adj','forecast_supply','forecast_advance_commit_irp','cumulative_forecast_advance_commit_irp',
            'system_original','system_adj','system_supply','system_advance_commit_irp','cumulative_system_advance_commit_irp',
            'manually_added','via_advance_commit','is_imported','is_processed','created_by','created_at','updated_by','updated_at']
            data1=pd.DataFrame(data=error_data1,columns=col)
            data2=pd.DataFrame(data=error_data2,columns=col)
            
            data3=pd.concat([data1,data2])
            df3=data3.applymap(lambda x: x[0] if type(x) is bytes else x)
            df3.columns=list(map(lambda x: x.replace('gsa',catogory_name['gsa']), df3))
            df3.columns=list(map(lambda x: x.replace('non{}'.format(catogory_name['gsa']),catogory_name['nongsa']), df3))
            
            final_df=df3.to_csv("s3://"+S3_BUCKET+"/Validation_reports/Exception_records_"+supplier_short_name+"("+sng_time+").csv",index=False,header=True,encoding='UTF-8')
            object_url="""https://{}.s3.ap-southeast-1.amazonaws.com/Validation_reports/Exception_records_{}({}).csv""".format(S3_BUCKET,supplier_short_name,formated_time)
            client = boto3.client('ses',"us-east-1")
            response = client.send_email(Source=SENDER,Destination={'ToAddresses':RECIPIENT},
                            Message={
                                'Subject': {
                                    'Data': "Exception Validation Result for {} work week-{}".format(supplier_short_name, week_num)
                                },
                                'Body': {
                                    'Text': {
                                        'Data': "Hello,\n\r\nErrors were found in exception stamping as of {}. Download the from belwo URL- {}\n\nThanks & Regards,\nKeysightIT-Team\n\n\n".format(sng_time,object_url)
                                    }
                                }
                            })
        
    # Get all suppliers and run through each of them
    suppliers=get_all_suppliers()
    for supplier in suppliers:
        if supplier:
            supp_id=supplier['source_supplier_id']
            supp_name=supplier['source_supplier']
            print(supp_id,"-",supp_name)
            validate_exception(supp_name, supp_id)
        else:
            print("Individual supplier list empty")    
