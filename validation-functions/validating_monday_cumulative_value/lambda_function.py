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
    sng_time=(current_time+datetime.timedelta(hours=8)).strftime("%Y-%m-%d %H:%M")
    formated_time=(current_time+datetime.timedelta(hours=8)).strftime("%Y-%m-%d %H_%M").replace(' ','+')
    object_url=[]
    # print(week_num)
    def error_handling():
        return '{},{} on Line:{}'.format(sys.exc_info()[0],sys.exc_info()[1],sys.exc_info()[2].tb_lineno)
        
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
    
    
    def validate(source_supplier, source_supplier_id):
        supplier_short_name = source_supplier.split()[0]
        Validate_KR_Summary_Report_Commit_Value="""select count(*) as count1
            from {db_name}.forecast_plan_monday as curr_mon,{db_name}.forecast_plan_last_monday_after_load as last_mon,{db_name}.forecast_plan_temp as fri
            where
            (((ifnull(last_mon.cumulative_nongsa_advance_commit_irp,0)+ifnull(fri.nongsa_advance_commit_irp,0))!=
            ifnull(curr_mon.cumulative_nongsa_advance_commit_irp,0)) or 
            ((ifnull(last_mon.cumulative_gsa_advance_commit_irp,0)+ifnull(fri.gsa_advance_commit_irp,0))!=
            ifnull(curr_mon.cumulative_gsa_advance_commit_irp,0)) or 
            ((ifnull(last_mon.cumulative_forecast_advance_commit_irp,0)+ifnull(fri.forecast_advance_commit_irp,0))!=
            ifnull(curr_mon.cumulative_forecast_advance_commit_irp,0)) or
            ((ifnull(last_mon.cumulative_system_advance_commit_irp,0)+ifnull(fri.system_advance_commit_irp,0))!=
            ifnull(curr_mon.cumulative_system_advance_commit_irp,0)))and 
            ((curr_mon.oripartnumber_week_year=last_mon.oripartnumber_week_year)and(curr_mon.oripartnumber_week_year=fri.oripartnumber_week_year)) and
            curr_mon.source_supplier_id={supplier_id} and last_mon.source_supplier_id={supplier_id} and fri.source_supplier_id={supplier_id};""".format(db_name=DB_NAME, supplier_id=source_supplier_id)
            
        RECIPIENT = list(set(get_recipients('Validations_Planning_Phase', supplier_short_name)) | set(get_recipients('Archival_Validation_common', supplier_short_name)))
        df1=condb_dict(Validate_KR_Summary_Report_Commit_Value)[0]
        print(df1['count1'])
        if df1['count1']==0:
            print("No errors found in Validate_KR_Summary_Report_Commit_Value")
            client = boto3.client('ses',"us-east-1")
            response = client.send_email(Source=SENDER,Destination={'ToAddresses':RECIPIENT},
                            Message={
                                'Subject': {
                                    'Data': "Validation of Monday Cumulative Value Result for {} WW-{}".format(supplier_short_name,week_num)
                                },
                                'Body': {
                                    'Text': {
                                        'Data':"""Hello,\n\r\nNo errors were found in Validation of Monday Cumulative Value for {} as of {}.\nValidation Details:-'Past Mon Cumm commit + Past Fri new commit inc adv commit YES = Mon curr cumm commit (Final - exception qty in CM IRP export)'\n\nThanks & Regards,\nKeysightIT-Team\n\n\n""".format(supplier_short_name, sng_time)
                                    }
                                }
                            })
        else:
            print("Errors found in the Validate_KR_Summary_Report_Commit_Value")
            error_records="""select (ifnull(curr_mon.nongsa_supply,0)+ifnull(curr_mon.cumulative_nongsa_advance_commit_irp,0)) as 'curr_KR',
                    (ifnull(last_mon.nongsa_supply,0)+ifnull(last_mon.cumulative_nongsa_advance_commit_irp,0)) as 'last_commit',
                    (ifnull(fri.nongsa_commit,0)+ifnull(fri.nongsa_advance_commit_irp,0)) as 'fri',curr_mon.oripartnumber_week_year,
                    last_mon.oripartnumber_week_year,fri.oripartnumber_week_year 
                    from {db_name}.forecast_plan_monday as curr_mon,{db_name}.forecast_plan_last_monday_after_load as last_mon,{db_name}.forecast_plan_temp as fri
                    where
                    (((ifnull(last_mon.cumulative_nongsa_advance_commit_irp,0)+ifnull(fri.nongsa_advance_commit_irp,0))!=
                    ifnull(curr_mon.cumulative_nongsa_advance_commit_irp,0)) or 
                    ((ifnull(last_mon.cumulative_gsa_advance_commit_irp,0)+ifnull(fri.gsa_advance_commit_irp,0))!=
                    ifnull(curr_mon.cumulative_gsa_advance_commit_irp,0)) or 
                    ((ifnull(last_mon.cumulative_forecast_advance_commit_irp,0)+ifnull(fri.forecast_advance_commit_irp,0))!=
                    ifnull(curr_mon.cumulative_forecast_advance_commit_irp,0)) or
                    ((ifnull(last_mon.cumulative_system_advance_commit_irp,0)+ifnull(fri.system_advance_commit_irp,0))!=
                    ifnull(curr_mon.cumulative_system_advance_commit_irp,0)))and 
                    ((curr_mon.oripartnumber_week_year=last_mon.oripartnumber_week_year)and(curr_mon.oripartnumber_week_year=fri.oripartnumber_week_year)) and
                    curr_mon.source_supplier_id={supplier_id} and last_mon.source_supplier_id={supplier_id} and fri.source_supplier_id={supplier_id};""".format(db_name=DB_NAME, supplier_id=source_supplier_id)
            
            result=condb(error_records)
            col=['curr_KR','last_commit','fri','curr_oripartnumber_week_year','last_mon_oripartnumber_week_year','fri_oripartnumber_week_year']
            data1=pd.DataFrame(data=result,columns=col)
            final_df=df3.to_csv("s3://"+S3_BUCKET+"/Validation_reports/Validate_KR_Summary_Report_Commit_Value_errors_"+supplier_short_name+"("+sng_time+").csv",index=False,header=True,encoding='UTF-8')
            object_url="""https://{}.s3-ap-southeast-1.amazonaws.com/Validation_reports/Validate_KR_Summary_Report_Commit_Value_errors_{}({}).csv""".format(S3_BUCKET, supplier_short_name, formated_time)
            client = boto3.client('ses',"us-east-1")
            response = client.send_email(Source=SENDER,Destination={'ToAddresses':RECIPIENT},
                            Message={
                                'Subject': {
                                    'Data': "Validation of Monday Cumulative Value Result for {} WW-{}".format(supplier_short_name, week_num)
                                },
                                'Body': {
                                    'Text': {
                                        'Data': """Hello,\n\r\nErrors were found in Validation of Monday Cumulative Value for {} as of {} in first 8 weeks, download the file from below url.\nURL- {}
                                        \n\nThanks & Regards,\nKeysightIT-Team\n\n\n""".format(supplier_short_name,sng_time,object_url)
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
            validate(supp_name, supp_id)
        else:
            print("Individual supplier list empty")     
        