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
    sng_time=(current_time+datetime.timedelta(hours=8)).strftime("%Y-%m-%d %H_%M")
    date_str=current_time.strftime("%Y-%m-%d %H_%M")
    
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
    
    catogory_name={}
    config_query = "select * from {}.config where jhi_key in ('GSA','NON-GSA','FORECAST','SYSTEM');".format(DB_NAME)
    result=condb_dict(config_query)
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

    def validate_total_req(source_supplier, source_supplier_id):
        supplier_short_name = source_supplier.split()[0]
        total_req_validation="""select 
            ac.ori_part_number,ac.prod_week,ac.prod_year,ac.demand_category,ac.process_week,
            ac.commit1,ac.commit1_date,ac.commit1_accepted,
            ac.commit2,ac.commit2_date,ac.commit2_accepted,
            ac.commit3,ac.commit3_date,ac.commit3_accepted,
            ac.commit4,ac.commit4_date,ac.commit4_accepted from {db_name}.advance_commit as ac,{db_name}.forecast_plan as fp 
            where ac.process_week={week} and (fp.ori_part_number=ac.ori_part_number and fp.prod_week=ac.prod_week and fp.prod_year=ac.prod_year) and
             ac.demand_category='Non GSA'
             and 
            ((ifnull(ac.commit1,0)!=ifnull(fp.nongsa_commit1,0) or ifnull(ac.commit1_date,0)!=ifnull(fp.nongsa_commit1_date ,0) or ifnull(ac.commit1_accepted,0)!=ifnull(fp.nongsa_commit1_accepted,0)) or
            (ifnull(ac.commit2,0)!=ifnull(fp.nongsa_commit2,0) or ifnull(ac.commit2_date,0)!=ifnull(fp.nongsa_commit2_date ,0) or ifnull(ac.commit2_accepted,0)!=ifnull(fp.nongsa_commit2_accepted,0)) or
            (ifnull(ac.commit3,0)!=ifnull(fp.nongsa_commit3,0) or ifnull(ac.commit3_date,0)!=ifnull(fp.nongsa_commit3_date ,0) or ifnull(ac.commit3_accepted,0)!=ifnull(fp.nongsa_commit3_accepted,0)) or
            (ifnull(ac.commit4,0)!=ifnull(fp.nongsa_commit4,0) or ifnull(ac.commit4_date,0)!=ifnull(fp.nongsa_commit4_date ,0) or ifnull(ac.commit4_accepted,0)!=ifnull(fp.nongsa_commit4_accepted,0)))
             and fp.source_supplier_id={supplier_id}
            union
            select 
            ac.ori_part_number,ac.prod_week,ac.prod_year,ac.demand_category,ac.process_week,
            ac.commit1,ac.commit1_date,ac.commit1_accepted,
            ac.commit2,ac.commit2_date,ac.commit2_accepted,
            ac.commit3,ac.commit3_date,ac.commit3_accepted,
            ac.commit4,ac.commit4_date,ac.commit4_accepted from {db_name}.advance_commit as ac,{db_name}.forecast_plan as fp 
            where ac.process_week={week} and (fp.ori_part_number=ac.ori_part_number and fp.prod_week=ac.prod_week and fp.prod_year=ac.prod_year) and
             ac.demand_category='GSA'
             and 
            ((ifnull(ac.commit1,0)!=ifnull(fp.gsa_commit1,0) or ifnull(ac.commit1_date,0)!=ifnull(fp.gsa_commit1_date ,0) or ifnull(ac.commit1_accepted,0)!=ifnull(fp.gsa_commit1_accepted,0)) or
            (ifnull(ac.commit2,0)!=ifnull(fp.gsa_commit2,0) or ifnull(ac.commit2_date,0)!=ifnull(fp.gsa_commit2_date ,0) or ifnull(ac.commit2_accepted,0)!=ifnull(fp.gsa_commit2_accepted,0)) or
            (ifnull(ac.commit3,0)!=ifnull(fp.gsa_commit3,0) or ifnull(ac.commit3_date,0)!=ifnull(fp.gsa_commit3_date ,0) or ifnull(ac.commit3_accepted,0)!=ifnull(fp.gsa_commit3_accepted,0)) or
            (ifnull(ac.commit4,0)!=ifnull(fp.gsa_commit4,0) or ifnull(ac.commit4_date,0)!=ifnull(fp.gsa_commit4_date ,0) or ifnull(ac.commit4_accepted,0)!=ifnull(fp.gsa_commit4_accepted,0)))
             and fp.source_supplier_id={supplier_id}
            union
            select 
            ac.ori_part_number,ac.prod_week,ac.prod_year,ac.demand_category,ac.process_week,
            ac.commit1,ac.commit1_date,ac.commit1_accepted,
            ac.commit2,ac.commit2_date,ac.commit2_accepted,
            ac.commit3,ac.commit3_date,ac.commit3_accepted,
            ac.commit4,ac.commit4_date,ac.commit4_accepted from {db_name}.advance_commit as ac,{db_name}.forecast_plan as fp 
            where ac.process_week={week} and (fp.ori_part_number=ac.ori_part_number and fp.prod_week=ac.prod_week and fp.prod_year=ac.prod_year) and
             ac.demand_category='Forecast'
             and 
            ((ifnull(ac.commit1,0)!=ifnull(fp.forecast_commit1,0) or ifnull(ac.commit1_date,0)!=ifnull(fp.forecast_commit1_date ,0) or ifnull(ac.commit1_accepted,0)!=ifnull(fp.forecast_commit1_accepted,0)) or
            (ifnull(ac.commit2,0)!=ifnull(fp.forecast_commit2,0) or ifnull(ac.commit2_date,0)!=ifnull(fp.forecast_commit2_date ,0) or ifnull(ac.commit2_accepted,0)!=ifnull(fp.forecast_commit2_accepted,0)) or
            (ifnull(ac.commit3,0)!=ifnull(fp.forecast_commit3,0) or ifnull(ac.commit3_date,0)!=ifnull(fp.forecast_commit3_date ,0) or ifnull(ac.commit3_accepted,0)!=ifnull(fp.forecast_commit3_accepted,0)) or
            (ifnull(ac.commit4,0)!=ifnull(fp.forecast_commit4,0) or ifnull(ac.commit4_date,0)!=ifnull(fp.forecast_commit4_date ,0) or ifnull(ac.commit4_accepted,0)!=ifnull(fp.forecast_commit4_accepted,0)))
             and fp.source_supplier_id={supplier_id}
            union
            select 
            ac.ori_part_number,ac.prod_week,ac.prod_year,ac.demand_category,ac.process_week,
            ac.commit1,ac.commit1_date,ac.commit1_accepted,
            ac.commit2,ac.commit2_date,ac.commit2_accepted,
            ac.commit3,ac.commit3_date,ac.commit3_accepted,
            ac.commit4,ac.commit4_date,ac.commit4_accepted from {db_name}.advance_commit as ac,{db_name}.forecast_plan as fp 
            where ac.process_week={week} and (fp.ori_part_number=ac.ori_part_number and fp.prod_week=ac.prod_week and fp.prod_year=ac.prod_year) and
             ac.demand_category='System'
             and 
            ((ifnull(ac.commit1,0)!=ifnull(fp.system_commit1,0) or ifnull(ac.commit1_date,0)!=ifnull(fp.system_commit1_date ,0) or ifnull(ac.commit1_accepted,0)!=ifnull(fp.system_commit1_accepted,0)) or
            (ifnull(ac.commit2,0)!=ifnull(fp.system_commit2,0) or ifnull(ac.commit2_date,0)!=ifnull(fp.system_commit2_date ,0) or ifnull(ac.commit2_accepted,0)!=ifnull(fp.system_commit2_accepted,0)) or
            (ifnull(ac.commit3,0)!=ifnull(fp.system_commit3,0) or ifnull(ac.commit3_date,0)!=ifnull(fp.system_commit3_date ,0) or ifnull(ac.commit3_accepted,0)!=ifnull(fp.system_commit3_accepted,0)) or
            (ifnull(ac.commit4,0)!=ifnull(fp.system_commit4,0) or ifnull(ac.commit4_date,0)!=ifnull(fp.system_commit4_date ,0) or ifnull(ac.commit4_accepted,0)!=ifnull(fp.system_commit4_accepted,0)))
             and fp.source_supplier_id={supplier_id};""".format(db_name=DB_NAME,week=week_num, supplier_id=source_supplier_id)
        col=['ori_part_number','prod_week','prod_year','demand_category','process_week',
        'commit1','commit1_date','commit1_accepted',
        'commit2','commit2_date','commit2_accepted',
        'commit3','commit3_date','commit3_accepted',
        'commit4','commit4_date','commit4_accepted']
        total_requirement_data=condb(total_req_validation)
        total_requirement_df1=pd.DataFrame(data=total_requirement_data,columns=col)
        df1=total_requirement_df1.applymap(lambda x: x[0] if type(x) is bytes else x)
        df1['demand_category']=df1['demand_category'].replace('GSA',catogory_name['gsa']).replace('Non GSA',catogory_name['nongsa'])
        test_df=df1.to_csv(index=False,header=True)
        s3=boto3.client("s3")
        s3.put_object(ACL='public-read',Body=test_df,Bucket=S3_BUCKET,Key="Validation_reports/IRP_vs_PlannerReview_validation_records_"+supplier_short_name+"("+date_str+").csv")
            
        # Get email recipients and remove duplicates using set and convert to list back
        RECIPIENT = list(set(get_recipients('Archival_Validation_IRP_SUMM', supplier_short_name)) | set(get_recipients('Archival_Validation_common', supplier_short_name)))
        
        error_records=total_requirement_df1.count()[0] 
        if error_records==0:
            print("No mismatch found in planner adjustment from past week")
            client = boto3.client('ses',"us-east-1")
            response = client.send_email(Source=SENDER,Destination={'ToAddresses':RECIPIENT},
                            Message={
                                'Subject': {
                                    'Data': "IRP_vs_Planner Review Validation Result for {} work week-{}".format(supplier_short_name, week_num)
                                },
                                'Body': {
                                    'Text': {
                                        'Data': "Hello,\n\r\nNo mismatch found in IRP and Planner review as of {}.\n\nThanks & Regards,\nKeysightIT-Team\n\n\n".format(sng_time)
                                    }
                                }
                            })
        else:
            print("records found with total requirement greater than zero")
            formated_time=current_time.strftime("%Y-%m-%d %H_%M").replace(' ','+')
            object_url="""https://{}.s3-ap-southeast-1.amazonaws.com/Validation_reports/IRP_vs_PlannerReview_validation_records_{}({}).csv""".format(S3_BUCKET, supplier_short_name, formated_time)
            client = boto3.client('ses',"us-east-1")
            response = client.send_email(Source=SENDER,Destination={'ToAddresses':RECIPIENT},
                            Message={
                                'Subject': {
                                    'Data': "IRP_vs_Planner Review Validation Result for {} work week-{}".format(supplier_short_name, week_num)
                                },
                                'Body': {
                                    'Text': {
                                        'Data': """Hello,\n\r\nRecords with mismatch found in IRP vs Planner review as of as of {} download the file from below url.\nURL- {}
                                        \n\nThanks & Regards,\nKeysightIT-Team\n\n\n""".format(sng_time,object_url)
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
            validate_total_req(supp_name, supp_id)
        else:
            print("Individual supplier list empty") 