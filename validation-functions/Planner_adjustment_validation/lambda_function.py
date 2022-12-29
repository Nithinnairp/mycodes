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
    
    def planner_adj_validation(source_supplier, source_supplier_id):
        supplier_short_name = source_supplier.split()[0]
        planner_adj_validation="""select test.id,adjn.ori_part_number,adjn.demand_category,
                test.sunday_nongsa_adj,test.monday_nongsa_adj,
                test.sunday_gsa_adj,test.monday_gsa_adj,
                test.sunday_forecast_adj,test.monday_forecast_adj,
                test.sunday_system_adj,test.monday_system_adj,
                new_value,test.mp_prod_week,test.mp_prod_year,
                (case when adjn.demand_category='Non GSA' then
                (test.sunday_nongsa_adj+new_value) end) as 'calculated_nongsa_value',
                (case when adjn.demand_category='GSA' then
                (test.sunday_gsa_adj+new_value) end) as 'calculated_gsa_value',
                (case when adjn.demand_category='Forecast' then
                (test.sunday_forecast_adj+new_value) end) as 'calculated_forecast_value',
                (case when adjn.demand_category='System' then
                (test.sunday_system_adj+new_value) end) as 'calculated_system_value',
                test.mp_created_at,test.mp_created_by,test.mp_updated_by,test.mp_updated_at
                 from {db_name}.adjustment_notification as adjn join 
                (select mp.id as id,mp.ori_part_number as 'part_number',mp.prod_week as 'mp_prod_week' ,mp.prod_year as 'mp_prod_year',
                mp.forecast_adj as 'monday_forecast_adj',sp.forecast_adj as 'sunday_forecast_adj',
                mp.nongsa_adj as 'monday_nongsa_adj',sp.nongsa_adj as 'sunday_nongsa_adj',
                mp.gsa_adj as 'monday_gsa_adj',sp.gsa_adj as 'sunday_gsa_adj',
                mp.system_adj as 'monday_system_adj',sp.system_adj as 'sunday_system_adj',mp.created_at as 'mp_created_at',mp.created_by as 'mp_created_by',
                mp.updated_at as 'mp_updated_at',mp.updated_by as 'mp_updated_by'
                from {db_name}.forecast_plan as mp join {db_name}.forecast_plan_temp as sp
                on (mp.id=sp.id) where
                ((ifnull(mp.forecast_adj,0)!=ifnull(sp.forecast_adj,0))or(ifnull(mp.nongsa_adj,0)!=ifnull(sp.nongsa_adj,0))or
                (ifnull(mp.gsa_adj,0)!=ifnull(sp.gsa_adj,0))or(ifnull(mp.system_adj,0)!=ifnull(sp.system_adj,0))) and mp.source_supplier_id={supplier_id} and sp.source_supplier_id={supplier_id}) as test
                on (adjn.ori_part_number=test.part_number) and (adjn.prod_week=test.mp_prod_week) and (adjn.prod_year=test.mp_prod_year)
                where adjn.process_week=week(curdate()) and 
                if (adjn.demand_category='Non GSA',(test.monday_nongsa_adj!=test.sunday_nongsa_adj+new_value),
                	if(adjn.demand_category='GSA',(test.monday_gsa_adj!=test.sunday_gsa_adj+new_value),
                		if(adjn.demand_category='Forecast',(test.monday_forecast_adj!=test.sunday_forecast_adj+new_value),
                			if(adjn.demand_category='System',(test.monday_system_adj!=test.sunday_system_adj+new_value),null))));""".format(db_name=DB_NAME, supplier_id=source_supplier_id)
                			
        col=["id","ori_part_number","demand_category","sunday_nongsa_adj","monday_nongsa_adj","sunday_gsa_adj","monday_gsa_adj","sunday_forecast_adj","monday_forecast_adj",
        "sunday_system_adj","monday_system_adj","new_value","prod_week","prod_year","calculated_nongsa_value","calculated_gsa_value","calculated_forecast_value","calculated_system_value",
        "created_at","created_by","updated_at","updated_by"]
        result=condb(planner_adj_validation)
        data1=pd.DataFrame(data=result,columns=col)
        data1.columns=list(map(lambda x: x.replace('gsa',catogory_name['gsa']), data1))
        data1.columns=list(map(lambda x: x.replace('non{}'.format(catogory_name['gsa']),catogory_name['nongsa']), data1))
        RECIPIENT = list(set(get_recipients('Validations_Planning_Phase', supplier_short_name)) | set(get_recipients('Archival_Validation_common', supplier_short_name))) 
        mismatched_records=data1.count()[0]
        print(mismatched_records)
        if mismatched_records==0:
            print("No mismatch found in planner adjustment from past week")
            client = boto3.client('ses',"us-east-1")
            response = client.send_email(Source=SENDER,Destination={'ToAddresses':RECIPIENT},
                            Message={
                                'Subject': {
                                    'Data': "Planner Adjustment Validation Result for {} work week-{}".format(supplier_short_name, week_num)
                                },
                                'Body': {
                                    'Text': {
                                        'Data': "Hello,\n\r\nNo mismatch found in Planner Adjustment from past week as of {}.\n\nThanks & Regards,\nKeysightIT-Team\n\n\n".format(sng_time)
                                    }
                                }
                            })
        else:
            print("Mismatch found in planner adjustment from past week")
            df1=data1.applymap(lambda x: x[0] if type(x) is bytes else x)
            final_df=df1.to_csv(index=False,header=True)
            # save_csv=final_df.to_csv("s3://b2b-irp-analytics-prod/Validation_reports/Planner_Adjustment_validation_records("+date_str+").csv",index=False,header=True,encoding='UTF-8')
            s3=boto3.client("s3")
            s3.put_object(ACL='public-read',Body=final_df,Bucket=S3_BUCKET,Key="Validation_reports/Planner_Adjustment_validation_records_"+supplier_short_name+"("+sng_time+").csv")
            object_url="""https://{}.s3-ap-southeast-1.amazonaws.com/Validation_reports/Planner_Adjustment_validation_records_{}({}).csv""".format(S3_BUCKET,supplier_short_name,formated_time)
            client = boto3.client('ses',"us-east-1")
            response = client.send_email(Source=SENDER,Destination={'ToAddresses':RECIPIENT},
                            Message={
                                'Subject': {
                                    'Data': "Planner Adjustment Validation Result for {} work week-{}".format(supplier_short_name, week_num)
                                },
                                'Body': {
                                    'Text': {
                                        'Data': """Hello,\n\r\nMismatch found in planner adjustment from past week as of {}, download the file using below url.
                                        \n\ndownload-URL- {}
                                        \n\nThanks & Regards,\nKeysightIT-Team\n\n\n""".format(sng_time,object_url)
                                    }
                                }
                            })
            
                
            return "completed"
    
    # Get all suppliers and run through each of them
    suppliers=get_all_suppliers()
    for supplier in suppliers:
        if supplier:
            supp_id=supplier['source_supplier_id']
            supp_name=supplier['source_supplier']
            print(supp_id,"-",supp_name)
            planner_adj_validation(supp_name, supp_id)
        else:
            print("Individual supplier list empty") 