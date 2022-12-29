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
    
    current_time=datetime.datetime.now()
    date_str=current_time.strftime("%Y-%m-%d %H:%M")
    sng_time=(current_time+datetime.timedelta(hours=8)).strftime("%Y-%m-%d %H:%M")
    
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
    
    def validate_rr(source_supplier, source_supplier_id):
        supplier_short_name = source_supplier.split()[0]
        rr_validation_query="""SELECT * FROM {db_name}.forecast_validation_rr
                                where 
                                ((forecast_original!=forecast_total_demand)
                                or(gsa_original!=firmreq_gsa_total_demand)or
                                (nongsa_original!=firmreq_nongsa_total_demand)or
                                (buffer!=rr_buffer)or(buffer_opt_adj!=rr_buffer_opt_adj)or
                                (on_hand_nettable_si!=rr_on_hand_nettable_si)or(on_hand_total_cm!=rr_on_hand_total_cm)or
                                (past_due_open_po!=rr_past_due_open_po)or(current_open_po!=rr_current_open_po)) and source_supplier='{supplier}';""".format(db_name=DB_NAME,supplier=source_supplier)
        result=condb(rr_validation_query)                        
        col=['ori_part_number','original_number','prod_week','production_week','prod_year','year','forecast_original','forecast_total_demand','gsa_original',
        'firmreq_gsa_total_demand','nongsa_original','firmreq_nongsa_total_demand','source_supplier','created_by','created_at','updated_by','updated_at',
        'buffer','rr_buffer','buffer_opt_adj','rr_buffer_opt_adj','on_hand_nettable_si','rr_on_hand_nettable_si','on_hand_total_cm','rr_on_hand_total_cm',
        'past_due_open_po','rr_past_due_open_po','current_open_po','rr_current_open_po']
        
        data1=pd.DataFrame(data=result,columns=col)
        data1.columns=list(map(lambda x: x.replace('gsa',catogory_name['gsa']), data1))
        data1.columns=list(map(lambda x: x.replace('non{}'.format(catogory_name['gsa']),catogory_name['nongsa']), data1))
        mismatch_records=data1.count()[0]
        RECIPIENT = list(set(get_recipients('Validations_Planning_Phase', supplier_short_name)) | set(get_recipients('Archival_Validation_common', supplier_short_name))) 
        if mismatch_records==0:
            print("No mismatched values found between forecast_plan and RR file")
            client = boto3.client('ses',"us-east-1")
            response = client.send_email(Source=SENDER,Destination={'ToAddresses':RECIPIENT},
                            Message={
                                'Subject': {
                                    'Data': "Forecast & RR file Validation Result for {} work week-{}".format(supplier_short_name,week_num)
                                },
                                'Body': {
                                    'Text': {
                                        'Data': "Hello,\n\r\nNo mismatched values found between forecast_plan and RR file as of {}.\n\nThanks & Regards,\nKeysightIT-Team\n\n\n".format(sng_time)
                                    }
                                }
                            })
        else:
            print("Mismatched values found between forecast_plan and RR file")
            final_df=data1.applymap(lambda x: x[0] if type(x) is bytes else x)
            save_csv=final_df.to_csv("s3://"+S3_BUCKET+"/Validation_reports/Forecast_RR_validation_records_"+supplier_short_name+"("+date_str+").csv",index=False,header=True,encoding='UTF-8')
            object_url="""https://{}.s3.ap-southeast-1.amazonaws.com/Validation_reports/Forecast_RR_validation_records_{}({}).csv""".format(S3_BUCKET,supplier_short_name,formated_time)
            client = boto3.client('ses',"us-east-1")
            response = client.send_email(Source=SENDER,Destination={'ToAddresses':RECIPIENT},
                            Message={
                                'Subject': {
                                    'Data': "Forecast & RR file Validation Result for {} work week-{}".format(supplier_short_name, week_num)
                                },
                                'Body': {
                                    'Text': {
                                        'Data': "Hello,\n\r\nMismatched values found between forecast_plan and RR file as of {} Download from the below URL {}.\n\nThanks & Regards,\nKeysightIT-Team\n\n\n".format(sng_time,object_url)
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
            validate_rr(supp_name, supp_id)
        else:
            print("Individual supplier list empty")
    