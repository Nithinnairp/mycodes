import json
import pandas as pd
import datetime 
import sys
import boto3
import pymysql
import logging
from DB_conn import condb,condb_dict
import os

my_date=datetime.datetime.today()
year,week_num,day_of_week=my_date.isocalendar()
current_year=str(year)
current_year=current_year[2:]
DB_NAME=os.environ['DB_NAME']
S3_BUCKET = os.environ['S3_BUCKET']
SENDER = os.environ['SENDER']

def lambda_handler(event, context): 
    current_time=datetime.datetime.now()
    date_str=current_time.strftime("%Y-%m-%d %H_%M")
    sng_time=(current_time+datetime.timedelta(hours=8)).strftime("%Y-%m-%d %H_%M")
    formated_time=(current_time+datetime.timedelta(hours=8)).strftime("%Y-%m-%d %H_%M").replace(' ','+')
    
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
    
    def get_all_suppliers():
        get_suppliers_query="""select source_supplier_id,source_supplier from {db_name}.forecast_plan group by source_supplier;""".format(db_name=DB_NAME)
        suppliers_list=condb_dict(get_suppliers_query)
        return suppliers_list

    def get_recipients(col, supplier_id):
        query="""select {column} from {db_name}.active_suppliers where id = {id};""".format(column=col, db_name=DB_NAME, id=supplier_id)
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
    
    def generating_planner_adj_csv(source_supplier_id, source_supplier):
        object_url_list=[]
        supplier_short_name = source_supplier.split()[0]
        get_advance_col_names="""SELECT column_name from information_schema.columns where table_schema='{}' and table_name='forecast_plan';""".format(DB_NAME)
        advance_col_names=list(condb(get_advance_col_names))
        advance_db_columns=list(sum(advance_col_names,()))
        
        primary_query="""select * from {}.forecast_plan where source_supplier_id={} and active=1 order by ori_part_number,prod_year,prod_week;""".format(DB_NAME, source_supplier_id)
        df=condb(primary_query)
        primary_query_data=pd.DataFrame(data=df,columns=advance_db_columns)
        
        def send_mail(supplier_id_input, supplier_name):
            # Get email recipients and remove duplicates using set and convert to list back
            RECIPIENT = list(set(get_recipients('Archival_ADJ_COMM', supplier_id_input)) | set(get_recipients('Archival_Validation_common', supplier_id_input)))  
            client = boto3.client('ses',"us-east-1")
            response = client.send_email(Source=SENDER,Destination={'ToAddresses':RECIPIENT},
                            Message={
                                'Subject': {
                                    'Data': "Planner Adjustment report for {} WW{}".format(supplier_name,week_num)
                                },
                                'Body': {
                                    'Text': {
                                        'Data': """Hello,\n\r\nPlease find the Planner Adjument report generated for {supplier} as of {datetime}, download the file using below url.
                                        \n\n{supplier}_URL- {url}\
                                        \n\nThanks & Regards,\nKeysightIT-Team\n\n\n""".format(datetime=sng_time,supplier=supplier_name,url=object_url_list[0])
                                    }
                                }
                            })
        
        test1=primary_query_data.applymap(lambda x: x[0] if type(x) is bytes else x)
        test2=test1.fillna({'nongsa_original':0,'nongsa_adj':0,'nongsa_final':0,
        'gsa_original':0,'gsa_adj':0,'gsa_final':0,
        'system_original':0,'system_adj':0,'system_final':0,
        'forecast_original':0,'forecast_adj':0,'forecast_final':0,
        'total_original':0,'total_adj':0,'total_final':0}) 
        
        data_df1=test2[['org','bu','division','planner_code','planner_name','ori_part_number','cid_mapped_part_number','instrument','product_family','description','dept_code',
        'item_type','date','prod_week','prod_month','prod_year','build_type','cal_option',
        'nongsa_original','nongsa_adj','nongsa_final','nongsa_adj_reason','nongsa_remarks',
        'gsa_original','gsa_adj','gsa_final','gsa_adj_reason','gsa_remarks',
        'system_original','system_adj','system_final','system_adj_reason','system_remarks',
        'forecast_original','forecast_adj','forecast_final','forecast_adj_reason','forecast_remarks',
        'total_original','total_adj','total_final',
        'buffer','buffer_opt_adj','process_type','on_hand_nettable_si','received_qty','past_due_open_po','current_open_po','on_hand_total_cm','dmp_orndmp','source_supplier']]
        
        data_df1['nongsa_final']=test2[['nongsa_original','nongsa_adj']].sum(axis=1)
        data_df1['gsa_final']=test2[['gsa_original','gsa_adj']].sum(axis=1)
        data_df1['system_final']=test2[['system_original','system_adj']].sum(axis=1)
        data_df1['forecast_final']=test2[['forecast_original','forecast_adj']].sum(axis=1)
        data_df1['total_original']=test2[['nongsa_original','gsa_original','system_original','forecast_original']].sum(axis=1)
        data_df1['total_adj']=test2[['nongsa_adj','gsa_adj','system_adj','forecast_adj']].sum(axis=1)
        data_df1['total_final']=data_df1[['total_original','total_adj']].sum(axis=1)
        
        data_df1_col=['Org','BU','COE','Planner Code','Planner Name','Ori Part Number','Cid Mapped Part Number','Instrument','Product Family','Description','Dept Code',
        'Item Type','Bucket Date','Production Week','Production Month','Production Year','Build Type','Cal-Option',
        'Non GSA Original','Non GSA Adj','Non GSA Final','Non GSA Adj Reason','Non GSA Planner Remark',
        'GSA Original','GSA Adj','GSA Final','GSA Adj Reason','GSA Planner Remark',
        'System Original','System Adj','System Final','System Adj Reason','System Planner Remark',
        'Forecast Original','Forecast Adj','Forecast Final','Forecast Adj Reason','Forecast Planner Remark',
        'Total Original','Total Adj','Total Final',
        'Buffer','Buffer Option Adj','Process Type','On Hand Nettable SI','Received Qty','Past Due Open Po','Current Open Po','On Hand Total CM','DMP/NDMP','Source Supplier']
        
        data_df1.columns=data_df1_col
        
        data_df1.columns=list(map(lambda x: x.replace('GSA',catogory_name['gsa']), data_df1_col))
        data_df1.columns=list(map(lambda x: x.replace('Non {}'.format(catogory_name['gsa']),catogory_name['nongsa']), data_df1))
        
        final_df=data_df1.to_csv(index=False,header=True)
        s3=boto3.client("s3")
        s3.put_object(ACL='public-read',Body=final_df,Bucket=S3_BUCKET,Key="Archival_Reports/"+supplier_short_name+"/Planner_Adj_reports/Planner_ADJ_"+supplier_short_name+"("+sng_time+").csv")
        object_url="""https://{}.s3-ap-southeast-1.amazonaws.com/Archival_Reports/{}/Planner_Adj_reports/Planner_ADJ_{}({}).csv""".format(S3_BUCKET, supplier_short_name,supplier_short_name,formated_time)
        object_url_list.append(object_url)
        send_mail(source_supplier_id, supplier_short_name)
        
        return "Files generated and placed in Planner_ADJ folder"
    
    
    # Get all suppliers and run through each of them
    suppliers=get_all_suppliers()
    for supplier in suppliers:
        if supplier:
            supp_id=supplier['source_supplier_id']
            supp_name=supplier['source_supplier']
            print(supp_id,"-",supp_name)
            generating_planner_adj_csv(supp_id,supp_name)  
        else:
            print("Individual supplier list empty")
                    