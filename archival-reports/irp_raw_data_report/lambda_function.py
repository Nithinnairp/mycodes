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
DB_NAME = os.environ['DB_NAME']
S3_BUCKET = os.environ['S3_BUCKET']
SENDER=os.environ['SENDER']

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
    
    def send_mail(supplier_id_input,supplier_name,object_url):
        # Get email recipients and remove duplicates using set and convert to list back
        RECIPIENT = list(set(get_recipients('Archival_Validation_IRP_SUMM', supplier_id_input)) | set(get_recipients('Archival_Validation_common', supplier_id_input))) 
        client = boto3.client('ses',"us-east-1")
        response = client.send_email(Source=SENDER,Destination={'ToAddresses':RECIPIENT},
                        Message={
                            'Subject': {
                                'Data': "IRP RAW report for {} WW{}".format(supplier_name,week_num)
                            },
                            'Body': {
                                'Text': {
                                    'Data': """Hello,\n\r\nPlease find the IRP RAW report generated for {supplier} as of {datetime}, download the file using below url.
                                    \n\n{supplier}_URL- {url}
                                    \n\nThanks & Regards,\nKeysightIT-Team\n\n\n""".format(datetime=sng_time,supplier=supplier_name,url=object_url)
                                }
                            }
                        })
    
    def generating_irp_raw_csv(source_supplier_id, source_supplier):
        supplier_short_name = source_supplier.split()[0]
        get_advance_col_names="""SELECT column_name from information_schema.columns where table_schema='{}' and table_name='forecast_plan';""".format(DB_NAME)
        advance_col_names=list(condb(get_advance_col_names))
        advance_db_columns=list(sum(advance_col_names,()))
        primary_query="""select * from {}.forecast_plan where source_supplier_id={} and active=1 and 
        (ifnull(nongsa_original,0)+(ifnull(nongsa_adj, 0))!= 0
        or  (ifnull(gsa_original,0)+(ifnull(gsa_adj , 0)))!= 0
        or  (ifnull(system_original,0)+(ifnull(system_adj , 0)))!= 0
        or  (ifnull(forecast_original,0)+(ifnull(forecast_adj,0)))!= 0
        or  (ifnull(nongsa_supply,0) + ifnull(nongsa_advance_commit_irp, 0) + ifnull(cumulative_nongsa_advance_commit_irp, 0)) != 0
        or  (ifnull(gsa_supply,0) + ifnull(gsa_advance_commit_irp, 0) + ifnull(cumulative_gsa_advance_commit_irp, 0))!= 0
        or  (ifnull(system_supply,0) + ifnull(system_advance_commit_irp, 0) + ifnull(cumulative_system_advance_commit_irp, 0))!=0
        or  (ifnull(forecast_supply,0) + ifnull(forecast_advance_commit_irp, 0) + ifnull(cumulative_forecast_advance_commit_irp, 0))!=0)
        order by ori_part_number, prod_year, prod_week;""".format(DB_NAME,source_supplier_id)
    # 	or  (ifnull(nongsa_de_commit,0) )!= 0
    #     or  (ifnull(gsa_de_commit,0) )!=0
    #     or  (ifnull(system_de_commit,0) )!= 0
    #     or  (ifnull(forecast_de_commit,0) )!= 0)
        
        df=condb(primary_query)
        primary_query_data=pd.DataFrame(data=df,columns=advance_db_columns)
        
        test1=primary_query_data.applymap(lambda x: x[0] if type(x) is bytes else x)
        test2=test1.fillna({'nongsa_original':0,'nongsa_adj':0,'nongsa_supply':0,'nongsa_de_commit':0,'nongsa_final':0,'nongsa_commit':0,'nongsa_advance_commit_irp':0,'cumulative_nongsa_advance_commit_irp':0,
        'gsa_original':0,'gsa_adj':0,'gsa_supply':0,'gsa_de_commit':0,'gsa_final':0,'gsa_commit':0,'gsa_advance_commit_irp':0,'cumulative_gsa_advance_commit_irp':0,
        'system_original':0,'system_adj':0,'system_supply':0,'system_de_commit':0,'system_final':0,'system_commit':0,'system_advance_commit_irp':0,'cumulative_system_advance_commit_irp':0,
        'forecast_original':0,'forecast_adj':0,'forecast_supply':0,'forecast_de_commit':0,'forecast_final':0,'forecast_commit':0,'forecast_advance_commit_irp':0,'cumulative_forecast_advance_commit_irp':0,
        'total_original':0,'total_adj':0,'total_final':0,'total_commit':0}) 
        
        test2.loc[test2['measurable']==0,'measurable']='N'
        test2.loc[test2['measurable']==1,'measurable']='Y'
        test2.loc[test2['countable']==0,'countable']='N'
        test2.loc[test2['countable']==1,'countable']='Y'
        
        data_df1=test2[['ori_part_number','source_supplier','bu','product_line','product_family','planner_name','prod_week','prod_month','prod_year','dmp_orndmp',
        'measurable','countable','total_final','nongsa_final','gsa_final','system_final','forecast_final',
        'total_commit','de_commit','nongsa_commit','nongsa_cause_code','nongsa_cause_code_remark','nongsa_de_commit',
        'gsa_commit','gsa_cause_code','gsa_cause_code_remark','gsa_de_commit',
        'system_commit','system_cause_code','system_cause_code_remark','system_de_commit',
        'forecast_commit','forecast_cause_code','forecast_cause_code_remark','forecast_de_commit']]
    
        data_df1['nongsa_final']=test2[['nongsa_original','nongsa_adj']].sum(axis=1)
        data_df1['gsa_final']=test2[['gsa_original','gsa_adj']].sum(axis=1)
        data_df1['system_final']=test2[['system_original','system_adj']].sum(axis=1)
        data_df1['forecast_final']=test2[['forecast_original','forecast_adj']].sum(axis=1)
        data_df1['total_final']=data_df1[['nongsa_final','gsa_final','system_final','forecast_final']].sum(axis=1)
        data_df1['nongsa_commit']=test2[['nongsa_supply','nongsa_advance_commit_irp','cumulative_nongsa_advance_commit_irp']].sum(axis=1)
        data_df1['gsa_commit']=test2[['gsa_supply','gsa_advance_commit_irp','cumulative_gsa_advance_commit_irp']].sum(axis=1)
        data_df1['system_commit']=test2[['system_supply','system_advance_commit_irp','cumulative_system_advance_commit_irp']].sum(axis=1)
        data_df1['forecast_commit']=test2[['forecast_supply','forecast_advance_commit_irp','cumulative_forecast_advance_commit_irp']].sum(axis=1)
        data_df1['total_commit']=data_df1[['nongsa_commit','gsa_commit','system_commit','forecast_commit']].sum(axis=1)
        data_df1['de_commit']=test2[['nongsa_de_commit','gsa_de_commit','system_de_commit','forecast_de_commit']].sum(axis=1)
        
        data_df1_col=[['Part Number','Supplier','COE','Product Line','Product family','Planner Name','Week','Month','Year','DMP/NDMP','Measurable','Countable',
        'Total Requirement','Total NonGSA Requirement','Total GSA Requirement','Total System Requirement','Total Forecast Requirement','Total Latest Commit',
        'Total Latest Decommit','Total nonGSA Commit','Total nonGSA Cause Code','Total nonGSA Cause Code Remark','Total nonGSA Decommit',
        'Total GSA Commit','Total GSA Cause Code','Total GSA Cause Code Remark','Total GSA Decommit','Total System Commit','Total System Cause Code',
        'Total System Cause Code Remark','Total System Decommit','Total Forecast Commit','Total Forecast Cause Code','Total Forecast Cause Code Remark','Total Forecast Decommit']]
        
        data_df1.columns=data_df1_col
        data_df1.columns=list(map(lambda x: x.replace('GSA',catogory_name['gsa']), data_df1_col[0]))
        data_df1.columns=list(map(lambda x: x.replace('non{}'.format(catogory_name['gsa']),catogory_name['nongsa']), data_df1))
        final_df=data_df1.to_csv(index=False,header=True)
        s3=boto3.client("s3")
        s3.put_object(ACL='public-read',Body=final_df,Bucket=S3_BUCKET,Key="Archival_Reports/"+supplier_short_name+"/IRP_raw_reports/IRP_RAW_"+supplier_short_name+"("+sng_time+").csv")
        object_url="""https://{}.s3-ap-southeast-1.amazonaws.com/Archival_Reports/{}/IRP_raw_reports/IRP_RAW_{}({}).csv""".format(S3_BUCKET,supplier_short_name,supplier_short_name,formated_time)
        send_mail(source_supplier_id,supplier_short_name,object_url)
        
        return "Files generated and placed in IRP_raw_reports folder"
        
    
    # Get all suppliers and run through each of them
    suppliers=get_all_suppliers()
    for supplier in suppliers:
        if supplier:
            supp_id=supplier['source_supplier_id']
            supp_name=supplier['source_supplier']
            print(supp_id,"-",supp_name)
            generating_irp_raw_csv(supp_id,supp_name)  
        else:
            print("Individual supplier list empty")
    
    
