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
    event_data=event['Subject']
    test=event['test']
    try:
        # Get supplier id from event data
        supplier_id=event['supplier_id']
    except:
        # If no supplier id in event data, assign to 0 as an indicator that no supplier id being passed in. We will get list of suppliers to loop through instead.
        supplier_id=0
        pass
        
    catogory_name={}
    config_query="""select * from {}.config where jhi_key in ('GSA','NON-GSA','FORECAST','SYSTEM');""".format(DB_NAME)
    result=condb_dict(config_query)
    for i in result:
        # print(i)
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
        
    print(event_data)
    if event_data=='with_commit':
        report_type="with commit"
    if event_data=='without_commit':
        report_type="without commit"
        
    def get_all_suppliers_prefix():
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
    
    def send_mail(source_supplier_id,source_supplier,report_type,test,object_url):
        # Get email recipients and remove duplicates using set and convert to list back
        RECIPIENT = list(set(get_recipients('Validations_Commit_Phase', source_supplier)) | set(get_recipients('Archival_Validation_common', source_supplier)))
        print(RECIPIENT)
        client = boto3.client('ses',"us-east-1")
        response = client.send_email(Source=SENDER,Destination={'ToAddresses':RECIPIENT},
                        Message={
                            'Subject': {
                                'Data': "IRP {} report for {} WW{}".format(report_type,source_supplier,week_num)
                            },
                            'Body': {
                                'Text': {
                                    'Data': """Hello,\n\r\nPlease find the IRP {} report generated for {} as of {}, download the file using below url.
                                    \n\n{}_URL- {}
                                    \n\nThanks & Regards,\nKeysightIT-Team\n\n\n""".format(report_type,source_supplier,sng_time,source_supplier,object_url)
                                }
                            }
                        })

    def generating_irp_csv(source_supplier_id,source_supplier):
        get_advance_col_names="""SELECT column_name from information_schema.columns where table_schema='{}' and table_name='forecast_plan';""".format(DB_NAME)
        advance_col_names=list(condb(get_advance_col_names))
        advance_db_columns=list(sum(advance_col_names,()))
        
        primary_query="""select * from {}.forecast_plan where source_supplier_id={} and active=1 order by ori_part_number, prod_year, prod_week;""".format(DB_NAME,source_supplier_id)
        df=condb(primary_query)
        primary_query_data=pd.DataFrame(data=df,columns=advance_db_columns)
        
        test1=primary_query_data.applymap(lambda x: x[0] if type(x) is bytes else x)
        test2=test1.fillna({'total_final':0,'exception':0,'nongsa_final':0,'nongsa_exception':0,'gsa_final':0,'gsa_exception':0,'system_final':0,'system_exception':0,'forecast_final':0,'forecast_exception':0,
        'nongsa_commit':0,'nongsa_commit1':0,'nongsa_commit1_accepted':'No','nongsa_commit2':0,'nongsa_commit2_accepted':'No','nongsa_commit3':0,
        'nongsa_commit3_accepted':'No','nongsa_commit4':0,'nongsa_commit4_accepted':'No',
        'gsa_commit':0,'gsa_commit1':0,'gsa_commit1_accepted':'No','gsa_commit2':0,'gsa_commit2_accepted':'No','gsa_commit3':0,
        'gsa_commit3_accepted':'No','gsa_commit4':0,'gsa_commit4_accepted':'No',
        'system_commit':0,'system_commit1':0,'system_commit1_accepted':'No','system_commit2':0,'system_commit2_accepted':'No','system_commit3':0,
        'system_commit3_accepted':'No','system_commit4':0,'system_commit4_accepted':'No',
        'forecast_commit':0,'forecast_commit1':0,'forecast_commit1_accepted':'No','forecast_commit2':0,'forecast_commit2_accepted':'No','forecast_commit3':0,
        'forecast_commit3_accepted':'No','forecast_commit4':0,'forecast_commit4_accepted':'No',
        'nongsa_adj':0,'gsa_adj':0,'forecast_adj':0,'system_adj':0,'nongsa_supply':0,'gsa_supply':0,'forecast_supply':0,'system_supply':0,
        'nongsa_advance_commit_irp':0,'gsa_advance_commit_irp':0,'forecast_advance_commit_irp':0,'system_advance_commit_irp':0,
        'cumulative_nongsa_advance_commit_irp':0,'cumulative_gsa_advance_commit_irp':0,'cumulative_forecast_advance_commit_irp':0,'cumulative_system_advance_commit_irp':0}) 
        
        
        test2.loc[test2['nongsa_commit1_accepted']==0 ,'nongsa_commit1_accepted']='No'
        test2.loc[test2['nongsa_commit1_accepted']==1 ,'nongsa_commit1_accepted']='Yes'
        test2.loc[test2['nongsa_commit2_accepted']==0 ,'nongsa_commit2_accepted']='No'
        test2.loc[test2['nongsa_commit2_accepted']==1 ,'nongsa_commit2_accepted']='Yes'
        test2.loc[test2['nongsa_commit3_accepted']==0 ,'nongsa_commit3_accepted']='No'
        test2.loc[test2['nongsa_commit3_accepted']==1 ,'nongsa_commit3_accepted']='Yes'
        test2.loc[test2['nongsa_commit4_accepted']==0 ,'nongsa_commit4_accepted']='No'
        test2.loc[test2['nongsa_commit4_accepted']==1 ,'nongsa_commit4_accepted']='Yes'
        test2.loc[test2['gsa_commit1_accepted']==0 ,'gsa_commit1_accepted']='No'
        test2.loc[test2['gsa_commit1_accepted']==1 ,'gsa_commit1_accepted']='Yes'
        test2.loc[test2['gsa_commit2_accepted']==0 ,'gsa_commit2_accepted']='No'
        test2.loc[test2['gsa_commit2_accepted']==1 ,'gsa_commit2_accepted']='Yes'
        test2.loc[test2['gsa_commit3_accepted']==0 ,'gsa_commit3_accepted']='No'
        test2.loc[test2['gsa_commit3_accepted']==1 ,'gsa_commit3_accepted']='Yes'
        test2.loc[test2['gsa_commit4_accepted']==0 ,'gsa_commit4_accepted']='No'
        test2.loc[test2['gsa_commit4_accepted']==1 ,'gsa_commit4_accepted']='Yes'
        test2.loc[test2['system_commit1_accepted']==0 ,'system_commit1_accepted']='No'
        test2.loc[test2['system_commit1_accepted']==1 ,'system_commit1_accepted']='Yes'
        test2.loc[test2['system_commit2_accepted']==0 ,'system_commit2_accepted']='No'
        test2.loc[test2['system_commit2_accepted']==1 ,'system_commit2_accepted']='Yes'
        test2.loc[test2['system_commit3_accepted']==0 ,'system_commit3_accepted']='No'
        test2.loc[test2['system_commit3_accepted']==1 ,'system_commit3_accepted']='Yes'
        test2.loc[test2['system_commit4_accepted']==0 ,'system_commit4_accepted']='No'
        test2.loc[test2['system_commit4_accepted']==1 ,'system_commit4_accepted']='Yes'
        test2.loc[test2['forecast_commit1_accepted']==0 ,'forecast_commit1_accepted']='No'
        test2.loc[test2['forecast_commit1_accepted']==1 ,'forecast_commit1_accepted']='Yes'
        test2.loc[test2['forecast_commit2_accepted']==0 ,'forecast_commit2_accepted']='No'
        test2.loc[test2['forecast_commit2_accepted']==1 ,'forecast_commit2_accepted']='Yes'
        test2.loc[test2['forecast_commit3_accepted']==0 ,'forecast_commit3_accepted']='No'
        test2.loc[test2['forecast_commit3_accepted']==1 ,'forecast_commit3_accepted']='Yes'
        test2.loc[test2['forecast_commit4_accepted']==0 ,'forecast_commit4_accepted']='No'
        test2.loc[test2['forecast_commit4_accepted']==1 ,'forecast_commit4_accepted']='Yes'
        
        
        carry_over_df1=test2[['source_supplier','org','bu','division','product_line','planner_code','planner_name','ori_part_number','cid_mapped_part_number','product_family','description',
        'item_type','build_type','cal_option','date','prod_week','prod_month','prod_year',
        'total_final','exception','nongsa_final','nongsa_exception','gsa_final','gsa_exception','system_final','system_exception','forecast_final','forecast_exception','process_type','dmp_orndmp',
        'date','nongsa_commit','nongsa_commit1_date','nongsa_commit1','nongsa_commit1_accepted','nongsa_commit2_date','nongsa_commit2','nongsa_commit2_accepted','nongsa_commit3_date','nongsa_commit3',
        'nongsa_commit3_accepted','nongsa_commit4_date','nongsa_commit4','nongsa_commit4_accepted','nongsa_cause_code','nongsa_cause_code_remark','nongsa_target_recovery','nongsa_remarks',
        'date','gsa_commit','gsa_commit1_date','gsa_commit1','gsa_commit1_accepted','gsa_commit2_date','gsa_commit2','gsa_commit2_accepted',
        'gsa_cause_code','gsa_cause_code_remark','gsa_target_recovery','gsa_remarks',
        'date','system_commit','system_commit1_date','system_commit1','system_commit1_accepted','system_commit2_date','system_commit2','system_commit2_accepted','system_commit3_date','system_commit3',
        'system_commit3_accepted','system_commit4_date','system_commit4','system_commit4_accepted','system_cause_code','system_cause_code_remark','system_target_recovery','system_remarks',
        'date','forecast_commit','forecast_commit1_date','forecast_commit1','forecast_commit1_accepted','forecast_commit2_date','forecast_commit2','forecast_commit2_accepted','forecast_commit3_date','forecast_commit3',
        'forecast_commit3_accepted','forecast_commit4_date','forecast_commit4','forecast_commit4_accepted','forecast_cause_code','forecast_cause_code_remark','forecast_target_recovery',
        'forecast_remarks']]
        carry_over_df1['CM Planner name']=None
        carry_over_df1['nongsa_final']=test2[['nongsa_original','nongsa_adj']].sum(axis=1)
        carry_over_df1['nongsa_exception']=(test2['nongsa_original']+test2['nongsa_adj'])-(test2['nongsa_supply']+test2['nongsa_advance_commit_irp']+test2['cumulative_nongsa_advance_commit_irp'])
        carry_over_df1['gsa_final']=test2[['gsa_original','gsa_adj']].sum(axis=1)
        carry_over_df1['gsa_exception']=(test2['gsa_original']+test2['gsa_adj'])-(test2['gsa_supply']+test2['gsa_advance_commit_irp']+test2['cumulative_gsa_advance_commit_irp'])
        carry_over_df1['system_final']=test2[['system_original','system_adj']].sum(axis=1)
        carry_over_df1['system_exception']=(test2['system_original']+test2['system_adj'])-(test2['system_supply']+test2['system_advance_commit_irp']+test2['cumulative_system_advance_commit_irp'])
        carry_over_df1['forecast_final']=test2[['forecast_original','forecast_adj']].sum(axis=1)
        carry_over_df1['forecast_exception']=(test2['forecast_original']+test2['forecast_adj'])-(test2['forecast_supply']+test2['forecast_advance_commit_irp']+test2['cumulative_forecast_advance_commit_irp'])
        carry_over_df1['total_final']=carry_over_df1[['nongsa_final','gsa_final','system_final','forecast_final']].sum(axis=1)
        carry_over_df1['exception']=carry_over_df1[['nongsa_exception','gsa_exception','system_exception','forecast_exception']].sum(axis=1)
        
        carry_over_df_renamed_cols=['CM','Org','BU','COE','PL','Planner Code','Planner Name','Ori Part Number','Cid Mapped Part Number','Product Family','Description',
        'Item Type','Build Type','Cal-Option','Bucket Date','Production Week','Production Month','Production Year',
        'Total KR','Total Exception','NonGSA KR','NonGSA Exception','GSA KR','GSA Exception','System KR','System Exception',
        'Forecast KR','Forecast Exception','Process Type','DMP/NDMP',
        'FirmReq (NonGSA) Commit Date','FirmReq (NonGSA) QTY','FirmReq (NonGSA2) Commit Date 2','FirmReq (NonGSA2) QTY','FirmReq (NonGSA2) Status',
        'FirmReq (NonGSA3) Commit Date 3','FirmReq (NonGSA3) QTY','FirmReq (NonGSA3) Status','FirmReq (NonGSA4) Commit Date 4','FirmReq (NonGSA4) QTY','FirmReq (NonGSA4) Status',
        'FirmReq (NonGSA4) Commit Date 5','FirmReq (NonGSA5) QTY','FirmReq (NonGSA5) Status','FirmReq (NonGSA) Cause Code','FirmReq (NonGSA) Cause Code Remark',
        'FirmReq (NonGSA) Target Recovery','FirmReq (NonGSA) Planner Remark',
        'FirmReq (GSA) Commit Date','FirmReq (GSA) QTY','FirmReq (GSA2) Commit Date 2','FirmReq (GSA2) QTY','FirmReq (GSA2) Status',
        'FirmReq (GSA3) Commit Date 3','FirmReq (GSA3) QTY','FirmReq (GSA3) Status','FirmReq (GSA) Cause Code','FirmReq (GSA) Cause Code Remark',
        'FirmReq (GSA) Target Recovery','FirmReq (GSA) Planner Remark',
        'FirmReq (System) Commit Date','FirmReq (System) QTY','FirmReq (System2) Commit Date 2','FirmReq (System2) QTY','FirmReq (System2) Status',
        'FirmReq (System3) Commit Date 3','FirmReq (System3) QTY','FirmReq (System3) Status','FirmReq (System4) Commit Date 4','FirmReq (System4) QTY','FirmReq (System4) Status',
        'FirmReq (System4) Commit Date 5','FirmReq (System5) QTY','FirmReq (System5) Status','FirmReq (System) Cause Code','FirmReq (System) Cause Code Remark',
        'FirmReq (System) Target Recovery','FirmReq (System) Planner Remark',
        'FirmReq (Forecast) Commit Date','FirmReq (Forecast) QTY','FirmReq (Forecast2) Commit Date 2','FirmReq (Forecast2) QTY','FirmReq (Forecast2) Status',
        'FirmReq (Forecast3) Commit Date 3','FirmReq (Forecast3) QTY','FirmReq (Forecast3) Status','FirmReq (Forecast4) Commit Date 4','FirmReq (Forecast4) QTY','FirmReq (Forecast4) Status',
        'FirmReq (Forecast4) Commit Date 5','FirmReq (Forecast5) QTY','FirmReq (Forecast5) Status','FirmReq (Forecast) Cause Code','FirmReq (Forecast) Cause Code Remark',
        'FirmReq (Forecast) Target Recovery','FirmReq (Forecast) Planner Remark','CM Planner name']
        carry_over_df1.columns=list(map(lambda x: x.replace('GSA',catogory_name['gsa']), carry_over_df_renamed_cols))
        carry_over_df1.columns=list(map(lambda x: x.replace('Non{}'.format(catogory_name['gsa']),catogory_name['nongsa']), carry_over_df1))
        
        
        test_df=carry_over_df1.to_csv(index=False,header=True)
        s3=boto3.client("s3")
        s3.put_object(ACL='public-read',Body=test_df,Bucket=S3_BUCKET,Key="Archival_Reports/"+source_supplier+"/IRP_commit_extract/"+source_supplier+"_IRP_"+event_data+"("+sng_time+").csv")
        object_url="""https://{}.s3-ap-southeast-1.amazonaws.com/Archival_Reports/{}/IRP_commit_extract/{}_IRP_{}({}).csv""".format(S3_BUCKET,source_supplier,source_supplier,event_data,formated_time)
        send_mail(source_supplier_id,source_supplier,report_type,test,object_url)
        
        return "Files generated and placed in IRP_commit_summary_extracts"
    
    if supplier_id is not None:
        if supplier_id == 0:
            # supplier id is not provided in event data, we loop through the list of suppliers and process all suppliers available
            suppliers=get_all_suppliers_prefix()
            for supplier in suppliers:
                if supplier:
                    supplier_id = supplier[0]
                    supplier_prefix = supplier[1].split()[0]
                    print(supplier_prefix)
                    generating_irp_csv(supplier_id,supplier_prefix)  
                else:
                    print("Individual supplier list empty")
        else:
            # supplier id provided from event data, only process that supplier
            supplier=get_supplier_prefix(supplier_id)
            supplier_prefix=supplier.split()[0]
            generating_irp_csv(supplier_id,supplier_prefix) 
    else:
        print("No supplier info to run!")
    
    