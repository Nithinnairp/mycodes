import json
import pandas as pd
import datetime 
import sys
import boto3
import pymysql
import logging
import requests
from DB_conn import condb,condb_dict
from urllib.parse import quote
import os

my_date=datetime.datetime.today()
year,week_num,day_of_week=my_date.isocalendar()
current_year=str(year)
current_year=current_year[2:]
DB_NAME = os.environ['DB_NAME']
DOMAIN = os.environ['DOMAIN']
S3_BUCKET = os.environ['S3_BUCKET']
SENDER=os.environ['SENDER']

def lambda_handler(event, context):
    current_time=datetime.datetime.now()
    date_str=current_time.strftime("%Y-%m-%d %H:%M")
    sng_time=(current_time+datetime.timedelta(hours=8)).strftime("%Y-%m-%d %H_%M")
    formated_time=(current_time+datetime.timedelta(hours=8)).strftime("%Y-%m-%d %H_%M").replace(' ','+')
    adj_url_list=[]
    escalation_url_list=[]
    recommit_url_list=[]
    commit_change_object_list=[]
    print(event)
    
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
    
    def isListEmpty(list):
        if list is None or not list:
            print("list empty")
            return True
        else:
            return False
    
    def send_mail(supplier_id_input,supplier_name,Planner_adjustment_url=None,Escalation_Notification_url=None,Recommit_Notification_url=None,commit_change_object_url=None):
        planner_obj = '' if isListEmpty(adj_url_list) else """\n\nPlanner Adjustment Notification- {}""".format(adj_url_list[0])
        escalation_obj = '' if isListEmpty(escalation_url_list) else """\n\nEscalation Notification- {}""".format(escalation_url_list[0])
        recommit_obj = '' if isListEmpty(recommit_url_list) else """\n\nRecommit Notification- {}""".format(recommit_url_list[0])
        commit_obj = '' if isListEmpty(commit_change_object_list) else """\n\nCommit Change Notification- {}""".format(commit_change_object_list[0])
        
        # Get email recipients and remove duplicates using set and convert to list back
        RECIPIENT = list(set(get_recipients('Archival_ADJ_COMM', supplier_id_input)) | set(get_recipients('Archival_Validation_common', supplier_id_input))) 
        client = boto3.client('ses',"us-east-1")
        response = client.send_email(Source=SENDER,Destination={'ToAddresses':RECIPIENT},
                        Message={
                            'Subject': {
                                'Data': "All Dashboard Data for {} WW{}".format(supplier_name,week_num)
                            },
                            'Body': {
                                'Text': {
                                    'Data': """Hello,\n\r\nPlease find All Dashboard data for {supplier} reports as of {datetime}, download the file using below url.
                                    {planner_url}{esc_url}{recommit_url}{commit_change_url}
                                    \n\nThanks & Regards,\nKeysightIT-Team\n\n\n""".format(supplier=supplier_name,datetime=sng_time,planner_url=planner_obj,esc_url=escalation_obj,recommit_url=recommit_obj,commit_change_url=commit_obj)
                                }
                            }
                        })     

    def generating_planner_adjustment_notification_csv(source_supplier):
        supplier_short_name = source_supplier.split()[0]
        get_adj_notifaction_col_names="""SELECT column_name from information_schema.columns where table_schema='{}' and table_name='adjustment_notification';""".format(DB_NAME)
        adj_notifaction_col_names=list(condb(get_adj_notifaction_col_names))
        adj_notifaction_col_names=list(sum(adj_notifaction_col_names,()))
        
        get_records="""select * from {}.adjustment_notification where process_week={} and process_year = {} and source_supplier='{}' order by created_at desc""".format(DB_NAME,week_num,year,source_supplier)
        result=condb(get_records)
        df1=pd.DataFrame(data=result,columns=adj_notifaction_col_names)
        test1=df1.applymap(lambda x: x[0] if type(x) is bytes else x)
        test1.loc[test1['status']=='PENDING','status']='NO'
        test1.loc[test1['status']=='COMPLETED','status']='YES'
        
        data_df1=test1[['ori_part_number','prod_week','prod_year','demand_category','new_value','adjustment_reason','planner_remark','created_by','created_at','status']]
        
        data_df1['demand_category']=data_df1['demand_category'].replace('GSA',catogory_name['gsa']).replace('Non GSA',catogory_name['nongsa']) 
        
        data_df1.columns=['Part Number','Work Week','Year','Demand Category','Adjustment Quantity','Adjustment Reason','Adjustment Remark','Adjustment By','Adjustment Date','Planner Ack Flag']
        final_df=data_df1.to_csv(index=False,header=True,date_format='%m-%d-%Y %H:%M')
        s3=boto3.client("s3")
        s3.put_object(ACL='public-read',Body=final_df,Bucket=S3_BUCKET,Key="All_Dashboard_notifications/Planner_adjustment_notifications/Planner_adjustment_notification_"+supplier_short_name+"("+sng_time+").csv")
        adj_object_url="""https://{}.s3-ap-southeast-1.amazonaws.com/All_Dashboard_notifications/Planner_adjustment_notifications/Planner_adjustment_notification_{}({}).csv""".format(S3_BUCKET,supplier_short_name,formated_time)
        adj_url_list.append(adj_object_url)
    
        
    def generating_escalation_summary_csv(source_supplier):
        supplier_short_name = source_supplier.split()[0]
        get_escalation_col_names="""SELECT column_name from information_schema.columns where table_schema='{}' and table_name='escalation';""".format(DB_NAME)
        escalation_col_names=list(condb(get_escalation_col_names))
        escalation_db_columns=list(sum(escalation_col_names,()))
        first_date=datetime.datetime.today() - datetime.timedelta(days=datetime.datetime.today().isoweekday() % 8)
        print(first_date)
        primary_query="""select * from {}.escalation where process_week={} and prod_week>={} and process_year = {} and source_supplier='{}' order by escalated_date desc""".format(DB_NAME,week_num,week_num,year,source_supplier)
        df=condb(primary_query)
        primary_query_data=pd.DataFrame(data=df,columns=escalation_db_columns)
        
        test1=primary_query_data.applymap(lambda x: x[0] if type(x) is bytes else x)
        primary_query_data=test1.fillna({'nongsa_adj':0,'gsa_adj':0,'system_adj':0,'forecast_adj':0,'full_commit':0,'acknowledgement_status':'PENDING'})
        primary_query_data.loc[primary_query_data['full_commit']==0,'full_commit']='INCOMPLETE'
        primary_query_data.loc[primary_query_data['full_commit']==1,'full_commit']='COMPLETE'
        primary_query_data.loc[primary_query_data['acknowledgement_status']=='PENDING','acknowledgement_status']='NO'
        primary_query_data.loc[primary_query_data['acknowledgement_status']=='COMPLETED','acknowledgement_status']='YES'
        primary_query_data.loc[primary_query_data['demand_type'].str.upper()=='NONGSA','escalated_reason']=primary_query_data['nongsa_adj_reason']
        primary_query_data.loc[primary_query_data['demand_type'].str.upper()=='GSA','escalated_reason']=primary_query_data['gsa_adj_reason']
        primary_query_data.loc[primary_query_data['demand_type'].str.upper()=='SYSTEM','escalated_reason']=primary_query_data['system_adj_reason']
        primary_query_data.loc[primary_query_data['demand_type'].str.upper()=='FORECAST','escalated_reason']=primary_query_data['forecast_adj_reason']
        primary_query_data.loc[primary_query_data['demand_type'].str.upper()=='NONGSA','escalated_qty']=primary_query_data['nongsa_adj']
        primary_query_data.loc[primary_query_data['demand_type'].str.upper()=='GSA','escalated_qty']=primary_query_data['gsa_adj']
        primary_query_data.loc[primary_query_data['demand_type'].str.upper()=='SYSTEM','escalated_qty']=primary_query_data['system_adj']
        primary_query_data.loc[primary_query_data['demand_type'].str.upper()=='FORECAST','escalated_qty']=primary_query_data['forecast_adj']
        primary_query_data.loc[primary_query_data['status'].str.upper()=='PENDING','status']=primary_query_data['status'].str.upper()
        primary_query_data.loc[primary_query_data['attachment_url']!=None,'attachment_url']=primary_query_data['attachment_url'].apply(lambda x:json.loads(x) if x is not None else None)
        primary_query_data.loc[primary_query_data['attachment_url']!=None,'attachment_url']=primary_query_data['attachment_url'].apply(lambda x:[(a+1,x[a]['url']) for a,b in enumerate(x)] if x is not None else None)
        
        
        data_df1=primary_query_data[['ori_part_number','prod_week','prod_year','demand_type','escalated_date','escalated_by','escalated_qty','escalated_reason','planner_remark','user_comment','acknowledgement_status','cm_comment','status','full_commit','attachment_url']]
        
        data_df1['demand_type']=data_df1['demand_type'].replace('GSA',catogory_name['gsa']).replace('NONGSA',catogory_name['nongsa'])
        
        data_df1_col=['Part Number','Work Week','Work Year','Demand Category','Escalated Date','Escalated By','Escalated Quantity','Escalation Reason','Planner Remarks','Justification','CM Ack flag','CM comment','Approval Status','Commit Status','Attachment Url']
        
        data_df1.columns=data_df1_col
        
        final_df=data_df1.to_csv(index=False,header=True,date_format='%m-%d-%Y %H:%M')
        s3=boto3.client("s3")
        s3.put_object(ACL='public-read',Body=final_df,Bucket=S3_BUCKET,Key="All_Dashboard_notifications/Escalation_summary_notification/Escalation_Notification_"+supplier_short_name+"("+sng_time+").csv")
        escalation_object_url="""https://{}.s3-ap-southeast-1.amazonaws.com/All_Dashboard_notifications/Escalation_summary_notification/Escalation_Notification_{}({}).csv""".format(S3_BUCKET,supplier_short_name,formated_time)
        escalation_url_list.append(escalation_object_url)
        
    
    def generating_recommit_summary_csv(source_supplier):
        supplier_short_name = source_supplier.split()[0]
        get_advance_commit_col_names="""SELECT column_name from information_schema.columns where table_schema='{}' and table_name='advance_commit';""".format(DB_NAME)
        advance_commit_col_names=list(condb(get_advance_commit_col_names))
        advance_commit_db_columns=list(sum(advance_commit_col_names,()))
        advance_commit_db_columns.extend(['nongsa_kr','gsa_kr','forecast_kr','system_kr','nongsa_commit','gsa_commit','forecast_commit','system_commit'])
        
        # primary_query="""select *
        # from User_App.advance_commit 
        # where recommited=1 and (is_delete=0 or is_delete is null) and process_week={} and process_year={} order by recommited_at desc;""".format(week_num,year)
        primary_query="""select ac.*,(ifnull(fp.nongsa_original,0)+ifnull(fp.nongsa_adj,0)) as 'nongsa_kr',(ifnull(fp.gsa_original,0)+ifnull(fp.gsa_adj,0)) as 'gsa_kr',
        (ifnull(fp.forecast_original,0)+ifnull(fp.forecast_adj,0)) as 'forecast_kr',
        (ifnull(fp.system_original,0)+ifnull(fp.system_adj,0)) as 'system_kr',
        (ifnull(fp.nongsa_commit,0)) as 'nongsa_commit',(ifnull(fp.gsa_commit,0)) as 'gsa_commit',
        (ifnull(fp.forecast_commit,0)) as 'forecast_commit',(ifnull(fp.system_commit,0)) as 'system_commit' from {db_name}.advance_commit as ac,{db_name}.forecast_plan as fp
        where (ac.recommited=1 and (ac.is_delete=0 or ac.is_delete is null) and ac.process_week={week} and ac.process_year={year} and ac.source_supplier='{supplier}') and 
        ((ac.ori_part_number=fp.ori_part_number)and(ac.prod_week=fp.prod_week)and(ac.prod_year=fp.prod_year))
        order by ac.recommited_at desc;""".format(db_name=DB_NAME,week=week_num,year=year,supplier=source_supplier)
        df=condb(primary_query)
        primary_query_data=pd.DataFrame(data=df,columns=advance_commit_db_columns)
        
        primary_query_data.loc[primary_query_data['status']=='PENDING','status']='NO'
        primary_query_data.loc[primary_query_data['status']=='COMPLETED','status']='YES'
        primary_query_data.loc[primary_query_data['demand_category'].str.upper()=='NONGSA','nongsa_kr']=primary_query_data['nongsa_kr']
        primary_query_data.loc[primary_query_data['demand_category'].str.upper()=='GSA','nongsa_kr']=primary_query_data['gsa_kr']
        primary_query_data.loc[primary_query_data['demand_category'].str.upper()=='FORECAST','nongsa_kr']=primary_query_data['forecast_kr']
        primary_query_data.loc[primary_query_data['demand_category'].str.upper()=='SYSTEM','nongsa_kr']=primary_query_data['system_kr']
        primary_query_data.loc[primary_query_data['demand_category'].str.upper()=='NONGSA','nongsa_commit']=primary_query_data['nongsa_commit']
        primary_query_data.loc[primary_query_data['demand_category'].str.upper()=='GSA','nongsa_commit']=primary_query_data['gsa_commit']
        primary_query_data.loc[primary_query_data['demand_category'].str.upper()=='FORECAST','nongsa_commit']=primary_query_data['forecast_commit']
        primary_query_data.loc[primary_query_data['demand_category'].str.upper()=='SYSTEM','nongsa_commit']=primary_query_data['system_commit']
        
        data_df1=primary_query_data[['ori_part_number','prod_week','prod_year','demand_category','nongsa_kr','cause_code','cause_code_remark',
        'nongsa_commit','commit_date','commit1','commit1_date','commit2','commit2_date','commit3','commit3_date','commit4','commit4_date',
        'recommit_note','recommited_by','recommited_at','status','recommit_cm_comment']]
        
        data_df1['demand_category']=data_df1['demand_category'].replace('GSA',catogory_name['gsa']).replace('Non GSA',catogory_name['nongsa'])
        
        data_df1_col=['Part Number','Work Week','Work Year','Demand Category','KR Qty','Cause Code','Cause Code Remark',
        'Commit1','Commit1 Date','Commit2','Commit2 Date','Commit3','Commit3 Date','Commit4','Commit4 Date','Commit5','Commit5 Date',
        'Recommit Note','Requestor','Recommit Date','CM Ack Flag','CM comment']
        
        data_df1.columns=data_df1_col
        final_df=data_df1.to_csv(index=False,header=True,date_format='%d-%m-%Y %H:%M')
        s3=boto3.client("s3")
        s3.put_object(ACL='public-read',Body=final_df,Bucket=S3_BUCKET,Key="All_Dashboard_notifications/Recommit_summary_notification/Recommit_Notification_"+supplier_short_name+"("+sng_time+").csv")
        recommit_object_url="""https://{}.s3-ap-southeast-1.amazonaws.com/All_Dashboard_notifications/Recommit_summary_notification/Recommit_Notification_{}({}).csv""".format(S3_BUCKET,supplier_short_name,formated_time)
        recommit_url_list.append(recommit_object_url)
    
    def generating_commit_change_notification(source_supplier):
        encoded_supplier_name = quote(source_supplier)
        supplier_short_name = source_supplier.split()[0]
        try:
            get_phase="""{}/api/commit-advance-commit-notification?page=0&size=2000&sort=createdAt,desc&sourceSupplier.in={}""".format(DOMAIN,encoded_supplier_name)
            response=requests.get(get_phase)
            # print(response.content)
            phase_object=json.loads(response.content)
            
            col=list(phase_object[0].keys())
            json_df=pd.DataFrame(data=phase_object,columns=col)
            json_df.loc[json_df['status']=='PENDING','status']='NO'
            json_df.loc[json_df['status']=='COMPLETED','status']='YES'
            json_df['kr_qty']=json_df[['original','adjustment']].sum(axis=1)
            data_df1=json_df[['oriPartNumber','demandCategory','changeStatus','commitDate','prodWeek','prodYear','kr_qty','commit',
                      'commitDate','commit1','commit1Date','commit2','commit2Date','commit3','commit3Date','commit4','commit4Date',
                      'causeCode','causeCodeRemark','createdBy','createdAt','status']]
                      
            data_df1['demand_category']=data_df1['demand_category'].replace('GSA',catogory_name['gsa']).replace('Non GSA',catogory_name['nongsa'])          
            
            data_df1.columns=['Part Number','Demad Category','Status','Date','Work Week','Work Year','KR Qty','Commit 1',
                      'Date 1','Commit 2','Date 2','Commit 3','Date 3','Commit 4','Date 4','Commit 5','Date 5',
                      'Cause Code','Cause Code Remark','Commited By','Commited Date','Planner Ack Flag ']
            final_df=data_df1.to_csv(index=False,header=True,date_format='%d-%m-%Y %H:%M')
            s3=boto3.client("s3")
            s3.put_object(ACL='public-read',Body=final_df,Bucket=S3_BUCKET,Key="All_Dashboard_notifications/Commit_Change_notifications/Commit_Change_notification_"+supplier_short_name+"("+sng_time+").csv")
            commit_change_object_url="""https://{}.s3-ap-southeast-1.amazonaws.com/All_Dashboard_notifications/Commit_Change_notifications/Commit_Change_notification_{}({}).csv""".format(S3_BUCKET,supplier_short_name,formated_time)
            commit_change_object_list.append(commit_change_object_url)
        except:
            commit_change_object_list.append(None)
                  
        
    # Get all suppliers and run through each of them
    suppliers=get_all_suppliers()
    for supplier in suppliers:
        if supplier:
            adj_url_list=[]
            escalation_url_list=[]
            recommit_url_list=[]
            commit_change_object_list=[]
            supp_id=supplier['source_supplier_id']
            supp_name=supplier['source_supplier']
            supplier_short_name = supp_name.split()[0]
            print(supp_id,"-",supp_name)
            generating_planner_adjustment_notification_csv(supp_name)
            generating_escalation_summary_csv(supp_name)
            generating_recommit_summary_csv(supp_name) 
            generating_commit_change_notification(supp_name)
            send_mail(supp_id,supplier_short_name)
        else:
            print("Individual supplier list empty")    
    
    
    