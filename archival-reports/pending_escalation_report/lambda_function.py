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
    date_str=current_time.strftime("%Y-%m-%d %H:%M")
    sng_time=(current_time+datetime.timedelta(hours=8)).strftime("%Y-%m-%d %H_%M")
    formated_time=(current_time+datetime.timedelta(hours=8)).strftime("%Y-%m-%d %H_%M").replace(' ','+')
    
    def get_all_suppliers():
        get_suppliers_query="""select source_supplier from {db_name}.escalation group by source_supplier;""".format(db_name=DB_NAME)
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
    
    def generating_escalation_summary_csv(source_supplier):
        supplier_short_name = source_supplier.split()[0]
        get_escalation_col_names="""SELECT column_name from information_schema.columns where table_schema='{}' and table_name='escalation';""".format(DB_NAME)
        escalation_col_names=list(condb(get_escalation_col_names))
        escalation_db_columns=list(sum(escalation_col_names,()))
        escalation_db_columns.append('ww')
        first_date=datetime.datetime.today() - datetime.timedelta(days=datetime.datetime.today().isoweekday() % 8)
        print(first_date)
        
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

        
        get_manager="""select manager_email from {}.escalation where UPPER(status)!='APPROVED' and source_supplier='{}' group by manager_email""".format(DB_NAME,source_supplier)
        result1=condb(get_manager)
        manager_email_df=pd.DataFrame(data=result1,columns=['manager_email'])
        # print(manager_email_df)
        
        count=0
        for x,y in manager_email_df.iterrows():
            date_str=current_time.strftime("%Y-%m-%d %H:%M")
            manager_email=str(y.manager_email)
            print(manager_email)
            # Get email recipients
            RECIPIENT = get_recipients('Archival_Validation_common', supplier_short_name)
            count+=1
            dummy_number=str(count)
            print(manager_email,dummy_number)
            primary_query="""select *,concat(" ",prod_week,'/',prod_year) as ww
            from {}.escalation where UPPER(status)!='APPROVED' and process_week={} and process_year={} and manager_email='{}' and source_supplier='{}' order by escalated_date desc;""".format(DB_NAME,week_num,current_year,manager_email,source_supplier)
            print(primary_query)
            df=condb(primary_query)
            primary_query_data=pd.DataFrame(data=df,columns=escalation_db_columns)
            test1=primary_query_data.applymap(lambda x: x[0] if type(x) is bytes else x)
            test2=test1.fillna({'nongsa_adj':0,'gsa_adj':0,'system_adj':0,'forecast_adj':0,'full_commit':0,'acknowledgement_status':'PENDING'})
            test2.loc[test2['full_commit']==0,'full_commit']='INCOMPLETE'
            test2.loc[test2['full_commit']==1,'full_commit']='COMPLETE'
            test2.loc[test2['acknowledgement_status']=='PENDING','acknowledgement_status']='NO'
            test2.loc[test2['acknowledgement_status']=='COMPLETED','acknowledgement_status']='YES'
            test2.loc[test2['demand_type'].str.upper()=='NONGSA','escalated_reason']=test2['nongsa_adj_reason']
            test2.loc[test2['demand_type'].str.upper()=='GSA','escalated_reason']=test2['gsa_adj_reason']
            test2.loc[test2['demand_type'].str.upper()=='SYSTEM','escalated_reason']=test2['system_adj_reason']
            test2.loc[test2['demand_type'].str.upper()=='FORECAST','escalated_reason']=test2['forecast_adj_reason']
            test2.loc[test2['demand_type'].str.upper()=='NONGSA','escalated_qty']=test2['nongsa_adj']
            test2.loc[test2['demand_type'].str.upper()=='GSA','escalated_qty']=test2['gsa_adj']
            test2.loc[test2['demand_type'].str.upper()=='SYSTEM','escalated_qty']=test2['system_adj']
            test2.loc[test2['demand_type'].str.upper()=='FORECAST','escalated_qty']=test2['forecast_adj']
            test2.loc[test2['attachment_url']!=None,'attachment_url']=test2['attachment_url'].apply(lambda x:json.loads(x) if x is not None else None)
            test2.loc[test2['attachment_url']!=None,'attachment_url']=test2['attachment_url'].apply(lambda x:[(a+1,x[a]['url']) for a,b in enumerate(x)] if x is not None else None)
            
            
            data_df1=test2[['manager_email','ori_part_number','ww','demand_type','escalated_date','escalated_by','escalated_qty','escalated_reason','planner_remark','user_comment','acknowledgement_status','cm_comment','status','full_commit','attachment_url']]
            
            data_df1['demand_type']=data_df1['demand_type'].replace('GSA',catogory_name['gsa']).replace('Non GSA',catogory_name['nongsa'])
            
            data_df1_col=['manager_email','Part Number','Work Week','Demand Category','Escalated Date','Escalated By','Escalated Quantity','Escalation Reason','Planner Remarks','Justification','CM Ack flag','CM comment','Approval Status','Commit Status','Attachment Url']
            
            data_df1.columns=data_df1_col
            records_count=data_df1.count()[0]
            if records_count>0:
                RECIPIENT.append(manager_email)
                print(data_df1.columns)
                object_url_list=[]
                final_df=data_df1.to_csv(index=False,header=True)
                s3=boto3.client("s3")
                s3.put_object(ACL='public-read',Body=final_df,Bucket=S3_BUCKET,Key="Escalation_Pending/Escalatation_Pending_Approval_"+supplier_short_name+"("+dummy_number+'_'+sng_time+").csv")
                object_url="""https://{}.s3-ap-southeast-1.amazonaws.com/Escalation_Pending/Escalatation_Pending_Approval_{}({}_{}).csv""".format(S3_BUCKET,supplier_short_name,dummy_number,formated_time)
                
                
                print('############# mail sent ##################')
                client = boto3.client('ses',"us-east-1")
                print(RECIPIENT)
                response = client.send_email(Source=SENDER,Destination={'ToAddresses':RECIPIENT},
                                Message={
                                    'Subject': {
                                        'Data': "Escalation Pending for approval for {} WW{}".format(supplier_short_name, week_num)
                                    },
                                    'Body': {
                                        'Text': {
                                            'Data': """Hello,\n\r\nFind the attachment with Escalations pending for your approval as of {}, download the file using below url.
                                            \n\ndownload-URL- {} {}
                                            \n\nThanks & Regards,\nKeysightIT-Team\n\n\n""".format(sng_time,object_url,manager_email)
                                        }
                                    }
                                })
                                
        return print("Files generated and placed in Escalated_summary")
        
    # Get all suppliers and run through each of them
    suppliers=get_all_suppliers()
    for supplier in suppliers:
        if supplier:
            supp_name=supplier['source_supplier']
            print(supp_name)
            generating_escalation_summary_csv(supp_name)
        else:
            print("Individual supplier list empty")
    