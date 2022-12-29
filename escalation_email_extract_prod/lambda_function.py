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

def lambda_handler(event, context):
    current_time=datetime.datetime.now()
    date_str=current_time.strftime("%Y-%m-%d %H_%M")
    
    def get_all_suppliers():
        get_suppliers_query="""select source_supplier_id,source_supplier from {db_name}.forecast_plan group by source_supplier;""".format(db_name=DB_NAME)
        suppliers_list=condb_dict(get_suppliers_query)
        return suppliers_list
    
    def generating_escalation_summary_csv(source_supplier):
        supplier_short_name = source_supplier.split()[0]
        get_escalation_col_names="""SELECT column_name from information_schema.columns where table_schema='{}' and table_name='escalation';""".format(DB_NAME)
        escalation_col_names=list(condb(get_escalation_col_names))
        escalation_db_columns=list(sum(escalation_col_names,()))
        escalation_db_columns.append('ww')
        # date for start of week
        first_date=datetime.datetime.today() - datetime.timedelta(days=datetime.datetime.today().isoweekday() % 8)
        print(first_date)
        
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
        
        primary_query="""select *,concat(" ",prod_week,'/',prod_year) as ww
        from {}.escalation where status='APPROVED' and source_supplier='{}'
        and escalated_date>='{}' order by escalated_date desc;""".format(DB_NAME,source_supplier,first_date)
        df=condb(primary_query)
        primary_query_data=pd.DataFrame(data=df,columns=escalation_db_columns)
        primary_query_data['escalated_date']=pd.to_datetime(primary_query_data['escalated_date'], format="%m/%d/%Y %H:%M")
        
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
        
        data_df1=test2[['ori_part_number','ww','demand_type','escalated_date','escalated_by','escalated_qty','escalated_reason','planner_remark','acknowledgement_status','cm_comment','status','full_commit']]
        
        data_df1['demand_type']=data_df1['demand_type'].replace('GSA',catogory_name['gsa']).replace('NON GSA',catogory_name['nongsa'])
        
        data_df1_col=['Part Number','Work Week','Demand Category','Escalated Date','Escalated By','Escalated Quantity','Escalation Reason','Planner Remarks','CM Ack flag','CM comment','Approval Status','Commit Status']
        
        data_df1.columns=data_df1_col
        
        
        
        final_df=data_df1.to_csv("s3://"+S3_BUCKET+"/Escalation_Summary/Escalation_summary_"+supplier_short_name+"("+date_str+").csv",index=False,header=True,date_format='%m/%d/%Y %I:%M %p')
        # s3=boto3.client("s3")
        # s3.put_object(Body=final_df,Bucket='b2b-irp-analytics',Key="Escalation_Summary/"+"Escalation_summary("+date_str+").csv",ContentEncoding='UTF-8')
        
        return print("Files generated and placed in Escalated_summary")
        
    # Get all suppliers and run through each of them
    suppliers=get_all_suppliers()
    for supplier in suppliers:
        if supplier:
            supp_id=supplier['source_supplier_id']
            supp_name=supplier['source_supplier']
            print(supp_id,"-",supp_name)
            generating_escalation_summary_csv(supp_name)
        else:
            print("Individual supplier list empty") 