
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
    sng_time=(current_time+datetime.timedelta(hours=8)).strftime("%Y-%m-%d %H_%M")
    
    def get_all_suppliers():
        get_suppliers_query="""select source_supplier_id,source_supplier from {db_name}.forecast_plan group by source_supplier;""".format(db_name=DB_NAME)
        suppliers_list=condb_dict(get_suppliers_query)
        return suppliers_list
    
    def generating_recommit_summary_csv(source_supplier):
        supplier_short_name = source_supplier.split()[0]
        get_advance_commit_col_names="""SELECT column_name from information_schema.columns where table_schema='{}' and table_name='advance_commit';""".format(DB_NAME)
        advance_commit_col_names=list(condb(get_advance_commit_col_names))
        advance_commit_db_columns=list(sum(advance_commit_col_names,()))
        advance_commit_db_columns.append('ww')
        advance_commit_db_columns.append('cm_comments')
        
        primary_query="""select *,concat(' ',prod_week,'/',prod_year) as ww,null as cm_comments
        from {}.advance_commit 
        where (recommited=1) and is_delete=0 and process_week={} and process_year={} and source_supplier='{}' order by recommited_at desc;""".format(DB_NAME,week_num,year, source_supplier)
        df=condb(primary_query)
        primary_query_data=pd.DataFrame(data=df,columns=advance_commit_db_columns)
        
        data_df1=primary_query_data[['ori_part_number','ww','demand_category','recommited_by','recommit_note','recommited_at','status','cm_comments']]
        data_df1_col=['Ori Part Number','Work Week','Demand Category','Recommited By','Recommit Note','Recommit Date','CM Ack Flag','CM Comment']
        data_df1.columns=data_df1_col
        final_df=data_df1.to_csv("s3://"+S3_BUCKET+"/Recommit_Summary/Recommit_Notification_"+supplier_short_name+"("+sng_time+").csv",index=False,header=True,date_format='%m/%d/%Y %H:%M %p')
        

    # Get all suppliers and run through each of them
    suppliers=get_all_suppliers()
    for supplier in suppliers:
        if supplier:
            supp_id=supplier['source_supplier_id']
            supp_name=supplier['source_supplier']
            print(supp_id,"-",supp_name)
            generating_recommit_summary_csv(supp_name)
        else:
            print("Individual supplier list empty") 