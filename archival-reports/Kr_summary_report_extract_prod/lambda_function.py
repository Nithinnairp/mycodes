import json
import pandas as pd
import numpy as np
import datetime 
import sys
import boto3
import pymysql
import logging
import itertools 
from DB_conn import condb, condb_dict
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
    object_url_list=[]
    
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
    
    def rename_col(col_position):
        count=0
        for i in range(11,19):
            todayy = datetime.date.today()
            monday=todayy - datetime.timedelta(days=todayy.weekday())
            date_stamp=(monday + datetime.timedelta(days=7*count)).strftime("%Y:%m:%d")
            a,b,c=((monday + datetime.timedelta(days=7*count)).isocalendar())
            col[i][0] = 'Week' + str(b) + '/' + str(a)[2:] + str('_kr')
            col[i][1] = 'Week' + str(b) + '/' + str(a)[2:] + str('_Commit')
            col[i][2] = 'Week' + str(b) + '/' + str(a)[2:] + str('_Delta')
            col[i][3] = 'Week' + str(b) + '/' + str(a)[2:] + str('_Cumu_Delta')
            count+=1
    
    def get_all_suppliers():
        get_suppliers_query="""select source_supplier_id,source_supplier from {db_name}.forecast_plan group by source_supplier;""".format(db_name=DB_NAME)
        suppliers_list=condb_dict(get_suppliers_query)
        return suppliers_list

    def get_recipients(column, supplier_id):
        query="""select {column} from {db_name}.active_suppliers where id = {id};""".format(column=column, db_name=DB_NAME, id=supplier_id)
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
    
    col=['CM','ORG','BU','COE','PL','Planner_Code','Planner_Name','Ori_Part_Number','Cid_Mapped_PartNumber','Product_Family','demand_category',
                        ['Week1_KR','Week1_Commit','Week1_Delta','Week1_Cum_Delta'],
                        ['Week2_KR','Week2_Commit','Week2_Delta','Week2_Cum_Delta'],
                        ['Week3_KR','Week3_Commit','Week3_Delta','Week3_Cum_Delta'],
                        ['Week4_KR','Week4_Commit','Week4_Delta','Week4_Cum_Delta'],
                        ['Week5_KR','Week5_Commit','Week5_Delta','Week5_Cum_Delta'],
                        ['Week6_KR','Week6_Commit','Week6_Delta','Week6_Cum_Delta'],
                        ['Week7_KR','Week7_Commit','Week7_Delta','Week7_Cum_Delta'],
                        ['Week8_KR','Week8_Commit','Week8_Delta','Week8_Cum_Delta'],
                        'Total_Kr','Total_Commit','Total_Delta',
                        'Planner_Remarks','Cause_Code','Cause_Code_Remark',
                        'FG_Qty','Intransit_Qty','WIP_Qty','Pending_Assy','Assy','Aging',
                        'Button_Up','Test','FVMI','Pack','FG_Logistic','Debug','Store_Core']
    
    get_dmp_week="""select distinct prod_week,prod_year from {db_name}.forecast_plan where dmp_orndmp='DMP' order by prod_week""".format(db_name=DB_NAME)
    week_year=condb(get_dmp_week)
    df=pd.DataFrame(week_year,columns=['prod_week','prod_year'])
    dmp_week=[]
    dmp_year=[]
    for x,y in df.iterrows():
        dmp_week.append(y.prod_week)
        dmp_year.append(y.prod_year)
    dmp_year=set(dmp_year)    
    print(dmp_week,dmp_year)
    x=[11,12,13,14,15,16,17,18]
    rename_col(x)
    final_col=[]
    print(final_col)
    def removeNestings(x): 
        for i in x: 
            if type(i) == list: 
                removeNestings(i) 
            else: 
                final_col.append(i)
    removeNestings(col)
    print(final_col)
    drop_view="drop view {}.Kr_summary".format(DB_NAME)
    condb(drop_view)
    procedure_call="call {}.Kr_summary_view".format(DB_NAME)
    condb(procedure_call)
    
    def send_mail(source_supplier, object_url, supplier_id_input):
        # Get email recipients and remove duplicates using set and convert to list back
        RECIPIENT = list(set(get_recipients('Archival_ADJ_COMM', supplier_id_input)) | set(get_recipients('Archival_Validation_common', supplier_id_input)))  
        client = boto3.client('ses',"us-east-1")
        response = client.send_email(Source=SENDER,Destination={'ToAddresses':RECIPIENT},
                        Message={
                            'Subject': {
                                'Data': "KR summary report for {} WW{}".format(source_supplier,week_num)
                            },
                            'Body': {
                                'Text': {
                                    'Data': """Hello,\n\r\nPlease find the KR summary report generated for all suppliers as of {}, download the file using below url.
                                    \n\n{}_URL- {}
                                    \n\nThanks & Regards,\nKeysightIT-Team\n\n\n""".format(sng_time,source_supplier,object_url)
                                }
                            }
                        })
                    

    
    def generating_kr_summary_csv(source_supplier_id,source_supplier_name):
        supplier_short_name = source_supplier_name.split()[0]
        primary_query="""select CM,ORG,BU,COE,PL,Planner_Code,Planner_Name,Ori_Part_Number,Cid_Mapped_PartNumber,Product_Family,demand_category,
                    Week1_KR,Week1_Commit,Week1_Delta,Week1_Delta as Week1_Cum_Delta,
                    Week2_KR,Week2_Commit,Week2_Delta,ifnull(Week1_Delta,0)+ifnull(Week2_Delta,0) as Week2_Cum_Delta,
                    Week3_KR,Week3_Commit,Week3_Delta,ifnull(Week1_Delta,0)+ifnull(Week2_Delta,0)+ifnull(Week3_Delta,0) as Week3_Cum_Delta,
                    Week4_KR,Week4_Commit,Week4_Delta,ifnull(Week1_Delta,0)+ifnull(Week2_Delta,0)+ifnull(Week3_Delta,0)+ifnull(Week4_Delta,0) as Week4_Cum_Delta,
                    Week5_KR,Week5_Commit,Week5_Delta,ifnull(Week1_Delta,0)+ifnull(Week2_Delta,0)+ifnull(Week3_Delta,0)+ifnull(Week4_Delta,0)+ifnull(Week5_Delta,0) as Week5_Cum_Delta,
                    Week6_KR,Week6_Commit,Week6_Delta,ifnull(Week1_Delta,0)+ifnull(Week2_Delta,0)+ifnull(Week3_Delta,0)+ifnull(Week4_Delta,0)+ifnull(Week5_Delta,0)+ifnull(Week6_Delta,0) as Week6_Cum_Delta,
                    Week7_KR,Week7_Commit,Week7_Delta,ifnull(Week1_Delta,0)+ifnull(Week2_Delta,0)+ifnull(Week3_Delta,0)+ifnull(Week4_Delta,0)+ifnull(Week5_Delta,0)+ifnull(Week6_Delta,0)+ifnull(Week7_Delta,0) as Week7_Cum_Delta,
                    Week8_KR,Week8_Commit,Week8_Delta,ifnull(Week1_Delta,0)+ifnull(Week2_Delta,0)+ifnull(Week3_Delta,0)+ifnull(Week4_Delta,0)+ifnull(Week5_Delta,0)+ifnull(Week6_Delta,0)+ifnull(Week7_Delta,0)+ifnull(Week8_Delta,0) as Week8_Cum_Delta,
                    (ifnull(Week1_KR,0)+ifnull(Week2_KR,0)+ifnull(Week3_KR,0)+ifnull(Week4_KR,0)+ifnull(Week5_KR,0)+ifnull(Week6_KR,0)+ifnull(Week7_KR,0)+ifnull(Week8_KR,0)) as Total_Kr,
                    (ifnull(Week1_Commit,0)+ifnull(Week2_Commit,0)+ifnull(Week3_Commit,0)+ifnull(Week4_Commit,0)+ifnull(Week5_Commit,0)+ifnull(Week6_Commit,0)+ifnull(Week7_Commit,0)+ifnull(Week8_Commit,0)) as Total_Commit,
                    (ifnull(Week1_Delta,0)+ifnull(Week2_Delta,0)+ifnull(Week3_Delta,0)+ifnull(Week4_Delta,0)+ifnull(Week5_Delta,0)+ifnull(Week6_Delta,0)+ifnull(Week7_Delta,0)+ifnull(Week8_Delta,0)) as Total_Delta,
                    concat(ifnull(W1_pr,''),' ',ifnull(W2_pr,''),' ',ifnull(W3_pr,''),' ',ifnull(W4_pr,''),' ',ifnull(W5_pr,''),
                    ' ',ifnull(W6_pr,''),' ',ifnull(W7_pr,''),' ',ifnull(W8_pr,'')) as planner_remark,
                    concat(ifnull(W1_cc,''),' ',ifnull(W2_cc,''),' ',ifnull(W3_cc,''),' ',ifnull(W4_cc,''),' ',ifnull(W5_cc,''),' ',
                    ifnull(W6_cc,''),' ',ifnull(W7_cc,''),' ',ifnull(W8_cc,'')) as cause_code,
                    concat(ifnull(W1_ccr,''),' ',ifnull(W2_ccr,''),' ',ifnull(W3_ccr,''),' ',ifnull(W4_ccr,''),' ',ifnull(W5_ccr,''),' ',
                    ifnull(W6_ccr,''),' ',ifnull(W7_ccr,''),' ',ifnull(W8_ccr,'')) as cause_code_remark,
                    FG_Qty,Intransit_Qty,WIP_Qty,Pending_Assy,Assy,Aging,
                    Button_Up,Test,FVMI,Pack,FG_Logistic,Debug,Store_Core
                    from {}.Kr_summary where source_supplier_id={};""".format(DB_NAME,source_supplier_id)
                    
        df=condb(primary_query)
        primary_query_data=pd.DataFrame(data=df,columns=final_col)
        
        for i in primary_query_data.columns:
            if primary_query_data[i].dtype=='float64':
                primary_query_data.fillna({i:0},inplace=True)
                
        primary_query_data['demand_category']=primary_query_data['demand_category'].replace('GSA',catogory_name['gsa']).replace('NONGSA',catogory_name['nongsa'])
        cols=['FG_Qty','Intransit_Qty','WIP_Qty','Pending_Assy','Assy','Aging','Button_Up','Test','FVMI','Pack','FG_Logistic','Debug','Store_Core']
        primary_query_data[cols]=primary_query_data[cols].replace(['0',0],np.nan)        
        final_df=primary_query_data.to_csv(index=False,header=True)
        s3=boto3.client("s3")
        s3.put_object(ACL='public-read',Body=final_df,Bucket=S3_BUCKET,Key="Archival_Reports/"+supplier_short_name+"/Kr_summary/Kr_summary_"+supplier_short_name+"("+sng_time+").csv")
        object_url="""https://{}.s3-ap-southeast-1.amazonaws.com/Archival_Reports/{}/Kr_summary/Kr_summary_{}({}).csv""".format(S3_BUCKET,supplier_short_name,supplier_short_name,formated_time)
        send_mail(supplier_short_name,object_url,source_supplier_id)
    

    # Get all suppliers and run through each of them
    suppliers=get_all_suppliers()
    for supplier in suppliers:
        if supplier:
            supp_id=supplier['source_supplier_id']
            supp_name=supplier['source_supplier']
            print(supp_id,"-",supp_name)
            generating_kr_summary_csv(supp_id,supp_name)  
        else:
            print("Individual supplier list empty")
    
    