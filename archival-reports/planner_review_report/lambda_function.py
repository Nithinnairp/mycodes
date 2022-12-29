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
    
    def generating_planner_review_csv(supplier_id, supplier_name):
        supplier_short_name = supplier_name.split()[0]
        get_advance_col_names="""SELECT column_name from information_schema.columns where table_schema='{}' and table_name='advance_commit';""".format(DB_NAME)
        advance_col_names=list(condb(get_advance_col_names))
        advance_db_columns=list(sum(advance_col_names,()))
        primary_query="""select * from {}.advance_commit where process_week={} and source_supplier='{}' and is_delete = 0 order by ori_part_number,prod_year,prod_week;""".format(DB_NAME,week_num,supplier_name)
        df=condb(primary_query)
        primary_query_data=pd.DataFrame(data=df,columns=advance_db_columns)
        
        test1=primary_query_data.applymap(lambda x: x[0] if type(x) is bytes else x)
        test2=test1.fillna({'commit':0,'commit1':0,'commit2':0,'commit3':0,'commit4':0,'commit1_accepted':0,'commit2_accepted':0,
            'commit3_accepted':0,'commit4_accepted':0}) 
        
        # test2.loc[test2['is_delete']==0 ,'is_delete']='False'
        # test2.loc[test2['is_delete']==1 ,'is_delete']='True'
        test2.loc[test2['commit1_accepted']==0 ,'commit1_accepted']='NO'
        test2.loc[test2['commit1_accepted']==1 ,'commit1_accepted']='YES'
        test2.loc[test2['commit2_accepted']==0 ,'commit2_accepted']='NO'
        test2.loc[test2['commit2_accepted']==1 ,'commit2_accepted']='YES'
        test2.loc[test2['commit3_accepted']==0 ,'commit3_accepted']='NO'
        test2.loc[test2['commit3_accepted']==1 ,'commit3_accepted']='YES'
        test2.loc[test2['commit4_accepted']==0 ,'commit4_accepted']='NO'
        test2.loc[test2['commit4_accepted']==1 ,'commit4_accepted']='YES'
        
        test2['demand_category']=test2['demand_category'].replace('GSA',catogory_name['gsa']).replace('Non GSA',catogory_name['nongsa'])
        
        data_df1=test2[['ori_part_number','demand_category','dmp_orndmp','product_family','planner_name','prod_week','prod_year','commit_date','adjustment','cause_code','cause_code_remark',
        'commit','commit_date','commit1','commit1_date','commit1_accepted','commit2','commit2_date','commit2_accepted','commit3','commit3_date','commit3_accepted',
        'commit4','commit4_date','commit4_accepted','recommit_note']]
        
        data_df1['adjustment']=test2[['original','adjustment']].sum(axis=1)
        
        data_df1_col=['Item','Demand Category','DMP/NDMP','Product Family','Planner Name','Week','Year','Bucket Date','KR Qty','Cause Code','CauseCode Reamark',
        'Commit1','Commit1 Date','Commit2','Commit2 Date','Commit2 Accepted','Commit3','Commit3 Date','Commit3 Accepted','Commit4','Commit4 Date','Commit4 Accepted',
        'Commit5','Commit5 Date','Commit5 Accepted','Note']
        
        data_df1.columns=data_df1_col
        final_df=data_df1.to_csv(index=False,header=True)
        s3=boto3.client("s3")
        s3.put_object(ACL='public-read',Body=final_df,Bucket=S3_BUCKET,Key="Archival_Reports/"+supplier_short_name+"/Planner_review_reports/"+"Planner_review("+sng_time+").csv")
        object_url="""https://{}.s3-ap-southeast-1.amazonaws.com/Archival_Reports/{}/Planner_review_reports/Planner_review({}).csv""".format(S3_BUCKET,supplier_short_name,formated_time)
        send_email(supplier_id, supplier_short_name, object_url)
        
        return "Files generated and placed in Planner_review folder"
    
    def send_email(supplier_id_input, supplier_name, object_url):
        # Get email recipients and remove duplicates using set and convert to list back
        RECIPIENT = list(set(get_recipients('Archival_ADJ_COMM', supplier_id_input)) | set(get_recipients('Archival_Validation_common', supplier_id_input))) 
        client = boto3.client('ses',"us-east-1")
        response = client.send_email(Source=SENDER,Destination={'ToAddresses':RECIPIENT},
                        Message={
                            'Subject': {
                                'Data': "Planner Review Report for {} WW{}".format(supplier_name,week_num)
                            },
                            'Body': {
                                'Text': {
                                    'Data': """Hello,\n\r\nPlease find the Planner Review Report generated for {supplier} as of {datetime}, download the file using below url.
                                    \n{supplier} URL- {url}
                                    \n\nThanks & Regards,\nKeysightIT-Team\n\n\n""".format(datetime=sng_time,supplier=supplier_name,url=object_url)
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
            generating_planner_review_csv(supp_id,supp_name)  
        else:
            print("Individual supplier list empty")
    
    
