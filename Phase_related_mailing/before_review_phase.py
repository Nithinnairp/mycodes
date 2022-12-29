import pymysql
import pandas as pd
import datetime
from DB_conn import condb,condb_dict
import io
import json
import sys
import boto3
import os

my_date=datetime.date.today()
current_time=datetime.datetime.now()
year,week_num,day_of_week=my_date.isocalendar()
formated_time=(current_time+datetime.timedelta(hours=8)).strftime("%Y-%m-%d %H_%M").replace(' ','+')
month=my_date.month
DB_NAME = os.environ['DB_NAME']

def CM_full_commmit_validation(supp_id):
    current_time=datetime.datetime.now()
    date_str=current_time.strftime("%Y-%m-%d %H:%M")
    sng_time=(current_time+datetime.timedelta(hours=8)).strftime("%Y-%m-%d %H:%M")
    

    Validation_details="""Database validation for first 8 wks:Validate KR Summary Report requirement & Commit Value = curr req qty & commit qty (inc YES/NO adv commit)"""
    sql="""select id,ori_part_number,prod_week,prod_year,process_week,process_year,nongsa_supply,cumulative_nongsa_advance_commit_irp,nongsa_advance_commit_kr,
    gsa_supply,cumulative_gsa_advance_commit_irp,gsa_advance_commit_kr,forecast_supply,cumulative_forecast_advance_commit_irp,forecast_advance_commit_kr,
    system_supply,cumulative_system_advance_commit_irp,system_advance_commit_kr
    from {db_name}.forecast_plan where dmp_orndmp='DMP' and prod_week<={week} and source_supplier_id={supplier_id} and 
    (ifnull(nongsa_supply,0)!=0 or ifnull(cumulative_nongsa_advance_commit_irp,0)!=0 or ifnull(nongsa_advance_commit_kr,0)!=0 or
    ifnull(gsa_supply,0)!=0 or ifnull(cumulative_gsa_advance_commit_irp,0)!=0 or ifnull(gsa_advance_commit_kr,0)!=0 or 
    ifnull(forecast_supply,0)!=0 or ifnull(cumulative_forecast_advance_commit_irp,0)!=0 or ifnull(forecast_advance_commit_kr,0)!=0 or  
    ifnull(system_supply,0)!=0 or ifnull(cumulative_system_advance_commit_irp,0)!=0 or ifnull(system_advance_commit_kr,0)!=0);""".format(db_name=DB_NAME,week=week_num+8,supplier_id=supp_id)
    DB_records=condb_dict(sql)
    Demand_category=['Non GSA','GSA','Forecast','System']
    error_records=[]
    for i in DB_records:
        record_id=i['id'] 
        prod_week=i['prod_week']
        prod_year=i['prod_year']
        ori_part_number=i['ori_part_number']
        nongsa_supply=i['nongsa_supply'] if i['nongsa_supply']!=None else 0
        cumulative_nongsa_advance_commit_irp=i['cumulative_nongsa_advance_commit_irp'] if i['cumulative_nongsa_advance_commit_irp']!=None else 0
        nongsa_advance_commit_kr=i['nongsa_advance_commit_kr'] if i['nongsa_advance_commit_kr']!=None else 0
        gsa_supply=i['gsa_supply'] if i['gsa_supply']!=None else 0
        cumulative_gsa_advance_commit_irp=i['cumulative_gsa_advance_commit_irp'] if i['cumulative_gsa_advance_commit_irp']!=None else 0
        gsa_advance_commit_kr=i['gsa_advance_commit_kr'] if i['gsa_advance_commit_kr']!=None else 0
        forecast_supply=i['forecast_supply'] if i['forecast_supply']!=None else 0
        cumulative_forecast_advance_commit_irp=i['cumulative_forecast_advance_commit_irp'] if i['cumulative_forecast_advance_commit_irp']!=None else 0
        forecast_advance_commit_kr=i['forecast_advance_commit_kr'] if i['forecast_advance_commit_kr']!=None else 0
        system_supply=i['system_supply'] if i['system_supply']!=None else 0
        cumulative_system_advance_commit_irp=i['cumulative_system_advance_commit_irp'] if i['cumulative_system_advance_commit_irp']!=None else 0
        system_advance_commit_kr=i['system_advance_commit_kr'] if i['system_advance_commit_kr']!=None else 0
        
        
        nongsa_category='Non GSA'
        nongsa_adv_commit_query="""select sum(if(commit1_week={0},ifnull(commit1,0),0)+if(commit2_week={0},ifnull(commit2,0),0)
                        +if(commit3_week={0},ifnull(commit3,0),0)+
                        if(commit4_week={0},ifnull(commit4,0),0)) as adv_commit from {3}.advance_commit 
                        where demand_category='{1}' and ori_part_number=%s and process_week={2};""".format(prod_week,nongsa_category,week_num,DB_NAME)
        nongsa_query_result=condb_dict(nongsa_adv_commit_query,[ori_part_number])[0]
        nongsa_adv_commit_value=nongsa_query_result['adv_commit'] if nongsa_query_result['adv_commit']!=None else 0
        nongsa_curr_commit=nongsa_supply+nongsa_adv_commit_value+cumulative_nongsa_advance_commit_irp
        nongsa_kr_commit=nongsa_supply+nongsa_advance_commit_kr
        
        gsa_category='GSA'
        gsa_adv_commit_query="""select sum(if(commit1_week={0},ifnull(commit1,0),0)+if(commit2_week={0},ifnull(commit2,0),0)
                        +if(commit3_week={0},ifnull(commit3,0),0)+
                        if(commit4_week={0},ifnull(commit4,0),0)) as adv_commit from {3}.advance_commit 
                        where demand_category='{1}' and ori_part_number=%s and process_week={2};""".format(prod_week,gsa_category,week_num,DB_NAME)
        gsa_query_result=condb_dict(gsa_adv_commit_query,[ori_part_number])[0]
        gsa_adv_commit_value=gsa_query_result['adv_commit'] if gsa_query_result['adv_commit']!=None else 0
        gsa_curr_commit=gsa_supply+gsa_adv_commit_value+cumulative_gsa_advance_commit_irp
        gsa_kr_commit=gsa_supply+gsa_advance_commit_kr
        
        forecast_category='Forecast'
        forecast_adv_commit_query="""select sum(if(commit1_week={0},ifnull(commit1,0),0)+if(commit2_week={0},ifnull(commit2,0),0)
                                +if(commit3_week={0},ifnull(commit3,0),0)+
                                if(commit4_week={0},ifnull(commit4,0),0)) as adv_commit from {3}.advance_commit 
                                where demand_category='{1}' and ori_part_number=%s and process_week={2};""".format(prod_week,forecast_category,week_num,DB_NAME)
        forecast_query_result=condb_dict(forecast_adv_commit_query,[ori_part_number])[0]
        forecast_adv_commit_value=forecast_query_result['adv_commit'] if forecast_query_result['adv_commit']!=None else 0
        forecast_curr_commit=forecast_supply+forecast_adv_commit_value+cumulative_forecast_advance_commit_irp
        forecast_kr_commit=forecast_supply+forecast_advance_commit_kr
        
        system_category='System'
        system_adv_commit_query="""select sum(if(commit1_week={0},ifnull(commit1,0),0)+if(commit2_week={0},ifnull(commit2,0),0)
        +if(commit3_week={0},ifnull(commit3,0),0)+
        if(commit4_week={0},ifnull(commit4,0),0)) as adv_commit from {3}.advance_commit 
        where demand_category='{1}' and ori_part_number=%s and process_week={2};""".format(prod_week,system_category,week_num,DB_NAME)
        system_query_result=condb_dict(system_adv_commit_query,[ori_part_number])[0]
        system_adv_commit_value=system_query_result['adv_commit'] if system_query_result['adv_commit']!=None else 0
        system_curr_commit=system_supply+system_adv_commit_value+cumulative_system_advance_commit_irp
        system_kr_commit=system_supply+system_advance_commit_kr
        
        # print(i)
        if (nongsa_curr_commit!=nongsa_kr_commit) or (gsa_curr_commit!=gsa_kr_commit) or (forecast_curr_commit!=forecast_kr_commit) or (system_curr_commit!=system_kr_commit):
            # print(adv_commit_value,nongsa_supply,cumulative_nongsa_advance_commit_irp,nongsa_advance_commit_kr)
            print(i)
            error_records.append(i)
            print("not matching for nongsa ",record_id)
            
        else:
            pass
            # print("matching for nongsa ",record_id)
    print('error_records',error_records)             
    df=pd.DataFrame(error_records)
    errors=len(error_records)
    status="FAIL" if errors>0 else "OK"
    return status
        
