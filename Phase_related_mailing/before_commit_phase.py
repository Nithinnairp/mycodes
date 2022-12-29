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
current_time=datetime.datetime.now()
year,week_num,day_of_week=my_date.isocalendar()
formated_time=(current_time+datetime.timedelta(hours=8)).strftime("%Y-%m-%d %H_%M").replace(' ','+')
month=my_date.month
DB_NAME = os.environ['DB_NAME']

def cid_map_part_number_validation(supp_id):
    sql="""select count(*) as count from {db_name}.forecast_plan where cid_mapped_part_number is null and source_supplier_id={supplier_id};""".format(db_name=DB_NAME,supplier_id=supp_id)
    result=condb_dict(sql)[0]
    error_count=result['count']
    status="Fail" if error_count>0 else "OK"
    return status
    

def Exception_validation(supp_id):
    sql1="""select count(*) as count from {db_name}.forecast_plan
    where active=1 and dmp_orndmp='DMP' and  process_type='Exception' and source_supplier_id={supplier_id} and
    (((ifnull(forecast_original,0)+ifnull(forecast_adj,0))-(ifnull(forecast_supply,0)+ifnull(forecast_advance_commit_irp,0)+ifnull(cumulative_forecast_advance_commit_irp,0)))=0 and
    ((ifnull(system_original,0)+ifnull(system_adj,0))-(ifnull(system_supply,0)+ifnull(system_advance_commit_irp,0)+ifnull(cumulative_system_advance_commit_irp,0)))=0 and
    ((ifnull(gsa_original,0)+ifnull(gsa_adj,0))-(ifnull(gsa_supply,0)+ifnull(gsa_advance_commit_irp,0)+ifnull(cumulative_gsa_advance_commit_irp,0)))=0 and
    ((ifnull(nongsa_original,0)+ifnull(nongsa_adj,0))-(ifnull(nongsa_supply,0)+ifnull(nongsa_advance_commit_irp,0)+ifnull(cumulative_nongsa_advance_commit_irp,0)))=0);""".format(db_name=DB_NAME,supplier_id=supp_id)
    
    result1=condb_dict(sql1)
    
    sql2="""select count(*) as count from {db_name}.forecast_plan where active=1 and dmp_orndmp='NDMP' and  process_type='Exception' and source_supplier_id={supplier_id};""".format(db_name=DB_NAME,supplier_id=supp_id)
    result2=condb_dict(sql2)
    
    sql3="""select count(*) as count from {db_name}.forecast_plan where dmp_orndmp='NDMP' and source_supplier_id={supplier_id} and
    (nongsa_advance_commit_kr>0 or nongsa_advance_commit_irp>0 or cumulative_nongsa_advance_commit_irp>0 or
    gsa_advance_commit_kr>0 or gsa_advance_commit_irp>0 or cumulative_gsa_advance_commit_irp>0 or
    forecast_advance_commit_kr>0 or forecast_advance_commit_irp>0 or cumulative_forecast_advance_commit_irp>0 or
    system_advance_commit_kr>0 or system_advance_commit_irp>0 or cumulative_system_advance_commit_irp>0 or
    nongsa_commit>0 or gsa_commit>0 or forecast_commit>0 or system_commit>0 or
    nongsa_supply>0 or gsa_supply>0 or forecast_supply>0 or system_supply>0);""".format(db_name=DB_NAME,supplier_id=supp_id)
    result3=condb_dict(sql3)
    status="OK" if result1[0]['count']==0 and result2[0]['count']==0 and result3[0]['count']==0 else "Fail"
    return status


        
def CM_COMMIT_CF(supp_id):
    sql1="""select 'last_week' as 'week',(sum(forecast_supply)+sum(forecast_advance_commit_irp)+sum(cumulative_forecast_advance_commit_irp)) as 'forecast_commit',
    (sum(nongsa_supply)+sum(nongsa_advance_commit_irp)+sum(cumulative_nongsa_advance_commit_irp)) as 'nongsa_commit',
    (sum(gsa_supply)+sum(gsa_advance_commit_irp)+sum(cumulative_gsa_advance_commit_irp)) as 'gsa_commit',
    (sum(system_supply)+sum(system_advance_commit_irp)+sum(cumulative_system_advance_commit_irp)) as 'system_commit'
    from {db_name}.forecast_plan_temp 
    where ((prod_week>{week} and prod_year={year}) or (prod_year>{year})) and source_supplier_id={supplier_id}
     union
    select 'curr_week' as 'week',(sum(forecast_supply)+sum(forecast_advance_commit_irp)+sum(cumulative_forecast_advance_commit_irp)) as 'forecast_commit',
    (sum(nongsa_supply)+sum(nongsa_advance_commit_irp)+sum(cumulative_nongsa_advance_commit_irp)) as 'nongsa_commit',
    (sum(gsa_supply)+sum(gsa_advance_commit_irp)+sum(cumulative_gsa_advance_commit_irp)) as 'gsa_commit',
    (sum(system_supply)+sum(system_advance_commit_irp)+sum(cumulative_system_advance_commit_irp)) as 'system_commit'
    from {db_name}.forecast_plan
    where source_supplier_id={supplier_id};""".format(db_name=DB_NAME,week=week_num-1,year=str(year)[2:],supplier_id=supp_id)
    # print(sql1)
    result=condb_dict(sql1)
    last_forecast_commit=result[0]['forecast_commit']
    last_nongsa_commit=result[0]['nongsa_commit']
    last_gsa_commit=result[0]['gsa_commit']
    last_system_commit=result[0]['system_commit']
    curr_forecast_commit=result[1]['forecast_commit']
    curr_nongsa_commit=result[1]['nongsa_commit']
    curr_gsa_commit=result[1]['gsa_commit']
    curr_system_commit=result[1]['system_commit']
    status="Fail" if last_forecast_commit!=curr_forecast_commit or last_nongsa_commit!=curr_nongsa_commit or last_gsa_commit!=curr_gsa_commit or last_system_commit!=curr_system_commit else "OK"
    return result,status
    
def kr_qty_validation(supp_id):
    sql1="""select ori_part_number,prod_week,prod_year,nongsa_original,nongsa_adj,
    (ifnull(nongsa_original,0)+ifnull(nongsa_adj,0)) as total_nongsa,gsa_original,gsa_adj,
    (ifnull(gsa_original,0)+ifnull(gsa_adj,0)) as total_gsa,forecast_original,forecast_adj,
    (ifnull(forecast_original,0)+ifnull(forecast_adj,0)) as total_forecast,system_original,system_adj,
    (ifnull(system_original,0)+ifnull(system_adj,0)) as total_system,
    ((ifnull(nongsa_original,0)+ifnull(nongsa_adj,0))+(ifnull(gsa_original,0)+ifnull(gsa_adj,0))+
    (ifnull(forecast_original,0)+ifnull(forecast_adj,0))+(ifnull(system_original,0)+ifnull(system_adj,0))) as total_demand
    from {db_name}.forecast_plan 
    where 
    ((ifnull(nongsa_original,0)+ifnull(nongsa_adj,0))+(ifnull(gsa_original,0)+ifnull(gsa_adj,0))+
    (ifnull(forecast_original,0)+ifnull(forecast_adj,0))+(ifnull(system_original,0)+ifnull(system_adj,0)))<0 and source_supplier_id={supplier_id};""".format(db_name=DB_NAME,supplier_id=supp_id)
    result=condb_dict(sql1)
    status="Fail" if len(result)>0 else "OK"
    return status

         
    