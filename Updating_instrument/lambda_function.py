import json
import pandas as pd
import datetime 
import sys
import boto3
import json
import pymysql
from DB_conn import condb


my_date=datetime.datetime.today()
year,week_num,day_of_week=my_date.isocalendar()
current_year=str(year)
current_year=current_year[2:]

#Connecting to the DB
def lambda_handler(event, context):
    current_time=datetime.datetime.now()
    date=current_time.strftime("%Y-%m-%d")
    next_date=(current_time+datetime.timedelta(days=7)).strftime("%Y-%m-%d")
    time=(current_time+datetime.timedelta(hours=8)).strftime("%H:%M:%S")
    next_time="10:30:00"
    # print(date,'ttttt',time,'aaaa',next_date)
    print(year,week_num,current_year)
    get_col_names="""SELECT column_name from information_schema.columns where table_schema='User_App' and table_name='forecast_plan';"""
    col_names=list(condb(get_col_names))
    db_columns=list(sum(col_names,()))

    query="""select distinct(ori_part_number) from User_App.forecast_plan where instrument is null and active=1;"""
    df=condb(query)
    db_data=pd.DataFrame(data=df,columns=['ori_part_number'])
    
    for index,row in db_data.iterrows():
        ori_part_number=str(row.ori_part_number)
        sql="""UPDATE User_App.forecast_plan join User_App.iodm_part_list
                on User_App.forecast_plan.ori_part_number=User_App.iodm_part_list.part_name
                set 
                User_App.forecast_plan.bu=User_App.iodm_part_list.bu,
                User_App.forecast_plan.build_type=User_App.iodm_part_list.build_type,
                User_App.forecast_plan.dept_code=User_App.iodm_part_list.dept,
                User_App.forecast_plan.division=User_App.iodm_part_list.division,
                User_App.forecast_plan.planner_code=User_App.iodm_part_list.part_planner_code,
                User_App.forecast_plan.planner_name=User_App.iodm_part_list.part_planner_name,
                User_App.forecast_plan.product_family=User_App.iodm_part_list.product_family,
                User_App.forecast_plan.product_line=User_App.iodm_part_list.product_line,
                User_App.forecast_plan.instrument=User_App.iodm_part_list.instrument
                where User_App.forecast_plan.ori_part_number=%s;"""
        col=[ori_part_number]
        condb(sql,col)
        
        
    # query2="""select distinct(ori_part_number) from User_App.forecast_plan as fp,User_App.iodm_part_list as ip
    #         where fp.measurable is null and ip.measureable_flag='N' and 
    #         fp.ori_part_number=ip.part_name and fp.active=1;"""
    query2="""select distinct(ori_part_number) from User_App.forecast_plan where updated_by=2"""
    df1=condb(query2)
    db_data_test=pd.DataFrame(data=df1,columns=['ori_part_number'])
    
    for index,row in db_data_test.iterrows():
        # ori_part_number=row.ori_part_number
        # bu=row.bu
        ori_part_number=row.ori_part_number
        sql1="""UPDATE User_App.forecast_plan join User_App.iodm_part_list
                on User_App.forecast_plan.ori_part_number=User_App.iodm_part_list.part_name
                set 
                User_App.forecast_plan.bu=User_App.iodm_part_list.bu,
                User_App.forecast_plan.build_type=User_App.iodm_part_list.build_type,
                User_App.forecast_plan.dept_code=User_App.iodm_part_list.dept,
                User_App.forecast_plan.division=User_App.iodm_part_list.division,
                User_App.forecast_plan.planner_code=User_App.iodm_part_list.part_planner_code,
                User_App.forecast_plan.planner_name=User_App.iodm_part_list.part_planner_name,
                User_App.forecast_plan.product_family=User_App.iodm_part_list.product_family,
                User_App.forecast_plan.product_line=User_App.iodm_part_list.product_line,
                User_App.forecast_plan.instrument=User_App.iodm_part_list.instrument,
                User_App.forecast_plan.measurable=(case
                when User_App.iodm_part_list.measureable_flag='Y' then 1
                when User_App.iodm_part_list.measureable_flag='N' then 0
                else null
                end),
				User_App.forecast_plan.countable=(case
                when User_App.iodm_part_list.countable_flag='Yes' then 1
                else 0
                end)
                where User_App.forecast_plan.ori_part_number=%s;"""
        col=[ori_part_number]
        condb(sql1,col)
    
    condb("drop table User_App.forecast_plan_last_monday_after_load;")
    condb("create table User_App.forecast_plan_last_monday_after_load like User_App.forecast_plan_monday;")
    condb("insert into User_App.forecast_plan_last_monday_after_load select * from User_App.forecast_plan_monday;")
    condb("drop table User_App.forecast_plan_monday;")
    condb("create table User_App.forecast_plan_monday like User_App.forecast_plan;")
    condb("insert into User_App.forecast_plan_monday select * from User_App.forecast_plan;")
    condb("truncate User_App.forecast_validation_rr;")
    condb("call User_App.rr_validation();")
    
    plan_refresh={"date":date,"time":time,"status":"Up/Live"}
 
    plan_refresh_schedule=json.dumps(plan_refresh)
    sql="""update User_App.communication_dashboard set value=%s where module='plan-refresh-schedule';"""
    col=[plan_refresh_schedule]
    condb(sql,col)
    next_refresh={"date": next_date,"time": next_time}
    next_refresh_schedule=json.dumps(next_refresh)
    sql="""update User_App.communication_dashboard set value=%s where module='next-refresh-schedule';"""
    col=[next_refresh_schedule]
    condb(sql,col)
    
    lambda_client = boto3.client('lambda')
    response = lambda_client.invoke(
    FunctionName='phase_related_mailing',
    InvocationType='Event',
    Payload=json.dumps({'Phase':'Planning'}))
    