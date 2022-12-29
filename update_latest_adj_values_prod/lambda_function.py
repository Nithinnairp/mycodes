import json
import datetime 
import sys
import boto3
import pymysql
import logging
import requests
from DB_conn import condb,condb_dict

my_date=datetime.datetime.today()
year,week_num,day_of_week=my_date.isocalendar()
current_year=str(year)
current_year=current_year[2:]

def lambda_handler(event, context):
    current_time=datetime.datetime.now()
    date_str=current_time.strftime("%Y-%m-%d %H:%M")
    sng_time=(current_time+datetime.timedelta(hours=8)).strftime("%Y-%m-%d %H_%M")
    formated_time=(current_time+datetime.timedelta(hours=8)).strftime("%Y-%m-%d %H_%M").replace(' ','+')
    
    updating_advance_commit_table="""update advance_commit as ac inner join 
                                    forecast_plan as fp on ac.ori_part_number=fp.ori_part_number and ac.prod_week=fp.prod_week and
                                    ac.prod_year=fp.prod_year set ac.commit=
                                     (case
                                     when demand_category='Non GSA' then ifnull(fp.nongsa_commit,0)
                                     when demand_category='GSA' then ifnull(fp.gsa_commit,0)
                                     when demand_category='Forecast' then ifnull(fp.forecast_commit,0)
                                     when demand_category='System' then ifnull(fp.system_commit,0)
                                      end),ac.adjustment=
                                      (case
                                     when demand_category='Non GSA' then ifnull(fp.nongsa_adj,0)
                                     when demand_category='GSA' then ifnull(fp.gsa_adj,0)
                                     when demand_category='Forecast' then ifnull(fp.forecast_adj,0)
                                     when demand_category='System' then ifnull(fp.system_adj,0)
                                      end) where ac.process_week={};""".format(week_num)
    condb(updating_advance_commit_table)
    
    updating_commit_notification_table="""update commit_advance_commit_notification as cadv inner join 
                                    forecast_plan as fp
                                    on cadv.ori_part_number=fp.ori_part_number and
                                    cadv.prod_week=fp.prod_week and cadv.prod_year=fp.prod_year 
                                    set cadv.adjustment=
                                     (case
                                     when cadv.demand_category='Non GSA' then fp.nongsa_adj
                                     when cadv.demand_category='GSA'then fp.gsa_adj
                                     when cadv.demand_category='Forecast'then fp.forecast_adj
                                     when cadv.demand_category='System'then fp.system_adj
                                     end) where cadv.process_week={};""".format(week_num)
                                     
    condb(updating_commit_notification_table)
    