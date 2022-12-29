import sys
import json
import boto3 
import pandas as pd 
import numpy as np
import datetime
import pytz 
from DB_conn import condb

def lambda_handler(event, context):
    print(event,context)
    current_time=datetime.datetime.now()
    my_date=datetime.datetime.today()
    year,week_num,week_day=my_date.isocalendar()

    #return the columns in the DB table
    get_col_names="""SELECT column_name from information_schema.columns where table_schema='User_App' and table_name='forecast_plan';"""
    col_names=list(condb(get_col_names))
    db_columns=list(sum(col_names,()))
    
    get_records="""select * from User_App.forecast_plan where is_processed=0 and active=1"""
    df1=condb(sql=get_records) 
    db_data=pd.DataFrame(data=df1,columns=db_columns)
    db_data=db_data.fillna({'forecast_commit1':0,'forecast_commit2':0,'forecast_commit3':0,'forecast_commit4':0,
                            'forecast_commit1_accepted':b'\x00','forecast_commit2_accepted':b'\x00','forecast_commit3_accepted':b'\x00','forecast_commit4_accepted':b'\x00',
                            'gsa_commit1':0,'gsa_commit2':0,'gsa_commit3':0,'gsa_commit4':0,
                            'gsa_commit1_accepted':b'\x00','gsa_commit2_accepted':b'\x00','gsa_commit3_accepted':b'\x00','gsa_commit4_accepted':b'\x00',
                            'nongsa_commit1':0,'nongsa_commit2':0,'nongsa_commit3':0,'nongsa_commit4':0,
                            'nongsa_commit1_accepted':b'\x00','nongsa_commit2_accepted':b'\x00','nongsa_commit3_accepted':b'\x00','nongsa_commit4_accepted':b'\x00',
                            'system_commit1':0,'system_commit2':0,'system_commit3':0,'system_commit4':0,
                            'system_commit1_accepted':b'\x00','system_commit2_accepted':b'\x00','system_commit3_accepted':b'\x00','system_commit4_accepted':b'\x00'}) 
    
    def get_date_format(date=None):
        if date is None:
            year,week,day=None,None,None
            return year,week,day
        else:
            year,week,day=str(date).split('-')
            return int(year),int(week),int(day)
            

    
    for index, row in db_data.iterrows():
        business_unit = row.bu
        ori_part_number = row.ori_part_number
        process_week = week_num
        process_year = year
        prod_week = row.prod_week
        prod_year = row.prod_year
        source_supplier = row.source_supplier
        division = row.division
        product_family = row.product_family
        product_line = row.product_line
        dept_code = row.dept_code
        planner_name = row.planner_name
        planner_code = row.planner_code
        dmp_orndmp = row.dmp_orndmp
        build_type = row.build_type
        prod_year_week = row.prod_year_week
        commit_date=row.date
        current_time=datetime.datetime.now()
        my_date=datetime.datetime.today()
        year,week_num,week_day=my_date.isocalendar()
        id=row.id
        print(id)
        
        if ((row.forecast_commit1 != 0 and row.forecast_commit1_date is not None) or (row.forecast_commit2 != 0 and row.forecast_commit2_date is not None) or \
        (row.forecast_commit3 != 0 and row.forecast_commit3_date is not None) or (row.forecast_commit4 != 0 and row.forecast_commit4_date is not None))or\
        ((row.forecast_commit1==0 and row.forecast_commit1_date is None) or (row.forecast_commit2==0 and row.forecast_commit2_date is None) or \
        (row.forecast_commit3==0 and row.forecast_commit3_date is None) or (row.forecast_commit4==0 and row.forecast_commit4_date is None)):
            demand_category = 'Forecast'
            original = row.forecast_original
            adjustment = row.forecast_adj
            commit = row.forecast_commit
            updated_by = row.updated_by
            updated_at = current_time
            created_by = row.created_by
            cause_code=row.forecast_cause_code
            cause_code_remark=row.forecast_cause_code_remark
            commit_date=row.date
            created_at = current_time
            
            new_commit1 = int(row.forecast_commit1)
            forecast_commit1_accepted=ord(row.forecast_commit1_accepted)
            if row.forecast_commit1_date is not None:
                commit1_date =str(row.forecast_commit1_date)
                try:
                    x, y, z = get_date_format(commit1_date)
                    commit1_week = datetime.date(x, y, z).isocalendar()[1]
                    commit1_year = datetime.date(x, y, z).isocalendar()[0]
                except:
                    commit1_week=None
                    commit1_year=None
            else:
                commit1_week=None
                commit1_year=None
                commit1_date=None
                
            new_commit2 = int(row.forecast_commit2)
            forecast_commit2_accepted=ord(row.forecast_commit2_accepted)
            if row.forecast_commit2_date is not None:
                commit2_date =str(row.forecast_commit2_date)    
                try:
                    x, y, z = get_date_format(commit2_date)
                    commit2_week = datetime.date(x, y, z).isocalendar()[1]
                    commit2_year = datetime.date(x, y, z).isocalendar()[0]
                except:
                    commit2_week=None
                    commit2_year=None
            else:
                commit2_week=None
                commit2_year=None
                commit2_date=None
            
            new_commit3 = int(row.forecast_commit3)
            forecast_commit3_accepted=ord(row.forecast_commit3_accepted)
            if row.forecast_commit3_date is not None:
                commit3_date = str(row.forecast_commit3_date) 
                try:
                    x, y, z = get_date_format(commit3_date)
                    commit3_week = datetime.date(x, y, z).isocalendar()[1]
                    commit3_year = datetime.date(x, y, z).isocalendar()[0]
                except:
                    commit3_week=None
                    commit3_year=None
            else:
                commit3_week=None
                commit3_year=None
                commit3_date=None
                
            
            new_commit4 = int(row.forecast_commit4)
            forecast_commit4_accepted=ord(row.forecast_commit4_accepted)
            if row.forecast_commit4_date is not None:
                commit4_date = str(row.forecast_commit4_date) 
                try:
                    x, y, z = get_date_format(commit4_date)
                    commit4_week = datetime.date(x, y, z).isocalendar()[1]
                    commit4_year = datetime.date(x, y, z).isocalendar()[0]
                except:
                    commit4_week = None
                    commit4_year = None
            else:
                commit4_week=None
                commit4_year=None
                commit4_date=None
                
            
            print(ori_part_number,"prod_week:",prod_week,'prod_year:',prod_year,'committed value1:',new_commit1,'committed date1:',commit1_date,'\n',\
            'committed value2:',new_commit2,'committed date2:',commit2_date,'committed value3:',new_commit3,'committed date3:',commit3_date,'\n',\
            'committed value4:', new_commit4,'committed date4:',commit4_date,'cause_code:',cause_code,'cause_code_remark:',cause_code_remark)
            check = """select id from User_App.advance_commit where ori_part_number=%s and prod_week={} and prod_year={} and process_week={} and demand_category='{}'""".format(
            prod_week, prod_year, week_num, demand_category)
            col = [ori_part_number]
            id_found = list(condb(check, col))
            print(id_found, id)
            if id_found==[] and ((row.forecast_commit1!=0 and row.forecast_commit1_date is not None) or (row.forecast_commit2!=0 and row.forecast_commit2_date is not None) or \
            (row.forecast_commit3!=0 and row.forecast_commit3_date is not None) or (row.forecast_commit4!=0 and row.forecast_commit4_date is not None)):
                sql = """insert into User_App.advance_commit(business_unit,ori_part_number,process_week,process_year,prod_week,
                prod_year,source_supplier,division,product_family,product_line,
                dept_code,planner_name,planner_code,dmp_orndmp,build_type,
                prod_year_week,demand_category,original,adjustment,commit,
                commit1_old_value,commit1,commit1_date,commit1_week,commit1_year,commit1_irp_processed,
                commit2_old_value,commit2,commit2_date,commit2_week,commit2_year,commit2_irp_processed,
                commit3_old_value,commit3,commit3_date,commit3_week,commit3_year,commit3_irp_processed,
                commit4_old_value,commit4,commit4_date,commit4_week,commit4_year,commit4_irp_processed,
                kr_processed,irp_processed,created_by,created_at,updated_by,updated_at,active,
                commit_date,cause_code,cause_code_remark,
                commit1_accepted,commit2_accepted,commit3_accepted,commit4_accepted)
                values(%s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,
                null,%s,%s,%s,%s,False,
                null,%s,%s,%s,%s,False,
                null,%s,%s,%s,%s,False,
                null,%s,%s,%s,%s,False,
                0,0,%s,%s,%s,%s,1,
                %s,%s,%s,
                %s,%s,%s,%s)"""
                col = [business_unit, ori_part_number,week_num,year, prod_week,
                prod_year, source_supplier, division, product_family, product_line,
                dept_code, planner_name, planner_code, dmp_orndmp, build_type,
                prod_year_week, demand_category, original, adjustment, commit,
                new_commit1, commit1_date, commit1_week, commit1_year,
                new_commit2, commit2_date, commit2_week, commit2_year,
                new_commit3, commit3_date, commit3_week, commit3_year,
                new_commit4, commit4_date, commit4_week, commit4_year, created_by, created_at, updated_by,
                updated_at,commit_date,cause_code,cause_code_remark,
                forecast_commit1_accepted,forecast_commit2_accepted,forecast_commit3_accepted,forecast_commit4_accepted]
                print("record inserted to advance_commit table with ori_part_number", ori_part_number,"prod_week and prod_year", prod_week, prod_year,"for demand category",demand_category)
                condb(sql, col)
                sql1 = """update User_App.forecast_plan set is_processed=1,active=1 where ori_part_number=%s and prod_week={} and prod_year={}""".format(
                prod_week, prod_year)
                col1 = [ori_part_number]
                condb(sql1, col1)
                
            else:
                print("entered else part forecast",commit3_date,commit3_week,commit3_year)
                if (row.forecast_commit1==0 and row.forecast_commit1_date is None) and (row.forecast_commit2==0 and row.forecast_commit2_date is None) and \
                (row.forecast_commit3==0 and row.forecast_commit3_date is None) and (row.forecast_commit4==0 and row.forecast_commit4_date is None):
                    delete_records="""update User_App.advance_commit set 
                    commit1_old_value=commit1,commit2_old_value=commit2,commit3_old_value=commit3,commit4_old_value=commit4,
                    commit1_date_old_value=commit1_date,commit2_date_old_value=commit2_date,commit3_date_old_value=commit3_date,commit4_date_old_value=commit4_date,
                    commit1={},commit2={},commit3={},commit4={},
                    is_delete=1,kr_processed=0,irp_processed=0 
                    where ori_part_number=%s and prod_year={} and prod_week={} and process_week={} and demand_category='{}'""".format(new_commit1,new_commit2,new_commit3,new_commit4,prod_year,prod_week,process_week,demand_category)
                    col=[ori_part_number]
                    condb(delete_records,col)
                else:
                    sql = """update User_App.advance_commit set 
                    commit1_old_value=commit1,commit1_date_old_value=commit1_date,commit1={},commit1_date=%s,commit1_week=%s,commit1_year=%s,commit1_accepted=False,commit1_irp_processed=False,
                    commit2_old_value=commit2,commit2_date_old_value=commit2_date,commit2={},commit2_date=%s,commit2_week=%s,commit2_year=%s,commit2_accepted=False,commit2_irp_processed=False,
                    commit3_old_value=commit3,commit3_date_old_value=commit3_date,commit3={},commit3_date=%s,commit3_week=%s,commit3_year=%s,commit3_accepted=False,commit3_irp_processed=False,
                    commit4_old_value=commit4,commit4_date_old_value=commit4_date,commit4={},commit4_date=%s,commit4_week=%s,commit4_year=%s,commit4_accepted=False,commit4_irp_processed=False,kr_processed=0,irp_processed=0,
                    commit_date=%s,cause_code=%s,cause_code_remark=%s
                    where ori_part_number=%s and prod_year={} and prod_week={} and process_week={} and demand_category='{}'""".format(new_commit1,new_commit2,new_commit3,new_commit4,prod_year,prod_week,process_week,demand_category)
                    print("record updated to advance_commit table with ori_part_number", ori_part_number,"prod_week and prod_year", prod_week, prod_year,"for demand category",demand_category)
                    col = [commit1_date,commit1_week,commit1_year,commit2_date,commit2_week,commit2_year,\
                    commit3_date,commit3_week,commit3_year,commit4_date,commit4_week,commit4_year,\
                    commit_date,cause_code,cause_code_remark,ori_part_number]
                    print(sql)
                    print(commit1_date,commit2_date,commit3_date,commit4_date)
                    condb(sql,col)
                    sql1 = """update User_App.forecast_plan set is_processed=1,active=1 where ori_part_number=%s and prod_week={} and prod_year={}""".format(prod_week, prod_year)
                    col1 = [ori_part_number]
                    condb(sql1, col1)
            
                
        if ((row.gsa_commit1 != 0 and row.gsa_commit1_date is not None) or (row.gsa_commit2 != 0 and row.gsa_commit2_date is not None) or \
        (row.gsa_commit3 != 0 and row.gsa_commit3_date is not None) or (row.gsa_commit4 != 0 and row.gsa_commit4_date is not None)) or\
        ((row.gsa_commit1==0 and row.gsa_commit1_date is None) or (row.gsa_commit2==0 and row.gsa_commit2_date is None) or \
        (row.gsa_commit3==0 and row.gsa_commit3_date is None) or (row.gsa_commit4==0 and row.gsa_commit4_date is None)):
            demand_category = 'GSA'
            original = row.gsa_original
            adjustment = row.gsa_adj
            commit = row.gsa_commit
            updated_by = row.updated_by
            updated_at = current_time
            created_by = row.created_by
            created_at = current_time
            cause_code=row.gsa_cause_code
            cause_code_remark=row.gsa_cause_code_remark
            commit_date=row.date
            
            commit = row.gsa_commit
            new_commit1 = int(row.gsa_commit1)
            gsa_commit1_accepted=ord(row.gsa_commit1_accepted)
            if row.gsa_commit1_date is not None:
                commit1_date = str(row.gsa_commit1_date) 
                try:
                    x, y, z = get_date_format(commit1_date)
                    commit1_week = datetime.date(x, y, z).isocalendar()[1]
                    commit1_year = datetime.date(x, y, z).isocalendar()[0]
                except:
                    commit1_week=None
                    commit1_year=None
            else:
                commit1_week=None
                commit1_year=None
                commit1_date=None
                
            
            new_commit2 = int(row.gsa_commit2)
            gsa_commit2_accepted=ord(row.gsa_commit2_accepted)
            if row.gsa_commit2_date is not None:
                commit2_date =str(row.gsa_commit2_date) 
                try:
                    x, y, z = get_date_format(commit2_date)
                    commit2_week = datetime.date(x, y, z).isocalendar()[1]
                    commit2_year = datetime.date(x, y, z).isocalendar()[0]
                except:
                    commit2_week=None
                    commit2_year=None
            else:
                commit2_week=None
                commit2_year=None
                commit2_date=None
            
            new_commit3 = int(row.gsa_commit3)
            gsa_commit3_accepted=ord(row.gsa_commit3_accepted)
            if row.gsa_commit3_date is not None:
                commit3_date =str(row.gsa_commit3_date) 
                try:
                    x, y, z = get_date_format(commit3_date)
                    commit3_week = datetime.date(x, y, z).isocalendar()[1]
                    commit3_year = datetime.date(x, y, z).isocalendar()[0]
                except:
                    commit3_week=None
                    commit3_year=None
            else:
                commit3_week=None
                commit3_year=None
                commit3_date=None
                
            
            new_commit4 = int(row.gsa_commit4)
            gsa_commit4_accepted=ord(row.gsa_commit4_accepted)
            if row.gsa_commit4_date is not None:
                commit4_date =str(row.gsa_commit4_date)
                try:
                    x, y, z = get_date_format(commit4_date)
                    commit4_week = datetime.date(x, y, z).isocalendar()[1]
                    commit4_year = datetime.date(x, y, z).isocalendar()[0]
                except:
                    commit4_week = None
                    commit4_year = None
            else:
                commit4_week=None
                commit4_year=None
                commit4_date=None
            
            
            print(ori_part_number,"prod_week:",prod_week,'prod_year:',prod_year,'committed value1:',new_commit1,'committed date1:',commit1_date,'\n',\
            'committed value2:',new_commit2,'committed date2:',commit2_date,'committed value3:',new_commit3,'committed date3:',commit3_date,'\n',\
            'committed value4:', new_commit4,'committed date4:',commit4_date,'cause_code:',cause_code,'cause_code_remark:',cause_code_remark)
            check = """select id from User_App.advance_commit where ori_part_number=%s and prod_week={} and prod_year={} and process_week={} and demand_category='{}'""".format(
            prod_week, prod_year, week_num, demand_category)
            col = [ori_part_number]
            id_found = list(condb(check, col))
            print(id_found, id)
            if id_found==[] and ((row.gsa_commit1!=0 and row.gsa_commit1_date is not None) or (row.gsa_commit2!=0 and row.gsa_commit2_date is not None) or \
            (row.gsa_commit3!=0 and row.gsa_commit3_date is not None) or (row.gsa_commit4!=0 and row.gsa_commit4_date is not None)):
                sql = """insert into User_App.advance_commit(business_unit,ori_part_number,process_week,process_year,prod_week,
                prod_year,source_supplier,division,product_family,product_line,
                dept_code,planner_name,planner_code,dmp_orndmp,build_type,
                prod_year_week,demand_category,original,adjustment,commit,
                commit1_old_value,commit1,commit1_date,commit1_week,commit1_year,commit1_irp_processed,
                commit2_old_value,commit2,commit2_date,commit2_week,commit2_year,commit2_irp_processed,
                commit3_old_value,commit3,commit3_date,commit3_week,commit3_year,commit3_irp_processed,
                commit4_old_value,commit4,commit4_date,commit4_week,commit4_year,commit4_irp_processed,
                kr_processed,irp_processed,created_by,created_at,updated_by,updated_at,active,
                commit_date,cause_code,cause_code_remark,
                commit1_accepted,commit2_accepted,commit3_accepted,commit4_accepted)
                values(%s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,
                null,%s,%s,%s,%s,False,
                null,%s,%s,%s,%s,False,
                null,%s,%s,%s,%s,False,
                null,%s,%s,%s,%s,False,
                0,0,%s,%s,%s,%s,1,
                %s,%s,%s,
                %s,%s,%s,%s)"""
                col = [business_unit, ori_part_number,week_num,year, prod_week,
                prod_year, source_supplier, division, product_family, product_line,
                dept_code, planner_name, planner_code, dmp_orndmp, build_type,
                prod_year_week, demand_category, original, adjustment, commit,
                new_commit1, commit1_date, commit1_week, commit1_year,
                new_commit2, commit2_date, commit2_week, commit2_year,
                new_commit3, commit3_date, commit3_week, commit3_year,
                new_commit4, commit4_date, commit4_week, commit4_year, created_by, created_at, updated_by,
                updated_at,commit_date,cause_code,cause_code_remark,
                gsa_commit1_accepted,gsa_commit2_accepted,gsa_commit3_accepted,gsa_commit4_accepted]
                print("record inserted to advance_commit table with ori_part_number", ori_part_number,\
                "prod_week and prod_year", prod_week, prod_year,"for demand category",demand_category)
                condb(sql, col)
                sql1 = """update User_App.forecast_plan set is_processed=1,active=1 where ori_part_number=%s and prod_week={} and prod_year={}""".format(
                prod_week, prod_year)
                col1 = [ori_part_number]
                condb(sql1, col1)
                
                
            else:
                print("entered else part gsa")
                if (row.gsa_commit1==0 and row.gsa_commit1_date is None) and (row.gsa_commit2==0 and row.gsa_commit2_date is None) and \
                (row.gsa_commit3==0 and row.gsa_commit3_date is None) and (row.gsa_commit4==0 and row.gsa_commit4_date is None):
                    delete_records="""update User_App.advance_commit set 
                    commit1_old_value=commit1,commit2_old_value=commit2,commit3_old_value=commit3,commit4_old_value=commit4,
                    commit1={},commit2={},commit3={},commit4={},
                    is_delete=1,kr_processed=0,irp_processed=0 
                    where ori_part_number=%s and prod_year={} and prod_week={} and process_week={} and demand_category='{}'""".format(new_commit1,new_commit2,new_commit3,new_commit4,prod_year,prod_week,process_week,demand_category)
                    col=[ori_part_number]
                    condb(delete_records,col)
                else:
                    sql = """update User_App.advance_commit set 
                    commit1_old_value=commit1,commit1_date_old_value=commit1_date,commit1={},commit1_date=%s,commit1_week=%s,commit1_year=%s,commit1_accepted=False,commit1_irp_processed=False,
                    commit2_old_value=commit2,commit2_date_old_value=commit2_date,commit2={},commit2_date=%s,commit2_week=%s,commit2_year=%s,commit2_accepted=False,commit2_irp_processed=False,
                    commit3_old_value=commit3,commit3_date_old_value=commit3_date,commit3={},commit3_date=%s,commit3_week=%s,commit3_year=%s,commit3_accepted=False,commit3_irp_processed=False,
                    commit4_old_value=commit4,commit4_date_old_value=commit4_date,commit4={},commit4_date=%s,commit4_week=%s,commit4_year=%s,commit4_accepted=False,commit4_irp_processed=False,kr_processed=0,irp_processed=0,
                    commit_date=%s,cause_code=%s,cause_code_remark=%s
                    where ori_part_number=%s and prod_year={} and prod_week={} and process_week={} and demand_category='{}'""".format(new_commit1,new_commit2,new_commit3,new_commit4,prod_year,prod_week,process_week,demand_category)
                    print("record updated to advance_commit table with ori_part_number", ori_part_number,"prod_week and prod_year", prod_week, prod_year,"for demand category",demand_category)
                    col = [commit1_date,commit1_week,commit1_year,commit2_date,commit2_week,commit2_year,\
                    commit3_date,commit3_week,commit3_year,commit4_date,commit4_week,commit4_year,\
                    commit_date,cause_code,cause_code_remark,ori_part_number]
                    print(sql)
                    print(commit1_date,commit2_date,commit3_date,commit4_date)
                    condb(sql,col)
                    sql1 = """update User_App.forecast_plan set is_processed=1,active=1 where ori_part_number=%s and prod_week={} and prod_year={}""".format(prod_week, prod_year)
                    col1 = [ori_part_number]
                    condb(sql1, col1)
                    
         
            
        if ((row.nongsa_commit1 != 0 and row.nongsa_commit1_date is not None) or (row.nongsa_commit2 != 0 and row.nongsa_commit2_date is not None) or \
        (row.nongsa_commit3 != 0 and row.nongsa_commit3_date is not None) or (row.nongsa_commit4 != 0 and row.nongsa_commit4_date is not None)) or \
        ((row.nongsa_commit1==0 and row.nongsa_commit1_date is None) or (row.nongsa_commit2==0 and row.nongsa_commit2_date is None) or \
        (row.nongsa_commit3==0 and row.nongsa_commit3_date is None) or (row.nongsa_commit4==0 and row.nongsa_commit4_date is None)):
            demand_category = 'Non GSA'
            original = row.nongsa_original
            adjustment = row.nongsa_adj
            commit = row.nongsa_commit
            updated_by = row.updated_by
            updated_at = current_time
            created_by = row.created_by
            created_at = current_time
            process_week=week_num
            cause_code=row.nongsa_cause_code
            cause_code_remark=row.nongsa_cause_code_remark
            commit_date=row.date
            
            new_commit1 = int(row.nongsa_commit1)
            nongsa_commit1_accepted=ord(row.nongsa_commit1_accepted)
            print('commit1-',nongsa_commit1_accepted)
            if row.nongsa_commit1_date is not None:
                commit1_date = str(row.nongsa_commit1_date) 
                try:
                    x, y, z = get_date_format(commit1_date)
                    commit1_week = datetime.date(x, y, z).isocalendar()[1]
                    commit1_year = datetime.date(x, y, z).isocalendar()[0]
                except:
                    commit1_week=None
                    commit1_year=None
            else:
                commit1_week=None
                commit1_year=None
                commit1_date=None
            
            new_commit2 = int(row.nongsa_commit2)
            nongsa_commit2_accepted=ord(row.nongsa_commit2_accepted)
            if row.nongsa_commit2_date is not None:
                commit2_date = str(row.nongsa_commit2_date)
                try:
                    x, y, z = get_date_format(commit2_date)
                    commit2_week = datetime.date(x, y, z).isocalendar()[1]
                    commit2_year = datetime.date(x, y, z).isocalendar()[0]
                except:
                    commit2_week=None
                    commit2_year=None
            else:
                commit2_week=None
                commit2_year=None
                commit2_date=None
                
            
            new_commit3 = int(row.nongsa_commit3)
            print(ord(row.nongsa_commit3_accepted))
            nongsa_commit3_accepted=ord(row.nongsa_commit3_accepted) 
            if row.nongsa_commit3_date is not None:
                commit3_date = str(row.nongsa_commit3_date) 
                print(commit3_date)
                try:
                    x, y, z = get_date_format(commit3_date)
                    commit3_week = datetime.date(x, y, z).isocalendar()[1]
                    commit3_year = datetime.date(x, y, z).isocalendar()[0]
                except:
                    commit3_week=None
                    commit3_year=None
            else:
                commit3_week=None
                commit3_year=None
                commit3_date=None
            
            new_commit4 = int(row.nongsa_commit4)
            nongsa_commit4_accepted=ord(row.nongsa_commit4_accepted)
            if row.nongsa_commit4_date is not None:
                commit4_date = str(row.nongsa_commit4_date)
                print(commit4_date)
                try:
                    x, y, z = get_date_format(commit4_date)
                    commit4_week = datetime.date(x, y, z).isocalendar()[1]
                    commit4_year = datetime.date(x, y, z).isocalendar()[0]
                except:
                    commit4_week = None
                    commit4_year = None
            else:
                commit4_week = None
                commit4_year = None
                commit4_date = None
                
            print(process_week,week_num,prod_week,prod_year,demand_category,"Checking for none")
            print(ori_part_number,"prod_week:",prod_week,'prod_year:',prod_year,'committed value1:',new_commit1,'committed date1:',commit1_date,'\n',\
            'committed value2:',new_commit2,'committed date2:',commit2_date,'committed value3:',new_commit3,'committed date3:',commit3_date,'\n',\
            'committed value4:', new_commit4,'committed date4:',commit4_date,'cause_code:',cause_code,'cause_code_remark:',cause_code_remark)
            check = """select id from User_App.advance_commit where ori_part_number=%s and prod_week={} and prod_year={} and process_week={} and demand_category='{}'""".format(prod_week, prod_year, week_num, demand_category)
            col = [ori_part_number]
            id_found = list(condb(check, col))
            print(id_found, id)
            if id_found==[] and ((row.nongsa_commit1!=0 and row.nongsa_commit1_date is not None) or (row.nongsa_commit2!=0 and row.nongsa_commit2_date is not None) or \
            (row.nongsa_commit3!=0 and row.nongsa_commit3_date is not None) or (row.nongsa_commit4!=0 and row.nongsa_commit4_date is not None)):
                sql = """insert into User_App.advance_commit(business_unit,ori_part_number,process_week,process_year,prod_week,
                prod_year,source_supplier,division,product_family,product_line,
                dept_code,planner_name,planner_code,dmp_orndmp,build_type,
                prod_year_week,demand_category,original,adjustment,commit,
                commit1_old_value,commit1,commit1_date,commit1_week,commit1_year,commit1_irp_processed,
                commit2_old_value,commit2,commit2_date,commit2_week,commit2_year,commit2_irp_processed,
                commit3_old_value,commit3,commit3_date,commit3_week,commit3_year,commit3_irp_processed,
                commit4_old_value,commit4,commit4_date,commit4_week,commit4_year,commit4_irp_processed,
                kr_processed,irp_processed,created_by,created_at,updated_by,updated_at,active,
                commit_date,cause_code,cause_code_remark,
                commit1_accepted,commit2_accepted,commit3_accepted,commit4_accepted)
                values(%s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,
                null,%s,%s,%s,%s,False,
                null,%s,%s,%s,%s,False,
                null,%s,%s,%s,%s,False,
                null,%s,%s,%s,%s,False,
                0,0,%s,%s,%s,%s,1,
                %s,%s,%s,
                %s,%s,%s,%s)"""
                col = [business_unit, ori_part_number,week_num,year, prod_week,
                prod_year, source_supplier, division, product_family, product_line,
                dept_code, planner_name, planner_code, dmp_orndmp, build_type, 
                prod_year_week, demand_category, original, adjustment, commit,
                new_commit1, commit1_date, commit1_week, commit1_year,
                new_commit2, commit2_date, commit2_week, commit2_year,
                new_commit3, commit3_date, commit3_week, commit3_year,
                new_commit4, commit4_date, commit4_week, commit4_year, created_by, created_at, updated_by, updated_at,
                commit_date,cause_code,cause_code_remark,
                nongsa_commit1_accepted,nongsa_commit2_accepted,nongsa_commit3_accepted,nongsa_commit4_accepted]
                print("record inserted to advance_commit table with ori_part_number", ori_part_number,"prod_week and prod_year", prod_week, prod_year,"for demand category",demand_category)
                print(sql)
                condb(sql, col)
                sql1 = """update User_App.forecast_plan set is_processed=1,active=1 where ori_part_number=%s and prod_week={} and prod_year={}""".format(prod_week, prod_year)
                col1 = [ori_part_number]
                condb(sql1, col1)
                

               
            else:
                print("entered else part non gsa test")
                if (row.nongsa_commit1==0 and row.nongsa_commit1_date is None) and (row.nongsa_commit2==0 and row.nongsa_commit2_date is None) and \
                (row.nongsa_commit3==0 and row.nongsa_commit3_date is None) and (row.nongsa_commit4==0 and row.nongsa_commit4_date is None):
                    delete_records="""update User_App.advance_commit set 
                    commit1_old_value=commit1,commit2_old_value=commit2,commit3_old_value=commit3,commit4_old_value=commit4,
                    commit1={},commit2={},commit3={},commit4={},
                    is_delete=1,kr_processed=0,irp_processed=0 
                    where ori_part_number=%s and prod_year={} and prod_week={} and process_week={} and demand_category='{}'""".format(new_commit1,new_commit2,new_commit3,new_commit4,prod_year,prod_week,process_week,demand_category)
                    col=[ori_part_number]
                    condb(delete_records,col)
                else:
                    print(id,commit1_date,commit2_date,commit3_date,commit4_date)
                    sql = """update User_App.advance_commit set 
                    commit1_old_value=commit1,commit1_date_old_value=commit1_date,commit1={},commit1_date=%s,commit1_week=%s,commit1_year=%s,commit1_accepted=False,commit1_irp_processed=False,
                    commit2_old_value=commit2,commit2_date_old_value=commit2_date,commit2={},commit2_date=%s,commit2_week=%s,commit2_year=%s,commit2_accepted=False,commit2_irp_processed=False,
                    commit3_old_value=commit3,commit3_date_old_value=commit3_date,commit3={},commit3_date=%s,commit3_week=%s,commit3_year=%s,commit3_accepted=False,commit3_irp_processed=False,
                    commit4_old_value=commit4,commit4_date_old_value=commit4_date,commit4={},commit4_date=%s,commit4_week=%s,commit4_year=%s,commit4_accepted=False,commit4_irp_processed=False,kr_processed=0,irp_processed=0,
                    commit_date=%s,cause_code=%s,cause_code_remark=%s
                    where ori_part_number=%s and prod_year={} and prod_week={} and process_week={} and demand_category='{}'""".format(new_commit1,new_commit2,new_commit3,new_commit4,prod_year,prod_week,process_week,demand_category)
                    print("record updated to advance_commit table with ori_part_number", ori_part_number,"prod_week and prod_year", prod_week, prod_year,"for demand category",demand_category)
                    col = [commit1_date,commit1_week,commit1_year,commit2_date,commit2_week,commit2_year,\
                    commit3_date,commit3_week,commit3_year,commit4_date,commit4_week,commit4_year,\
                    commit_date,cause_code,cause_code_remark,ori_part_number]
                    print(sql)
                    print(commit1_date,commit2_date,commit3_date,commit4_date)
                    condb(sql,col)
                    sql1 = """update User_App.forecast_plan set is_processed=1,active=1 where ori_part_number=%s and prod_week={} and prod_year={}""".format(prod_week, prod_year)
                    col1 = [ori_part_number]
                    condb(sql1, col1)
                    

        
        if ((row.system_commit1 != 0 and row.system_commit1_date is not None) or (row.system_commit2 != 0 and row.system_commit2_date is not None) or \
        (row.system_commit3 != 0 and row.system_commit3_date is not None) or (row.system_commit4 != 0 and row.system_commit4_date is not None))or\
        ((row.system_commit1==0 and row.system_commit1_date is None) or (row.system_commit2==0 and row.system_commit2_date is None) or \
        (row.system_commit3==0 and row.system_commit3_date is None) or (row.system_commit4==0 and row.system_commit4_date is None)):
            demand_category = 'System'
            original = row.system_original
            adjustment = row.system_adj
            commit = row.system_commit
            updated_by = row.updated_by
            updated_at = current_time
            created_by = row.created_by
            created_at = current_time
            cause_code=row.system_cause_code
            cause_code_remark=row.system_cause_code_remark
            commit_date=row.date
            
            new_commit1 = int(row.system_commit1)
            system_commit1_accepted=ord(row.system_commit1_accepted) 
            if row.system_commit1_date is not None:
                commit1_date = str(row.system_commit1_date) 
                try:
                    x, y, z = get_date_format(commit1_date)
                    commit1_week = datetime.date(x, y, z).isocalendar()[1]
                    commit1_year = datetime.date(x, y, z).isocalendar()[0]
                except:
                    commit1_week=None
                    commit1_year=None
            else:
                commit1_week = None
                commit1_year = None
                commit1_date = None
            
            new_commit2 = int(row.system_commit2)
            system_commit2_accepted=ord(row.system_commit2_accepted)
            if row.system_commit2_date is not None:
                commit2_date = str(row.system_commit2_date) 
                try:
                    x, y, z = get_date_format(commit2_date)
                    commit2_week = datetime.date(x, y, z).isocalendar()[1]
                    commit2_year = datetime.date(x, y, z).isocalendar()[0]
                except:
                    commit2_week=None
                    commit2_year=None
            else:
                commit2_week = None
                commit2_year = None
                commit2_date = None
            
            new_commit3 = int(row.system_commit3)
            system_commit3_accepted=ord(row.system_commit3_accepted)
            if row.system_commit3_date is not None:
                commit3_date = str(row.system_commit3_date)
                try:
                    x, y, z = get_date_format(commit3_date)
                    commit3_week = datetime.date(x, y, z).isocalendar()[1]
                    commit3_year = datetime.date(x, y, z).isocalendar()[0]
                except:
                    commit3_week=None
                    commit3_year=None
            else:
                commit3_week = None
                commit3_year = None
                commit3_date = None
            
            new_commit4 = int(row.system_commit4)
            system_commit4_accepted=ord(row.system_commit4_accepted)
            if row.system_commit4_date is not None:
                commit4_date = str(row.system_commit4_date) 
                try:
                    x, y, z = get_date_format(commit4_date)
                    commit4_week = datetime.date(x, y, z).isocalendar()[1]
                    commit4_year = datetime.date(x, y, z).isocalendar()[0]
                except:
                    commit4_week=None
                    commit4_year=None
            else:
                commit4_week = None
                commit4_year = None
                commit4_date = None
    
            print(ori_part_number, prod_week, prod_year, new_commit1, commit1_date, new_commit2, commit2_date, new_commit3,commit3_date, new_commit4, commit4_date)
            print(ori_part_number,"prod_week:",prod_week,'prod_year:',prod_year,'committed value1:',new_commit1,'committed date1:',commit1_date,'\n',\
            'committed value2:',new_commit2,'committed date2:',commit2_date,'committed value3:',new_commit3,'committed date3:',commit3_date,'\n',\
            'committed value4:', new_commit4,'committed date4:',commit4_date,'cause_code:',cause_code,'cause_code_remark:',cause_code_remark)
            check = """select id from User_App.advance_commit where ori_part_number=%s and prod_week={} and prod_year={} and process_week={} and demand_category='{}'""".format(prod_week, prod_year, week_num, demand_category)
            col = [ori_part_number]
            id_found = list(condb(check, col))
            print(id_found, id)
            if id_found==[] and ((row.system_commit1!=0 and row.system_commit1_date is not None) or (row.system_commit2!=0 and row.system_commit2_date is not None) or \
            (row.system_commit3!=0 and row.system_commit3_date is not None) or (row.system_commit4!=0 and row.system_commit4_date is not None)):
                sql = """insert into User_App.advance_commit(business_unit,ori_part_number,process_week,process_year,prod_week,
                prod_year,source_supplier,division,product_family,product_line,
                dept_code,planner_name,planner_code,dmp_orndmp,build_type,
                prod_year_week,demand_category,original,adjustment,commit,
                commit1_old_value,commit1,commit1_date,commit1_week,commit1_year,commit1_irp_processed,
                commit2_old_value,commit2,commit2_date,commit2_week,commit2_year,commit2_irp_processed,
                commit3_old_value,commit3,commit3_date,commit3_week,commit3_year,commit3_irp_processed,
                commit4_old_value,commit4,commit4_date,commit4_week,commit4_year,commit4_irp_processed,
                kr_processed,irp_processed,created_by,created_at,updated_by,updated_at,active,
                commit_date,cause_code,cause_code_remark,
                commit1_accepted,commit2_accepted,commit3_accepted,commit4_accepted)
                values(%s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,
                null,%s,%s,%s,%s,False,
                null,%s,%s,%s,%s,False,
                null,%s,%s,%s,%s,False,
                null,%s,%s,%s,%s,False,
                0,0,%s,%s,%s,%s,1,
                %s,%s,%s,
                %s,%s,%s,%s)"""
                col = [business_unit, ori_part_number,week_num,year, prod_week,
                prod_year, source_supplier, division, product_family, product_line,
                dept_code, planner_name, planner_code, dmp_orndmp, build_type,
                prod_year_week, demand_category, original, adjustment, commit,
                new_commit1, commit1_date, commit1_week, commit1_year,
                new_commit2, commit2_date, commit2_week, commit2_year,
                new_commit3, commit3_date, commit3_week, commit3_year,
                new_commit4, commit4_date, commit4_week, commit4_year, created_by, created_at, updated_by, updated_at,
                commit_date,cause_code,cause_code_remark,
                system_commit1_accepted,system_commit2_accepted,system_commit3_accepted,system_commit4_accepted]
                print("record inserted to advance_commit table with ori_part_number", ori_part_number, "prod_week and prod_year",prod_week, prod_year,"for demand category",demand_category)
                condb(sql, col)
                sql1 = """update User_App.forecast_plan set is_processed=1,active=1 where ori_part_number=%s and prod_week={} and prod_year={}""".format(prod_week, prod_year)
                col1 = [ori_part_number]
                condb(sql1, col1)
                
            
            else:
                print("entered else part system")
                if (row.system_commit1==0 and row.system_commit1_date is None) and (row.system_commit2==0 and row.system_commit2_date is None) and \
                (row.system_commit3==0 and row.system_commit3_date is None) and (row.system_commit4==0 and row.system_commit4_date is None):
                    delete_records="""update User_App.advance_commit set 
                    commit1_old_value=commit1,commit2_old_value=commit2,commit3_old_value=commit3,commit4_old_value=commit4,
                    commit1={},commit2={},commit3={},commit4={},
                    is_delete=1,kr_processed=0,irp_processed=0 
                    where ori_part_number=%s and prod_year={} and prod_week={} and process_week={} and demand_category='{}'""".format(new_commit1,new_commit2,new_commit3,new_commit4,prod_year,prod_week,process_week,demand_category)
                    col=[ori_part_number]
                    condb(delete_records,col)
                else:
                    sql = """update User_App.advance_commit set 
                    commit1_old_value=commit1,commit1_date_old_value=commit1_date,commit1={},commit1_date=%s,commit1_week=%s,commit1_year=%s,commit1_accepted=False,commit1_irp_processed=False,
                    commit2_old_value=commit2,commit2_date_old_value=commit2_date,commit2={},commit2_date=%s,commit2_week=%s,commit2_year=%s,commit2_accepted=False,commit2_irp_processed=False,
                    commit3_old_value=commit3,commit3_date_old_value=commit3_date,commit3={},commit3_date=%s,commit3_week=%s,commit3_year=%s,commit3_accepted=False,commit3_irp_processed=False,
                    commit4_old_value=commit4,commit4_date_old_value=commit4_date,commit4={},commit4_date=%s,commit4_week=%s,commit4_year=%s,commit4_accepted=False,commit4_irp_processed=False,kr_processed=0,irp_processed=0,
                    commit_date=%s,cause_code=%s,cause_code_remark=%s
                    where ori_part_number=%s and prod_year={} and prod_week={} and process_week={} and demand_category='{}'""".format(new_commit1,new_commit2,new_commit3,new_commit4,prod_year,prod_week,process_week,demand_category)
                    print("record updated to advance_commit table with ori_part_number", ori_part_number,"prod_week and prod_year", prod_week, prod_year,"for demand category",demand_category)
                    col = [commit1_date,commit1_week,commit1_year,commit2_date,commit2_week,commit2_year,\
                    commit3_date,commit3_week,commit3_year,commit4_date,commit4_week,commit4_year,\
                    commit_date,cause_code,cause_code_remark,ori_part_number]
                    print(sql)
                    print(commit1_date,commit2_date,commit3_date,commit4_date)
                    condb(sql,col)
                    sql1 = """update User_App.forecast_plan set is_processed=1,active=1 where ori_part_number=%s and prod_week={} and prod_year={}""".format(prod_week, prod_year)
                    col1 = [ori_part_number]
                    condb(sql1, col1)
                    
                
            
    return "Success" 
