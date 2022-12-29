import json
import pandas as pd
import datetime 
import sys
import boto3
import pymysql
import logging
from DB_conn import condb,condb_dict

my_date=datetime.datetime.today()
year,week_num,day_of_week=my_date.isocalendar()
current_year=str(year)
current_year=current_year[2:]


def lambda_handler(event, context):
    current_time=datetime.datetime.now()
    get_advance_col_names="""SELECT column_name from information_schema.columns where table_schema='User_App' and table_name='advance_commit';"""
    advance_col_names=list(condb(get_advance_col_names))
    advance_db_columns=list(sum(advance_col_names,()))
    
    primary_advance_commit_query="""select * from User_App.advance_commit where process_week={} and is_delete=1 and irp_processed=0""".format(week_num)
    df=condb(primary_advance_commit_query)
    primary_advance_commit_data=pd.DataFrame(data=df,columns=advance_db_columns)
    primary_advance_commit_data=primary_advance_commit_data.fillna({'commit1':0,'commit2':0,'commit3':0,'commit4':0,
            'commit1_accepted':b'\x00','commit2_accepted':b'\x00','commit3_accepted':b'\x00','commit4_accepted':b'\x00','irp_processed':False,
            'commit1_irp_processed':b'\x00','commit2_irp_processed':b'\x00','commit3_irp_processed':b'\x00','commit4_irp_processed':b'\x00','planner_review_updated':b'\x00',
            'commit1_old_value':0,'commit2_old_value':0,'commit3_old_value':0,'commit4_old_value':0,'is_delete':b'\x00',
            'commit1_week':0,'commit2_week':0,'commit3_week':0,'commit4_week':0,
            'commit1_year':2000,'commit2_year':2000,'commit3_year':2000,'commit4_year':2000})
    
    
    def handling_null(demand_category,ori_part_number,prod_week,prod_year):
            commit_week=prod_week
            commit_year=prod_year
            ori_part_number=ori_part_number
            demand_category=demand_category.lower().replace(' ','')
            try:
                irp_value="select " + demand_category + "_advance_commit_irp from User_App.forecast_plan where prod_week={} and prod_year={} and ori_part_number=%s".format(commit_week,commit_year)
                col=[ori_part_number]
                advance_commit_irp=condb(irp_value,col)
                print(advance_commit_irp)
                if advance_commit_irp[0][0]==None:
                    update_irp="update User_App.forecast_plan set " +demand_category+"_advance_commit_irp=0 where prod_week={} and prod_year={} and ori_part_number=%s".format(commit_week,commit_year)
                    col=[ori_part_number]  
                    condb(update_irp,col)
            except:
                pass
            
    def process_type_calculation(ori_part_number,commit_week,commit_year):
        print("entered exception calculation")
        exception_checking="""SELECT forecast_original,forecast_adj,forecast_supply,forecast_advance_commit_irp,gsa_original,gsa_adj,gsa_supply,gsa_advance_commit_irp,
        nongsa_original,nongsa_adj,nongsa_supply,nongsa_advance_commit_irp,system_original,system_adj,system_supply,system_advance_commit_irp,
        cumulative_forecast_advance_commit_irp,cumulative_system_advance_commit_irp,cumulative_gsa_advance_commit_irp,cumulative_nongsa_advance_commit_irp,id,is_dmp        
        FROM User_App.forecast_plan where ori_part_number=%s and prod_week={} and prod_year={};""".format(commit_week,commit_year)
        col=[ori_part_number]
        print(condb_dict(exception_checking,col))
        df1=condb_dict(exception_checking,col)[0]
        print(df1)
        forecast_original=df1['forecast_original'] if df1['forecast_original'] is not None else 0
        forecast_adj=df1['forecast_adj'] if df1['forecast_adj'] is not None else 0
        forecast_supply=df1['forecast_supply'] if df1['forecast_supply'] is not None else 0
        forecast_advance_commit_irp=df1['forecast_advance_commit_irp'] if df1['forecast_advance_commit_irp'] is not None else 0
        cumulative_forecast_advance_commit_irp=df1['cumulative_forecast_advance_commit_irp'] if df1['cumulative_forecast_advance_commit_irp'] is not None else 0
        forecast_exception=(forecast_original+forecast_adj)-(forecast_supply+forecast_advance_commit_irp+cumulative_forecast_advance_commit_irp)
        print('forecast_exception',forecast_exception,'O-',forecast_original,'A-',forecast_adj,'S-',forecast_supply,'IRP-',forecast_advance_commit_irp)
        gsa_original=df1['gsa_original'] if df1['gsa_original'] is not None else 0
        gsa_adj=df1['gsa_adj'] if df1['gsa_adj'] is not None else 0
        gsa_supply=df1['gsa_supply'] if df1['gsa_supply'] is not None else 0
        gsa_advance_commit_irp=df1['gsa_advance_commit_irp'] if df1['gsa_advance_commit_irp'] is not None else 0
        cumulative_gsa_advance_commit_irp=df1['cumulative_gsa_advance_commit_irp'] if df1['cumulative_gsa_advance_commit_irp'] is not None else 0
        gsa_exception=(gsa_original+gsa_adj)-(gsa_supply+gsa_advance_commit_irp+cumulative_gsa_advance_commit_irp)
        print('gsa_exception',gsa_exception,'O-',gsa_original,'A-',gsa_adj,'S-',gsa_supply,'IRP-',gsa_advance_commit_irp)
        nongsa_original=df1['nongsa_original'] if df1['nongsa_original'] is not None else 0
        nongsa_adj=df1['nongsa_adj'] if df1['nongsa_adj'] is not None else 0
        nongsa_supply=df1['nongsa_supply'] if df1['nongsa_supply'] is not None else 0
        nongsa_advance_commit_irp=df1['nongsa_advance_commit_irp'] if df1['nongsa_advance_commit_irp'] is not None else 0
        cumulative_nongsa_advance_commit_irp=df1['cumulative_nongsa_advance_commit_irp'] if df1['cumulative_nongsa_advance_commit_irp'] is not None else 0
        nongsa_exception=(nongsa_original+nongsa_adj)-(nongsa_supply+nongsa_advance_commit_irp+cumulative_nongsa_advance_commit_irp)
        print('nongsa_exception',nongsa_exception,'O-',nongsa_original,'A-',nongsa_adj,'S-',nongsa_supply,'IRP-',nongsa_advance_commit_irp)
        system_original=df1['system_original'] if df1['system_original'] is not None else 0
        system_adj=df1['system_adj'] if df1['system_adj'] is not None else 0
        system_supply=df1['system_supply'] if df1['system_supply'] is not None else 0
        system_advance_commit_irp=df1['system_advance_commit_irp'] if df1['system_advance_commit_irp'] is not None else 0
        cumulative_system_advance_commit_irp=df1['cumulative_system_advance_commit_irp'] if df1['cumulative_system_advance_commit_irp'] is not None else 0
        system_exception=(system_original+system_adj)-(system_supply+system_advance_commit_irp+cumulative_system_advance_commit_irp)
        print('system_exception',system_exception,'O-',system_original,'A-',system_adj,'S-',system_supply,'IRP-',system_advance_commit_irp)
        id=df1['id']
        is_dmp=1 if ord(df1['is_dmp'])==1 else 0
        process_type='Exception' if (((forecast_exception!=0)or(gsa_exception!=0)or(nongsa_exception!=0)or(system_exception!=0)) and is_dmp==1) else None
        print(process_type,is_dmp)
        sql="update User_App.forecast_plan set process_type=%s where id=%s"
        col=[process_type,id]
        print(sql,process_type,id)
        condb(sql,col)
        return print("process_type processed for",ori_part_number,process_type,commit_week,commit_year)

    
    
    for index,primary in primary_advance_commit_data.iterrows():
        id=primary.id
        commit1=primary.commit1
        commit1_old_value=primary.commit1_old_value
        commit1_week=primary.commit1_week
        commit1_year=str(primary.commit1_year)
        commit1_year=commit1_year[2:] 
        commit1_accepted=ord(primary.commit1_accepted)
        commit1_irp_processed=ord(primary.commit1_irp_processed)
        commit1_date=primary.commit1_date
        commit2=primary.commit2
        commit2_old_value=primary.commit2_old_value
        commit2_week=primary.commit2_week
        commit2_year=str(primary.commit2_year)
        commit2_year=commit2_year[2:]
        commit2_accepted=ord(primary.commit2_accepted) 
        commit2_irp_processed=ord(primary.commit2_irp_processed) 
        commit2_date=primary.commit2_date
        commit3=primary.commit3
        commit3_old_value=primary.commit3_old_value
        commit3_week=primary.commit3_week
        commit3_year=str(primary.commit3_year)
        commit3_year=commit3_year[2:]
        commit3_accepted=ord(primary.commit3_accepted) 
        commit3_irp_processed=ord(primary.commit3_irp_processed)
        commit3_date=primary.commit3_date 
        commit4=primary.commit4
        commit4_old_value=primary.commit4_old_value
        commit4_week=primary.commit4_week 
        commit4_year=str(primary.commit4_year)
        commit4_year=commit4_year[2:]
        commit4_accepted=ord(primary.commit4_accepted)
        commit4_irp_processed=ord(primary.commit4_irp_processed)
        commit4_date=primary.commit4_date 
        demand_category=primary.demand_category
        ori_part_number=str(primary.ori_part_number)
        prod_week=primary.prod_week
        prod_year=primary.prod_year
        updated_by=primary.updated_by
        planner_review_updated=ord(primary.planner_review_updated)
        is_delete=ord(primary.is_delete)
        irp_processed=ord(primary.irp_processed)

        if demand_category=='Forecast':
            print("deleted record found",id)
            if commit1_accepted==1:
                handling_null(demand_category,ori_part_number,commit1_week,commit1_year)
                sql="""update User_App.forecast_plan set forecast_advance_commit_irp=(forecast_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                where ori_part_number=%s and prod_week={} and prod_year={}""".format(commit1_old_value,commit1_week,commit1_year)
                col=[updated_by,current_time,ori_part_number]
                condb(sql,col)
                process_type_calculation(ori_part_number,commit1_week,commit1_year)
            if commit2_accepted==1:
                handling_null(demand_category,ori_part_number,commit2_week,commit2_year)
                sql="""update User_App.forecast_plan set forecast_advance_commit_irp=(forecast_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                where ori_part_number=%s and prod_week={} and prod_year={}""".format(commit2_old_value,commit2_week,commit2_year)
                col=[updated_by,current_time,ori_part_number]
                condb(sql,col)
                process_type_calculation(ori_part_number,commit2_week,commit2_year)
            if commit3_accepted==1:
                handling_null(demand_category,ori_part_number,commit3_week,commit3_year)
                sql="""update User_App.forecast_plan set forecast_advance_commit_irp=(forecast_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                where ori_part_number=%s and prod_week={} and prod_year={}""".format(commit3_old_value,commit3_week,commit3_year)
                col=[updated_by,current_time,ori_part_number]
                condb(sql,col)
                process_type_calculation(ori_part_number,commit3_week,commit3_year)
            if commit4_accepted==1:
                handling_null(demand_category,ori_part_number,commit4_week,commit4_year)
                sql="""update User_App.forecast_plan set forecast_advance_commit_irp=(forecast_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                where ori_part_number=%s and prod_week={} and prod_year={}""".format(commit4_old_value,commit4_week,commit4_year)
                col=[updated_by,current_time,ori_part_number]
                condb(sql,col)
                process_type_calculation(ori_part_number,commit4_week,commit4_year)
        if demand_category=='GSA':
            print("deleted record found",id)
            if commit1_accepted==1:
                handling_null(demand_category,ori_part_number,commit1_week,commit1_year)
                sql="""update User_App.forecast_plan set gsa_advance_commit_irp=(gsa_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                where ori_part_number=%s and prod_week={} and prod_year={}""".format(commit1_old_value,commit1_week,commit1_year)
                col=[updated_by,current_time,ori_part_number]
                condb(sql,col)
                process_type_calculation(ori_part_number,commit1_week,commit1_year)
            if commit2_accepted==1:
                handling_null(demand_category,ori_part_number,commit2_week,commit2_year)
                sql="""update User_App.forecast_plan set gsa_advance_commit_irp=(gsa_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                where ori_part_number=%s and prod_week={} and prod_year={}""".format(commit2_old_value,commit2_week,commit2_year)
                col=[updated_by,current_time,ori_part_number]
                condb(sql,col)
                process_type_calculation(ori_part_number,commit2_week,commit2_year)
            if commit3_accepted==1:
                handling_null(demand_category,ori_part_number,commit3_week,commit3_year)
                sql="""update User_App.forecast_plan set gsa_advance_commit_irp=(gsa_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                where ori_part_number=%s and prod_week={} and prod_year={}""".format(commit3_old_value,commit3_week,commit3_year)
                col=[updated_by,current_time,ori_part_number]
                condb(sql,col)
                process_type_calculation(ori_part_number,commit3_week,commit3_year)
            if commit4_accepted==1:
                handling_null(demand_category,ori_part_number,commit4_week,commit4_year)
                sql="""update User_App.forecast_plan set gsa_advance_commit_irp=(gsa_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                where ori_part_number=%s and prod_week={} and prod_year={}""".format(commit4_old_value,commit4_week,commit4_year)
                col=[updated_by,current_time,ori_part_number]
                condb(sql,col)
                process_type_calculation(ori_part_number,commit4_week,commit4_year)
        
        if demand_category=='Non GSA':
            print("deleted record found",id)
            if commit1_accepted==1:
                handling_null(demand_category,ori_part_number,commit1_week,commit1_year)
                sql="""update User_App.forecast_plan set nongsa_advance_commit_irp=(nongsa_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                where ori_part_number=%s and prod_week={} and prod_year={}""".format(commit1_old_value,commit1_week,commit1_year)
                col=[updated_by,current_time,ori_part_number]
                condb(sql,col)
                process_type_calculation(ori_part_number,commit1_week,commit1_year)
            if commit2_accepted==1:
                handling_null(demand_category,ori_part_number,commit2_week,commit2_year)
                sql="""update User_App.forecast_plan set nongsa_advance_commit_irp=(nongsa_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                where ori_part_number=%s and prod_week={} and prod_year={}""".format(commit2_old_value,commit2_week,commit2_year)
                col=[updated_by,current_time,ori_part_number]
                condb(sql,col)
                process_type_calculation(ori_part_number,commit2_week,commit2_year)
            if commit3_accepted==1:
                handling_null(demand_category,ori_part_number,commit3_week,commit3_year)
                sql="""update User_App.forecast_plan set nongsa_advance_commit_irp=(nongsa_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                where ori_part_number=%s and prod_week={} and prod_year={}""".format(commit3_old_value,commit3_week,commit3_year)
                col=[updated_by,current_time,ori_part_number]
                condb(sql,col)
                process_type_calculation(ori_part_number,commit3_week,commit3_year)
            if commit4_accepted==1:
                handling_null(demand_category,ori_part_number,commit4_week,commit4_year)
                sql="""update User_App.forecast_plan set nongsa_advance_commit_irp=(nongsa_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                where ori_part_number=%s and prod_week={} and prod_year={}""".format(commit4_old_value,commit4_week,commit4_year)
                col=[updated_by,current_time,ori_part_number]
                condb(sql,col)
                process_type_calculation(ori_part_number,commit4_week,commit4_year)
                
        if demand_category=='System':
            print("deleted record found",id)
            if commit1_accepted==1:
                handling_null(demand_category,ori_part_number,commit1_week,commit1_year)
                sql="""update User_App.forecast_plan set system_advance_commit_irp=(system_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                where ori_part_number=%s and prod_week={} and prod_year={}""".format(commit1_old_value,commit1_week,commit1_year)
                col=[updated_by,current_time,ori_part_number]
                condb(sql,col)
                process_type_calculation(ori_part_number,commit1_week,commit1_year)
            if commit2_accepted==1:
                handling_null(demand_category,ori_part_number,commit2_week,commit2_year)
                sql="""update User_App.forecast_plan set system_advance_commit_irp=(system_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                where ori_part_number=%s and prod_week={} and prod_year={}""".format(commit2_old_value,commit2_week,commit2_year)
                col=[updated_by,current_time,ori_part_number]
                condb(sql,col)
                process_type_calculation(ori_part_number,commit2_week,commit2_year)
            if commit3_accepted==1:
                handling_null(demand_category,ori_part_number,commit3_week,commit3_year)
                sql="""update User_App.forecast_plan set system_advance_commit_irp=(system_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                where ori_part_number=%s and prod_week={} and prod_year={}""".format(commit3_old_value,commit3_week,commit3_year)
                col=[updated_by,current_time,ori_part_number]
                condb(sql,col)
                process_type_calculation(ori_part_number,commit3_week,commit3_year)
            if commit4_accepted==1:
                handling_null(demand_category,ori_part_number,commit4_week,commit4_year)
                sql="""update User_App.forecast_plan set system_advance_commit_irp=(system_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                where ori_part_number=%s and prod_week={} and prod_year={}""".format(commit4_old_value,commit4_week,commit4_year)
                col=[updated_by,current_time,ori_part_number]
                condb(sql,col)
                process_type_calculation(ori_part_number,commit4_week,commit4_year)

        insert_delete="""insert into User_App.advance_commit_deleted_records select * from User_App.advance_commit where id={}""".format(id)
        condb(insert_delete)
        print("inserted the deleted record to deleted records table for id:",id)
        delete_record="""delete from User_App.advance_commit where id={}""".format(id)
        condb(delete_record)
        print("deleted record from advance commit table")
        break

    return "Success"