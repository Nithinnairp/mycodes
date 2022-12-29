import json
import pandas as pd
import datetime 
import sys
import boto3
import json
import pymysql
import logging
import requests
from DB_conn import condb,condb_dict


my_date=datetime.datetime.today()
year,week_num,day_of_week=my_date.isocalendar()
current_year=str(year)
current_year=current_year[2:]

#Connecting to the DB 
def lambda_handler(event, context):
    current_time=datetime.datetime.now()
    start_time=current_time.strftime("%Y-%m-%d %H:%M")
    next_time=(current_time+datetime.timedelta(minutes=15)).strftime("%Y-%m-%d %H:%M")
    print(start_time)
    print(next_time)
    get_phase='https://collabportal.keysight.com/api/forecast-calender-phases/{}'.format(week_num)
    reponse=requests.get(get_phase)
    reponse.content
    phase_object=json.loads(reponse.content)
    current_phase=phase_object["phase"]
    print(reponse.content)
    phase=['COMMIT','REVIEW','CLOSED']
    if current_phase in phase:
        print('phase is REVIEW')
        get_advance_col_names="""SELECT column_name from information_schema.columns where table_schema='User_App' and table_name='advance_commit';"""
        advance_col_names=list(condb(get_advance_col_names))
        advance_db_columns=list(sum(advance_col_names,()))
        
        primary_advance_commit_query="""select * from User_App.advance_commit where process_week={} and irp_processed=0 or irp_processed is null""".format(week_num)
        df=condb(primary_advance_commit_query)
        primary_advance_commit_data=pd.DataFrame(data=df,columns=advance_db_columns)
        primary_advance_commit_data=primary_advance_commit_data.fillna({'commit1':0,'commit2':0,'commit3':0,'commit4':0,
                'commit1_accepted':b'\x00','commit2_accepted':b'\x00','commit3_accepted':b'\x00','commit4_accepted':b'\x00','irp_processed':False,
                'commit1_irp_processed':b'\x00','commit2_irp_processed':b'\x00','commit3_irp_processed':b'\x00','commit4_irp_processed':b'\x00','planner_review_updated':b'\x00',
                'commit1_old_value':0,'commit2_old_value':0,'commit3_old_value':0,'commit4_old_value':0,'is_delete':b'\x00',
                'commit1_week':0,'commit2_week':0,'commit3_week':0,'commit4_week':0,
                'commit1_year':2000,'commit2_year':2000,'commit3_year':2000,'commit4_year':2000})
        
        
        def error_handling():
            return 'Status Success {},{} on Line:{}'.format(sys.exc_info()[0],sys.exc_info()[1],sys.exc_info()[2])
        
        def get_date_format(date=None):
            if date is None:
                year, week, day = None, None, None
                return year, week, day
            else:
                x,y,z= str(date).split('-')
                year,week,day=datetime.date(int(x),int(y),int(z)).isocalendar()
                return year,week,day
        
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
            print(id)
            # if (ori_part_number==s_ori_part_number and process_year==s_process_year and process_week==s_process_week and prod_week==s_prod_week and prod_year==s_prod_year and demand_category==s_demand_category):
            test="""select * from User_App.advance_commit_backup where id={}""".format(id)
            df_test=condb(test)
            id_found=list(df_test) 
            print(id_found,id)
            if id_found!=[]:
                print('id found',id_found)
                secondary_advance_commit_data=pd.DataFrame(data=df_test,columns=advance_db_columns)
                secondary_advance_commit_data=secondary_advance_commit_data.fillna({'commit1':0,'commit2':0,'commit3':0,'commit4':0,
                'commit1_accepted':b'\x00','commit2_accepted':b'\x00','commit3_accepted':b'\x00','commit4_accepted':b'\x00','irp_processed':False,
                'commit1_irp_processed':b'\x00','commit2_irp_processed':b'\x00','commit3_irp_processed':b'\x00','commit4_irp_processed':b'\x00','planner_review_updated':b'\x00',
                'commit1_old_value':0,'commit2_old_value':0,'commit3_old_value':0,'commit4_old_value':0,
                'commit1_week':0,'commit2_week':0,'commit3_week':0,'commit4_week':0,
                'commit1_year':2000,'commit2_year':2000,'commit3_year':2000,'commit4_year':2000})
                for s_index,secondary in secondary_advance_commit_data.iterrows():
                    s_id=secondary.id
                    if id==s_id:
                        print(id,s_id)
                        commit1=primary.commit1
                        commit1_week=primary.commit1_week
                        commit1_year=str(primary.commit1_year)
                        commit1_year=commit1_year[2:] 
                        commit1_accepted=ord(primary.commit1_accepted)
                        commit1_irp_processed=ord(primary.commit1_irp_processed)
                        commit1_date=primary.commit1_date
                        commit2=primary.commit2
                        commit2_week=primary.commit2_week
                        commit2_year=str(primary.commit2_year)
                        commit2_year=commit2_year[2:]
                        commit2_accepted=ord(primary.commit2_accepted) 
                        commit2_irp_processed=ord(primary.commit2_irp_processed) 
                        commit2_date=primary.commit2_date
                        commit3=primary.commit3
                        commit3_week=primary.commit3_week
                        commit3_year=str(primary.commit3_year)
                        commit3_year=commit3_year[2:]
                        commit3_accepted=ord(primary.commit3_accepted) 
                        commit3_irp_processed=ord(primary.commit3_irp_processed)
                        commit3_date=primary.commit3_date 
                        commit4=primary.commit4
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
                        
                        s_commit1=secondary.commit1
                        s_commit1_week=secondary.commit1_week
                        s_commit1_year=str(secondary.commit1_year)
                        s_commit1_year=s_commit1_year[2:] 
                        s_commit1_accepted=ord(secondary.commit1_accepted)
                        s_commit1_irp_processed=ord(secondary.commit1_irp_processed) 
                        s_commit2=secondary.commit2
                        s_commit2_week=secondary.commit2_week
                        s_commit2_year=str(secondary.commit2_year)
                        s_commit2_year=s_commit2_year[2:]
                        s_commit2_accepted=ord(secondary.commit2_accepted) 
                        s_commit2_irp_processed=ord(secondary.commit2_irp_processed)
                        s_commit3=secondary.commit3
                        s_commit3_week=secondary.commit3_week
                        s_commit3_year=str(secondary.commit3_year)
                        s_commit3_year=s_commit3_year[2:]
                        s_commit3_accepted=ord(secondary.commit3_accepted) 
                        s_commit3_irp_processed=ord(secondary.commit3_irp_processed) 
                        s_commit4=secondary.commit4
                        s_commit4_week=secondary.commit4_week 
                        s_commit4_year=str(secondary.commit4_year)
                        s_commit4_year=s_commit4_year[2:]
                        s_commit4_accepted=ord(secondary.commit4_accepted)
                        s_commit4_irp_processed=ord(secondary.commit4_irp_processed)
                        s_demand_category=secondary.demand_category
                        s_ori_part_number=str(secondary.ori_part_number)
                        s_updated_by=secondary.updated_by
                        s_prod_week=secondary.prod_week
                        s_prod_year=secondary.prod_year
                        s_planner_review_updated=ord(secondary.planner_review_updated)
                        s_commit1_date=secondary.commit1_date
                        s_commit2_date=secondary.commit2_date
                        s_commit3_date=secondary.commit3_date
                        s_commit4_date=secondary.commit4_date
                        
                        
                        if demand_category=='Forecast' and s_demand_category=='Forecast':
                            print('Forecast',id,s_id,ori_part_number,s_ori_part_number,prod_week,s_prod_week,prod_year,s_prod_year)
                            if (is_delete==1):
                                print("deleted record found",id)
                                if s_commit1_accepted==1:
                                    handling_null(demand_category,ori_part_number,commit1_week,commit1_year)
                                    sql="""update User_App.forecast_plan set forecast_advance_commit_irp=(forecast_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                                    where ori_part_number=%s and prod_week={} and prod_year={}""".format(s_commit1,s_commit1_week,s_commit1_year)
                                    col=[updated_by,current_time,ori_part_number]
                                    condb(sql,col)
                                    process_type_calculation(ori_part_number,commit1_week,commit1_year)
                                if s_commit2_accepted==1:
                                    handling_null(demand_category,ori_part_number,commit2_week,commit2_year)
                                    sql="""update User_App.forecast_plan set forecast_advance_commit_irp=(forecast_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                                    where ori_part_number=%s and prod_week={} and prod_year={}""".format(s_commit2,s_commit2_week,s_commit2_year)
                                    col=[updated_by,current_time,ori_part_number]
                                    condb(sql,col)
                                    process_type_calculation(ori_part_number,commit2_week,commit2_year)
                                if s_commit3_accepted==1:
                                    handling_null(demand_category,ori_part_number,commit3_week,commit3_year)
                                    sql="""update User_App.forecast_plan set forecast_advance_commit_irp=(forecast_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                                    where ori_part_number=%s and prod_week={} and prod_year={}""".format(s_commit3,s_commit3_week,s_commit3_year)
                                    col=[updated_by,current_time,ori_part_number]
                                    condb(sql,col)
                                    process_type_calculation(ori_part_number,commit3_week,commit3_year)
                                if s_commit4_accepted==1:
                                    handling_null(demand_category,ori_part_number,commit4_week,commit4_year)
                                    sql="""update User_App.forecast_plan set forecast_advance_commit_irp=(forecast_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                                    where ori_part_number=%s and prod_week={} and prod_year={}""".format(s_commit4,s_commit4_week,s_commit4_year)
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
                                    
                            if (commit1 is not None)and(commit1_date==s_commit1_date):
                                print("No change in date found for commit1")
                                if (commit1!=s_commit1):
                                    print("change in commit value")
                                    if (s_commit1_accepted==1):
                                        print("previous commit was accepted")
                                        handling_null(demand_category,ori_part_number,commit1_week,commit1_year)
                                        sql="""update User_App.forecast_plan set forecast_advance_commit_irp=(forecast_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                                        where ori_part_number=%s and prod_week={} and prod_year={}""".format(s_commit1,s_commit1_week,s_commit1_year)
                                        col=[updated_by,current_time,ori_part_number]
                                        condb(sql,col)
                                        process_type_calculation(ori_part_number,s_commit1_week,s_commit1_year)
                                    if (commit1_accepted==1):
                                        print("new commit is accepted")
                                        handling_null(demand_category,ori_part_number,commit1_week,commit1_year)
                                        sql="""update User_App.forecast_plan set forecast_advance_commit_irp=(forecast_advance_commit_irp)+({}),updated_by=%s,updated_at=%s,active=1
                                        where ori_part_number=%s and prod_week={} and prod_year={}""".format(commit1,commit1_week,commit1_year)
                                        col=[updated_by,current_time,ori_part_number] 
                                        condb(sql,col)
                                        sql1="""update User_App.advance_commit set irp_processed=1,commit1_irp_processed=0,planner_review_updated=0 where id={}""".format(id)
                                        condb(sql1)
                                        process_type_calculation(ori_part_number,commit1_week,commit1_year)
                                if (commit1==s_commit1):
                                    print("No change in the commit value for commit1")
                                    if (commit1_accepted==1)and(s_commit1_accepted==0):
                                        print('latest commit is Accepted')
                                        handling_null(demand_category,ori_part_number,commit1_week,commit1_year)
                                        sql="""update User_App.forecast_plan set forecast_advance_commit_irp=(forecast_advance_commit_irp)+({}),updated_by=%s,updated_at=%s,active=1
                                        where ori_part_number=%s and prod_week={} and prod_year={}""".format(commit1,commit1_week,commit1_year)
                                        col=[updated_by,current_time,ori_part_number] 
                                        condb(sql,col)
                                        sql1="""update User_App.advance_commit set irp_processed=1,commit1_irp_processed=0,planner_review_updated=0 where id={}""".format(id)
                                        condb(sql1)
                                        process_type_calculation(ori_part_number,commit1_week,commit1_year)
                                    if (commit1_accepted==0)and(s_commit1_accepted==1):
                                        print('latest commit is Rejected')
                                        handling_null(demand_category,ori_part_number,commit1_week,commit1_year)
                                        sql="""update User_App.forecast_plan set forecast_advance_commit_irp=(forecast_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                                        where ori_part_number=%s and prod_week={} and prod_year={}""".format(s_commit1,s_commit1_week,s_commit1_year)
                                        col=[updated_by,current_time,ori_part_number]
                                        condb(sql,col)
                                        process_type_calculation(ori_part_number,s_commit1_week,s_commit1_year)
                            if (commit1 is not None)and(commit1_date!=s_commit1_date):
                                print("Change in date found for commit1")
                                if (s_commit1_accepted==1):
                                    print("previous value was accepted")
                                    handling_null(demand_category,ori_part_number,commit1_week,commit1_year)
                                    sql="""update User_App.forecast_plan set forecast_advance_commit_irp=(forecast_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                                    where ori_part_number=%s and prod_week={} and prod_year={}""".format(s_commit1,s_commit1_week,s_commit1_year)
                                    col=[updated_by,current_time,ori_part_number]
                                    condb(sql,col)
                                    process_type_calculation(ori_part_number,s_commit1_week,s_commit1_year)
                                if (commit1_accepted==1):
                                    print('current commit is accepted for new date')
                                    handling_null(demand_category,ori_part_number,commit1_week,commit1_year)
                                    sql="""update User_App.forecast_plan set forecast_advance_commit_irp=(forecast_advance_commit_irp)+({}),updated_by=%s,updated_at=%s,active=1
                                    where ori_part_number=%s and prod_week={} and prod_year={}""".format(commit1,commit1_week,commit1_year)
                                    col=[updated_by,current_time,ori_part_number] 
                                    condb(sql,col)
                                    sql1="""update User_App.advance_commit set irp_processed=1,commit1_irp_processed=0,planner_review_updated=0 where id={}""".format(id)
                                    condb(sql1)
                                    process_type_calculation(ori_part_number,commit1_week,commit1_year)
                            if (commit1 is None)and(s_commit1!=0)and(s_commit1_accepted==1):
                                handling_null(demand_category,ori_part_number,commit1_week,commit1_year)
                                sql="""update User_App.forecast_plan set forecast_advance_commit_irp=(forecast_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                                where ori_part_number=%s and prod_week={} and prod_year={}""".format(s_commit1,s_commit1_week,s_commit1_year)
                                col=[updated_by,current_time,ori_part_number]
                                condb(sql,col)
                                process_type_calculation(ori_part_number,s_commit1_week,s_commit1_year)
                                
                                
                            if (commit2 is not None)and(commit2_date==s_commit2_date):
                                print("No change in date found for commit2")
                                if (commit2!=s_commit2):
                                    print("change in commit value")
                                    if (s_commit2_accepted==1):
                                        print("previous commit was accepted")
                                        handling_null(demand_category,ori_part_number,commit2_week,commit2_year)
                                        sql="""update User_App.forecast_plan set forecast_advance_commit_irp=(forecast_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                                        where ori_part_number=%s and prod_week={} and prod_year={}""".format(s_commit2,s_commit2_week,s_commit2_year)
                                        col=[updated_by,current_time,ori_part_number]
                                        condb(sql,col)
                                        process_type_calculation(ori_part_number,s_commit2_week,s_commit2_year)
                                    if (commit2_accepted==1):
                                        print("new commit is accepted")
                                        handling_null(demand_category,ori_part_number,commit2_week,commit2_year)
                                        sql="""update User_App.forecast_plan set forecast_advance_commit_irp=(forecast_advance_commit_irp)+({}),updated_by=%s,updated_at=%s,active=1
                                        where ori_part_number=%s and prod_week={} and prod_year={}""".format(commit2,commit2_week,commit2_year)
                                        col=[updated_by,current_time,ori_part_number] 
                                        condb(sql,col)
                                        sql1="""update User_App.advance_commit set irp_processed=1,commit2_irp_processed=0,planner_review_updated=0 where id={}""".format(id)
                                        condb(sql1)
                                        process_type_calculation(ori_part_number,commit2_week,commit2_year)
                                if (commit2==s_commit2):
                                    print("No change in the commit value for commit2")
                                    if (commit2_accepted==1)and(s_commit2_accepted==0):
                                        print('latest commit is Accepted',commit2)
                                        handling_null(demand_category,ori_part_number,commit2_week,commit2_year)
                                        sql="""update User_App.forecast_plan set forecast_advance_commit_irp=(forecast_advance_commit_irp)+({}),updated_by=%s,updated_at=%s,active=1
                                        where ori_part_number=%s and prod_week={} and prod_year={}""".format(commit2,commit2_week,commit2_year)
                                        print(sql)
                                        col=[updated_by,current_time,ori_part_number] 
                                        condb(sql,col)
                                        sql1="""update User_App.advance_commit set irp_processed=1,commit2_irp_processed=0,planner_review_updated=0 where id={}""".format(id)
                                        condb(sql1)
                                        process_type_calculation(ori_part_number,commit2_week,commit2_year)
                                    if (commit2_accepted==0)and(s_commit2_accepted==1):
                                        print('latest commit is Rejected')
                                        handling_null(demand_category,ori_part_number,commit2_week,commit2_year)
                                        sql="""update User_App.forecast_plan set forecast_advance_commit_irp=(forecast_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                                        where ori_part_number=%s and prod_week={} and prod_year={}""".format(s_commit2,s_commit2_week,s_commit2_year)
                                        col=[updated_by,current_time,ori_part_number]
                                        condb(sql,col)
                                        process_type_calculation(ori_part_number,s_commit2_week,s_commit2_year)
                            if (commit2 is not None)and(commit2_date!=s_commit2_date):
                                print("Change in date found for commit2")
                                if (s_commit2_accepted==1):
                                    print("previous value was accepted")
                                    handling_null(demand_category,ori_part_number,commit2_week,commit2_year)
                                    sql="""update User_App.forecast_plan set forecast_advance_commit_irp=(forecast_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                                    where ori_part_number=%s and prod_week={} and prod_year={}""".format(s_commit2,s_commit2_week,s_commit2_year)
                                    col=[updated_by,current_time,ori_part_number]
                                    condb(sql,col)
                                    process_type_calculation(ori_part_number,s_commit2_week,s_commit2_year)
                                if (commit2_accepted==1):
                                    print('current commit is accepted for new date')
                                    handling_null(demand_category,ori_part_number,commit2_week,commit2_year)
                                    sql="""update User_App.forecast_plan set forecast_advance_commit_irp=(forecast_advance_commit_irp)+({}),updated_by=%s,updated_at=%s,active=1
                                    where ori_part_number=%s and prod_week={} and prod_year={}""".format(commit2,commit2_week,commit2_year)
                                    col=[updated_by,current_time,ori_part_number] 
                                    condb(sql,col)
                                    sql1="""update User_App.advance_commit set irp_processed=1,commit2_irp_processed=0,planner_review_updated=0 where id={}""".format(id)
                                    condb(sql1)
                                    process_type_calculation(ori_part_number,commit2_week,commit2_year)
                            if (commit2 is None)and(s_commit2!=0)and(s_commit2_accepted==1):
                                handling_null(demand_category,ori_part_number,commit2_week,commit2_year)
                                sql="""update User_App.forecast_plan set forecast_advance_commit_irp=(forecast_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                                where ori_part_number=%s and prod_week={} and prod_year={}""".format(s_commit2,s_commit2_week,s_commit2_year)
                                col=[updated_by,current_time,ori_part_number]
                                condb(sql,col)
                                process_type_calculation(ori_part_number,s_commit2_week,s_commit2_year)
                                
                                
                            if (commit3 is not None)and(commit3_date==s_commit3_date):
                                print("No change in date found for commit3")
                                if (commit3!=s_commit3):
                                    print("change in commit value")
                                    if (s_commit3_accepted==1):
                                        print("previous commit was accepted")
                                        handling_null(demand_category,ori_part_number,commit3_week,commit3_year)
                                        sql="""update User_App.forecast_plan set forecast_advance_commit_irp=(forecast_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                                        where ori_part_number=%s and prod_week={} and prod_year={}""".format(s_commit3,s_commit3_week,s_commit3_year)
                                        col=[updated_by,current_time,ori_part_number]
                                        condb(sql,col)
                                        process_type_calculation(ori_part_number,s_commit3_week,s_commit3_year)
                                    if (commit3_accepted==1):
                                        print("new commit is accepted")
                                        handling_null(demand_category,ori_part_number,commit3_week,commit3_year)
                                        sql="""update User_App.forecast_plan set forecast_advance_commit_irp=(forecast_advance_commit_irp)+({}),updated_by=%s,updated_at=%s,active=1
                                        where ori_part_number=%s and prod_week={} and prod_year={}""".format(commit3,commit3_week,commit3_year)
                                        col=[updated_by,current_time,ori_part_number] 
                                        condb(sql,col)
                                        sql1="""update User_App.advance_commit set irp_processed=1,commit3_irp_processed=0,planner_review_updated=0 where id={}""".format(id)
                                        condb(sql1)
                                        process_type_calculation(ori_part_number,commit3_week,commit3_year)
                                if (commit3==s_commit3):
                                    print("No change in the commit value for commit3")
                                    if (commit3_accepted==1)and(s_commit3_accepted==0):
                                        print('latest commit is Accepted',commit3)
                                        handling_null(demand_category,ori_part_number,commit3_week,commit3_year)
                                        sql="""update User_App.forecast_plan set forecast_advance_commit_irp=(forecast_advance_commit_irp)+({}),updated_by=%s,updated_at=%s,active=1
                                        where ori_part_number=%s and prod_week={} and prod_year={}""".format(commit3,commit3_week,commit3_year)
                                        col=[updated_by,current_time,ori_part_number] 
                                        condb(sql,col)
                                        sql1="""update User_App.advance_commit set irp_processed=1,commit3_irp_processed=0,planner_review_updated=0 where id={}""".format(id)
                                        condb(sql1)
                                        process_type_calculation(ori_part_number,commit3_week,commit3_year)
                                    if (commit3_accepted==0)and(s_commit3_accepted==1):
                                        print('latest commit is Rejected',s_commit3)
                                        handling_null(demand_category,ori_part_number,commit3_week,commit3_year)
                                        sql="""update User_App.forecast_plan set forecast_advance_commit_irp=(forecast_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                                        where ori_part_number=%s and prod_week={} and prod_year={}""".format(s_commit3,s_commit3_week,s_commit3_year)
                                        col=[updated_by,current_time,ori_part_number]
                                        condb(sql,col)
                                        process_type_calculation(ori_part_number,s_commit3_week,s_commit3_year)
                            if (commit3 is not None)and(commit3_date!=s_commit3_date):
                                print("Change in date found for commit3")
                                if (s_commit3_accepted==1):
                                    print("previous value was accepted")
                                    handling_null(demand_category,ori_part_number,commit3_week,commit3_year)
                                    sql="""update User_App.forecast_plan set forecast_advance_commit_irp=(forecast_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                                    where ori_part_number=%s and prod_week={} and prod_year={}""".format(s_commit3,s_commit3_week,s_commit3_year)
                                    col=[updated_by,current_time,ori_part_number]
                                    condb(sql,col)
                                    process_type_calculation(ori_part_number,s_commit3_week,s_commit3_year)
                                if (commit3_accepted==1):
                                    print('current commit is accepted for new date')
                                    handling_null(demand_category,ori_part_number,commit3_week,commit3_year)
                                    sql="""update User_App.forecast_plan set forecast_advance_commit_irp=(forecast_advance_commit_irp)+({}),updated_by=%s,updated_at=%s,active=1
                                    where ori_part_number=%s and prod_week={} and prod_year={}""".format(commit3,commit3_week,commit3_year)
                                    col=[updated_by,current_time,ori_part_number] 
                                    condb(sql,col)
                                    sql1="""update User_App.advance_commit set irp_processed=1,commit3_irp_processed=0,planner_review_updated=0 where id={}""".format(id)
                                    condb(sql1)
                                    process_type_calculation(ori_part_number,commit3_week,commit3_year)
                            if (commit3 is None)and(s_commit3!=0)and(s_commit3_accepted==1):
                                handling_null(demand_category,ori_part_number,commit3_week,commit3_year)
                                sql="""update User_App.forecast_plan set forecast_advance_commit_irp=(forecast_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                                where ori_part_number=%s and prod_week={} and prod_year={}""".format(s_commit3,s_commit3_week,s_commit3_year)
                                col=[updated_by,current_time,ori_part_number]
                                condb(sql,col)
                                process_type_calculation(ori_part_number,s_commit3_week,s_commit3_year)
                            
                                
                            if (commit4 is not None)and(commit4_date==s_commit4_date):
                                print("No change in date found for commit4")
                                if (commit4!=s_commit4):
                                    print("change in commit value")
                                    if (s_commit4_accepted==1):
                                        print("previous commit was accepted")
                                        handling_null(demand_category,ori_part_number,commit4_week,commit4_year)
                                        sql="""update User_App.forecast_plan set forecast_advance_commit_irp=(forecast_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                                        where ori_part_number=%s and prod_week={} and prod_year={}""".format(s_commit4,s_commit4_week,s_commit4_year)
                                        col=[updated_by,current_time,ori_part_number]
                                        condb(sql,col)
                                        process_type_calculation(ori_part_number,s_commit4_week,s_commit4_year)
                                    if (commit4_accepted==1):
                                        print("new commit is accepted")
                                        handling_null(demand_category,ori_part_number,commit4_week,commit4_year)
                                        sql="""update User_App.forecast_plan set forecast_advance_commit_irp=(forecast_advance_commit_irp)+({}),updated_by=%s,updated_at=%s,active=1
                                        where ori_part_number=%s and prod_week={} and prod_year={}""".format(commit4,commit4_week,commit4_year)
                                        col=[updated_by,current_time,ori_part_number] 
                                        condb(sql,col)
                                        sql1="""update User_App.advance_commit set irp_processed=1,commit4_irp_processed=0,planner_review_updated=0 where id={}""".format(id)
                                        condb(sql1)
                                        process_type_calculation(ori_part_number,commit4_week,commit4_year)
                                if (commit4==s_commit4):
                                    print("No change in the commit value for commit4")
                                    if(commit4_accepted==1)and(s_commit4_accepted==0):
                                        print('latest commit is Accepted')
                                        handling_null(demand_category,ori_part_number,commit4_week,commit4_year)
                                        sql="""update User_App.forecast_plan set forecast_advance_commit_irp=(forecast_advance_commit_irp)+({}),updated_by=%s,updated_at=%s,active=1
                                        where ori_part_number=%s and prod_week={} and prod_year={}""".format(commit4,commit4_week,commit4_year)
                                        col=[updated_by,current_time,ori_part_number] 
                                        condb(sql,col)
                                        sql1="""update User_App.advance_commit set irp_processed=1,commit4_irp_processed=0,planner_review_updated=0 where id={}""".format(id)
                                        condb(sql1)
                                        process_type_calculation(ori_part_number,commit4_week,commit4_year)
                                    if (commit4_accepted==0)and(s_commit4_accepted==1):
                                        print('latest commit is Rejected')
                                        handling_null(demand_category,ori_part_number,commit4_week,commit4_year)
                                        sql="""update User_App.forecast_plan set forecast_advance_commit_irp=(forecast_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                                        where ori_part_number=%s and prod_week={} and prod_year={}""".format(s_commit4,s_commit4_week,s_commit4_year)
                                        col=[updated_by,current_time,ori_part_number]
                                        condb(sql,col)
                                        process_type_calculation(ori_part_number,s_commit4_week,s_commit4_year)
                            if (commit4 is not None)and(commit4_date!=s_commit4_date):
                                print("Change in date found for commit4")
                                if (s_commit4_accepted==1):
                                    print("previous value was accepted")
                                    handling_null(demand_category,ori_part_number,commit4_week,commit4_year)
                                    sql="""update User_App.forecast_plan set forecast_advance_commit_irp=(forecast_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                                    where ori_part_number=%s and prod_week={} and prod_year={}""".format(s_commit4,s_commit4_week,s_commit4_year)
                                    col=[updated_by,current_time,ori_part_number]
                                    condb(sql,col)
                                    process_type_calculation(ori_part_number,s_commit4_week,s_commit4_year)
                                if (commit4_accepted==1):
                                    print('current commit is accepted for new date')
                                    handling_null(demand_category,ori_part_number,commit4_week,commit4_year)
                                    sql="""update User_App.forecast_plan set forecast_advance_commit_irp=(forecast_advance_commit_irp)+({}),updated_by=%s,updated_at=%s,active=1
                                    where ori_part_number=%s and prod_week={} and prod_year={}""".format(commit4,commit4_week,commit4_year)
                                    col=[updated_by,current_time,ori_part_number] 
                                    condb(sql,col)
                                    sql1="""update User_App.advance_commit set irp_processed=1,commit4_irp_processed=0,planner_review_updated=0 where id={}""".format(id)
                                    condb(sql1)
                                    process_type_calculation(ori_part_number,commit4_week,commit4_year)
                            if (commit4 is None)and(s_commit4!=0)and(s_commit4_accepted==1):
                                handling_null(demand_category,ori_part_number,commit4_week,commit4_year)
                                sql="""update User_App.forecast_plan set forecast_advance_commit_irp=(forecast_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                                where ori_part_number=%s and prod_week={} and prod_year={}""".format(s_commit4,s_commit4_week,s_commit4_year)
                                col=[updated_by,current_time,ori_part_number]
                                condb(sql,col)
                                process_type_calculation(ori_part_number,s_commit4_week,s_commit4_year)
                                
                        
                        
                        if demand_category=='GSA' and s_demand_category=='GSA':
                            print('GSA',id,s_id,ori_part_number,s_ori_part_number,prod_week,s_prod_week,prod_year,s_prod_year)
                            if (is_delete==1):
                                print("deleted record found",id)
                                if s_commit1_accepted==1:
                                    handling_null(demand_category,ori_part_number,commit1_week,commit1_year)
                                    sql="""update User_App.forecast_plan set gsa_advance_commit_irp=(gsa_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                                    where ori_part_number=%s and prod_week={} and prod_year={}""".format(s_commit1,s_commit1_week,s_commit1_year)
                                    col=[updated_by,current_time,ori_part_number]
                                    condb(sql,col)
                                    process_type_calculation(ori_part_number,commit1_week,commit1_year)
                                if s_commit2_accepted==1:
                                    handling_null(demand_category,ori_part_number,commit2_week,commit2_year)
                                    sql="""update User_App.forecast_plan set gsa_advance_commit_irp=(gsa_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                                    where ori_part_number=%s and prod_week={} and prod_year={}""".format(s_commit2,s_commit2_week,s_commit2_year)
                                    col=[updated_by,current_time,ori_part_number]
                                    condb(sql,col)
                                    process_type_calculation(ori_part_number,commit2_week,commit2_year)
                                if s_commit3_accepted==1:
                                    handling_null(demand_category,ori_part_number,commit3_week,commit3_year)
                                    sql="""update User_App.forecast_plan set gsa_advance_commit_irp=(gsa_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                                    where ori_part_number=%s and prod_week={} and prod_year={}""".format(s_commit3,s_commit3_week,s_commit3_year)
                                    col=[updated_by,current_time,ori_part_number]
                                    condb(sql,col)
                                    process_type_calculation(ori_part_number,commit3_week,commit3_year)
                                if s_commit4_accepted==1:
                                    handling_null(demand_category,ori_part_number,commit4_week,commit4_year)
                                    sql="""update User_App.forecast_plan set gsa_advance_commit_irp=(gsa_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                                    where ori_part_number=%s and prod_week={} and prod_year={}""".format(s_commit4,s_commit4_week,s_commit4_year)
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
                            
                                
                            if (commit1 is not None)and(commit1_date==s_commit1_date):
                                print("No change in date found for commit1")
                                if (commit1!=s_commit1):
                                    print("change in commit value")
                                    if (s_commit1_accepted==1):
                                        print("previous commit was accepted")
                                        handling_null(demand_category,ori_part_number,commit1_week,commit1_year)
                                        sql="""update User_App.forecast_plan set gsa_advance_commit_irp=(gsa_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                                        where ori_part_number=%s and prod_week={} and prod_year={}""".format(s_commit1,s_commit1_week,s_commit1_year)
                                        col=[updated_by,current_time,ori_part_number]
                                        condb(sql,col)
                                        process_type_calculation(ori_part_number,s_commit1_week,s_commit1_year)
                                    if (commit1_accepted==1):
                                        print("new commit is accepted")
                                        handling_null(demand_category,ori_part_number,commit1_week,commit1_year)
                                        sql="""update User_App.forecast_plan set gsa_advance_commit_irp=(gsa_advance_commit_irp)+({}),updated_by=%s,updated_at=%s,active=1
                                        where ori_part_number=%s and prod_week={} and prod_year={}""".format(commit1,commit1_week,commit1_year)
                                        col=[updated_by,current_time,ori_part_number] 
                                        condb(sql,col)
                                        sql1="""update User_App.advance_commit set irp_processed=1,commit1_irp_processed=0,planner_review_updated=0 where id={}""".format(id)
                                        condb(sql1)
                                        process_type_calculation(ori_part_number,commit1_week,commit1_year)
                                if (commit1==s_commit1):
                                    print("No change in the commit value for commit1")
                                    if (commit1_accepted==1)and(s_commit1_accepted==0):
                                        print('latest commit is Accepted')
                                        handling_null(demand_category,ori_part_number,commit1_week,commit1_year)
                                        sql="""update User_App.forecast_plan set gsa_advance_commit_irp=(gsa_advance_commit_irp)+({}),updated_by=%s,updated_at=%s,active=1
                                        where ori_part_number=%s and prod_week={} and prod_year={}""".format(commit1,commit1_week,commit1_year)
                                        col=[updated_by,current_time,ori_part_number] 
                                        condb(sql,col)
                                        sql1="""update User_App.advance_commit set irp_processed=1,commit1_irp_processed=0,planner_review_updated=0 where id={}""".format(id)
                                        condb(sql1)
                                        process_type_calculation(ori_part_number,commit1_week,commit1_year)
                                    if (commit1_accepted==0)and(s_commit1_accepted==1):
                                        print('latest commit is Rejected')
                                        handling_null(demand_category,ori_part_number,commit1_week,commit1_year)
                                        sql="""update User_App.forecast_plan set gsa_advance_commit_irp=(gsa_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                                        where ori_part_number=%s and prod_week={} and prod_year={}""".format(s_commit1,s_commit1_week,s_commit1_year)
                                        col=[updated_by,current_time,ori_part_number]
                                        condb(sql,col)
                                        process_type_calculation(ori_part_number,s_commit1_week,s_commit1_year)
                            if (commit1 is not None)and(commit1_date!=s_commit1_date):
                                print("Change in date found for commit1")
                                if (s_commit1_accepted==1):
                                    print("previous value was accepted")
                                    handling_null(demand_category,ori_part_number,commit1_week,commit1_year)
                                    sql="""update User_App.forecast_plan set gsa_advance_commit_irp=(gsa_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                                    where ori_part_number=%s and prod_week={} and prod_year={}""".format(s_commit1,s_commit1_week,s_commit1_year)
                                    col=[updated_by,current_time,ori_part_number]
                                    condb(sql,col)
                                    process_type_calculation(ori_part_number,s_commit1_week,s_commit1_year)
                                if (commit1_accepted==1):
                                    print('current commit is accepted for new date')
                                    handling_null(demand_category,ori_part_number,commit1_week,commit1_year)
                                    sql="""update User_App.forecast_plan set gsa_advance_commit_irp=(gsa_advance_commit_irp)+({}),updated_by=%s,updated_at=%s,active=1
                                    where ori_part_number=%s and prod_week={} and prod_year={}""".format(commit1,commit1_week,commit1_year)
                                    col=[updated_by,current_time,ori_part_number] 
                                    condb(sql,col)
                                    sql1="""update User_App.advance_commit set irp_processed=1,commit1_irp_processed=0,planner_review_updated=0 where id={}""".format(id)
                                    condb(sql1)
                                    process_type_calculation(ori_part_number,commit1_week,commit1_year)
                            if (commit1 is None)and(s_commit1!=0)and(s_commit1_accepted==1):
                                handling_null(demand_category,ori_part_number,commit1_week,commit1_year)
                                sql="""update User_App.forecast_plan set gsa_advance_commit_irp=(gsa_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                                where ori_part_number=%s and prod_week={} and prod_year={}""".format(s_commit1,s_commit1_week,s_commit1_year)
                                col=[updated_by,current_time,ori_part_number]
                                condb(sql,col)
                                process_type_calculation(ori_part_number,s_commit1_week,s_commit1_year)
                                
                                
                            if (commit2 is not None)and(commit2_date==s_commit2_date):
                                print("No change in date found for commit2")
                                if (commit2!=s_commit2):
                                    print("change in commit value")
                                    if (s_commit2_accepted==1):
                                        print("previous commit was accepted")
                                        handling_null(demand_category,ori_part_number,commit2_week,commit2_year)
                                        sql="""update User_App.forecast_plan set gsa_advance_commit_irp=(gsa_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                                        where ori_part_number=%s and prod_week={} and prod_year={}""".format(s_commit2,s_commit2_week,s_commit2_year)
                                        col=[updated_by,current_time,ori_part_number]
                                        condb(sql,col)
                                        process_type_calculation(ori_part_number,s_commit2_week,s_commit2_year)
                                    if (commit2_accepted==1):
                                        print("new commit is accepted")
                                        handling_null(demand_category,ori_part_number,commit2_week,commit2_year)
                                        sql="""update User_App.forecast_plan set gsa_advance_commit_irp=(gsa_advance_commit_irp)+({}),updated_by=%s,updated_at=%s,active=1
                                        where ori_part_number=%s and prod_week={} and prod_year={}""".format(commit2,commit2_week,commit2_year)
                                        col=[updated_by,current_time,ori_part_number] 
                                        condb(sql,col)
                                        sql1="""update User_App.advance_commit set irp_processed=1,commit2_irp_processed=0,planner_review_updated=0 where id={}""".format(id)
                                        condb(sql1)
                                        process_type_calculation(ori_part_number,commit2_week,commit2_year)
                                if (commit2==s_commit2):
                                    print("No change in the commit value for commit2")
                                    if (commit2_accepted==1)and(s_commit2_accepted==0):
                                        print('latest commit is Accepted')
                                        handling_null(demand_category,ori_part_number,commit2_week,commit2_year)
                                        sql="""update User_App.forecast_plan set gsa_advance_commit_irp=(gsa_advance_commit_irp)+({}),updated_by=%s,updated_at=%s,active=1
                                        where ori_part_number=%s and prod_week={} and prod_year={}""".format(commit2,commit2_week,commit2_year)
                                        col=[updated_by,current_time,ori_part_number] 
                                        condb(sql,col)
                                        sql1="""update User_App.advance_commit set irp_processed=1,commit2_irp_processed=0,planner_review_updated=0 where id={}""".format(id)
                                        condb(sql1)
                                        process_type_calculation(ori_part_number,commit2_week,commit2_year)
                                    if (commit2_accepted==0)and(s_commit2_accepted==1):
                                        print('latest commit is Rejected')
                                        handling_null(demand_category,ori_part_number,commit2_week,commit2_year)
                                        sql="""update User_App.forecast_plan set gsa_advance_commit_irp=(gsa_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                                        where ori_part_number=%s and prod_week={} and prod_year={}""".format(s_commit2,s_commit2_week,s_commit2_year)
                                        col=[updated_by,current_time,ori_part_number]
                                        condb(sql,col)
                                        process_type_calculation(ori_part_number,s_commit2_week,s_commit2_year)
                            if (commit2 is not None)and(commit2_date!=s_commit2_date):
                                print("Change in date found for commit2")
                                if (s_commit2_accepted==1):
                                    print("previous value was accepted")
                                    handling_null(demand_category,ori_part_number,commit2_week,commit2_year)
                                    sql="""update User_App.forecast_plan set gsa_advance_commit_irp=(gsa_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                                    where ori_part_number=%s and prod_week={} and prod_year={}""".format(s_commit2,s_commit2_week,s_commit2_year)
                                    col=[updated_by,current_time,ori_part_number]
                                    condb(sql,col)
                                    process_type_calculation(ori_part_number,s_commit2_week,s_commit2_year)
                                if (commit2_accepted==1):
                                    print('current commit is accepted for new date')
                                    handling_null(demand_category,ori_part_number,commit2_week,commit2_year)
                                    sql="""update User_App.forecast_plan set gsa_advance_commit_irp=(gsa_advance_commit_irp)+({}),updated_by=%s,updated_at=%s,active=1
                                    where ori_part_number=%s and prod_week={} and prod_year={}""".format(commit2,commit2_week,commit2_year)
                                    col=[updated_by,current_time,ori_part_number] 
                                    condb(sql,col)
                                    sql1="""update User_App.advance_commit set irp_processed=1,commit2_irp_processed=0,planner_review_updated=0 where id={}""".format(id)
                                    condb(sql1)
                                    process_type_calculation(ori_part_number,commit2_week,commit2_year)
                            if (commit2 is None)and(s_commit2!=0)and(s_commit2_accepted==1):
                                handling_null(demand_category,ori_part_number,commit2_week,commit2_year)
                                sql="""update User_App.forecast_plan set gsa_advance_commit_irp=(gsa_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                                where ori_part_number=%s and prod_week={} and prod_year={}""".format(s_commit2,s_commit2_week,s_commit2_year)
                                col=[updated_by,current_time,ori_part_number]
                                condb(sql,col)
                                process_type_calculation(ori_part_number,s_commit2_week,s_commit2_year)
                                
                                
                            if (commit3 is not None)and(commit3_date==s_commit3_date):
                                print("No change in date found for commit3")
                                if (commit3!=s_commit3):
                                    print("change in commit value")
                                    if (s_commit3_accepted==1):
                                        print("previous commit was accepted")
                                        handling_null(demand_category,ori_part_number,commit3_week,commit3_year)
                                        sql="""update User_App.forecast_plan set gsa_advance_commit_irp=(gsa_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                                        where ori_part_number=%s and prod_week={} and prod_year={}""".format(s_commit3,s_commit3_week,s_commit3_year)
                                        col=[updated_by,current_time,ori_part_number]
                                        condb(sql,col)
                                        process_type_calculation(ori_part_number,s_commit3_week,s_commit3_year)
                                    if (commit3_accepted==1):
                                        print("new commit is accepted")
                                        handling_null(demand_category,ori_part_number,commit3_week,commit3_year)
                                        sql="""update User_App.forecast_plan set gsa_advance_commit_irp=(gsa_advance_commit_irp)+({}),updated_by=%s,updated_at=%s,active=1
                                        where ori_part_number=%s and prod_week={} and prod_year={}""".format(commit3,commit3_week,commit3_year)
                                        col=[updated_by,current_time,ori_part_number] 
                                        condb(sql,col)
                                        sql1="""update User_App.advance_commit set irp_processed=1,commit3_irp_processed=0,planner_review_updated=0 where id={}""".format(id)
                                        condb(sql1)
                                        process_type_calculation(ori_part_number,commit3_week,commit3_year)
                                if (commit3==s_commit3):
                                    print("No change in the commit value for commit3")
                                    if (commit3_accepted==1)and(s_commit3_accepted==0):
                                        print('latest commit is Accepted')
                                        handling_null(demand_category,ori_part_number,commit3_week,commit3_year)
                                        sql="""update User_App.forecast_plan set gsa_advance_commit_irp=(gsa_advance_commit_irp)+({}),updated_by=%s,updated_at=%s,active=1
                                        where ori_part_number=%s and prod_week={} and prod_year={}""".format(commit3,commit3_week,commit3_year)
                                        col=[updated_by,current_time,ori_part_number] 
                                        condb(sql,col)
                                        sql1="""update User_App.advance_commit set irp_processed=1,commit3_irp_processed=0,planner_review_updated=0 where id={}""".format(id)
                                        condb(sql1)
                                        process_type_calculation(ori_part_number,commit3_week,commit3_year)
                                    if (commit3_accepted==0)and(s_commit3_accepted==1):
                                        print('latest commit is Rejected')
                                        handling_null(demand_category,ori_part_number,commit3_week,commit3_year)
                                        sql="""update User_App.forecast_plan set gsa_advance_commit_irp=(gsa_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                                        where ori_part_number=%s and prod_week={} and prod_year={}""".format(s_commit3,s_commit3_week,s_commit3_year)
                                        col=[updated_by,current_time,ori_part_number]
                                        condb(sql,col)
                                        process_type_calculation(ori_part_number,s_commit3_week,s_commit3_year)
                            if (commit3 is not None)and(commit3_date!=s_commit3_date):
                                print("Change in date found for commit3")
                                if (s_commit3_accepted==1):
                                    print("previous value was accepted")
                                    handling_null(demand_category,ori_part_number,commit3_week,commit3_year)
                                    sql="""update User_App.forecast_plan set gsa_advance_commit_irp=(gsa_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                                    where ori_part_number=%s and prod_week={} and prod_year={}""".format(s_commit3,s_commit3_week,s_commit3_year)
                                    col=[updated_by,current_time,ori_part_number]
                                    condb(sql,col)
                                    process_type_calculation(ori_part_number,s_commit3_week,s_commit3_year)
                                if (commit3_accepted==1):
                                    print('current commit is accepted for new date')
                                    handling_null(demand_category,ori_part_number,commit3_week,commit3_year)
                                    sql="""update User_App.forecast_plan set gsa_advance_commit_irp=(gsa_advance_commit_irp)+({}),updated_by=%s,updated_at=%s,active=1
                                    where ori_part_number=%s and prod_week={} and prod_year={}""".format(commit3,commit3_week,commit3_year)
                                    col=[updated_by,current_time,ori_part_number] 
                                    condb(sql,col)
                                    sql1="""update User_App.advance_commit set irp_processed=1,commit3_irp_processed=0,planner_review_updated=0 where id={}""".format(id)
                                    condb(sql1)
                                    process_type_calculation(ori_part_number,commit3_week,commit3_year)
                            if (commit3 is None)and(s_commit3!=0)and(s_commit3_accepted==1):
                                handling_null(demand_category,ori_part_number,commit3_week,commit3_year)
                                sql="""update User_App.forecast_plan set gsa_advance_commit_irp=(gsa_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                                where ori_part_number=%s and prod_week={} and prod_year={}""".format(s_commit3,s_commit3_week,s_commit3_year)
                                col=[updated_by,current_time,ori_part_number]
                                condb(sql,col)
                                process_type_calculation(ori_part_number,s_commit3_week,s_commit3_year)
                                
                                
                            if (commit4 is not None)and(commit4_date==s_commit4_date):
                                print("No change in date found for commit4")
                                if (commit4!=s_commit4):
                                    print("change in commit value")
                                    if (s_commit4_accepted==1):
                                        print("previous commit was accepted")
                                        handling_null(demand_category,ori_part_number,commit4_week,commit4_year)
                                        sql="""update User_App.forecast_plan set gsa_advance_commit_irp=(gsa_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                                        where ori_part_number=%s and prod_week={} and prod_year={}""".format(s_commit4,s_commit4_week,s_commit4_year)
                                        col=[updated_by,current_time,ori_part_number]
                                        condb(sql,col)
                                        process_type_calculation(ori_part_number,s_commit4_week,s_commit4_year)
                                    if (commit4_accepted==1):
                                        print("new commit is accepted")
                                        handling_null(demand_category,ori_part_number,commit4_week,commit4_year)
                                        sql="""update User_App.forecast_plan set gsa_advance_commit_irp=(gsa_advance_commit_irp)+({}),updated_by=%s,updated_at=%s,active=1
                                        where ori_part_number=%s and prod_week={} and prod_year={}""".format(commit4,commit4_week,commit4_year)
                                        col=[updated_by,current_time,ori_part_number] 
                                        condb(sql,col)
                                        sql1="""update User_App.advance_commit set irp_processed=1,commit4_irp_processed=0,planner_review_updated=0 where id={}""".format(id)
                                        condb(sql1)
                                        process_type_calculation(ori_part_number,commit4_week,commit4_year)
                                if (commit4==s_commit4):
                                    print("No change in the commit value for commit4")
                                    if(commit4_accepted==1)and(s_commit4_accepted==0):
                                        print('latest commit is Accepted')
                                        handling_null(demand_category,ori_part_number,commit4_week,commit4_year)
                                        sql="""update User_App.forecast_plan set gsa_advance_commit_irp=(gsa_advance_commit_irp)+({}),updated_by=%s,updated_at=%s,active=1
                                        where ori_part_number=%s and prod_week={} and prod_year={}""".format(commit4,commit4_week,commit4_year)
                                        col=[updated_by,current_time,ori_part_number] 
                                        condb(sql,col)
                                        sql1="""update User_App.advance_commit set irp_processed=1,commit4_irp_processed=0,planner_review_updated=0 where id={}""".format(id)
                                        condb(sql1)
                                        process_type_calculation(ori_part_number,commit4_week,commit4_year)
                                    if (commit4_accepted==0)and(s_commit4_accepted==1):
                                        print('latest commit is Rejected')
                                        handling_null(demand_category,ori_part_number,commit4_week,commit4_year)
                                        sql="""update User_App.forecast_plan set gsa_advance_commit_irp=(gsa_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                                        where ori_part_number=%s and prod_week={} and prod_year={}""".format(s_commit4,s_commit4_week,s_commit4_year)
                                        col=[updated_by,current_time,ori_part_number]
                                        condb(sql,col)
                                        process_type_calculation(ori_part_number,s_commit4_week,s_commit4_year)
                            if (commit4 is not None)and(commit4_date!=s_commit4_date):
                                print("Change in date found for commit4")
                                if (s_commit4_accepted==1):
                                    print("previous value was accepted")
                                    handling_null(demand_category,ori_part_number,commit4_week,commit4_year)
                                    sql="""update User_App.forecast_plan set gsa_advance_commit_irp=(gsa_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                                    where ori_part_number=%s and prod_week={} and prod_year={}""".format(s_commit4,s_commit4_week,s_commit4_year)
                                    col=[updated_by,current_time,ori_part_number]
                                    condb(sql,col)
                                    process_type_calculation(ori_part_number,s_commit4_week,s_commit4_year)
                                if (commit4_accepted==1):
                                    print('current commit is accepted for new date')
                                    handling_null(demand_category,ori_part_number,commit4_week,commit4_year)
                                    sql="""update User_App.forecast_plan set gsa_advance_commit_irp=(gsa_advance_commit_irp)+({}),updated_by=%s,updated_at=%s,active=1
                                    where ori_part_number=%s and prod_week={} and prod_year={}""".format(commit4,commit4_week,commit4_year)
                                    col=[updated_by,current_time,ori_part_number] 
                                    condb(sql,col)
                                    sql1="""update User_App.advance_commit set irp_processed=1,commit4_irp_processed=0,planner_review_updated=0 where id={}""".format(id)
                                    condb(sql1)
                                    process_type_calculation(ori_part_number,commit4_week,commit4_year)
                            if (commit4 is None)and(s_commit4!=0)and(s_commit4_accepted==1):
                                handling_null(demand_category,ori_part_number,commit4_week,commit4_year)
                                sql="""update User_App.forecast_plan set gsa_advance_commit_irp=(gsa_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                                where ori_part_number=%s and prod_week={} and prod_year={}""".format(s_commit4,s_commit4_week,s_commit4_year)
                                col=[updated_by,current_time,ori_part_number]
                                condb(sql,col)
                                process_type_calculation(ori_part_number,s_commit4_week,s_commit4_year)
        
                        
                        
                        if demand_category=='Non GSA' and s_demand_category=='Non GSA':
                            print('Non GSA',id,s_id,ori_part_number,s_ori_part_number,prod_week,s_prod_week,prod_year,s_prod_year)
                            if (is_delete==1):
                                print("deleted record found",id)
                                if s_commit1_accepted==1:
                                    handling_null(demand_category,ori_part_number,commit1_week,commit1_year)
                                    sql="""update User_App.forecast_plan set nongsa_advance_commit_irp=(nongsa_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                                    where ori_part_number=%s and prod_week={} and prod_year={}""".format(s_commit1,s_commit1_week,s_commit1_year)
                                    col=[updated_by,current_time,ori_part_number]
                                    condb(sql,col)
                                    process_type_calculation(ori_part_number,commit1_week,commit1_year)
                                if s_commit2_accepted==1:
                                    handling_null(demand_category,ori_part_number,commit2_week,commit2_year)
                                    sql="""update User_App.forecast_plan set nongsa_advance_commit_irp=(nongsa_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                                    where ori_part_number=%s and prod_week={} and prod_year={}""".format(s_commit2,s_commit2_week,s_commit2_year)
                                    col=[updated_by,current_time,ori_part_number]
                                    condb(sql,col)
                                    process_type_calculation(ori_part_number,commit2_week,commit2_year)
                                if s_commit3_accepted==1:
                                    handling_null(demand_category,ori_part_number,commit3_week,commit3_year)
                                    sql="""update User_App.forecast_plan set nongsa_advance_commit_irp=(nongsa_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                                    where ori_part_number=%s and prod_week={} and prod_year={}""".format(s_commit3,s_commit3_week,s_commit3_year)
                                    col=[updated_by,current_time,ori_part_number]
                                    condb(sql,col)
                                    process_type_calculation(ori_part_number,commit3_week,commit3_year)
                                if s_commit4_accepted==1:
                                    handling_null(demand_category,ori_part_number,commit4_week,commit4_year)
                                    sql="""update User_App.forecast_plan set nongsa_advance_commit_irp=(nongsa_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                                    where ori_part_number=%s and prod_week={} and prod_year={}""".format(s_commit4,s_commit4_week,s_commit4_year)
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
                            
                                
                            if (commit1 is not None)and(commit1_date==s_commit1_date):
                                print("No change in date found for commit1")
                                if (commit1!=s_commit1):
                                    print("change in commit value")
                                    if (s_commit1_accepted==1):
                                        print("previous commit was accepted")
                                        handling_null(demand_category,ori_part_number,commit1_week,commit1_year)
                                        sql="""update User_App.forecast_plan set nongsa_advance_commit_irp=(nongsa_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                                        where ori_part_number=%s and prod_week={} and prod_year={}""".format(s_commit1,s_commit1_week,s_commit1_year)
                                        col=[updated_by,current_time,ori_part_number]
                                        condb(sql,col)
                                        process_type_calculation(ori_part_number,s_commit1_week,s_commit1_year)
                                    if (commit1_accepted==1):
                                        print("new commit is accepted")
                                        handling_null(demand_category,ori_part_number,commit1_week,commit1_year)
                                        sql="""update User_App.forecast_plan set nongsa_advance_commit_irp=(nongsa_advance_commit_irp)+({}),updated_by=%s,updated_at=%s,active=1
                                        where ori_part_number=%s and prod_week={} and prod_year={}""".format(commit1,commit1_week,commit1_year)
                                        col=[updated_by,current_time,ori_part_number] 
                                        condb(sql,col)
                                        sql1="""update User_App.advance_commit set irp_processed=1,commit1_irp_processed=0,planner_review_updated=0 where id={}""".format(id)
                                        condb(sql1)
                                        process_type_calculation(ori_part_number,commit1_week,commit1_year)
                                if (commit1==s_commit1):
                                    print("No change in the commit value for commit1")
                                    if (commit1_accepted==1)and(s_commit1_accepted==0):
                                        print('latest commit is Accepted')
                                        handling_null(demand_category,ori_part_number,commit1_week,commit1_year)
                                        sql="""update User_App.forecast_plan set nongsa_advance_commit_irp=(nongsa_advance_commit_irp)+({}),updated_by=%s,updated_at=%s,active=1
                                        where ori_part_number=%s and prod_week={} and prod_year={}""".format(commit1,commit1_week,commit1_year)
                                        col=[updated_by,current_time,ori_part_number] 
                                        condb(sql,col)
                                        sql1="""update User_App.advance_commit set irp_processed=1,commit1_irp_processed=0,planner_review_updated=0 where id={}""".format(id)
                                        condb(sql1)
                                        process_type_calculation(ori_part_number,commit1_week,commit1_year)
                                    if (commit1_accepted==0)and(s_commit1_accepted==1):
                                        print('latest commit is Rejected')
                                        handling_null(demand_category,ori_part_number,commit1_week,commit1_year)
                                        sql="""update User_App.forecast_plan set nongsa_advance_commit_irp=(nongsa_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                                        where ori_part_number=%s and prod_week={} and prod_year={}""".format(s_commit1,s_commit1_week,s_commit1_year)
                                        col=[updated_by,current_time,ori_part_number]
                                        condb(sql,col)
                                        process_type_calculation(ori_part_number,s_commit1_week,s_commit1_year)
                            if (commit1 is not None)and(commit1_date!=s_commit1_date):
                                print("Change in date found for commit1")
                                if (s_commit1_accepted==1):
                                    print("previous value was accepted")
                                    handling_null(demand_category,ori_part_number,commit1_week,commit1_year)
                                    sql="""update User_App.forecast_plan set nongsa_advance_commit_irp=(nongsa_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                                    where ori_part_number=%s and prod_week={} and prod_year={}""".format(s_commit1,s_commit1_week,s_commit1_year)
                                    col=[updated_by,current_time,ori_part_number]
                                    condb(sql,col)
                                    process_type_calculation(ori_part_number,s_commit1_week,s_commit1_year)
                                if (commit1_accepted==1):
                                    print('current commit is accepted for new date')
                                    handling_null(demand_category,ori_part_number,commit1_week,commit1_year)
                                    sql="""update User_App.forecast_plan set nongsa_advance_commit_irp=(nongsa_advance_commit_irp)+({}),updated_by=%s,updated_at=%s,active=1
                                    where ori_part_number=%s and prod_week={} and prod_year={}""".format(commit1,commit1_week,commit1_year)
                                    col=[updated_by,current_time,ori_part_number] 
                                    condb(sql,col)
                                    sql1="""update User_App.advance_commit set irp_processed=1,commit1_irp_processed=0,planner_review_updated=0 where id={}""".format(id)
                                    condb(sql1)
                                    process_type_calculation(ori_part_number,commit1_week,commit1_year)
                            if (commit1 is None)and(s_commit1!=0)and(s_commit1_accepted==1):
                                handling_null(demand_category,ori_part_number,commit1_week,commit1_year)
                                sql="""update User_App.forecast_plan set nongsa_advance_commit_irp=(nongsa_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                                where ori_part_number=%s and prod_week={} and prod_year={}""".format(s_commit1,s_commit1_week,s_commit1_year)
                                col=[updated_by,current_time,ori_part_number]
                                condb(sql,col)
                                process_type_calculation(ori_part_number,s_commit1_week,s_commit1_year)
                                
                                
                            if (commit2 is not None)and(commit2_date==s_commit2_date):
                                print("No change in date found for commit2")
                                if (commit2!=s_commit2):
                                    print("change in commit value")
                                    if (s_commit2_accepted==1):
                                        print("previous commit was accepted")
                                        handling_null(demand_category,ori_part_number,commit2_week,commit2_year)
                                        sql="""update User_App.forecast_plan set nongsa_advance_commit_irp=(nongsa_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                                        where ori_part_number=%s and prod_week={} and prod_year={}""".format(s_commit2,s_commit2_week,s_commit2_year)
                                        col=[updated_by,current_time,ori_part_number]
                                        condb(sql,col)
                                        process_type_calculation(ori_part_number,s_commit2_week,s_commit2_year)
                                    if (commit2_accepted==1):
                                        print("new commit is accepted")
                                        handling_null(demand_category,ori_part_number,commit2_week,commit2_year)
                                        sql="""update User_App.forecast_plan set nongsa_advance_commit_irp=(nongsa_advance_commit_irp)+({}),updated_by=%s,updated_at=%s,active=1
                                        where ori_part_number=%s and prod_week={} and prod_year={}""".format(commit2,commit2_week,commit2_year)
                                        col=[updated_by,current_time,ori_part_number] 
                                        condb(sql,col)
                                        sql1="""update User_App.advance_commit set irp_processed=1,commit2_irp_processed=0,planner_review_updated=0 where id={}""".format(id)
                                        condb(sql1)
                                        process_type_calculation(ori_part_number,commit2_week,commit2_year)
                                if (commit2==s_commit2):
                                    print("No change in the commit value for commit2")
                                    if (commit2_accepted==1)and(s_commit2_accepted==0):
                                        print('latest commit is Accepted')
                                        handling_null(demand_category,ori_part_number,commit2_week,commit2_year)
                                        sql="""update User_App.forecast_plan set nongsa_advance_commit_irp=(nongsa_advance_commit_irp)+({}),updated_by=%s,updated_at=%s,active=1
                                        where ori_part_number=%s and prod_week={} and prod_year={}""".format(commit2,commit2_week,commit2_year)
                                        col=[updated_by,current_time,ori_part_number] 
                                        condb(sql,col)
                                        sql1="""update User_App.advance_commit set irp_processed=1,commit2_irp_processed=0,planner_review_updated=0 where id={}""".format(id)
                                        condb(sql1)
                                        process_type_calculation(ori_part_number,commit2_week,commit2_year)
                                    if (commit2_accepted==0)and(s_commit2_accepted==1):
                                        print('latest commit is Rejected')
                                        handling_null(demand_category,ori_part_number,commit2_week,commit2_year)
                                        sql="""update User_App.forecast_plan set nongsa_advance_commit_irp=(nongsa_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                                        where ori_part_number=%s and prod_week={} and prod_year={}""".format(s_commit2,s_commit2_week,s_commit2_year)
                                        col=[updated_by,current_time,ori_part_number]
                                        condb(sql,col)
                                        process_type_calculation(ori_part_number,s_commit2_week,s_commit2_year)
                            if (commit2 is not None)and(commit2_date!=s_commit2_date):
                                print("Change in date found for commit2")
                                if (s_commit2_accepted==1):
                                    print("previous value was accepted")
                                    handling_null(demand_category,ori_part_number,commit2_week,commit2_year)
                                    sql="""update User_App.forecast_plan set nongsa_advance_commit_irp=(nongsa_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                                    where ori_part_number=%s and prod_week={} and prod_year={}""".format(s_commit2,s_commit2_week,s_commit2_year)
                                    col=[updated_by,current_time,ori_part_number]
                                    condb(sql,col)
                                    process_type_calculation(ori_part_number,s_commit2_week,s_commit2_year)
                                if (commit2_accepted==1):
                                    print('current commit is accepted for new date')
                                    handling_null(demand_category,ori_part_number,commit2_week,commit2_year)
                                    sql="""update User_App.forecast_plan set nongsa_advance_commit_irp=(nongsa_advance_commit_irp)+({}),updated_by=%s,updated_at=%s,active=1
                                    where ori_part_number=%s and prod_week={} and prod_year={}""".format(commit2,commit2_week,commit2_year)
                                    col=[updated_by,current_time,ori_part_number] 
                                    condb(sql,col)
                                    sql1="""update User_App.advance_commit set irp_processed=1,commit2_irp_processed=0,planner_review_updated=0 where id={}""".format(id)
                                    condb(sql1)
                                    process_type_calculation(ori_part_number,commit2_week,commit2_year)
                            if (commit2 is None)and(s_commit2!=0)and(s_commit2_accepted==1):
                                handling_null(demand_category,ori_part_number,commit2_week,commit2_year)
                                sql="""update User_App.forecast_plan set nongsa_advance_commit_irp=(nongsa_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                                where ori_part_number=%s and prod_week={} and prod_year={}""".format(s_commit2,s_commit2_week,s_commit2_year)
                                col=[updated_by,current_time,ori_part_number]
                                condb(sql,col)
                                process_type_calculation(ori_part_number,s_commit2_week,s_commit2_year)
                                
                                
                            if (commit3 is not None)and(commit3_date==s_commit3_date):
                                print("No change in date found for commit3")
                                if (commit3!=s_commit3):
                                    print("change in commit value")
                                    if (s_commit3_accepted==1):
                                        print("previous commit was accepted")
                                        handling_null(demand_category,ori_part_number,commit3_week,commit3_year)
                                        sql="""update User_App.forecast_plan set nongsa_advance_commit_irp=(nongsa_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                                        where ori_part_number=%s and prod_week={} and prod_year={}""".format(s_commit3,s_commit3_week,s_commit3_year)
                                        col=[updated_by,current_time,ori_part_number]
                                        condb(sql,col)
                                        process_type_calculation(ori_part_number,s_commit3_week,s_commit3_year)
                                    if (commit3_accepted==1):
                                        print("new commit is accepted")
                                        handling_null(demand_category,ori_part_number,commit3_week,commit3_year)
                                        sql="""update User_App.forecast_plan set nongsa_advance_commit_irp=(nongsa_advance_commit_irp)+({}),updated_by=%s,updated_at=%s,active=1
                                        where ori_part_number=%s and prod_week={} and prod_year={}""".format(commit3,commit3_week,commit3_year)
                                        col=[updated_by,current_time,ori_part_number] 
                                        condb(sql,col)
                                        sql1="""update User_App.advance_commit set irp_processed=1,commit3_irp_processed=0,planner_review_updated=0 where id={}""".format(id)
                                        condb(sql1)
                                        process_type_calculation(ori_part_number,commit3_week,commit3_year)
                                if (commit3==s_commit3):
                                    print("No change in the commit value for commit3")
                                    if (commit3_accepted==1)and(s_commit3_accepted==0):
                                        print('latest commit is Accepted')
                                        handling_null(demand_category,ori_part_number,commit3_week,commit3_year)
                                        sql="""update User_App.forecast_plan set nongsa_advance_commit_irp=(nongsa_advance_commit_irp)+({}),updated_by=%s,updated_at=%s,active=1
                                        where ori_part_number=%s and prod_week={} and prod_year={}""".format(commit3,commit3_week,commit3_year)
                                        col=[updated_by,current_time,ori_part_number] 
                                        condb(sql,col)
                                        sql1="""update User_App.advance_commit set irp_processed=1,commit3_irp_processed=0,planner_review_updated=0 where id={}""".format(id)
                                        condb(sql1)
                                        process_type_calculation(ori_part_number,commit3_week,commit3_year)
                                    if (commit3_accepted==0)and(s_commit3_accepted==1):
                                        print('latest commit is Rejected')
                                        handling_null(demand_category,ori_part_number,commit3_week,commit3_year)
                                        sql="""update User_App.forecast_plan set nongsa_advance_commit_irp=(nongsa_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                                        where ori_part_number=%s and prod_week={} and prod_year={}""".format(s_commit3,s_commit3_week,s_commit3_year)
                                        col=[updated_by,current_time,ori_part_number]
                                        condb(sql,col)
                                        process_type_calculation(ori_part_number,s_commit3_week,s_commit3_year)
                            if (commit3 is not None)and(commit3_date!=s_commit3_date):
                                print("Change in date found for commit3")
                                if (s_commit3_accepted==1):
                                    print("previous value was accepted")
                                    handling_null(demand_category,ori_part_number,commit3_week,commit3_year)
                                    sql="""update User_App.forecast_plan set nongsa_advance_commit_irp=(nongsa_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                                    where ori_part_number=%s and prod_week={} and prod_year={}""".format(s_commit3,s_commit3_week,s_commit3_year)
                                    col=[updated_by,current_time,ori_part_number]
                                    condb(sql,col)
                                    process_type_calculation(ori_part_number,s_commit3_week,s_commit3_year)
                                if (commit3_accepted==1):
                                    print('current commit is accepted for new date')
                                    handling_null(demand_category,ori_part_number,commit3_week,commit3_year)
                                    sql="""update User_App.forecast_plan set nongsa_advance_commit_irp=(nongsa_advance_commit_irp)+({}),updated_by=%s,updated_at=%s,active=1
                                    where ori_part_number=%s and prod_week={} and prod_year={}""".format(commit3,commit3_week,commit3_year)
                                    col=[updated_by,current_time,ori_part_number] 
                                    condb(sql,col)
                                    sql1="""update User_App.advance_commit set irp_processed=1,commit3_irp_processed=0,planner_review_updated=0 where id={}""".format(id)
                                    condb(sql1)
                                    process_type_calculation(ori_part_number,commit3_week,commit3_year)
                            if (commit3 is None)and(s_commit3!=0)and(s_commit3_accepted==1):
                                handling_null(demand_category,ori_part_number,commit3_week,commit3_year)
                                sql="""update User_App.forecast_plan set nongsa_advance_commit_irp=(nongsa_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                                where ori_part_number=%s and prod_week={} and prod_year={}""".format(s_commit3,s_commit3_week,s_commit3_year)
                                col=[updated_by,current_time,ori_part_number]
                                condb(sql,col)
                                process_type_calculation(ori_part_number,s_commit3_week,s_commit3_year)
     
                                
                            if (commit4 is not None)and(commit4_date==s_commit4_date):
                                print("No change in date found for commit4")
                                if (commit4!=s_commit4):
                                    print("change in commit value")
                                    if (s_commit4_accepted==1):
                                        print("previous commit was accepted")
                                        handling_null(demand_category,ori_part_number,commit4_week,commit4_year)
                                        sql="""update User_App.forecast_plan set nongsa_advance_commit_irp=(nongsa_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                                        where ori_part_number=%s and prod_week={} and prod_year={}""".format(s_commit4,s_commit4_week,s_commit4_year)
                                        col=[updated_by,current_time,ori_part_number]
                                        condb(sql,col)
                                        process_type_calculation(ori_part_number,s_commit4_week,s_commit4_year)
                                    if (commit4_accepted==1):
                                        print("new commit is accepted")
                                        handling_null(demand_category,ori_part_number,commit4_week,commit4_year)
                                        sql="""update User_App.forecast_plan set nongsa_advance_commit_irp=(nongsa_advance_commit_irp)+({}),updated_by=%s,updated_at=%s,active=1
                                        where ori_part_number=%s and prod_week={} and prod_year={}""".format(commit4,commit4_week,commit4_year)
                                        col=[updated_by,current_time,ori_part_number] 
                                        condb(sql,col)
                                        sql1="""update User_App.advance_commit set irp_processed=1,commit4_irp_processed=0,planner_review_updated=0 where id={}""".format(id)
                                        condb(sql1)
                                        process_type_calculation(ori_part_number,commit4_week,commit4_year)
                                if (commit4==s_commit4):
                                    print("No change in the commit value for commit4")
                                    if(commit4_accepted==1)and(s_commit4_accepted==0):
                                        print('latest commit is Accepted')
                                        handling_null(demand_category,ori_part_number,commit4_week,commit4_year)
                                        sql="""update User_App.forecast_plan set nongsa_advance_commit_irp=(nongsa_advance_commit_irp)+({}),updated_by=%s,updated_at=%s,active=1
                                        where ori_part_number=%s and prod_week={} and prod_year={}""".format(commit4,commit4_week,commit4_year)
                                        col=[updated_by,current_time,ori_part_number] 
                                        condb(sql,col)
                                        sql1="""update User_App.advance_commit set irp_processed=1,commit4_irp_processed=0,planner_review_updated=0 where id={}""".format(id)
                                        condb(sql1)
                                        process_type_calculation(ori_part_number,commit4_week,commit4_year)
                                    if (commit4_accepted==0)and(s_commit4_accepted==1):
                                        print('latest commit is Rejected')
                                        handling_null(demand_category,ori_part_number,commit4_week,commit4_year)
                                        sql="""update User_App.forecast_plan set nongsa_advance_commit_irp=(nongsa_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                                        where ori_part_number=%s and prod_week={} and prod_year={}""".format(s_commit4,s_commit4_week,s_commit4_year)
                                        col=[updated_by,current_time,ori_part_number]
                                        condb(sql,col)
                                        process_type_calculation(ori_part_number,s_commit4_week,s_commit4_year)
                            if (commit4 is not None)and(commit4_date!=s_commit4_date):
                                print("Change in date found for commit4")
                                if (s_commit4_accepted==1):
                                    print("previous value was accepted")
                                    handling_null(demand_category,ori_part_number,commit4_week,commit4_year)
                                    sql="""update User_App.forecast_plan set nongsa_advance_commit_irp=(nongsa_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                                    where ori_part_number=%s and prod_week={} and prod_year={}""".format(s_commit4,s_commit4_week,s_commit4_year)
                                    col=[updated_by,current_time,ori_part_number]
                                    condb(sql,col)
                                    process_type_calculation(ori_part_number,s_commit4_week,s_commit4_year)
                                if (commit4_accepted==1):
                                    print('current commit is accepted for new date')
                                    handling_null(demand_category,ori_part_number,commit4_week,commit4_year)
                                    sql="""update User_App.forecast_plan set nongsa_advance_commit_irp=(nongsa_advance_commit_irp)+({}),updated_by=%s,updated_at=%s,active=1
                                    where ori_part_number=%s and prod_week={} and prod_year={}""".format(commit4,commit4_week,commit4_year)
                                    col=[updated_by,current_time,ori_part_number] 
                                    condb(sql,col)
                                    sql1="""update User_App.advance_commit set irp_processed=1,commit4_irp_processed=0,planner_review_updated=0 where id={}""".format(id)
                                    condb(sql1)
                                    process_type_calculation(ori_part_number,commit4_week,commit4_year)
                            if (commit4 is None)and(s_commit4!=0)and(s_commit4_accepted==1):
                                handling_null(demand_category,ori_part_number,commit4_week,commit4_year)
                                sql="""update User_App.forecast_plan set nongsa_advance_commit_irp=(nongsa_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                                where ori_part_number=%s and prod_week={} and prod_year={}""".format(s_commit4,s_commit4_week,s_commit4_year)
                                col=[updated_by,current_time,ori_part_number]
                                condb(sql,col)
                                process_type_calculation(ori_part_number,s_commit4_week,s_commit4_year)
                                
                        
                        
                        if demand_category=='System' and s_demand_category=='System':
                            print('System',id,s_id,ori_part_number,s_ori_part_number,prod_week,s_prod_week,prod_year,s_prod_year)
                            if (is_delete==1):
                                print("deleted record found",id)
                                if s_commit1_accepted==1:
                                    handling_null(demand_category,ori_part_number,commit1_week,commit1_year)
                                    sql="""update User_App.forecast_plan set system_advance_commit_irp=(system_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                                    where ori_part_number=%s and prod_week={} and prod_year={}""".format(s_commit1,s_commit1_week,s_commit1_year)
                                    col=[updated_by,current_time,ori_part_number]
                                    condb(sql,col)
                                    process_type_calculation(ori_part_number,commit1_week,commit1_year)
                                if s_commit2_accepted==1:
                                    handling_null(demand_category,ori_part_number,commit2_week,commit2_year)
                                    sql="""update User_App.forecast_plan set system_advance_commit_irp=(system_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                                    where ori_part_number=%s and prod_week={} and prod_year={}""".format(s_commit2,s_commit2_week,s_commit2_year)
                                    col=[updated_by,current_time,ori_part_number]
                                    condb(sql,col)
                                    process_type_calculation(ori_part_number,commit2_week,commit2_year)
                                if s_commit3_accepted==1:
                                    handling_null(demand_category,ori_part_number,commit3_week,commit3_year)
                                    sql="""update User_App.forecast_plan set system_advance_commit_irp=(system_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                                    where ori_part_number=%s and prod_week={} and prod_year={}""".format(s_commit3,s_commit3_week,s_commit3_year)
                                    col=[updated_by,current_time,ori_part_number]
                                    condb(sql,col)
                                    process_type_calculation(ori_part_number,commit3_week,commit3_year)
                                if s_commit4_accepted==1:
                                    handling_null(demand_category,ori_part_number,commit4_week,commit4_year)
                                    sql="""update User_App.forecast_plan set system_advance_commit_irp=(system_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                                    where ori_part_number=%s and prod_week={} and prod_year={}""".format(s_commit4,s_commit4_week,s_commit4_year)
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
                            
                            
                                
                            if (commit1 is not None)and(commit1_date==s_commit1_date):
                                print("No change in date found for commit1")
                                if (commit1!=s_commit1):
                                    print("change in commit value")
                                    if (s_commit1_accepted==1):
                                        print("previous commit was accepted")
                                        handling_null(demand_category,ori_part_number,commit1_week,commit1_year)
                                        sql="""update User_App.forecast_plan set system_advance_commit_irp=(system_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                                        where ori_part_number=%s and prod_week={} and prod_year={}""".format(s_commit1,s_commit1_week,s_commit1_year)
                                        col=[updated_by,current_time,ori_part_number]
                                        condb(sql,col)
                                        process_type_calculation(ori_part_number,s_commit1_week,s_commit1_year)
                                    if (commit1_accepted==1):
                                        print("new commit is accepted")
                                        handling_null(demand_category,ori_part_number,commit1_week,commit1_year)
                                        sql="""update User_App.forecast_plan set system_advance_commit_irp=(system_advance_commit_irp)+({}),updated_by=%s,updated_at=%s,active=1
                                        where ori_part_number=%s and prod_week={} and prod_year={}""".format(commit1,commit1_week,commit1_year)
                                        col=[updated_by,current_time,ori_part_number] 
                                        condb(sql,col)
                                        sql1="""update User_App.advance_commit set irp_processed=1,commit1_irp_processed=0,planner_review_updated=0 where id={}""".format(id)
                                        condb(sql1)
                                        process_type_calculation(ori_part_number,commit1_week,commit1_year)
                                if (commit1==s_commit1):
                                    print("No change in the commit value for commit1")
                                    if (commit1_accepted==1)and(s_commit1_accepted==0):
                                        print('latest commit is Accepted')
                                        handling_null(demand_category,ori_part_number,commit1_week,commit1_year)
                                        sql="""update User_App.forecast_plan set system_advance_commit_irp=(system_advance_commit_irp)+({}),updated_by=%s,updated_at=%s,active=1
                                        where ori_part_number=%s and prod_week={} and prod_year={}""".format(commit1,commit1_week,commit1_year)
                                        col=[updated_by,current_time,ori_part_number] 
                                        condb(sql,col)
                                        sql1="""update User_App.advance_commit set irp_processed=1,commit1_irp_processed=0,planner_review_updated=0 where id={}""".format(id)
                                        condb(sql1)
                                        process_type_calculation(ori_part_number,commit1_week,commit1_year)
                                    if (commit1_accepted==0)and(s_commit1_accepted==1):
                                        print('latest commit is Rejected')
                                        handling_null(demand_category,ori_part_number,commit1_week,commit1_year)
                                        sql="""update User_App.forecast_plan set system_advance_commit_irp=(system_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                                        where ori_part_number=%s and prod_week={} and prod_year={}""".format(s_commit1,s_commit1_week,s_commit1_year)
                                        col=[updated_by,current_time,ori_part_number]
                                        condb(sql,col)
                                        process_type_calculation(ori_part_number,s_commit1_week,s_commit1_year)
                            if (commit1 is not None)and(commit1_date!=s_commit1_date):
                                print("Change in date found for commit1")
                                if (s_commit1_accepted==1):
                                    print("previous value was accepted")
                                    handling_null(demand_category,ori_part_number,commit1_week,commit1_year)
                                    sql="""update User_App.forecast_plan set system_advance_commit_irp=(system_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                                    where ori_part_number=%s and prod_week={} and prod_year={}""".format(s_commit1,s_commit1_week,s_commit1_year)
                                    col=[updated_by,current_time,ori_part_number]
                                    condb(sql,col)
                                    process_type_calculation(ori_part_number,s_commit1_week,s_commit1_year)
                                if (commit1_accepted==1):
                                    print('current commit is accepted for new date')
                                    handling_null(demand_category,ori_part_number,commit1_week,commit1_year)
                                    sql="""update User_App.forecast_plan set system_advance_commit_irp=(system_advance_commit_irp)+({}),updated_by=%s,updated_at=%s,active=1
                                    where ori_part_number=%s and prod_week={} and prod_year={}""".format(commit1,commit1_week,commit1_year)
                                    col=[updated_by,current_time,ori_part_number] 
                                    condb(sql,col)
                                    sql1="""update User_App.advance_commit set irp_processed=1,commit1_irp_processed=0,planner_review_updated=0 where id={}""".format(id)
                                    condb(sql1)
                                    process_type_calculation(ori_part_number,commit1_week,commit1_year)
                            if (commit1 is None)and(s_commit1!=0)and(s_commit1_accepted==1):
                                handling_null(demand_category,ori_part_number,commit1_week,commit1_year)
                                sql="""update User_App.forecast_plan set system_advance_commit_irp=(system_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                                where ori_part_number=%s and prod_week={} and prod_year={}""".format(s_commit1,s_commit1_week,s_commit1_year)
                                col=[updated_by,current_time,ori_part_number]
                                condb(sql,col)
                                process_type_calculation(ori_part_number,s_commit1_week,s_commit1_year)
                                
                                
                            if (commit2 is not None)and(commit2_date==s_commit2_date):
                                print("No change in date found for commit2")
                                if (commit2!=s_commit2):
                                    print("change in commit value")
                                    if (s_commit2_accepted==1):
                                        print("previous commit was accepted")
                                        handling_null(demand_category,ori_part_number,commit2_week,commit2_year)
                                        sql="""update User_App.forecast_plan set system_advance_commit_irp=(system_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                                        where ori_part_number=%s and prod_week={} and prod_year={}""".format(s_commit2,s_commit2_week,s_commit2_year)
                                        col=[updated_by,current_time,ori_part_number]
                                        condb(sql,col)
                                        process_type_calculation(ori_part_number,s_commit2_week,s_commit2_year)
                                    if (commit2_accepted==1):
                                        print("new commit is accepted")
                                        handling_null(demand_category,ori_part_number,commit2_week,commit2_year)
                                        sql="""update User_App.forecast_plan set system_advance_commit_irp=(system_advance_commit_irp)+({}),updated_by=%s,updated_at=%s,active=1
                                        where ori_part_number=%s and prod_week={} and prod_year={}""".format(commit2,commit2_week,commit2_year)
                                        col=[updated_by,current_time,ori_part_number] 
                                        condb(sql,col)
                                        sql1="""update User_App.advance_commit set irp_processed=1,commit2_irp_processed=0,planner_review_updated=0 where id={}""".format(id)
                                        condb(sql1)
                                        process_type_calculation(ori_part_number,commit2_week,commit2_year)
                                if (commit2==s_commit2):
                                    print("No change in the commit value for commit2")
                                    if (commit2_accepted==1)and(s_commit2_accepted==0):
                                        print('latest commit is Accepted')
                                        handling_null(demand_category,ori_part_number,commit2_week,commit2_year)
                                        sql="""update User_App.forecast_plan set system_advance_commit_irp=(system_advance_commit_irp)+({}),updated_by=%s,updated_at=%s,active=1
                                        where ori_part_number=%s and prod_week={} and prod_year={}""".format(commit2,commit2_week,commit2_year)
                                        col=[updated_by,current_time,ori_part_number] 
                                        condb(sql,col)
                                        sql1="""update User_App.advance_commit set irp_processed=1,commit2_irp_processed=0,planner_review_updated=0 where id={}""".format(id)
                                        condb(sql1)
                                        process_type_calculation(ori_part_number,commit2_week,commit2_year)
                                    if (commit2_accepted==0)and(s_commit2_accepted==1):
                                        print('latest commit is Rejected')
                                        handling_null(demand_category,ori_part_number,commit2_week,commit2_year)
                                        sql="""update User_App.forecast_plan set system_advance_commit_irp=(system_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                                        where ori_part_number=%s and prod_week={} and prod_year={}""".format(s_commit2,s_commit2_week,s_commit2_year)
                                        col=[updated_by,current_time,ori_part_number]
                                        condb(sql,col)
                                        process_type_calculation(ori_part_number,s_commit2_week,s_commit2_year)
                            if (commit2 is not None)and(commit2_date!=s_commit2_date):
                                print("Change in date found for commit2")
                                if (s_commit2_accepted==1):
                                    print("previous value was accepted")
                                    handling_null(demand_category,ori_part_number,commit2_week,commit2_year)
                                    sql="""update User_App.forecast_plan set system_advance_commit_irp=(system_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                                    where ori_part_number=%s and prod_week={} and prod_year={}""".format(s_commit2,s_commit2_week,s_commit2_year)
                                    col=[updated_by,current_time,ori_part_number]
                                    condb(sql,col)
                                    process_type_calculation(ori_part_number,s_commit2_week,s_commit2_year)
                                if (commit2_accepted==1):
                                    print('current commit is accepted for new date')
                                    handling_null(demand_category,ori_part_number,commit2_week,commit2_year)
                                    sql="""update User_App.forecast_plan set system_advance_commit_irp=(system_advance_commit_irp)+({}),updated_by=%s,updated_at=%s,active=1
                                    where ori_part_number=%s and prod_week={} and prod_year={}""".format(commit2,commit2_week,commit2_year)
                                    col=[updated_by,current_time,ori_part_number] 
                                    condb(sql,col)
                                    sql1="""update User_App.advance_commit set irp_processed=1,commit2_irp_processed=0,planner_review_updated=0 where id={}""".format(id)
                                    condb(sql1)
                                    process_type_calculation(ori_part_number,commit2_week,commit2_year)
                            if (commit2 is None)and(s_commit2!=0)and(s_commit2_accepted==1):
                                handling_null(demand_category,ori_part_number,commit2_week,commit2_year)
                                sql="""update User_App.forecast_plan set system_advance_commit_irp=(system_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                                where ori_part_number=%s and prod_week={} and prod_year={}""".format(s_commit2,s_commit2_week,s_commit2_year)
                                col=[updated_by,current_time,ori_part_number]
                                condb(sql,col)
                                process_type_calculation(ori_part_number,s_commit2_week,s_commit2_year)
                                
       
                            if (commit3 is not None)and(commit3_date==s_commit3_date):
                                print("No change in date found for commit3")
                                if (commit3!=s_commit3):
                                    print("change in commit value")
                                    if (s_commit3_accepted==1):
                                        print("previous commit was accepted")
                                        handling_null(demand_category,ori_part_number,commit3_week,commit3_year)
                                        sql="""update User_App.forecast_plan set system_advance_commit_irp=(system_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                                        where ori_part_number=%s and prod_week={} and prod_year={}""".format(s_commit3,s_commit3_week,s_commit3_year)
                                        col=[updated_by,current_time,ori_part_number]
                                        condb(sql,col)
                                        process_type_calculation(ori_part_number,s_commit3_week,s_commit3_year)
                                    if (commit3_accepted==1):
                                        print("new commit is accepted")
                                        handling_null(demand_category,ori_part_number,commit3_week,commit3_year)
                                        sql="""update User_App.forecast_plan set system_advance_commit_irp=(system_advance_commit_irp)+({}),updated_by=%s,updated_at=%s,active=1
                                        where ori_part_number=%s and prod_week={} and prod_year={}""".format(commit3,commit3_week,commit3_year)
                                        col=[updated_by,current_time,ori_part_number] 
                                        condb(sql,col)
                                        sql1="""update User_App.advance_commit set irp_processed=1,commit3_irp_processed=0,planner_review_updated=0 where id={}""".format(id)
                                        condb(sql1)
                                        process_type_calculation(ori_part_number,commit3_week,commit3_year)
                                if (commit3==s_commit3):
                                    print("No change in the commit value for commit3")
                                    if (commit3_accepted==1)and(s_commit3_accepted==0):
                                        print('latest commit is Accepted')
                                        handling_null(demand_category,ori_part_number,commit3_week,commit3_year)
                                        sql="""update User_App.forecast_plan set system_advance_commit_irp=(system_advance_commit_irp)+({}),updated_by=%s,updated_at=%s,active=1
                                        where ori_part_number=%s and prod_week={} and prod_year={}""".format(commit3,commit3_week,commit3_year)
                                        col=[updated_by,current_time,ori_part_number] 
                                        condb(sql,col)
                                        sql1="""update User_App.advance_commit set irp_processed=1,commit3_irp_processed=0,planner_review_updated=0 where id={}""".format(id)
                                        condb(sql1)
                                        process_type_calculation(ori_part_number,commit3_week,commit3_year)
                                    if (commit3_accepted==0)and(s_commit3_accepted==1):
                                        print('latest commit is Rejected')
                                        handling_null(demand_category,ori_part_number,commit3_week,commit3_year)
                                        sql="""update User_App.forecast_plan set system_advance_commit_irp=(system_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                                        where ori_part_number=%s and prod_week={} and prod_year={}""".format(s_commit3,s_commit3_week,s_commit3_year)
                                        col=[updated_by,current_time,ori_part_number]
                                        condb(sql,col)
                                        process_type_calculation(ori_part_number,s_commit3_week,s_commit3_year)
                            if (commit3 is not None)and(commit3_date!=s_commit3_date):
                                print("Change in date found for commit3")
                                if (s_commit3_accepted==1):
                                    print("previous value was accepted")
                                    handling_null(demand_category,ori_part_number,commit3_week,commit3_year)
                                    sql="""update User_App.forecast_plan set system_advance_commit_irp=(system_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                                    where ori_part_number=%s and prod_week={} and prod_year={}""".format(s_commit3,s_commit3_week,s_commit3_year)
                                    col=[updated_by,current_time,ori_part_number]
                                    condb(sql,col)
                                    process_type_calculation(ori_part_number,s_commit3_week,s_commit3_year)
                                if (commit3_accepted==1):
                                    print('current commit is accepted for new date')
                                    handling_null(demand_category,ori_part_number,commit3_week,commit3_year)
                                    sql="""update User_App.forecast_plan set system_advance_commit_irp=(system_advance_commit_irp)+({}),updated_by=%s,updated_at=%s,active=1
                                    where ori_part_number=%s and prod_week={} and prod_year={}""".format(commit3,commit3_week,commit3_year)
                                    col=[updated_by,current_time,ori_part_number] 
                                    condb(sql,col)
                                    sql1="""update User_App.advance_commit set irp_processed=1,commit3_irp_processed=0,planner_review_updated=0 where id={}""".format(id)
                                    condb(sql1)
                                    process_type_calculation(ori_part_number,commit3_week,commit3_year)
                            if (commit3 is None)and(s_commit3!=0)and(s_commit3_accepted==1):
                                handling_null(demand_category,ori_part_number,commit3_week,commit3_year)
                                sql="""update User_App.forecast_plan set system_advance_commit_irp=(system_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                                where ori_part_number=%s and prod_week={} and prod_year={}""".format(s_commit3,s_commit3_week,s_commit3_year)
                                col=[updated_by,current_time,ori_part_number]
                                condb(sql,col)
                                process_type_calculation(ori_part_number,s_commit3_week,s_commit3_year)
    
                                                
                            if (commit4 is not None)and(commit4_date==s_commit4_date):
                                print("No change in date found for commit4")
                                if (commit4!=s_commit4):
                                    print("change in commit value")
                                    if (s_commit4_accepted==1):
                                        print("previous commit was accepted")
                                        handling_null(demand_category,ori_part_number,commit4_week,commit4_year)
                                        sql="""update User_App.forecast_plan set system_advance_commit_irp=(system_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                                        where ori_part_number=%s and prod_week={} and prod_year={}""".format(s_commit4,s_commit4_week,s_commit4_year)
                                        col=[updated_by,current_time,ori_part_number]
                                        condb(sql,col)
                                        process_type_calculation(ori_part_number,s_commit4_week,s_commit4_year)
                                    if (commit4_accepted==1):
                                        print("new commit is accepted")
                                        handling_null(demand_category,ori_part_number,commit4_week,commit4_year)
                                        sql="""update User_App.forecast_plan set system_advance_commit_irp=(system_advance_commit_irp)+({}),updated_by=%s,updated_at=%s,active=1
                                        where ori_part_number=%s and prod_week={} and prod_year={}""".format(commit4,commit4_week,commit4_year)
                                        col=[updated_by,current_time,ori_part_number] 
                                        condb(sql,col)
                                        sql1="""update User_App.advance_commit set irp_processed=1,commit4_irp_processed=0,planner_review_updated=0 where id={}""".format(id)
                                        condb(sql1)
                                        process_type_calculation(ori_part_number,commit4_week,commit4_year)
                                if (commit4==s_commit4):
                                    print("No change in the commit value for commit4")
                                    if(commit4_accepted==1)and(s_commit4_accepted==0):
                                        print('latest commit is Accepted')
                                        handling_null(demand_category,ori_part_number,commit4_week,commit4_year)
                                        sql="""update User_App.forecast_plan set system_advance_commit_irp=(system_advance_commit_irp)+({}),updated_by=%s,updated_at=%s,active=1
                                        where ori_part_number=%s and prod_week={} and prod_year={}""".format(commit4,commit4_week,commit4_year)
                                        col=[updated_by,current_time,ori_part_number] 
                                        condb(sql,col)
                                        sql1="""update User_App.advance_commit set irp_processed=1,commit4_irp_processed=0,planner_review_updated=0 where id={}""".format(id)
                                        condb(sql1)
                                        process_type_calculation(ori_part_number,commit4_week,commit4_year)
                                    if (commit4_accepted==0)and(s_commit4_accepted==1):
                                        print('latest commit is Rejected')
                                        handling_null(demand_category,ori_part_number,commit4_week,commit4_year)
                                        sql="""update User_App.forecast_plan set system_advance_commit_irp=(system_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                                        where ori_part_number=%s and prod_week={} and prod_year={}""".format(s_commit4,s_commit4_week,s_commit4_year)
                                        col=[updated_by,current_time,ori_part_number]
                                        condb(sql,col)
                                        process_type_calculation(ori_part_number,s_commit4_week,s_commit4_year)
                            if (commit4 is not None)and(commit4_date!=s_commit4_date):
                                print("Change in date found for commit4")
                                if (s_commit4_accepted==1):
                                    print("previous value was accepted")
                                    handling_null(demand_category,ori_part_number,commit4_week,commit4_year)
                                    sql="""update User_App.forecast_plan set system_advance_commit_irp=(system_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                                    where ori_part_number=%s and prod_week={} and prod_year={}""".format(s_commit4,s_commit4_week,s_commit4_year)
                                    col=[updated_by,current_time,ori_part_number]
                                    condb(sql,col)
                                    process_type_calculation(ori_part_number,s_commit4_week,s_commit4_year)
                                if (commit4_accepted==1):
                                    print('current commit is accepted for new date')
                                    handling_null(demand_category,ori_part_number,commit4_week,commit4_year)
                                    sql="""update User_App.forecast_plan set system_advance_commit_irp=(system_advance_commit_irp)+({}),updated_by=%s,updated_at=%s,active=1
                                    where ori_part_number=%s and prod_week={} and prod_year={}""".format(commit4,commit4_week,commit4_year)
                                    col=[updated_by,current_time,ori_part_number] 
                                    condb(sql,col)
                                    sql1="""update User_App.advance_commit set irp_processed=1,commit4_irp_processed=0,planner_review_updated=0 where id={}""".format(id)
                                    condb(sql1)
                                    process_type_calculation(ori_part_number,commit4_week,commit4_year)
                            if (commit4 is None)and(s_commit4!=0)and(s_commit4_accepted==1):
                                handling_null(demand_category,ori_part_number,commit4_week,commit4_year)
                                sql="""update User_App.forecast_plan set system_advance_commit_irp=(system_advance_commit_irp)-({}),updated_by=%s,updated_at=%s,active=1
                                where ori_part_number=%s and prod_week={} and prod_year={}""".format(s_commit4,s_commit4_week,s_commit4_year)
                                col=[updated_by,current_time,ori_part_number]
                                condb(sql,col)
                                process_type_calculation(ori_part_number,s_commit4_week,s_commit4_year)
                    
            else:
                else_id=primary.id
                print("else",else_id)
                test_primary="""select * from User_App.advance_commit where id={}""".format(else_id)
                df_else=condb(test_primary)
                else_primary_advance_commit_data=pd.DataFrame(data=df_else,columns=advance_db_columns)
                else_primary_advance_commit_data=else_primary_advance_commit_data.fillna({'commit1':0,'commit2':0,'commit3':0,'commit4':0,
                'commit1_accepted':b'\x00','commit2_accepted':b'\x00','commit3_accepted':b'\x00','commit4_accepted':b'\x00','irp_processed':False,
                'commit1_irp_processed':b'\x00','commit2_irp_processed':b'\x00','commit3_irp_processed':b'\x00','commit4_irp_processed':b'\x00','planner_review_updated':b'\x00',
                'commit1_old_value':0,'commit2_old_value':0,'commit3_old_value':0,'commit4_old_value':0,'is_delete':b'\x00',
                'commit1_week':0,'commit2_week':0,'commit3_week':0,'commit4_week':0,
                'commit1_year':2000,'commit2_year':2000,'commit3_year':2000,'commit4_year':2000})
                for index,primary in else_primary_advance_commit_data.iterrows():
                    commit1=primary.commit1
                    commit1_week=primary.commit1_week
                    commit1_year=str(primary.commit1_year)
                    commit1_year=commit1_year[2:] 
                    commit1_accepted=ord(primary.commit1_accepted)
                    commit1_irp_processed=ord(primary.commit1_irp_processed)
                    commit1_date=primary.commit1_date
                    commit2=primary.commit2
                    commit2_week=primary.commit2_week
                    commit2_year=str(primary.commit2_year)
                    commit2_year=commit2_year[2:]
                    commit2_accepted=ord(primary.commit2_accepted) 
                    commit2_irp_processed=ord(primary.commit2_irp_processed) 
                    commit2_date=primary.commit2_date
                    commit3=primary.commit3
                    commit3_week=primary.commit3_week
                    commit3_year=str(primary.commit3_year)
                    commit3_year=commit3_year[2:]
                    commit3_accepted=ord(primary.commit3_accepted) 
                    commit3_irp_processed=ord(primary.commit3_irp_processed)
                    commit3_date=primary.commit3_date 
                    commit4=primary.commit4
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
                    
                    if demand_category=='Forecast':
                        print("Forecast")
                        if(commit1 is not None):
                            if commit1_accepted==1:
                                handling_null(demand_category,ori_part_number,commit1_week,commit1_year)
                                sql="""update User_App.forecast_plan set forecast_advance_commit_irp=(forecast_advance_commit_irp)+({}),updated_by=%s,updated_at=%s,active=1
                                where ori_part_number=%s and prod_week={} and prod_year={}""".format(commit1,commit1_week,commit1_year)
                                col=[updated_by,current_time,ori_part_number] 
                                condb(sql,col)
                                sql1="""update User_App.advance_commit set irp_processed=1,commit1_irp_processed=0,planner_review_updated=0 where id={}""".format(id)
                                condb(sql1)
                                process_type_calculation(ori_part_number,commit1_week,commit1_year)
                        if(commit2 is not None):
                            print("else part")
                            if commit2_accepted==1:
                                handling_null(demand_category,ori_part_number,commit2_week,commit2_year)
                                sql="""update User_App.forecast_plan set forecast_advance_commit_irp=(forecast_advance_commit_irp)+({}),updated_by=%s,updated_at=%s,active=1
                                where ori_part_number=%s and prod_week={} and prod_year={}""".format(commit2,commit2_week,commit2_year)
                                col=[updated_by,current_time,ori_part_number] 
                                condb(sql,col)
                                sql1="""update User_App.advance_commit set irp_processed=1,commit1_irp_processed=0,planner_review_updated=0 where id={}""".format(id)
                                condb(sql1)
                                process_type_calculation(ori_part_number,commit2_week,commit2_year)
                        if(commit3 is not None):
                            if commit3_accepted==1:
                                handling_null(demand_category,ori_part_number,commit3_week,commit3_year)
                                sql="""update User_App.forecast_plan set forecast_advance_commit_irp=(forecast_advance_commit_irp)+({}),updated_by=%s,updated_at=%s,active=1
                                where ori_part_number=%s and prod_week={} and prod_year={}""".format(commit3,commit3_week,commit3_year)
                                col=[updated_by,current_time,ori_part_number] 
                                condb(sql,col)
                                sql1="""update User_App.advance_commit set irp_processed=1,commit1_irp_processed=0,planner_review_updated=0 where id={}""".format(id)
                                condb(sql1)
                                process_type_calculation(ori_part_number,commit3_week,commit3_year)
                        if(commit4 is not None):
                            if commit4_accepted==1:
                                handling_null(demand_category,ori_part_number,commit4_week,commit4_year)
                                sql="""update User_App.forecast_plan set forecast_advance_commit_irp=(forecast_advance_commit_irp)+({}),updated_by=%s,updated_at=%s,active=1
                                where ori_part_number=%s and prod_week={} and prod_year={}""".format(commit4,commit4_week,commit4_year)
                                col=[updated_by,current_time,ori_part_number] 
                                condb(sql,col)
                                sql1="""update User_App.advance_commit set irp_processed=1,commit1_irp_processed=0,planner_review_updated=0 where id={}""".format(id)
                                condb(sql1)
                                process_type_calculation(ori_part_number,commit4_week,commit4_year)
                        
                    if demand_category=='GSA':
                        print("GSA")
                        if(commit1 is not None):
                            if commit1_accepted==1:
                                handling_null(demand_category,ori_part_number,commit1_week,commit1_year)
                                sql="""update User_App.forecast_plan set gsa_advance_commit_irp=(gsa_advance_commit_irp)+({}),updated_by=%s,updated_at=%s,active=1
                                where ori_part_number=%s and prod_week={} and prod_year={}""".format(commit1,commit1_week,commit1_year)
                                col=[updated_by,current_time,ori_part_number] 
                                condb(sql,col)
                                sql1="""update User_App.advance_commit set irp_processed=1,commit1_irp_processed=0,planner_review_updated=0 where id={}""".format(id)
                                condb(sql1)
                                process_type_calculation(ori_part_number,commit1_week,commit1_year)
                        if(commit2 is not None):
                            if commit2_accepted==1:
                                handling_null(demand_category,ori_part_number,commit2_week,commit2_year)
                                sql="""update User_App.forecast_plan set gsa_advance_commit_irp=(gsa_advance_commit_irp)+({}),updated_by=%s,updated_at=%s,active=1
                                where ori_part_number=%s and prod_week={} and prod_year={}""".format(commit2,commit2_week,commit2_year)
                                col=[updated_by,current_time,ori_part_number] 
                                condb(sql,col)
                                sql1="""update User_App.advance_commit set irp_processed=1,commit1_irp_processed=0,planner_review_updated=0 where id={}""".format(id)
                                condb(sql1)
                                process_type_calculation(ori_part_number,commit2_week,commit2_year)
                        if(commit3 is not None):
                            if commit3_accepted==1:
                                handling_null(demand_category,ori_part_number,commit3_week,commit3_year)
                                sql="""update User_App.forecast_plan set gsa_advance_commit_irp=(gsa_advance_commit_irp)+({}),updated_by=%s,updated_at=%s,active=1
                                where ori_part_number=%s and prod_week={} and prod_year={}""".format(commit3,commit3_week,commit3_year)
                                col=[updated_by,current_time,ori_part_number] 
                                condb(sql,col)
                                sql1="""update User_App.advance_commit set irp_processed=1,commit1_irp_processed=0,planner_review_updated=0 where id={}""".format(id)
                                condb(sql1)
                                process_type_calculation(ori_part_number,commit3_week,commit3_year)
                        if(commit4 is not None):
                            if commit4_accepted==1:
                                handling_null(demand_category,ori_part_number,commit4_week,commit4_year)
                                sql="""update User_App.forecast_plan set gsa_advance_commit_irp=(gsa_advance_commit_irp)+({}),updated_by=%s,updated_at=%s,active=1
                                where ori_part_number=%s and prod_week={} and prod_year={}""".format(commit4,commit4_week,commit4_year)
                                col=[updated_by,current_time,ori_part_number] 
                                condb(sql,col)
                                sql1="""update User_App.advance_commit set irp_processed=1,commit1_irp_processed=0,planner_review_updated=0 where id={}""".format(id)
                                condb(sql1)
                                process_type_calculation(ori_part_number,commit4_week,commit4_year)
                    
                    if demand_category=='Non GSA':
                        print("Non GSA")
                        if(commit1 is not None):
                            if commit1_accepted==1:
                                handling_null(demand_category,ori_part_number,commit1_week,commit1_year)
                                sql="""update User_App.forecast_plan set nongsa_advance_commit_irp=(nongsa_advance_commit_irp)+({}),updated_by=%s,updated_at=%s,active=1
                                where ori_part_number=%s and prod_week={} and prod_year={}""".format(commit1,commit1_week,commit1_year)
                                col=[updated_by,current_time,ori_part_number] 
                                condb(sql,col)
                                sql1="""update User_App.advance_commit set irp_processed=1,commit1_irp_processed=0,planner_review_updated=0 where id={}""".format(id)
                                condb(sql1)
                                process_type_calculation(ori_part_number,commit1_week,commit1_year)
                        if(commit2 is not None):
                            if commit2_accepted==1:
                                handling_null(demand_category,ori_part_number,commit2_week,commit2_year)
                                sql="""update User_App.forecast_plan set nongsa_advance_commit_irp=(nongsa_advance_commit_irp)+({}),updated_by=%s,updated_at=%s,active=1
                                where ori_part_number=%s and prod_week={} and prod_year={}""".format(commit2,commit2_week,commit2_year)
                                col=[updated_by,current_time,ori_part_number] 
                                condb(sql,col)
                                sql1="""update User_App.advance_commit set irp_processed=1,commit1_irp_processed=0,planner_review_updated=0 where id={}""".format(id)
                                condb(sql1)
                                process_type_calculation(ori_part_number,commit2_week,commit2_year)
                        if(commit3 is not None):
                            if commit3_accepted==1:
                                handling_null(demand_category,ori_part_number,commit3_week,commit3_year)
                                sql="""update User_App.forecast_plan set nongsa_advance_commit_irp=(nongsa_advance_commit_irp)+({}),updated_by=%s,updated_at=%s,active=1
                                where ori_part_number=%s and prod_week={} and prod_year={}""".format(commit3,commit3_week,commit3_year)
                                col=[updated_by,current_time,ori_part_number] 
                                condb(sql,col)
                                sql1="""update User_App.advance_commit set irp_processed=1,commit1_irp_processed=0,planner_review_updated=0 where id={}""".format(id)
                                condb(sql1)
                                process_type_calculation(ori_part_number,commit3_week,commit3_year)
                        if(commit4 is not None):
                            if commit4_accepted==1:
                                handling_null(demand_category,ori_part_number,commit4_week,commit4_year)
                                sql="""update User_App.forecast_plan set nongsa_advance_commit_irp=(nongsa_advance_commit_irp)+({}),updated_by=%s,updated_at=%s,active=1
                                where ori_part_number=%s and prod_week={} and prod_year={}""".format(commit4,commit4_week,commit4_year)
                                col=[updated_by,current_time,ori_part_number] 
                                condb(sql,col)
                                sql1="""update User_App.advance_commit set irp_processed=1,commit1_irp_processed=0,planner_review_updated=0 where id={}""".format(id)
                                condb(sql1)
                                process_type_calculation(ori_part_number,commit4_week,commit4_year)
                    
                    if demand_category=='System':
                        print("System")
                        if(commit1 is not None):
                            if commit1_accepted==1:
                                handling_null(demand_category,ori_part_number,commit1_week,commit1_year)
                                sql="""update User_App.forecast_plan set system_advance_commit_irp=(system_advance_commit_irp)+({}),updated_by=%s,updated_at=%s,active=1
                                where ori_part_number=%s and prod_week={} and prod_year={}""".format(commit1,commit1_week,commit1_year)
                                col=[updated_by,current_time,ori_part_number] 
                                condb(sql,col)
                                sql1="""update User_App.advance_commit set irp_processed=1,commit1_irp_processed=0,planner_review_updated=0 where id={}""".format(id)
                                condb(sql1)
                                process_type_calculation(ori_part_number,commit1_week,commit1_year)
                        if(commit2 is not None):
                            if commit2_accepted==1:
                                handling_null(demand_category,ori_part_number,commit2_week,commit2_year)
                                sql="""update User_App.forecast_plan set system_advance_commit_irp=(system_advance_commit_irp)+({}),updated_by=%s,updated_at=%s,active=1
                                where ori_part_number=%s and prod_week={} and prod_year={}""".format(commit2,commit2_week,commit2_year)
                                col=[updated_by,current_time,ori_part_number] 
                                condb(sql,col)
                                sql1="""update User_App.advance_commit set irp_processed=1,commit1_irp_processed=0,planner_review_updated=0 where id={}""".format(id)
                                condb(sql1)
                                process_type_calculation(ori_part_number,commit2_week,commit2_year)
                        if(commit3 is not None):
                            if commit3_accepted==1:
                                handling_null(demand_category,ori_part_number,commit3_week,commit3_year)
                                sql="""update User_App.forecast_plan set system_advance_commit_irp=(system_advance_commit_irp)+({}),updated_by=%s,updated_at=%s,active=1
                                where ori_part_number=%s and prod_week={} and prod_year={}""".format(commit3,commit3_week,commit3_year)
                                col=[updated_by,current_time,ori_part_number] 
                                condb(sql,col)
                                sql1="""update User_App.advance_commit set irp_processed=1,commit1_irp_processed=0,planner_review_updated=0 where id={}""".format(id)
                                condb(sql1)
                                process_type_calculation(ori_part_number,commit3_week,commit3_year)
                        if(commit4 is not None):
                            if commit4_accepted==1:
                                handling_null(demand_category,ori_part_number,commit4_week,commit4_year)
                                sql="""update User_App.forecast_plan set system_advance_commit_irp=(system_advance_commit_irp)+({}),updated_by=%s,updated_at=%s,active=1
                                where ori_part_number=%s and prod_week={} and prod_year={}""".format(commit4,commit4_week,commit4_year)
                                col=[updated_by,current_time,ori_part_number] 
                                condb(sql,col)
                                sql1="""update User_App.advance_commit set irp_processed=1,commit1_irp_processed=0,planner_review_updated=0 where id={}""".format(id)
                                condb(sql1)
                                process_type_calculation(ori_part_number,commit4_week,commit4_year) 
    
    
        cleaning_the_secondary_table="""truncate User_App.advance_commit_backup"""
        condb(cleaning_the_secondary_table)
        loading_the_latest_records="""insert into User_App.advance_commit_backup select * from User_App.advance_commit where process_week={}""".format(week_num)
        condb(loading_the_latest_records)
        s3_client = boto3.client('s3')
        df_json={
            "last_processed_at":str(start_time),
            "Status":"Success",
            "Next_process_starts_at":str(next_time)
                }
        json_body=json.dumps(df_json)
        s3_client.put_object(Body=json_body,Bucket='rapid-response-prod',Key='irp_event_timestamp/irp_processing_timestamp_prod.json')
    
        return "Job completed"
    else:
        print("Phase is not in Review")
        return "Not in REVIEW PHASE"    
                        
    