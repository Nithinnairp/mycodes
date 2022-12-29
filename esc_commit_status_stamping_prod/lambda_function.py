import json
import pandas as pd
import datetime 
import sys
import boto3
import pymysql
import logging
import requests
from DB_conn import condb,condb_dict
from Delete import delete_functionality

my_date=datetime.datetime.today()
year,week_num,day_of_week=my_date.isocalendar()
current_year=str(year)
current_year=current_year[2:]

def lambda_handler(event, context):
    current_time=datetime.datetime.now()
    date_str=current_time.strftime("%Y-%m-%d %H:%M")
    sng_time=(current_time+datetime.timedelta(hours=8)).strftime("%Y-%m-%d %H_%M")
    formated_time=(current_time+datetime.timedelta(hours=8)).strftime("%Y-%m-%d %H_%M").replace(' ','+')
    print(event)
    print(current_time)
    mass_updated=event['mass_updated']
                
    def change_status(ori_part_number,demand_type,prod_week,prod_year,updated_by):
        def manual_adj_cal(ori_part_number,prod_week,prod_year,demand_type):
            adj='nongsa_adj' if demand_type=='NONGSA' else 'gsa_adj' if demand_type=='GSA' else 'forecast_adj' if demand_type=='FORECAST' else 'system_adj'
            try:
                manual_adj_query="""select  sum(esc.{}) as esc_adj,fp.{} as total_adj from forecast_plan as fp join
                escalation as esc on fp.ori_part_number=esc.ori_part_number and fp.process_week=esc.process_week and 
                fp.prod_week=esc.prod_week and fp.prod_year=esc.prod_year where fp.ori_part_number=%s and 
                fp.prod_week={} and fp.prod_year={} and fp.process_week={} and esc.demand_type='{}' 
                and esc.status='APPROVED';""".format(adj,adj,prod_week,prod_year,week_num,demand_type)
                print(manual_adj_query)
                col=[ori_part_number]
                manual_adj_qty=condb_dict(manual_adj_query,col)[0]
                esc_adj_qty=manual_adj_qty['esc_adj'] if manual_adj_qty['esc_adj'] is not None else 0
                total_adj_qty=manual_adj_qty['total_adj'] if manual_adj_qty['total_adj'] is not None else 0
                final_manual_adj=total_adj_qty-esc_adj_qty if total_adj_qty-esc_adj_qty is not None else 0
                print('final_manual_adj',final_manual_adj,'esc_adj_qty',esc_adj_qty,'total_adj_qty',total_adj_qty)
                return final_manual_adj,esc_adj_qty
            except:
                print("No records found")
                return 0
            
        def total_commit_cal(ori_part_number,prod_week,prod_year,demand_type):
            sql1="""select * from forecast_plan as fp where
                        fp.process_week={} and fp.ori_part_number=%s and fp.prod_week={} and fp.prod_year={}""".format(week_num,prod_week,prod_year)
            col1=[ori_part_number]
            result1=condb_dict(sql1,col1)
            for i in result1:
                adj='nongsa_adj' if demand_type=='NONGSA' else 'gsa_adj' if demand_type=='GSA' else 'forecast_adj' if demand_type=='FORECAST' else 'system_adj'
                original='nongsa_original' if demand_type=='NONGSA' else 'gsa_original' if demand_type=='GSA' else 'forecast_original' if demand_type=='FORECAST' else 'system_original'
                supply='nongsa_supply' if demand_type=='NONGSA' else 'gsa_supply' if demand_type=='GSA' else 'forecast_supply' if demand_type=='FORECAST' else 'system_supply'
                cumulative='cumulative_nongsa_advance_commit_irp' if demand_type=='NONGSA' else 'cumulative_gsa_advance_commit_irp' if demand_type=='GSA' else 'cumulative_forecast_advance_commit_irp' if demand_type=='FORECAST' else 'cumulative_system_advance_commit_irp'
                D_commit1='nongsa_commit1' if demand_type=='NONGSA' else 'gsa_commit1' if demand_type=='GSA' else 'forecast_commit1' if demand_type=='FORECAST' else 'system_commit1'
                D_commit2='nongsa_commit2' if demand_type=='NONGSA' else 'gsa_commit2' if demand_type=='GSA' else 'forecast_commit2' if demand_type=='FORECAST' else 'system_commit2'
                D_commit3='nongsa_commit3' if demand_type=='NONGSA' else 'gsa_commit3' if demand_type=='GSA' else 'forecast_commit3' if demand_type=='FORECAST' else 'system_commit3'
                D_commit4='nongsa_commit4' if demand_type=='NONGSA' else 'gsa_commit4' if demand_type=='GSA' else 'forecast_commit4' if demand_type=='FORECAST' else 'system_commit4'
                commit1=i[D_commit1] if i[D_commit1] is not None else 0
                commit2=i[D_commit2] if i[D_commit2] is not None else 0
                commit3=i[D_commit3] if i[D_commit3] is not None else 0
                commit4=i[D_commit4] if i[D_commit4] is not None else 0
                print('commits',commit1,commit2,commit3,commit4)
                D_adj=i[adj] if i[adj] is not None else 0
                D_original=i[original] if i[original] is not None else 0
                D_supply=i[supply] if i[supply] is not None else 0
                D_cummu=i[cumulative] if i[cumulative] is not None else 0
                D_commit=commit1+commit2+commit3+commit4
                total_commit=D_supply+D_cummu+D_commit if D_supply+D_cummu+D_commit is not None else 0
                fp_id=i['id']
                print('supply',D_supply,'cummu',D_cummu,'total_adv_commit',D_commit,'original',D_original,'Adj',D_adj)
            return D_original,D_adj,D_supply,D_cummu,D_commit,fp_id
            
        sql="""select * from escalation where status='APPROVED' and ori_part_number=%s and process_week={} and prod_week={} and prod_year={}
        and demand_type='{}';""".format(week_num,prod_week,prod_year,demand_type)
        col=[ori_part_number]
        result=condb_dict(sql,col)
        adj='nongsa_adj' if demand_type=='NONGSA' else 'gsa_adj' if demand_type=='GSA' else 'forecast_adj' if demand_type=='FORECAST' else 'system_adj'
        
        manual_adj,total_esc=manual_adj_cal(ori_part_number,prod_week,prod_year,demand_type)
        original,D_adj,D_supply,D_cummu,D_commit,f_id=total_commit_cal(ori_part_number,prod_week,prod_year,demand_type)
        print("D_S",D_supply,'D_C:',D_cummu,'total_commit',D_commit,'fp_id:',f_id)
        balance=(D_supply+D_commit+D_cummu)-(original+manual_adj)
        print('starting balance',balance)
        deciding_factor=total_esc-((D_supply+D_cummu+D_commit)-(original+manual_adj))
        print(deciding_factor,'deciding_factor')
        updated_by_code=updated_by
        
        if deciding_factor==0:
            print("commit matching the demand marking all escalations as complete")
            sql2="""update escalation set full_commit=1,cm_commit_status_changed_by={} where ori_part_number=%s and prod_week={} and 
            prod_year={} and demand_type='{}' and process_week={};""".format(updated_by_code,prod_week,prod_year,demand_type,week_num)
            col2=[ori_part_number]
            condb(sql2,col2)
            processed_records="""update forecast_plan set commit_advance_commit_updated=0,adj_updated=0 where ori_part_number=%s and 
            prod_week={} and prod_year={} and process_week={};""".format(prod_week,prod_year,week_num)
            condb(processed_records,col2)
            
        if deciding_factor>0:
            print("deciding factor is positive hence marking all negative as complete")
            sql1="""select * from escalation where status='APPROVED' and ori_part_number=%s and process_week={} and prod_week={} and prod_year={}
            and demand_type='{}' and {}>0 order by approved_at;""".format(week_num,prod_week,prod_year,demand_type,adj)
            col1=[ori_part_number]
            result1=condb_dict(sql1,col1)
            sql3="""update escalation set cm_commit_status_changed_by={},full_commit=
                        (case
                        when {}<0 then 1
                        else 0
                        end)where {}<0 and ori_part_number=%s and prod_week={} and   
                        prod_year={} and demand_type='{}' and process_week={};""".format(updated_by_code,adj,adj,prod_week,prod_year,demand_type,week_num)
            col3=[ori_part_number]
            condb(sql3,col3)
            sql4="""select ifnull(sum(ifnull({},0)),0) as temp_balance from escalation where {}<0 and ori_part_number=%s and prod_week={} and   
                        prod_year={} and demand_type='{}' and process_week={}""".format(adj,adj,prod_week,prod_year,demand_type,week_num)
            result4=condb_dict(sql4,col3)[0]
            print(result4)
            temp_balance=result4['temp_balance']
            print('balance',balance,'temp_balance',temp_balance)
            balance=balance-(temp_balance)
            for i in result1:
                id=i['id']
                adj='nongsa_adj' if demand_type=='NONGSA' else 'gsa_adj' if demand_type=='GSA' else 'forecast_adj' if demand_type=='FORECAST' else 'system_adj'
                print('Adj:',i[adj],'balance:',balance,'id:',i['id'])
                if i[adj]>0 and (balance-(i[adj])>=0):
                    sql5="""update escalation set full_commit=1,cm_commit_status_changed_by={} where id={}""".format(updated_by_code,id)
                    condb(sql5)
                    balance=balance-(i[adj])
                else:
                    sql5="""update escalation set full_commit=0,cm_commit_status_changed_by={} 
                    where ori_part_number=%s and prod_week={} and   
                    prod_year={} and demand_type='{}' and process_week={} 
                    and approved_at>='{}' and {}>0;""".format(updated_by_code,prod_week,prod_year,demand_type,week_num,i['approved_at'],adj)   
                    col5=[ori_part_number]
                    condb(sql5,col5)
                    break
            processed_records="""update forecast_plan set commit_advance_commit_updated=0,adj_updated=0 where ori_part_number=%s and 
            prod_week={} and prod_year={} and process_week={};""".format(prod_week,prod_year,week_num)
            condb(processed_records,col3)
                    
        if deciding_factor<0:
            print("deciding factor is negative hence marking all positive as complete")
            sql1="""select * from escalation where status='APPROVED' and ori_part_number=%s and process_week={} and prod_week={} and prod_year={}
            and demand_type='{}' and {}<0 order by approved_at;""".format(week_num,prod_week,prod_year,demand_type,adj)
            col1=[ori_part_number]
            result1=condb_dict(sql1,col1)
            sql3="""update escalation set cm_commit_status_changed_by={},full_commit=
                        (case
                        when {}>0 then 1
                        else 0
                        end)where {}>0 and ori_part_number=%s and prod_week={} and   
                        prod_year={} and demand_type='{}' and process_week={};""".format(updated_by_code,adj,adj,prod_week,prod_year,demand_type,week_num)
            col3=[ori_part_number]
            condb(sql3,col3)
            sql4="""select ifnull(sum(ifnull({},0)),0) as temp_balance from escalation where {}>0 and ori_part_number=%s and prod_week={} and   
                        prod_year={} and demand_type='{}' and process_week={}""".format(adj,adj,prod_week,prod_year,demand_type,week_num)
            result4=condb_dict(sql4,col3)[0]
            temp_balance=result4['temp_balance']
            print('sql4','balance',balance,'temp_balance',temp_balance)
            balance=balance-(temp_balance)
            for i in result1:
                id=i['id']
                adj='nongsa_adj' if demand_type=='NONGSA' else 'gsa_adj' if demand_type=='GSA' else 'forecast_adj' if demand_type=='FORECAST' else 'system_adj'
                print('Adj:',i[adj],'balance:',balance,'id:',i['id'])
                
                if i[adj]<0 and (balance-(i[adj])<=0):
                    sql5="""update escalation set full_commit=1,cm_commit_status_changed_by={} where id={}""".format(updated_by_code,id)
                    condb(sql5)
                    balance=balance-(i[adj])
                else:
                    sql5="""update escalation set full_commit=0,cm_commit_status_changed_by={} 
                    where ori_part_number=%s and prod_week={} and   
                    prod_year={} and demand_type='{}' and process_week={} 
                    and approved_at>='{}' and {}<0;""".format(updated_by_code,prod_week,prod_year,demand_type,week_num,i['approved_at'],adj)   
                    col5=[ori_part_number]
                    condb(sql5,col5)
                    break
            processed_records="""update forecast_plan set commit_advance_commit_updated=0,adj_updated=0 where ori_part_number=%s and 
            prod_week={} and prod_year={} and process_week={};""".format(prod_week,prod_year,week_num)
            condb(processed_records,col3)
    
    if mass_updated==1:
        print('mass updation')
        updated_by=4
        fetch_records="""select fp.ori_part_number,fp.prod_week,fp.prod_year,esc.demand_type from forecast_plan as fp join 
            escalation as esc on fp.ori_part_number=esc.ori_part_number and fp.prod_week=esc.prod_week and fp.process_week=esc.process_week and
            fp.prod_year=esc.prod_year where (fp.commit_advance_commit_updated=1 or fp.adj_updated=1) and esc.status='APPROVED' and fp.process_week={}
            group by esc.ori_part_number,esc.prod_week,esc.prod_year,esc.demand_type;""".format(week_num)
        records=condb_dict(fetch_records)
        for row in records:
            ori_part_number=row['ori_part_number']
            prod_week=row['prod_week']
            prod_year=row['prod_year']
            demand_type=row['demand_type']
            print(ori_part_number,demand_type,prod_week,prod_year,updated_by)
            change_status(ori_part_number,demand_type,prod_week,prod_year,updated_by)
           
    else:
        print('manual updation')
        delete_functionality()
        updated_by=5
        fetch_records="""select fp.ori_part_number,fp.prod_week,fp.prod_year,esc.demand_type from forecast_plan as fp join 
            escalation as esc on fp.ori_part_number=esc.ori_part_number and fp.prod_week=esc.prod_week and fp.process_week=esc.process_week and
            fp.prod_year=esc.prod_year where (fp.commit_advance_commit_updated=1 or fp.adj_updated=1) and esc.status='APPROVED' and fp.process_week={}
            group by esc.ori_part_number,esc.prod_week,esc.prod_year,esc.demand_type;""".format(week_num)
        records=condb_dict(fetch_records)
        # data=pd.DataFrame(data=records,columns=['ori_part_number','prod_week','prod_year','demand_type'])
        for row in records:
            ori_part_number=row['ori_part_number']
            prod_week=row['prod_week']
            prod_year=row['prod_year']
            demand_type=row['demand_type']
            print(ori_part_number,demand_type,prod_week,prod_year,updated_by)
            change_status(ori_part_number,demand_type,prod_week,prod_year,updated_by)
            
        
    return 'success'
    