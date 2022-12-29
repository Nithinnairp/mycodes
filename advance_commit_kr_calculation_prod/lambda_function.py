import json
import pandas as pd
import datetime 
import sys
import boto3
import json
import pymysql
from DB_conn import condb 
import logging
import os

my_date=datetime.datetime.today()
year,week_num,day_of_week=my_date.isocalendar()
current_year=str(year)
current_year=current_year[2:]
DB_NAME = os.environ['DB_NAME']

#Connecting to the DB
def lambda_handler(event, context):
    current_time=datetime.datetime.now()

    get_advance_col_names="""SELECT column_name from information_schema.columns where table_schema='{}' and table_name='advance_commit';""".format(DB_NAME)
    advance_col_names=list(condb(get_advance_col_names))
    advance_db_columns=list(sum(advance_col_names,()))
    #### FIX ME ####[]
    print(datetime.datetime.now(),"Before")
    advance_commit_query="""select * from {}.advance_commit where process_week={} and (kr_processed=0 or kr_processed is null)""".format(DB_NAME,week_num)
    df=condb(advance_commit_query)
    print(datetime.datetime.now(),"After select")
    update_advance_kr_processed="""Update {}.advance_commit set kr_processed=-1 where process_week={} and (kr_processed=0 or kr_processed is null)""".format(DB_NAME,week_num)
    condb(update_advance_kr_processed)
    print(datetime.datetime.now(),"After update")
    advance_commit_data=pd.DataFrame(data=df,columns=advance_db_columns)
    advance_commit_data=advance_commit_data.fillna({'commit1':0,'commit2':0,'commit3':0,'commit4':0,
          'commit1_accepted':0,'commit2_accepted':0,'commit3_accepted':0,'commit4_accepted':0,
          'commit1_old_value':0,'commit2_old_value':0,'commit3_old_value':0,'commit4_old_value':0,
          'commit1_week':0,'commit2_week':0,'commit3_week':0,'commit4_week':0,
          'commit1_year':2000,'commit2_year':2000,'commit3_year':2000,'commit4_year':2000,'kr_processed':0})
    
    
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
            kr_value="select " + demand_category + "_advance_commit_kr from {}.forecast_plan where prod_week={} and prod_year={} and ori_part_number=%s".format(DB_NAME,commit_week,commit_year)
            # print(kr_value) 
            col=[ori_part_number]
            advance_commit_kr=condb(kr_value,col) 
            # print(advance_commit_kr)
            if advance_commit_kr[0][0]==None:
                update_kr="update {}.forecast_plan set ".format(DB_NAME)+demand_category+"_advance_commit_kr=0 where prod_week={} and prod_year={} and ori_part_number=%s".format(commit_week,commit_year)
                # print(update_kr) 
                col=[ori_part_number]  
                condb(update_kr,col)
        except:
            pass

    def get_supplier_id(supplier_name):
        get_supplier_name="""select id from {db_name}.active_suppliers where supplier_name='{supplier_name}';""".format(db_name=DB_NAME,supplier_name=supplier_name)
        supp_name=condb(get_supplier_name)
        try:
            id = supp_name[0][0]
        except:
            id = None
        return id

    def insert_records(ori_part_number,prod_week,prod_year,demand_category,commit):

        get_iodm_col_names="""SELECT column_name from information_schema.columns where table_schema='{}' and table_name='iodm_part_list';""".format(DB_NAME)
        iodm_col_names=list(condb(get_iodm_col_names))
        iodm_db_columns=list(sum(iodm_col_names,()))
        ori_part_number=str(ori_part_number)
        iodm_query="""select * from {}.iodm_part_list where part_name=%s""".format(DB_NAME)
        col=[ori_part_number] 
        df=condb(iodm_query,col)
        iodm_data=pd.DataFrame(data=df,columns=iodm_db_columns)
        # print(iodm_data)
        print(ori_part_number,prod_week,prod_year,demand_category,commit,id)
        
        if (prod_week!=0 and prod_year!=00):
            for index,row in iodm_data.iterrows():
                bu=row.bu
                buffer=0
                buffer_opt_adj=0
                build_type=row.build_type
                current_open_po=0
                date=datetime.datetime.strftime(list(condb('select prod_date from {}.keysight_calendar where prod_week={} and prod_year={} limit 1'.format(DB_NAME,prod_week,prod_year)))[0][0],"%Y-%m-%d")
                dept_code=row.dept
                division=row.division
                forecast_adj=0  
                forecast_commit=0
                forecast_final=0
                forecast_original=0
                forecast_adj_reason=None
                gsa_adj=0
                gsa_commit=0
                gsa_final=0
                gsa_original=0
                gsa_adj_reason=None
                nongsa_adj=0
                nongsa_commit=0
                nongsa_final=0
                nongsa_original=0
                nongsa_adj_reason=None
                system_adj=0
                system_commit=0
                system_final=0
                system_original=0
                system_adj_reason=None
                total_adj=0
                total_commit=0
                total_final=0
                total_original=0
                exception=0
                is_escalated=0
                on_hand_nettable_si=0
                on_hand_total_cm=0
                ori_part_number=row.part_name
                past_due_open_po=0
                planner_code=row.part_planner_code
                planner_name=row.part_planner_name
                print(prod_week,prod_year,id)
                prod_month=list(condb('select prod_month from {}.keysight_calendar where prod_week={} and prod_year={}'.format(DB_NAME,prod_week,prod_year)))[0][0]
                prod_year_week=int(str(int(prod_year))+str(int(prod_week)))
                prod_year_month=int(str(int(prod_year))+str(int(prod_month)))
                product_family=row.product_family
                received_qty=0
                source_supplier=row.supplier_name
                source_supplier_id=get_supplier_id(source_supplier)
                print(source_supplier_id)
                # source_supplier_id=list(condb('select source_supplier_id from User_App.forecast_plan where ori_part_number=%s limit 1',ori_part_number))[0][0]
                forecast_commit1=0
                forecast_commit2=0
                forecast_commit3=0
                forecast_commit4=0
                forecast_exception=0
                gsa_commit1=0
                gsa_commit2=0
                gsa_commit3=0
                gsa_commit4=0
                gsa_exception=0
                nongsa_commit1=0
                nongsa_commit2=0
                nongsa_commit3=0
                nongsa_commit4=0
                nongsa_exception=0
                system_commit1=0
                system_commit2=0
                system_commit3=0
                system_commit4=0
                system_exception=0
                de_commit=0
                forecast_de_commit=0
                gsa_de_commit=0
                nongsa_de_commit=0
                system_de_commit=0
                created_at=current_time
                created_by=2
                product_line=row.product_line
                finished_goods='N'
                active=1
                total_supply=0
                cid_mapped_part_number=row.part_name
                try:
                    dmp_orndmp=list(condb('select dmp_orndmp from {}.forecast_plan where prod_month={} and prod_year={} limit 1'.format(DB_NAME,prod_month,prod_year)))[0][0]
                except:
                    print("commited for past week")
                    break
                org=row.part_site
                instrument=row.instrument
                description=row.part_description
                item_type=row.part_item_type
                process_week=week_num
                process_year=year
                # is_dmp=ord(list(condb("select is_dmp from User_App.forecast_plan where dmp_orndmp='{}' limit 1".format(dmp_orndmp)))[0][0])
                is_dmp=1 if dmp_orndmp=='DMP' else 0
                old_data=0 
                manually_added=0
                oripartnumber_week_year=str(str(ori_part_number)+'-'+str(prod_week)+'-'+str(prod_year))
                cal_option=row.kr_special_opt_cal
                measurable=1 if row.measureable_flag=='Y' else 0 if row.measureable_flag=='N' else None
                countable=1 if row.countable_flag=='Yes' else 0
                advance_commit = {demand_category.lower().replace(' ','') + "_advance_commit_kr": commit}
                kr = list(advance_commit.keys())[0]
                commit = list(advance_commit.values())[0]
                print(date,prod_month,source_supplier_id,dmp_orndmp,is_dmp,measurable,countable,cal_option)
                check="""select id from {}.forecast_plan where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,prod_week,prod_year)
                col=[ori_part_number]
                id_found=list(condb(check,col)) 
                if id_found==[]:
                    sql1 = """insert into {}.forecast_plan(bu,buffer,buffer_opt_adj,build_type,current_open_po,date,dept_code,division,
                    forecast_adj,forecast_commit,forecast_final,forecast_original,forecast_adj_reason,
                    gsa_adj,gsa_commit,gsa_final,gsa_original,gsa_adj_reason,
                    nongsa_adj,nongsa_commit,nongsa_final,nongsa_original,nongsa_adj_reason,
                    system_adj,system_commit,system_final,system_original,system_adj_reason,
                    total_adj,total_commit,total_final,total_original,exception,is_escalated,
                    on_hand_nettable_si,on_hand_total_cm,ori_part_number,past_due_open_po,planner_code,
                    planner_name,prod_month,prod_week,prod_year,prod_year_week,
                    prod_year_month,product_family,received_qty,source_supplier,source_supplier_id,
                    forecast_commit1,forecast_commit2,forecast_commit3,forecast_commit4,forecast_exception,
                    gsa_commit1,gsa_commit2,gsa_commit3,gsa_commit4,gsa_exception,
                    nongsa_commit1,nongsa_commit2,nongsa_commit3,nongsa_commit4,nongsa_exception,
                    system_commit1,system_commit2,system_commit3,system_commit4,system_exception,
                    de_commit,forecast_de_commit,gsa_de_commit,nongsa_de_commit,system_de_commit,
                    created_at,created_by,product_line,finished_goods,active,total_supply,
                    cid_mapped_part_number,dmp_orndmp,org,instrument,description,
                    item_type,process_week,process_year,is_dmp,old_data,manually_added,
                    cal_option,measurable,countable,via_advance_commit,oripartnumber_week_year,{}) 
                    values (%s,%s,%s,%s,%s,%s,%s,%s,
                    		%s,%s,%s,%s,%s,
                    		%s,%s,%s,%s,%s,
                    		%s,%s,%s,%s,%s,
                    		%s,%s,%s,%s,%s,
                    		%s,%s,%s,%s,%s,
                    		%s,%s,%s,%s,%s,
                    		%s,%s,%s,%s,%s,
                    		%s,%s,%s,%s,%s,%s,
                    		%s,%s,%s,%s,%s,
                    		%s,%s,%s,%s,%s,
                    		%s,%s,%s,%s,%s,
                    		%s,%s,%s,%s,%s,
                    		%s,%s,%s,%s,%s,
                    		%s,%s,%s,%s,%s,%s,
                    		%s,%s,%s,%s,%s,
                    		%s,%s,%s,%s,%s,%s,
                    		%s,%s,%s,1,%s,{})""".format(DB_NAME,kr,commit)
                    col=[bu,buffer,buffer_opt_adj,build_type,current_open_po,date,dept_code,division,
                            forecast_adj,forecast_commit,forecast_final,forecast_original,forecast_adj_reason,
                            gsa_adj,gsa_commit,gsa_final,gsa_original,gsa_adj_reason,
                            nongsa_adj,nongsa_commit,nongsa_final,nongsa_original,nongsa_adj_reason,
                            system_adj,system_commit,system_final,system_original,system_adj_reason,
                            total_adj,total_commit,total_final,total_original,exception,is_escalated,
                            on_hand_nettable_si,on_hand_total_cm,ori_part_number,past_due_open_po,planner_code,
                            planner_name,prod_month,prod_week,prod_year,prod_year_week,
                            prod_year_month,product_family,received_qty,source_supplier,source_supplier_id,
                            forecast_commit1,forecast_commit2,forecast_commit3,forecast_commit4,forecast_exception,
                            gsa_commit1,gsa_commit2,gsa_commit3,gsa_commit4,gsa_exception,
                            nongsa_commit1,nongsa_commit2,nongsa_commit3,nongsa_commit4,nongsa_exception,
                            system_commit1,system_commit2,system_commit3,system_commit4,system_exception,
                            de_commit,forecast_de_commit,gsa_de_commit,nongsa_de_commit,system_de_commit,
                            created_at,created_by,product_line,finished_goods,active,total_supply,
                            cid_mapped_part_number,dmp_orndmp,org,instrument,description,
                            item_type,process_week,process_year,is_dmp,old_data,manually_added,cal_option,measurable,countable,oripartnumber_week_year]
                    condb(sql1,col)
                else:
                    print("record already exists")

            
    for index,row in advance_commit_data.iterrows():
        id=row.id 
        ori_part_number=str(row.ori_part_number)
        commit1=row.commit1
        commit1_week=int(float(row.commit1_week))
        commit1_year=str(row.commit1_year)
        commit1_year=int(float(commit1_year[2:]))
        commit1_old_value=row.commit1_old_value
        commit2=row.commit2
        commit2_week=int(float(row.commit2_week))
        commit2_year=str(row.commit2_year)
        print(id,commit2_year)
        commit2_year=int(float(commit2_year[2:]))
        commit2_old_value=row.commit2_old_value
        commit3=row.commit3
        commit3_week=int(float(row.commit3_week))
        commit3_year=str(row.commit3_year)
        print(row.commit3_year,commit3_year,id)
        commit3_year=int(float(commit3_year[2:]))
        commit3_old_value=row.commit3_old_value
        commit4=row.commit4
        commit4_week=int(float(row.commit4_week))
        commit4_year=str(row.commit4_year)
        commit4_year=int(float(commit4_year[2:]))
        commit4_old_value=row.commit4_old_value
        demand_category=row.demand_category
        updated_by=row.updated_by
        commit1_date=row.commit1_date
        commit1_date_old_value=row.commit1_date_old_value
        commit2_date=row.commit2_date
        commit2_date_old_value=row.commit2_date_old_value
        commit3_date=row.commit3_date
        commit3_date_old_value=row.commit3_date_old_value
        commit4_date=row.commit4_date
        commit4_date_old_value=row.commit4_date_old_value
        
        
        x,y,z=get_date_format(row.commit1_date)
        print(row.commit1_date,commit1_week,commit1_year,x,y,z)

        if demand_category=='Forecast':
            try:
                if (commit1 is not None)and(commit1_date_old_value is None or commit1_date==commit1_date_old_value):
                    # print(commit1,commit1_week,commit1_year,ori_part_number)
                    check="""select id from {}.forecast_plan where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit1_week,commit1_year)
                    col=[ori_part_number]
                    id_found=list(condb(check,col)) 
                    print(id_found,id)
                    if id_found==[]:
                        insert_records(ori_part_number,commit1_week,commit1_year,demand_category,commit1)
                        print('Added a Workweek under forecast_commit1 for',ori_part_number,commit1_week,commit1_year,id)
                        sql1="""update {}.advance_commit set kr_processed=1 where id={}""".format(DB_NAME,id)
                        condb(sql1)
                    else:
                        handling_null(demand_category,ori_part_number,commit1_week,commit1_year)
                        sql="""update {}.forecast_plan set forecast_advance_commit_kr=forecast_advance_commit_kr+({}-{}),updated_by=%s,updated_at=%s,active=1,process_week={},process_year={}
                        where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit1,commit1_old_value,week_num,year,commit1_week,commit1_year)
                        col=[updated_by,current_time,ori_part_number]
                        condb(sql,col)
                        print('updated a Workweek under forecast_commit1 for',ori_part_number,commit1_week,id)
                        sql1="""update {}.advance_commit set kr_processed=1 where id={}""".format(DB_NAME,id)
                        condb(sql1)
                else:
                    check="""select id from {}.forecast_plan where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit1_week,commit1_year)
                    col=[ori_part_number]
                    id_found=list(condb(check,col)) 
                    print(id_found,id)
                    if id_found==[]:
                        insert_records(ori_part_number,commit1_week,commit1_year,demand_category,commit1)
                        print('Added a Workweek under forecast_commit1 for',ori_part_number,commit1_week,commit1_year,id)
                        sql1="""update {}.advance_commit set kr_processed=1 where id={}""".format(DB_NAME,id)
                        condb(sql1)
                        commit1_date_old_value=(row.commit1_date_old_value)
                        old_commit_year,old_commit_week,day=get_date_format(commit1_date_old_value)
                        old_commit_year=str(old_commit_year)[2:]
                        sql="""update {}.forecast_plan set forecast_advance_commit_kr=forecast_advance_commit_kr-({}),updated_by=%s,updated_at=%s,active=1,process_week={},process_year={}
                            where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit1_old_value,week_num,year,old_commit_week,old_commit_year)
                        col=[updated_by,current_time,ori_part_number]
                        condb(sql,col)
                    else:
                        handling_null(demand_category,ori_part_number,commit1_week,commit1_year)
                        commit1_date_old_value=(row.commit1_date_old_value)
                        old_commit_year,old_commit_week,day=get_date_format(commit1_date_old_value)
                        old_commit_year=str(old_commit_year)[2:]
                        sql="""update {}.forecast_plan set forecast_advance_commit_kr=forecast_advance_commit_kr-({}),updated_by=%s,updated_at=%s,active=1,process_week={},process_year={}
                            where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit1_old_value,week_num,year,old_commit_week,old_commit_year)
                        col=[updated_by,current_time,ori_part_number]
                        condb(sql,col)
                        print("Date change occurred reducing the KR commit from previous date")
                        sql1="""update {}.forecast_plan set forecast_advance_commit_kr=forecast_advance_commit_kr+({}),updated_by=%s,updated_at=%s,active=1,process_week={},process_year={}
                            where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit1,week_num,year,commit1_week,commit1_year)
                        col1=[updated_by,current_time,ori_part_number]
                        condb(sql1,col1)
                        print('updated a Workweek under forecast_commit1 for',ori_part_number,commit1_week,id)
                        sql2="""update {}.advance_commit set kr_processed=1 where id={}""".format(DB_NAME,id)
                        condb(sql2)
                    
                if (commit2 is not None) and(commit2_date_old_value is None or commit2_date==commit2_date_old_value):
                    # print(commit2,commit2_week,commit2_year)
                    check="""select id from {}.forecast_plan where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit2_week,commit2_year)
                    col=[ori_part_number]
                    id_found=list(condb(check,col)) 
                    print(id_found,id)
                    if id_found==[]:
                        insert_records(ori_part_number,commit2_week,commit2_year,demand_category,commit2)
                        print('Added a Workweek under forecast_commit2 for',ori_part_number,commit2_week,commit2_year,id)
                        sql1="""update {}.advance_commit set kr_processed=1 where id={}""".format(DB_NAME,id)
                        condb(sql1)
                    else:
                        handling_null(demand_category,ori_part_number,commit2_week,commit2_year)
                        sql="""update {}.forecast_plan set forecast_advance_commit_kr=forecast_advance_commit_kr+({}-{}),updated_by=%s,updated_at=%s,active=1,process_week={},process_year={}
                        where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit2,commit2_old_value,week_num,year,commit2_week,commit2_year)
                        col=[updated_by,current_time,ori_part_number]
                        condb(sql,col)
                        print('updated a Workweek under forecast_commit2 for',ori_part_number,commit2_week,id)
                        sql1="""update {}.advance_commit set kr_processed=1 where id={}""".format(DB_NAME,id)
                        condb(sql1)
                else:
                    check="""select id from {}.forecast_plan where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit2_week,commit2_year)
                    col=[ori_part_number]
                    id_found=list(condb(check,col)) 
                    print(id_found,id)
                    if id_found==[]:
                        insert_records(ori_part_number,commit2_week,commit2_year,demand_category,commit2)
                        print('Added a Workweek under forecast_commit2 for',ori_part_number,commit2_week,commit2_year,id)
                        sql1="""update {}.advance_commit set kr_processed=1 where id={}""".format(DB_NAME,id)
                        condb(sql1)
                        commit2_date_old_value=(row.commit2_date_old_value)
                        old_commit_year,old_commit_week,day=get_date_format(commit2_date_old_value)
                        old_commit_year=str(old_commit_year)[2:]
                        sql="""update {}.forecast_plan set forecast_advance_commit_kr=forecast_advance_commit_kr-({}),updated_by=%s,updated_at=%s,active=1,process_week={},process_year={}
                            where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit2_old_value,week_num,year,old_commit_week,old_commit_year)
                        col=[updated_by,current_time,ori_part_number]
                        condb(sql,col)
                    else:
                        handling_null(demand_category,ori_part_number,commit2_week,commit2_year)
                        commit2_date_old_value=(row.commit2_date_old_value)
                        old_commit_year,old_commit_week,day=get_date_format(commit2_date_old_value)
                        old_commit_year=str(old_commit_year)[2:]
                        sql="""update {}.forecast_plan set forecast_advance_commit_kr=forecast_advance_commit_kr-({}),updated_by=%s,updated_at=%s,active=1,process_week={},process_year={}
                            where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit2_old_value,week_num,year,old_commit_week,old_commit_year)
                        col=[updated_by,current_time,ori_part_number]
                        condb(sql,col)
                        print("Date change occurred reducing the KR commit from previous date")
                        sql1="""update {}.forecast_plan set forecast_advance_commit_kr=forecast_advance_commit_kr+({}),updated_by=%s,updated_at=%s,active=1,process_week={},process_year={}
                            where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit2,week_num,year,commit2_week,commit2_year)
                        col1=[updated_by,current_time,ori_part_number]
                        condb(sql1,col1)
                        print('updated a Workweek under forecast_commit2 for',ori_part_number,commit2_week,id)
                        sql2="""update {}.advance_commit set kr_processed=1 where id={}""".format(DB_NAME,id)
                        condb(sql2)
                    
                    
                if (commit3 is not None) and (commit3_date_old_value is None or commit3_date==commit3_date_old_value):
                    # print(commit3,commit3_week,commit3_year)
                    check="""select id from {}.forecast_plan where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit3_week,commit3_year)
                    col=[ori_part_number]
                    id_found=list(condb(check,col)) 
                    print(id_found,id)
                    if id_found==[]:
                        insert_records(ori_part_number,commit3_week,commit3_year,demand_category,commit3)
                        print('Added a Workweek under forecast_commit3 for',ori_part_number,commit3_week,commit3_year,id)
                        sql1="""update {}.advance_commit set kr_processed=1 where id={}""".format(DB_NAME,id)
                        condb(sql1)
                    else:
                        handling_null(demand_category,ori_part_number,commit3_week,commit3_year)
                        sql="""update {}.forecast_plan set forecast_advance_commit_kr=forecast_advance_commit_kr+({}-{}),updated_by=%s,updated_at=%s,active=1,process_week={},process_year={}
                        where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit3,commit3_old_value,week_num,year,commit3_week,commit3_year)
                        col=[updated_by,current_time,ori_part_number]
                        condb(sql,col)
                        print('updated a Workweek under forecast_commit3 for',ori_part_number,commit3_week,id)
                        sql1="""update {}.advance_commit set kr_processed=1 where id={}""".format(DB_NAME,id)
                        condb(sql1)
                
                else:
                    check="""select id from {}.forecast_plan where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit3_week,commit3_year)
                    col=[ori_part_number]
                    id_found=list(condb(check,col)) 
                    print(id_found,id)
                    if id_found==[]:
                        insert_records(ori_part_number,commit3_week,commit3_year,demand_category,commit3)
                        print('Added a Workweek under forecast_commit3 for',ori_part_number,commit3_week,commit3_year,id)
                        sql1="""update {}.advance_commit set kr_processed=1 where id={}""".format(DB_NAME,id)
                        condb(sql1)
                        commit3_date_old_value=(row.commit3_date_old_value)
                        old_commit_year,old_commit_week,day=get_date_format(commit3_date_old_value)
                        old_commit_year=str(old_commit_year)[2:]
                        sql="""update {}.forecast_plan set forecast_advance_commit_kr=forecast_advance_commit_kr-({}),updated_by=%s,updated_at=%s,active=1,process_week={},process_year={}
                            where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit3_old_value,week_num,year,old_commit_week,old_commit_year)
                        col=[updated_by,current_time,ori_part_number]
                        condb(sql,col)
                    else:
                        handling_null(demand_category,ori_part_number,commit3_week,commit3_year)
                        commit3_date_old_value=(row.commit3_date_old_value)
                        old_commit_year,old_commit_week,day=get_date_format(commit3_date_old_value)
                        old_commit_year=str(old_commit_year)[2:]
                        sql="""update {}.forecast_plan set forecast_advance_commit_kr=forecast_advance_commit_kr-({}),updated_by=%s,updated_at=%s,active=1,process_week={},process_year={}
                            where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit3_old_value,week_num,year,old_commit_week,old_commit_year)
                        col=[updated_by,current_time,ori_part_number]
                        condb(sql,col)
                        print("Date change occurred reducing the KR commit from previous date")
                        sql1="""update {}.forecast_plan set forecast_advance_commit_kr=forecast_advance_commit_kr+({}),updated_by=%s,updated_at=%s,active=1,process_week={},process_year={} 
                            where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit3,week_num,year,commit3_week,commit3_year)
                        col1=[updated_by,current_time,ori_part_number]
                        condb(sql1,col1)
                        print('updated a Workweek under forecast_commit3 for',ori_part_number,commit3_week,id)
                        sql2="""update {}.advance_commit set kr_processed=1 where id={}""".format(DB_NAME,id)
                        condb(sql2)        
                		
                if (commit4 is not None) and (commit4_date_old_value is None or commit4_date==commit4_date_old_value):
                    # print(commit4,commit4_week,commit4_year)
                    check="""select id from {}.forecast_plan where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit4_week,commit4_year)
                    col=[ori_part_number]
                    id_found=list(condb(check,col)) 
                    print(id_found,id)
                    if id_found==[]:
                        insert_records(ori_part_number,commit4_week,commit4_year,demand_category,commit4)
                        print('Added a Workweek under forecast_commit4 for',ori_part_number,commit4_week,commit4_year,id)
                        sql1="""update {}.advance_commit set kr_processed=1 where id={}""".format(DB_NAME,id)
                        condb(sql1)
                    else:
                        handling_null(demand_category,ori_part_number,commit4_week,commit4_year)
                        sql="""update {}.forecast_plan set forecast_advance_commit_kr=forecast_advance_commit_kr+({}-{}),updated_by=%s,updated_at=%s,active=1,process_week={},process_year={} 
                        where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit4,commit4_old_value,week_num,year,commit4_week,commit4_year)
                        col=[updated_by,current_time,ori_part_number]
                        condb(sql,col)
                        print('updated a Workweek under forecast_commit4 for',ori_part_number,commit4_week,id)
                        sql1="""update {}.advance_commit set kr_processed=1 where id={}""".format(DB_NAME,id)
                        condb(sql1)
                        
                else:
                    check="""select id from {}.forecast_plan where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit4_week,commit4_year)
                    col=[ori_part_number]
                    id_found=list(condb(check,col)) 
                    print(id_found,id)
                    if id_found==[]:
                        insert_records(ori_part_number,commit4_week,commit4_year,demand_category,commit4)
                        print('Added a Workweek under forecast_commit4 for',ori_part_number,commit4_week,commit4_year,id)
                        sql1="""update {}.advance_commit set kr_processed=1 where id={}""".format(DB_NAME,id)
                        condb(sql1)
                        commit4_date_old_value=(row.commit4_date_old_value)
                        old_commit_year,old_commit_week,day=get_date_format(commit4_date_old_value)
                        old_commit_year=str(old_commit_year)[2:]
                        sql="""update {}.forecast_plan set forecast_advance_commit_kr=forecast_advance_commit_kr-({}),updated_by=%s,updated_at=%s,active=1,process_week={},process_year={}
                            where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit4_old_value,week_num,year,old_commit_week,old_commit_year)
                        col=[updated_by,current_time,ori_part_number]
                        condb(sql,col)
                    else:
                        handling_null(demand_category,ori_part_number,commit4_week,commit4_year)
                        commit4_date_old_value=(row.commit4_date_old_value)
                        old_commit_year,old_commit_week,day=get_date_format(commit4_date_old_value)
                        old_commit_year=str(old_commit_year)[2:]
                        sql="""update {}.forecast_plan set forecast_advance_commit_kr=forecast_advance_commit_kr-({}),updated_by=%s,updated_at=%s,active=1,process_week={},process_year={} 
                            where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit4_old_value,week_num,year,old_commit_week,old_commit_year)
                        col=[updated_by,current_time,ori_part_number]
                        condb(sql,col)
                        print("Date change occurred reducing the KR commit from previous date")
                        sql1="""update {}.forecast_plan set forecast_advance_commit_kr=forecast_advance_commit_kr+({}),updated_by=%s,updated_at=%s,active=1,process_week={},process_year={}
                            where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit4,week_num,year,commit4_week,commit4_year)
                        col1=[updated_by,current_time,ori_part_number]
                        condb(sql1,col1)
                        print('updated a Workweek under forecast_commit4 for',ori_part_number,commit4_week,id)
                        sql2="""update {}.advance_commit set kr_processed=1 where id={}""".format(DB_NAME,id)
                        condb(sql2)
                    
            except:
                print("Forecast category record not found for particular ori_part_number",ori_part_number)
                logging.error(error_handling())
                
            		
        if demand_category=='GSA':
            try:
                if (commit1 is not None) and (commit1_date_old_value is None or commit1_date==commit1_date_old_value):
                    # print(commit1,commit1_week,commit1_year,ori_part_number)
                    check="""select id from {}.forecast_plan where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit1_week,commit1_year)
                    col=[ori_part_number]
                    id_found=list(condb(check,col)) 
                    print(id_found,id)
                    if id_found==[]:
                        insert_records(ori_part_number,commit1_week,commit1_year,demand_category,commit1)
                        print('Added a Workweek under gsa_commit1 for',ori_part_number,commit1_week,commit1_year,id)
                        sql1="""update {}.advance_commit set kr_processed=1 where id={}""".format(DB_NAME,id)
                        condb(sql1)
                    else:
                        handling_null(demand_category,ori_part_number,commit1_week,commit1_year)
                        sql="""update {}.forecast_plan set gsa_advance_commit_kr=gsa_advance_commit_kr+({}-{}),updated_by=%s,updated_at=%s,active=1,process_week={},process_year={}
                        where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit1,commit1_old_value,week_num,year,commit1_week,commit1_year)
                        col=[updated_by,current_time,ori_part_number]
                        condb(sql,col)
                        print('updated a Workweek under gsa_commit1 for',ori_part_number,commit1_week,id)
                        sql1="""update {}.advance_commit set kr_processed=1 where id={}""".format(DB_NAME,id)
                        condb(sql1)
                else:
                    check="""select id from {}.forecast_plan where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit1_week,commit1_year)
                    col=[ori_part_number]
                    id_found=list(condb(check,col)) 
                    print(id_found,id)
                    if id_found==[]:
                        insert_records(ori_part_number,commit1_week,commit1_year,demand_category,commit1)
                        print('Added a Workweek under gsa_commit1 for',ori_part_number,commit1_week,commit1_year,id)
                        sql1="""update {}.advance_commit set kr_processed=1 where id={}""".format(DB_NAME,id)
                        condb(sql1)
                        commit1_date_old_value=(row.commit1_date_old_value)
                        old_commit_year,old_commit_week,day=get_date_format(commit1_date_old_value)
                        old_commit_year=str(old_commit_year)[2:]
                        sql="""update {}.forecast_plan set gsa_advance_commit_kr=gsa_advance_commit_kr-({}),updated_by=%s,updated_at=%s,active=1,process_week={},process_year={}
                            where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit1_old_value,week_num,year,old_commit_week,old_commit_year)
                        col=[updated_by,current_time,ori_part_number]
                        condb(sql,col)
                    else:
                        handling_null(demand_category,ori_part_number,commit1_week,commit1_year)
                        commit1_date_old_value=(row.commit1_date_old_value)
                        old_commit_year,old_commit_week,day=get_date_format(commit1_date_old_value)
                        old_commit_year=str(old_commit_year)[2:]
                        sql="""update {}.forecast_plan set gsa_advance_commit_kr=gsa_advance_commit_kr-({}),updated_by=%s,updated_at=%s,active=1,process_week={},process_year={}
                            where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit1_old_value,week_num,year,old_commit_week,old_commit_year)
                        col=[updated_by,current_time,ori_part_number]
                        condb(sql,col)
                        print("Date change occurred reducing the KR commit from previous date")
                        sql1="""update {}.forecast_plan set gsa_advance_commit_kr=gsa_advance_commit_kr+({}),updated_by=%s,updated_at=%s,active=1,process_week={},process_year={} 
                            where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit1,week_num,year,commit1_week,commit1_year)
                        col1=[updated_by,current_time,ori_part_number]
                        condb(sql1,col1)
                        print('updated a Workweek under gsa_commit1 for',ori_part_number,commit1_week,id)
                        sql2="""update {}.advance_commit set kr_processed=1 where id={}""".format(DB_NAME,id)
                        condb(sql2)
                
                if (commit2 is not None) and (commit2_date_old_value is None or commit2_date==commit2_date_old_value):
                    # print(commit2,commit2_week,commit2_year)
                    check="""select id from {}.forecast_plan where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit2_week,commit2_year)
                    col=[ori_part_number]
                    id_found=list(condb(check,col)) 
                    print(id_found,id)
                    if id_found==[]:
                        insert_records(ori_part_number,commit2_week,commit2_year,demand_category,commit2)
                        print('Added a Workweek under gsa_commit2 for',ori_part_number,commit2_week,commit2_year,id)
                        sql1="""update {}.advance_commit set kr_processed=1 where id={}""".format(DB_NAME,id)
                        condb(sql1)
                    else:
                        handling_null(demand_category,ori_part_number,commit2_week,commit2_year)
                        sql="""update {}.forecast_plan set gsa_advance_commit_kr=gsa_advance_commit_kr+({}-{}),updated_by=%s,updated_at=%s,active=1,process_week={},process_year={}
                        where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit2,commit2_old_value,week_num,year,commit2_week,commit2_year)
                        col=[updated_by,current_time,ori_part_number]
                        condb(sql,col)
                        print('updated a Workweek under gsa_commit2 for',ori_part_number,commit2_week,id)
                        sql1="""update {}.advance_commit set kr_processed=1 where id={}""".format(DB_NAME,id)
                        condb(sql1)
                else:
                    check="""select id from {}.forecast_plan where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit2_week,commit2_year)
                    col=[ori_part_number]
                    id_found=list(condb(check,col)) 
                    print(id_found,id)
                    if id_found==[]:
                        insert_records(ori_part_number,commit2_week,commit2_year,demand_category,commit2)
                        print('Added a Workweek under gsa_commit2 for',ori_part_number,commit2_week,commit2_year,id)
                        sql1="""update {}.advance_commit set kr_processed=1 where id={}""".format(DB_NAME,id)
                        condb(sql1)
                        commit2_date_old_value=(row.commit2_date_old_value)
                        old_commit_year,old_commit_week,day=get_date_format(commit2_date_old_value)
                        old_commit_year=str(old_commit_year)[2:]
                        sql="""update {}.forecast_plan set gsa_advance_commit_kr=gsa_advance_commit_kr-({}),updated_by=%s,updated_at=%s,active=1,process_week={},process_year={}
                            where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit2_old_value,week_num,year,old_commit_week,old_commit_year)
                        col=[updated_by,current_time,ori_part_number]
                        condb(sql,col)
                    else:
                        handling_null(demand_category,ori_part_number,commit2_week,commit2_year)
                        commit2_date_old_value=(row.commit2_date_old_value)
                        old_commit_year,old_commit_week,day=get_date_format(commit2_date_old_value)
                        old_commit_year=str(old_commit_year)[2:]
                        sql="""update {}.forecast_plan set gsa_advance_commit_kr=gsa_advance_commit_kr-({}),updated_by=%s,updated_at=%s,active=1,process_week={},process_year={}
                            where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit2_old_value,week_num,year,old_commit_week,old_commit_year)
                        col=[updated_by,current_time,ori_part_number]
                        condb(sql,col)
                        print("Date change occurred reducing the KR commit from previous date")
                        sql1="""update {}.forecast_plan set gsa_advance_commit_kr=gsa_advance_commit_kr+({}),updated_by=%s,updated_at=%s,active=1,process_week={},process_year={}
                            where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit2,week_num,year,commit2_week,commit2_year)
                        col1=[updated_by,current_time,ori_part_number]
                        condb(sql1,col1)
                        print('updated a Workweek under gsa_commit2 for',ori_part_number,commit2_week,id)
                        sql2="""update {}.advance_commit set kr_processed=1 where id={}""".format(DB_NAME,id)
                        condb(sql2)
            
                if (commit3 is not None)and(commit3_date_old_value is None or commit3_date==commit3_date_old_value):
                    print(commit3,commit3_week,commit3_year)
                    check="""select id from {}.forecast_plan where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit3_week,commit3_year)
                    col=[ori_part_number]
                    id_found=list(condb(check,col)) 
                    print(id_found,id)
                    if id_found==[]:
                        insert_records(ori_part_number,commit3_week,commit3_year,demand_category,commit3)
                        print('Added a Workweek under gsa_commit3 for',ori_part_number,commit3_week,commit3_year,id)
                        sql1="""update {}.advance_commit set kr_processed=1 where id={}""".format(DB_NAME,id)
                        condb(sql1)
                    else:
                        handling_null(demand_category,ori_part_number,commit3_week,commit3_year)
                        sql="""update {}.forecast_plan set gsa_advance_commit_kr=gsa_advance_commit_kr+({}-{}),updated_by=%s,updated_at=%s,active=1,process_week={},process_year={}
                        where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit3,commit3_old_value,week_num,year,commit3_week,commit3_year)
                        col=[updated_by,current_time,ori_part_number]
                        condb(sql,col)
                        print('updated a Workweek under gsa_commit3 for',ori_part_number,commit3_week,id)
                        sql1="""update {}.advance_commit set kr_processed=1 where id={}""".format(DB_NAME,id)
                        condb(sql1)
                else:
                    check="""select id from {}.forecast_plan where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit3_week,commit3_year)
                    col=[ori_part_number]
                    id_found=list(condb(check,col)) 
                    print(id_found,id)
                    if id_found==[]:
                        insert_records(ori_part_number,commit3_week,commit3_year,demand_category,commit3)
                        print('Added a Workweek under gsa_commit3 for',ori_part_number,commit3_week,commit3_year,id)
                        sql1="""update {}.advance_commit set kr_processed=1 where id={}""".format(DB_NAME,id)
                        condb(sql1)
                        commit3_date_old_value=(row.commit3_date_old_value)
                        old_commit_year,old_commit_week,day=get_date_format(commit3_date_old_value)
                        old_commit_year=str(old_commit_year)[2:]
                        sql="""update {}.forecast_plan set gsa_advance_commit_kr=gsa_advance_commit_kr-({}),updated_by=%s,updated_at=%s,active=1,process_week={},process_year={} 
                            where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit3_old_value,week_num,year,old_commit_week,old_commit_year)
                        col=[updated_by,current_time,ori_part_number]
                        condb(sql,col)
                    else:
                        handling_null(demand_category,ori_part_number,commit3_week,commit3_year)
                        commit3_date_old_value=(row.commit3_date_old_value)
                        old_commit_year,old_commit_week,day=get_date_format(commit3_date_old_value)
                        old_commit_year=str(old_commit_year)[2:]
                        sql="""update {}.forecast_plan set gsa_advance_commit_kr=gsa_advance_commit_kr-({}),updated_by=%s,updated_at=%s,active=1,process_week={},process_year={}
                            where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit3_old_value,week_num,year,old_commit_week,old_commit_year)
                        col=[updated_by,current_time,ori_part_number]
                        condb(sql,col)
                        print("Date change occurred reducing the KR commit from previous date")
                        sql1="""update {}.forecast_plan set gsa_advance_commit_kr=gsa_advance_commit_kr+({}),updated_by=%s,updated_at=%s,active=1,process_week={},process_year={}
                            where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit3,week_num,year,commit3_week,commit3_year)
                        col1=[updated_by,current_time,ori_part_number]
                        condb(sql1,col1)
                        print('updated a Workweek under gsa_commit3 for',ori_part_number,commit3_week,id)
                        sql2="""update {}.advance_commit set kr_processed=1 where id={}""".format(DB_NAME,id)
                        condb(sql2)
                
                if (commit4 is not None)and(commit4_date_old_value is None or commit4_date==commit4_date_old_value):
                    # print(commit4,commit4_week,commit4_year)
                    check="""select id from {}.forecast_plan where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit4_week,commit4_year)
                    col=[ori_part_number]
                    id_found=list(condb(check,col)) 
                    print(id_found,id)
                    if id_found==[]:
                        insert_records(ori_part_number,commit4_week,commit4_year,demand_category,commit4)
                        print('Added a Workweek under gsa_commit4 for',ori_part_number,commit4_week,commit4_year,id)
                        sql1="""update {}.advance_commit set kr_processed=1 where id={}""".format(DB_NAME,id)
                        condb(sql1)
                    else:
                        handling_null(demand_category,ori_part_number,commit4_week,commit4_year)
                        sql="""update {}.forecast_plan set gsa_advance_commit_kr=gsa_advance_commit_kr+({}-{}),updated_by=%s,updated_at=%s,active=1,process_week={},process_year={}
                        where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit4,commit4_old_value,week_num,year,commit4_week,commit4_year)
                        col=[updated_by,current_time,ori_part_number]
                        condb(sql,col)
                        print('updated a Workweek under gsa_commit4 for',ori_part_number,commit4_week,id)
                        sql1="""update {}.advance_commit set kr_processed=1 where id={}""".format(DB_NAME,id)
                        condb(sql1)
                else:
                    check="""select id from {}.forecast_plan where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit4_week,commit4_year)
                    col=[ori_part_number]
                    id_found=list(condb(check,col)) 
                    print(id_found,id)
                    if id_found==[]:
                        insert_records(ori_part_number,commit4_week,commit4_year,demand_category,commit4)
                        print('Added a Workweek under gsa_commit4 for',ori_part_number,commit4_week,commit4_year,id)
                        sql1="""update {}.advance_commit set kr_processed=1 where id={}""".format(DB_NAME,id)
                        condb(sql1)
                        commit4_date_old_value=(row.commit4_date_old_value)
                        old_commit_year,old_commit_week,day=get_date_format(commit4_date_old_value)
                        old_commit_year=str(old_commit_year)[2:]
                        sql="""update {}.forecast_plan set gsa_advance_commit_kr=gsa_advance_commit_kr-({}),updated_by=%s,updated_at=%s,active=1,process_week={},process_year={} 
                            where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit4_old_value,week_num,year,old_commit_week,old_commit_year)
                        col=[updated_by,current_time,ori_part_number]
                        condb(sql,col)
                    else:
                        handling_null(demand_category,ori_part_number,commit4_week,commit4_year)
                        commit4_date_old_value=(row.commit4_date_old_value)
                        old_commit_year,old_commit_week,day=get_date_format(commit4_date_old_value)
                        old_commit_year=str(old_commit_year)[2:]
                        sql="""update {}.forecast_plan set gsa_advance_commit_kr=gsa_advance_commit_kr-({}),updated_by=%s,updated_at=%s,active=1,process_week={},process_year={} 
                            where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit4_old_value,week_num,year,old_commit_week,old_commit_year)
                        col=[updated_by,current_time,ori_part_number]
                        condb(sql,col)
                        print("Date change occurred reducing the KR commit from previous date")
                        sql1="""update {}.forecast_plan set gsa_advance_commit_kr=gsa_advance_commit_kr+({}),updated_by=%s,updated_at=%s,active=1,process_week={},process_year={} 
                            where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit4,week_num,year,commit4_week,commit4_year)
                        col1=[updated_by,current_time,ori_part_number]
                        condb(sql1,col1)
                        print('updated a Workweek under gsa_commit4 for',ori_part_number,commit4_week,id)
                        sql2="""update {}.advance_commit set kr_processed=1 where id={}""".format(DB_NAME,id)
                        condb(sql2)
            except:
                print("GSA category record not found for particular ori_part_number",ori_part_number)
                logging.error(error_handling())

            
        if demand_category=='Non GSA':
            try:
                if (commit1 is not None)and(commit1_date_old_value is None or commit1_date==commit1_date_old_value):
                    print(commit1,commit1_week,commit1_year,ori_part_number,"for Non gsa category,id")
                    check="""select id from {}.forecast_plan where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit1_week,commit1_year)
                    col=[ori_part_number]
                    id_found=list(condb(check,col)) 
                    print(id_found,id)
                    if id_found==[]:
                        insert_records(ori_part_number,commit1_week,commit1_year,demand_category,commit1)
                        print('Added a Workweek under nongsa_commit1 for',ori_part_number,commit1_week,commit1_year,id)
                        sql1="""update {}.advance_commit set kr_processed=1 where id={}""".format(DB_NAME,id)
                        condb(sql1)
                    else:
                        handling_null(demand_category,ori_part_number,commit1_week,commit1_year)
                        sql="""update {}.forecast_plan set nongsa_advance_commit_kr=nongsa_advance_commit_kr+({}-{}),updated_by=%s,updated_at=%s,active=1,process_week={},process_year={} 
                        where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit1,commit1_old_value,week_num,year,commit1_week,commit1_year)
                        col=[updated_by,current_time,ori_part_number]
                        condb(sql,col)
                        print('updated a Workweek under nongsa_commit1 for',ori_part_number,commit1_week,id)
                        sql1="""update {}.advance_commit set kr_processed=1 where id={}""".format(DB_NAME,id)
                        condb(sql1)
                else:
                    check="""select id from {}.forecast_plan where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit1_week,commit1_year)
                    col=[ori_part_number]
                    id_found=list(condb(check,col)) 
                    print(id_found,id)
                    if id_found==[]:
                        insert_records(ori_part_number,commit1_week,commit1_year,demand_category,commit1)
                        print('Added a Workweek under nongsa_commit1 for',ori_part_number,commit1_week,commit1_year,id)
                        sql1="""update {}.advance_commit set kr_processed=1 where id={}""".format(DB_NAME,id)
                        condb(sql1)
                        commit1_date_old_value=(row.commit1_date_old_value)
                        old_commit_year,old_commit_week,day=get_date_format(commit1_date_old_value)
                        old_commit_year=str(old_commit_year)[2:]
                        sql="""update {}.forecast_plan set nongsa_advance_commit_kr=nongsa_advance_commit_kr-({}),updated_by=%s,updated_at=%s,active=1,process_week={},process_year={} 
                            where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit1_old_value,week_num,year,old_commit_week,old_commit_year)
                        col=[updated_by,current_time,ori_part_number]
                        condb(sql,col)
                    else:
                        handling_null(demand_category,ori_part_number,commit1_week,commit1_year)
                        commit1_date_old_value=(row.commit1_date_old_value)
                        old_commit_year,old_commit_week,day=get_date_format(commit1_date_old_value)
                        old_commit_year=str(old_commit_year)[2:]
                        sql="""update {}.forecast_plan set nongsa_advance_commit_kr=nongsa_advance_commit_kr-({}),updated_by=%s,updated_at=%s,active=1,process_week={},process_year={} 
                            where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit1_old_value,week_num,year,old_commit_week,old_commit_year)
                        col=[updated_by,current_time,ori_part_number]
                        condb(sql,col)
                        print("Date change occurred reducing the KR commit from previous date")
                        sql1="""update {}.forecast_plan set nongsa_advance_commit_kr=nongsa_advance_commit_kr+({}),updated_by=%s,updated_at=%s,active=1,process_week={},process_year={}  
                            where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit1,week_num,year,commit1_week,commit1_year)
                        col1=[updated_by,current_time,ori_part_number]
                        condb(sql1,col1)
                        print('updated a Workweek under nongsa_commit1 for',ori_part_number,commit1_week,id)
                        sql2="""update {}.advance_commit set kr_processed=1 where id={}""".format(DB_NAME,id)
                        condb(sql2)
                    
                if (commit2 is not None)and(commit2_date_old_value is None or commit2_date==commit2_date_old_value):
                    # print(commit2,commit2_week,commit2_year)
                    check="""select id from {}.forecast_plan where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit2_week,commit2_year)
                    col=[ori_part_number]
                    id_found=list(condb(check,col)) 
                    print(id_found,id)
                    if id_found==[]:
                        insert_records(ori_part_number,commit2_week,commit2_year,demand_category,commit2)
                        print('Added a Workweek under nongsa_commit2 for',ori_part_number,commit2_week,commit2_year,id)
                        sql1="""update {}.advance_commit set kr_processed=1 where id={}""".format(DB_NAME,id)
                        condb(sql1)
                    else:
                        handling_null(demand_category,ori_part_number,commit2_week,commit2_year)
                        sql="""update {}.forecast_plan set nongsa_advance_commit_kr=nongsa_advance_commit_kr+({}-{}),updated_by=%s,updated_at=%s,active=1,process_week={},process_year={} 
                        where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit2,commit2_old_value,week_num,year,commit2_week,commit2_year)
                        col=[updated_by,current_time,ori_part_number]
                        condb(sql,col)
                        print('updated a Workweek under nongsa_commit2 for',ori_part_number,commit2_week,id)
                        sql1="""update {}.advance_commit set kr_processed=1 where id={}""".format(DB_NAME,id)
                        condb(sql1)
                else:
                    check="""select id from {}.forecast_plan where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit2_week,commit2_year)
                    col=[ori_part_number]
                    id_found=list(condb(check,col)) 
                    print(id_found,id)
                    if id_found==[]:
                        insert_records(ori_part_number,commit2_week,commit2_year,demand_category,commit2)
                        print('Added a Workweek under nongsa_commit2 for',ori_part_number,commit2_week,commit2_year,id)
                        sql1="""update {}.advance_commit set kr_processed=1 where id={}""".format(DB_NAME,id)
                        condb(sql1)
                        commit2_date_old_value=(row.commit2_date_old_value)
                        old_commit_year,old_commit_week,day=get_date_format(commit2_date_old_value)
                        old_commit_year=str(old_commit_year)[2:]
                        sql="""update {}.forecast_plan set nongsa_advance_commit_kr=nongsa_advance_commit_kr-({}),updated_by=%s,updated_at=%s,active=1,process_week={},process_year={} 
                            where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit2_old_value,week_num,year,old_commit_week,old_commit_year)
                        col=[updated_by,current_time,ori_part_number]
                        condb(sql,col)
                    else:
                        handling_null(demand_category,ori_part_number,commit2_week,commit2_year)
                        commit2_date_old_value=(row.commit2_date_old_value)
                        old_commit_year,old_commit_week,day=get_date_format(commit2_date_old_value)
                        old_commit_year=str(old_commit_year)[2:]
                        sql="""update {}.forecast_plan set nongsa_advance_commit_kr=nongsa_advance_commit_kr-({}),updated_by=%s,updated_at=%s,active=1,process_week={},process_year={}
                            where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit2_old_value,week_num,year,old_commit_week,old_commit_year)
                        col=[updated_by,current_time,ori_part_number]
                        condb(sql,col)
                        print("Date change occurred reducing the KR commit from previous date")
                        sql1="""update {}.forecast_plan set nongsa_advance_commit_kr=nongsa_advance_commit_kr+({}),updated_by=%s,updated_at=%s,active=1,process_week={},process_year={} 
                            where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit2,week_num,year,commit2_week,commit2_year)
                        col1=[updated_by,current_time,ori_part_number]
                        condb(sql1,col1)
                        print('updated a Workweek under nongsa_commit2 for',ori_part_number,commit2_week,id)
                        sql2="""update {}.advance_commit set kr_processed=1 where id={}""".format(DB_NAME,id)
                        condb(sql2)
            
                if (commit3 is not None)and(commit3_date_old_value is None or commit3_date==commit3_date_old_value):
                    # print(commit3,commit3_week,commit3_year)
                    check="""select id from {}.forecast_plan where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit3_week,commit3_year)
                    col=[ori_part_number]
                    id_found=list(condb(check,col)) 
                    print(id_found,id)
                    if id_found==[]:
                        insert_records(ori_part_number,commit3_week,commit3_year,demand_category,commit3)
                        print('Added a Workweek under nongsa_commit3 for',ori_part_number,commit3_week,commit3_year,id)
                        sql1="""update {}.advance_commit set kr_processed=1 where id={}""".format(DB_NAME,id)
                        condb(sql1)
                    else:
                        handling_null(demand_category,ori_part_number,commit3_week,commit3_year)
                        sql="""update {}.forecast_plan set nongsa_advance_commit_kr=nongsa_advance_commit_kr+({}-{}),updated_by=%s,updated_at=%s,active=1,process_week={},process_year={} 
                        where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit3,commit3_old_value,week_num,year,commit3_week,commit3_year)
                        col=[updated_by,current_time,ori_part_number]
                        condb(sql,col)
                        print('updated a Workweek under nongsa_commit3 for',ori_part_number,commit3_week,id)
                        sql1="""update {}.advance_commit set kr_processed=1 where id={}""".format(DB_NAME,id)
                        condb(sql1)
                else:
                    check="""select id from {}.forecast_plan where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit3_week,commit3_year)
                    col=[ori_part_number]
                    id_found=list(condb(check,col)) 
                    print(id_found,id)
                    if id_found==[]:
                        insert_records(ori_part_number,commit3_week,commit3_year,demand_category,commit3)
                        print('Added a Workweek under nongsa_commit3 for',ori_part_number,commit3_week,commit3_year,id)
                        sql1="""update {}.advance_commit set kr_processed=1 where id={}""".format(DB_NAME,id)
                        condb(sql1)
                        commit3_date_old_value=(row.commit3_date_old_value)
                        old_commit_year,old_commit_week,day=get_date_format(commit3_date_old_value)
                        old_commit_year=str(old_commit_year)[2:]
                        sql="""update {}.forecast_plan set nongsa_advance_commit_kr=nongsa_advance_commit_kr-({}),updated_by=%s,updated_at=%s,active=1,process_week={},process_year={} 
                            where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit3_old_value,week_num,year,old_commit_week,old_commit_year)
                        col=[updated_by,current_time,ori_part_number]
                        condb(sql,col)
                    else:
                        handling_null(demand_category,ori_part_number,commit3_week,commit3_year)
                        commit3_date_old_value=(row.commit3_date_old_value)
                        old_commit_year,old_commit_week,day=get_date_format(commit3_date_old_value)
                        old_commit_year=str(old_commit_year)[2:]
                        sql="""update {}.forecast_plan set nongsa_advance_commit_kr=nongsa_advance_commit_kr-({}),updated_by=%s,updated_at=%s,active=1,process_week={},process_year={} 
                            where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit3_old_value,week_num,year,old_commit_week,old_commit_year)
                        col=[updated_by,current_time,ori_part_number]
                        condb(sql,col)
                        print("Date change occurred reducing the KR commit from previous date")
                        sql1="""update {}.forecast_plan set nongsa_advance_commit_kr=nongsa_advance_commit_kr+({}),updated_by=%s,updated_at=%s,active=1,process_week={},process_year={} 
                            where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit3,week_num,year,commit3_week,commit3_year)
                        col1=[updated_by,current_time,ori_part_number]
                        condb(sql1,col1)
                        print('updated a Workweek under nongsa_commit3 for',ori_part_number,commit3_week,id)
                        sql2="""update {}.advance_commit set kr_processed=1 where id={}""".format(DB_NAME,id)
                        condb(sql2)
                
                if (commit4 is not None)and(commit4_date_old_value is None or commit4_date==commit4_date_old_value):
                    # print(commit4,commit4_week,commit4_year)
                    check="""select id from {}.forecast_plan where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit4_week,commit4_year)
                    col=[ori_part_number]
                    id_found=list(condb(check,col)) 
                    print(id_found,id)
                    if id_found==[]:
                        insert_records(ori_part_number,commit4_week,commit4_year,demand_category,commit4)
                        print('Added a Workweek under nongsa_commit4 for',ori_part_number,commit4_week,commit4_year,id)
                        sql1="""update {}.advance_commit set kr_processed=1 where id={}""".format(DB_NAME,id)
                        condb(sql1)
                    else:
                        handling_null(demand_category,ori_part_number,commit4_week,commit4_year)
                        sql="""update {}.forecast_plan set nongsa_advance_commit_kr=nongsa_advance_commit_kr+({}-{}),updated_by=%s,updated_at=%s,active=1,process_week={},process_year={} 
                        where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit4,commit4_old_value,week_num,year,commit4_week,commit4_year)
                        col=[updated_by,current_time,ori_part_number]
                        condb(sql,col)
                        print('updated a Workweek under nongsa_commit4 for',ori_part_number,commit4_week,id)
                        sql1="""update {}.advance_commit set kr_processed=1 where id={}""".format(DB_NAME,id)
                        condb(sql1)
                else:
                    check="""select id from {}.forecast_plan where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit4_week,commit4_year)
                    col=[ori_part_number]
                    id_found=list(condb(check,col)) 
                    print(id_found,id)
                    if id_found==[]:
                        insert_records(ori_part_number,commit4_week,commit4_year,demand_category,commit4)
                        print('Added a Workweek under nongsa_commit4 for',ori_part_number,commit4_week,commit4_year,id)
                        sql1="""update {}.advance_commit set kr_processed=1 where id={}""".format(DB_NAME,id)
                        condb(sql1)
                        commit4_date_old_value=(row.commit4_date_old_value)
                        old_commit_year,old_commit_week,day=get_date_format(commit4_date_old_value)
                        old_commit_year=str(old_commit_year)[2:]
                        sql="""update {}.forecast_plan set nongsa_advance_commit_kr=nongsa_advance_commit_kr-({}),updated_by=%s,updated_at=%s,active=1,process_week={},process_year={}
                            where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit4_old_value,week_num,year,old_commit_week,old_commit_year)
                        col=[updated_by,current_time,ori_part_number]
                        condb(sql,col)
                    else:
                        handling_null(demand_category,ori_part_number,commit4_week,commit4_year)
                        commit4_date_old_value=(row.commit4_date_old_value)
                        old_commit_year,old_commit_week,day=get_date_format(commit4_date_old_value)
                        old_commit_year=str(old_commit_year)[2:]
                        sql="""update {}.forecast_plan set nongsa_advance_commit_kr=nongsa_advance_commit_kr-({}),updated_by=%s,updated_at=%s,active=1,process_week={},process_year={} 
                            where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit4_old_value,week_num,year,old_commit_week,old_commit_year)
                        col=[updated_by,current_time,ori_part_number]
                        condb(sql,col)
                        print("Date change occurred reducing the KR commit from previous date")
                        sql1="""update {}.forecast_plan set nongsa_advance_commit_kr=nongsa_advance_commit_kr+({}),updated_by=%s,updated_at=%s,active=1,process_week={},process_year={} 
                            where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit4,week_num,year,commit4_week,commit4_year)
                        col1=[updated_by,current_time,ori_part_number]
                        condb(sql1,col1)
                        print('updated a Workweek under nongsa_commit4 for',ori_part_number,commit4_week,id)
                        sql2="""update {}.advance_commit set kr_processed=1 where id={}""".format(DB_NAME,id)
                        condb(sql2)
                    
            except:
                print("Non gsa record not found for particular ori_part_number",ori_part_number)
                logging.error(error_handling())


        if demand_category=='System':
            try:
                if (commit1 is not None)and(commit1_date_old_value is None or commit1_date==commit1_date_old_value):
                    # print(commit1,commit1_week,commit1_year,ori_part_number)
                    check="""select id from {}.forecast_plan where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit1_week,commit1_year)
                    col=[ori_part_number]
                    id_found=list(condb(check,col)) 
                    print(id_found,id)
                    if id_found==[]:
                        insert_records(ori_part_number,commit1_week,commit1_year,demand_category,commit1)
                        print('Added a Workweek under system_commit1 for',ori_part_number,commit1_week,commit1_year,id)
                        sql1="""update {}.advance_commit set kr_processed=1 where id={}""".format(DB_NAME,id)
                        condb(sql1)
                    else:
                        handling_null(demand_category,ori_part_number,commit1_week,commit1_year)
                        sql="""update {}.forecast_plan set system_advance_commit_kr=system_advance_commit_kr+({}-{}),updated_by=%s,updated_at=%s,active=1,process_week={},process_year={} 
                        where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit1,commit1_old_value,week_num,year,commit1_week,commit1_year)
                        col=[updated_by,current_time,ori_part_number]
                        condb(sql,col)
                        print(commit1,commit1_old_value)
                        print('updated a Workweek under system_commit1 for',ori_part_number,commit1_week,id)
                        sql1="""update {}.advance_commit set kr_processed=1 where id={}""".format(DB_NAME,id)
                        condb(sql1)
                else:
                    check="""select id from {}.forecast_plan where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit1_week,commit1_year)
                    col=[ori_part_number]
                    id_found=list(condb(check,col)) 
                    print(id_found,id)
                    if id_found==[]:
                        insert_records(ori_part_number,commit1_week,commit1_year,demand_category,commit1)
                        print('Added a Workweek under system_commit1 for',ori_part_number,commit1_week,commit1_year,id)
                        sql1="""update {}.advance_commit set kr_processed=1 where id={}""".format(DB_NAME,id)
                        condb(sql1)
                        commit1_date_old_value=(row.commit1_date_old_value)
                        old_commit_year,old_commit_week,day=get_date_format(commit1_date_old_value)
                        old_commit_year=str(old_commit_year)[2:]
                        sql="""update {}.forecast_plan set system_advance_commit_kr=system_advance_commit_kr-({}),updated_by=%s,updated_at=%s,active=1,process_week={},process_year={} 
                            where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit1_old_value,week_num,year,old_commit_week,old_commit_year)
                        col=[updated_by,current_time,ori_part_number]
                        condb(sql,col)
                    else:
                        handling_null(demand_category,ori_part_number,commit1_week,commit1_year)
                        commit1_date_old_value=(row.commit1_date_old_value)
                        old_commit_year,old_commit_week,day=get_date_format(commit1_date_old_value)
                        old_commit_year=str(old_commit_year)[2:]
                        sql="""update {}.forecast_plan set system_advance_commit_kr=system_advance_commit_kr-({}),updated_by=%s,updated_at=%s,active=1,process_week={},process_year={} 
                            where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit1_old_value,week_num,year,old_commit_week,old_commit_year)
                        col=[updated_by,current_time,ori_part_number]
                        condb(sql,col)
                        print("Date change occurred reducing the KR commit from previous date")
                        sql1="""update {}.forecast_plan set system_advance_commit_kr=system_advance_commit_kr+({}),updated_by=%s,updated_at=%s,active=1,process_week={},process_year={} 
                            where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit1,week_num,year,commit1_week,commit1_year)
                        col1=[updated_by,current_time,ori_part_number]
                        condb(sql1,col1)
                        print('updated a Workweek under system_commit1 for',ori_part_number,commit1_week,id)
                        sql2="""update {}.advance_commit set kr_processed=1 where id={}""".format(DB_NAME,id)
                        condb(sql2)
                    
                if (commit2 is not None)and(commit2_date_old_value is None or commit2_date==commit2_date_old_value):
                    # print(commit2,commit2_week,commit2_year)
                    check="""select id from {}.forecast_plan where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit2_week,commit2_year)
                    col=[ori_part_number]
                    id_found=list(condb(check,col)) 
                    print(id_found,id)
                    if id_found==[]:
                        insert_records(ori_part_number,commit2_week,commit2_year,demand_category,commit2)
                        print('Added a Workweek under system_commit2 for',ori_part_number,commit2_week,commit2_year,id)
                        sql1="""update {}.advance_commit set kr_processed=1 where id={}""".format(DB_NAME,id)
                        condb(sql1)
                    else:
                        handling_null(demand_category,ori_part_number,commit2_week,commit2_year)
                        sql="""update {}.forecast_plan set system_advance_commit_kr=system_advance_commit_kr+({}-{}),updated_by=%s,updated_at=%s,active=1,process_week={},process_year={}  
                        where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit2,commit2_old_value,week_num,year,commit2_week,commit2_year)
                        col=[updated_by,current_time,ori_part_number]
                        condb(sql,col)
                        print(commit2,commit2_old_value)
                        print('updated a Workweek under system_commit2 for',ori_part_number,commit2_week,id)
                        sql1="""update {}.advance_commit set kr_processed=1 where id={}""".format(DB_NAME,id)
                        condb(sql1)
                else:
                    check="""select id from {}.forecast_plan where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit2_week,commit2_year)
                    col=[ori_part_number]
                    id_found=list(condb(check,col)) 
                    print(id_found,id)
                    if id_found==[]:
                        insert_records(ori_part_number,commit2_week,commit2_year,demand_category,commit2)
                        print('Added a Workweek under system_commit2 for',ori_part_number,commit2_week,commit2_year,id)
                        sql1="""update {}.advance_commit set kr_processed=1 where id={}""".format(DB_NAME,id)
                        condb(sql1)
                        commit2_date_old_value=(row.commit2_date_old_value)
                        old_commit_year,old_commit_week,day=get_date_format(commit2_date_old_value)
                        old_commit_year=str(old_commit_year)[2:]
                        sql="""update {}.forecast_plan set system_advance_commit_kr=system_advance_commit_kr-({}),updated_by=%s,updated_at=%s,active=1,process_week={},process_year={}  
                            where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit2_old_value,week_num,year,old_commit_week,old_commit_year)
                        col=[updated_by,current_time,ori_part_number]
                        condb(sql,col)
                    else:
                        handling_null(demand_category,ori_part_number,commit2_week,commit2_year)
                        commit2_date_old_value=(row.commit2_date_old_value)
                        old_commit_year,old_commit_week,day=get_date_format(commit2_date_old_value)
                        old_commit_year=str(old_commit_year)[2:]
                        sql="""update {}.forecast_plan set system_advance_commit_kr=system_advance_commit_kr-({}),updated_by=%s,updated_at=%s,active=1,process_week={},process_year={}  
                            where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit2_old_value,week_num,year,old_commit_week,old_commit_year)
                        col=[updated_by,current_time,ori_part_number]
                        condb(sql,col)
                        print("Date change occurred reducing the KR commit from previous date")
                        sql1="""update {}.forecast_plan set system_advance_commit_kr=system_advance_commit_kr+({}),updated_by=%s,updated_at=%s,active=1,process_week={},process_year={}  
                            where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit2,week_num,year,commit2_week,commit2_year)
                        col1=[updated_by,current_time,ori_part_number]
                        condb(sql1,col1)
                        print('updated a Workweek under system_commit2 for',ori_part_number,commit2_week,id)
                        sql2="""update {}.advance_commit set kr_processed=1 where id={}""".format(DB_NAME,id)
                        condb(sql2)
            
                if (commit3 is not None)and(commit3_date_old_value is None or commit3_date==commit3_date_old_value):
                    # print(commit3,commit3_week,commit3_year)
                    check="""select id from {}.forecast_plan where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit3_week,commit3_year)
                    col=[ori_part_number]
                    id_found=list(condb(check,col)) 
                    print(id_found,id)
                    if id_found==[]:
                        insert_records(ori_part_number,commit3_week,commit3_year,demand_category,commit3)
                        print('Added a Workweek under system_commit2 for',ori_part_number,commit3_week,commit3_year,id)
                        sql1="""update {}.advance_commit set kr_processed=1 where id={}""".format(DB_NAME,id)
                        condb(sql1)
                    else:
                        handling_null(demand_category,ori_part_number,commit3_week,commit3_year)
                        sql="""update {}.forecast_plan set system_advance_commit_kr=system_advance_commit_kr+({}-{}),updated_by=%s,updated_at=%s,active=1,process_week={},process_year={} 
                        where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit3,commit3_old_value,week_num,year,commit3_week,commit3_year)
                        col=[updated_by,current_time,ori_part_number]
                        print(commit3,commit3_old_value)
                        condb(sql,col)
                        print('updated a Workweek under system_commit2 for',ori_part_number,commit3_week,id)
                        sql1="""update {}.advance_commit set kr_processed=1 where id={}""".format(DB_NAME,id)
                        condb(sql1)
                else:
                    check="""select id from {}.forecast_plan where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit3_week,commit3_year)
                    col=[ori_part_number]
                    id_found=list(condb(check,col)) 
                    print(id_found,id)
                    if id_found==[]:
                        insert_records(ori_part_number,commit3_week,commit3_year,demand_category,commit3)
                        print('Added a Workweek under system_commit2 for',ori_part_number,commit3_week,commit3_year,id)
                        sql1="""update {}.advance_commit set kr_processed=1 where id={}""".format(DB_NAME,id)
                        condb(sql1)
                        commit3_date_old_value=(row.commit3_date_old_value)
                        old_commit_year,old_commit_week,day=get_date_format(commit3_date_old_value)
                        old_commit_year=str(old_commit_year)[2:]
                        sql="""update {}.forecast_plan set system_advance_commit_kr=system_advance_commit_kr-({}),updated_by=%s,updated_at=%s,active=1,process_week={},process_year={} 
                            where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit3_old_value,week_num,year,old_commit_week,old_commit_year)
                        col=[updated_by,current_time,ori_part_number]
                        condb(sql,col)
                    else:
                        handling_null(demand_category,ori_part_number,commit3_week,commit3_year)
                        commit3_date_old_value=(row.commit3_date_old_value)
                        old_commit_year,old_commit_week,day=get_date_format(commit3_date_old_value)
                        old_commit_year=str(old_commit_year)[2:]
                        sql="""update {}.forecast_plan set system_advance_commit_kr=system_advance_commit_kr-({}),updated_by=%s,updated_at=%s,active=1,process_week={},process_year={}  
                            where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit3_old_value,week_num,year,old_commit_week,old_commit_year)
                        col=[updated_by,current_time,ori_part_number]
                        condb(sql,col)
                        print("Date change occurred reducing the KR commit from previous date")
                        sql1="""update {}.forecast_plan set system_advance_commit_kr=system_advance_commit_kr+({}),updated_by=%s,updated_at=%s,active=1,process_week={},process_year={}  
                            where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit3,week_num,year,commit3_week,commit3_year)
                        col1=[updated_by,current_time,ori_part_number]
                        condb(sql1,col1)
                        print('updated a Workweek under system_commit3 for',ori_part_number,commit3_week,id)
                        sql2="""update {}.advance_commit set kr_processed=1 where id={}""".format(DB_NAME,id)
                        condb(sql2)
            
                if (commit4 is not None)and(commit4_date_old_value is None or commit4_date==commit4_date_old_value):
                    # print(commit4,commit4_week,commit4_year)
                    check="""select id from {}.forecast_plan where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit4_week,commit4_year)
                    col=[ori_part_number]
                    id_found=list(condb(check,col))
                    print(id_found,id)
                    if id_found==[]:
                        insert_records(ori_part_number,commit4_week,commit4_year,demand_category,commit4)
                        print('Added a Workweek under system_commit4 for',ori_part_number,commit4_week,commit4_year,id)
                        sql1="""update {}.advance_commit set kr_processed=1 where id={}""".format(DB_NAME,id)
                        condb(sql1)
                    else:
                        handling_null(demand_category,ori_part_number,commit4_week,commit4_year)
                        sql="""update {}.forecast_plan set system_advance_commit_kr=system_advance_commit_kr+({}-{}),updated_by=%s,updated_at=%s,active=1,process_week={},process_year={}  
                        where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit4,commit4_old_value,week_num,year,commit4_week,commit4_year)
                        col=[updated_by,current_time,ori_part_number]
                        print(commit4,commit4_old_value)
                        condb(sql,col)
                        print('updated a Workweek under system_commit4 for',ori_part_number,commit4_week,id)
                        sql1="""update {}.advance_commit set kr_processed=1 where id={}""".format(DB_NAME,id)
                        condb(sql1)
                else:
                    check="""select id from {}.forecast_plan where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit4_week,commit4_year)
                    col=[ori_part_number]
                    id_found=list(condb(check,col)) 
                    print(id_found,id)
                    if id_found==[]:
                        insert_records(ori_part_number,commit4_week,commit4_year,demand_category,commit4)
                        print('Added a Workweek under system_commit4 for',ori_part_number,commit4_week,commit4_year,id)
                        sql1="""update {}.advance_commit set kr_processed=1 where id={}""".format(DB_NAME,id)
                        condb(sql1)
                        commit4_date_old_value=(row.commit4_date_old_value)
                        old_commit_year,old_commit_week,day=get_date_format(commit4_date_old_value)
                        old_commit_year=str(old_commit_year)[2:]
                        sql="""update {}.forecast_plan set system_advance_commit_kr=system_advance_commit_kr-({}),updated_by=%s,updated_at=%s,active=1,process_week={},process_year={}  
                            where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit4_old_value,week_num,year,old_commit_week,old_commit_year)
                        col=[updated_by,current_time,ori_part_number]
                        condb(sql,col)
                    else:
                        handling_null(demand_category,ori_part_number,commit4_week,commit4_year)
                        commit4_date_old_value=(row.commit4_date_old_value)
                        old_commit_year,old_commit_week,day=get_date_format(commit4_date_old_value)
                        old_commit_year=str(old_commit_year)[2:]
                        sql="""update {}.forecast_plan set system_advance_commit_kr=system_advance_commit_kr-({}),updated_by=%s,updated_at=%s,active=1,process_week={},process_year={}  
                            where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit4_old_value,week_num,year,old_commit_week,old_commit_year)
                        col=[updated_by,current_time,ori_part_number]
                        condb(sql,col)
                        print("Date change occurred reducing the KR commit from previous date")
                        sql1="""update {}.forecast_plan set system_advance_commit_kr=system_advance_commit_kr+({}),updated_by=%s,updated_at=%s,active=1,process_week={},process_year={}  
                            where ori_part_number=%s and prod_week={} and prod_year={}""".format(DB_NAME,commit4,week_num,year,commit4_week,commit4_year)
                        col1=[updated_by,current_time,ori_part_number]
                        condb(sql1,col1)
                        print('updated a Workweek under system_commit4 for',ori_part_number,commit4_week,id)
                        sql2="""update {}.advance_commit set kr_processed=1 where id={}""".format(DB_NAME,id)
                        condb(sql2)
            except:
                print("System record not found for particular ori_part_number",ori_part_number)
                logging.error(error_handling())
        
    return "Success"

