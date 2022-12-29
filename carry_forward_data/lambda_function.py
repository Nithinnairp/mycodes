###########################
## carry forward snippet ##
###########################


import sys
import json
import boto3
import pymysql
import pandas as pd
import logging
import datetime
from DB_conn import condb,condb_dict
import os

my_date=datetime.datetime.today()
year,week_num,day_of_week=my_date.isocalendar()
print(week_num)
complete_current_year=str(year)
current_year=complete_current_year[2:]
DB_NAME = os.environ['DB_NAME']
S3_BUCKET = os.environ['S3_BUCKET']

def lambda_handler(event, context):
    print(week_num)
    def error_handling():
        return '{},{} on Line:{}'.format(sys.exc_info()[0],sys.exc_info()[1],sys.exc_info()[2].tb_lineno)
    
    def get_all_suppliers():
        get_suppliers_query="""select source_supplier_id,source_supplier from {db_name}.forecast_plan group by source_supplier;""".format(db_name=DB_NAME)
        suppliers_list=condb_dict(get_suppliers_query)
        return suppliers_list
    
    get_advance_col_names="""SELECT column_name from information_schema.columns where table_schema='{}' and table_name='advance_commit';""".format(DB_NAME)
    advance_col_names=list(condb(get_advance_col_names))
    advance_db_columns=list(sum(advance_col_names,()))
    # print(advance_db_columns)
    prev_week=53 if week_num==1 else week_num-1
    prev_year=year-1 if week_num==1 else complete_current_year
    print(prev_week,prev_year)
    #### FIX ME ####
    advance_commit_query="""select * from {}.advance_commit where process_week={} and process_year={};""".format(DB_NAME,prev_week,prev_year)
    df=condb(advance_commit_query)
    advance_commit_data=pd.DataFrame(data=df,columns=advance_db_columns)
    advance_commit_data=advance_commit_data.fillna({'commit1':0,'commit2':0,'commit3':0,'commit4':0,'kr_processed':0,
      'commit1_week':0,'commit2_week':0,'commit3_week':0,'commit4_week':0,
      'commit1_year':2000,'commit2_year':2000,'commit3_year':2000,'commit4_year':2000})
      
    get_dmp_week="""select distinct prod_week,prod_year from {}.forecast_plan where dmp_orndmp='DMP' order by prod_week""".format(DB_NAME)
    week_year=condb(get_dmp_week)
    df=pd.DataFrame(week_year,columns=['prod_week','prod_year'])
    dmp_week=[]
    dmp_year=[]
    for x,y in df.iterrows():
        dmp_week.append(int(y.prod_week))
        dmp_year.append(int(y.prod_year))
    dmp_year=sorted(list(set(dmp_year)))
    
    print(dmp_week,dmp_year)
    s3_client = boto3.client('s3')
    response=s3_client.get_object(Bucket='rapid-response-prod',Key='last_week_dmp_period/last_dmp_period.json')
    content=response['Body'].read()
    json_file=json.loads(content)
    print(type(json_file),json_file,"json file")
    last_dmp_period=json_file["last_dmp_period"]
    last_dmp_year=json_file["last_dmp_year"]
    print(last_dmp_period,last_dmp_year)
    for index,row in advance_commit_data.iterrows():
        id=row.id
        prod_week=int(row.prod_week)
        prod_year=int(row.prod_year)
        ori_part_number=str(row.ori_part_number)
        demand_category=row.demand_category.lower().replace(' ','')
        commit1=int(row.commit1)
        commit2=int(row.commit2)
        commit3=int(row.commit3)
        commit4=int(row.commit4)
        commit1_week=int(row.commit1_week)        
        commit1_year=str(row.commit1_year)
        commit1_year=int(float(commit1_year[2:]))
        commit2_week=int(row.commit2_week)
        commit2_year=str(row.commit2_year)
        commit2_year=int(float(commit2_year[2:]))
        commit3_week=int(row.commit3_week)
        commit3_year=str(row.commit3_year)
        commit3_year=int(float(commit3_year[2:]))
        commit4_week=int(row.commit4_week)
        commit4_year=str(row.commit4_year)
        commit4_year=int(float(commit4_year[2:]))
        commit1_accepted=ord(row.commit1_accepted) if row.commit1_accepted is not None else None;
        commit2_accepted=ord(row.commit2_accepted) if row.commit2_accepted is not None else None;
        commit3_accepted=ord(row.commit3_accepted) if row.commit3_accepted is not None else None;
        commit4_accepted=ord(row.commit4_accepted) if row.commit4_accepted is not None else None;
        print(id,ori_part_number,demand_category,commit1,commit1_week,commit1_year,commit2,commit2_week,commit2_year,commit3,commit3_week,commit3_year,commit4,commit4_week,commit4_year)
        
        if (commit1!=0 or commit1 is not None) and commit1_accepted==0:
            sql="""update {db_name}.forecast_plan set {category}_advance_commit_kr=({category}_advance_commit_kr-({commit1})) where ori_part_number=%s and prod_week={week} and prod_year={year};""".format(db_name=DB_NAME,category=demand_category,commit1=commit1,week=commit1_week,year=commit1_year)
            col=[ori_part_number]
            condb(sql,col)
            print('advance_commit is reduced from',ori_part_number,'for',demand_category,'by quantity',commit1,'for week and month',commit1_week,commit1_year)
            sql1="""update {db_name}.advance_commit set commit1=null,commit1_week=null,commit1_year=null,commit1_date=null,commit1_old_value=null where id={id}""".format(db_name=DB_NAME,id=id)
            condb(sql1)
            
        if commit1_accepted==1:
            print("carrying forward this record for id",row.id)
            sql="""update {db_name}.advance_commit set active=1 where id={id}""".format(db_name=DB_NAME,id=id)
            condb(sql)
            sql1="""update {db_name}.forecast_plan set carry_forward=1,active=1 where ori_part_number=%s and prod_week={week} and prod_year={year}""".format(db_name=DB_NAME,week=prod_week,year=prod_year)
            col=[ori_part_number]
            condb(sql1,col)
            
        if (commit2!=0 or commit2 is not None) and commit2_accepted==0:
            sql="""update {db_name}.forecast_plan set {category}_advance_commit_kr=({category}_advance_commit_kr-({commit2})) where ori_part_number=%s and prod_week={week} and prod_year={year};""".format(db_name=DB_NAME,category=demand_category,commit2=commit2,week=commit2_week,year=commit2_year)
            col=[ori_part_number]
            condb(sql,col)
            print('advance_commit is reduced from',ori_part_number,'for',demand_category,'by quantity',commit2,'for week and month',commit2_week,commit2_year)
            sql="""update {db_name}.advance_commit set commit2=null,commit2_week=null,commit2_year=null,commit2_date=null,commit2_old_value=null where id={id}""".format(db_name=DB_NAME,id=id)
            condb(sql) 
            
        if commit2_accepted==1:
            print("carrying forward this record for id",row.id)
            sql="""update {db_name}.advance_commit set active=1 where id={id}""".format(db_name=DB_NAME,id=id)
            condb(sql)
            sql1="""update {db_name}.forecast_plan set carry_forward=1,active=1 where ori_part_number=%s and prod_week={week} and prod_year={year}""".format(db_name=DB_NAME,week=prod_week,year=prod_year)
            col=[ori_part_number]
            condb(sql1,col)

        if (commit3!=0 or commit3 is not None) and commit3_accepted==0:
            sql="""update {db_name}.forecast_plan set {category}_advance_commit_kr=({category}_advance_commit_kr-({commit3})) where ori_part_number=%s and prod_week={week} and prod_year={year}""".format(db_name=DB_NAME,category=demand_category,commit3=commit3,week=commit3_week,year=commit3_year)
            col=[ori_part_number]
            condb(sql,col)
            print('advance_commit is reduced from',ori_part_number,'for',demand_category,'by quantity',commit3,'for week and month',commit3_week,commit3_year)
            sql="""update {db_name}.advance_commit set commit3=null,commit3_week=null,commit3_year=null,commit3_date=null,commit3_old_value=null where id={id}""".format(db_name=DB_NAME,id=id)
            condb(sql) 
            
        if commit3_accepted==1:
            print("carrying forward this record for id",row.id)
            sql="""update {db_name}.advance_commit set active=1 where id={id}""".format(db_name=DB_NAME,id=id)
            condb(sql) 
            sql1="""update {db_name}.forecast_plan set carry_forward=1,active=1 where ori_part_number=%s and prod_week={week} and prod_year={year}""".format(db_name=DB_NAME,week=prod_week,year=prod_year)
            col=[ori_part_number]
            condb(sql1,col)
            
        if (commit4!=0 or commit4 is not None) and commit4_accepted==0:
            sql="""update {db_name}.forecast_plan set {category}_advance_commit_kr=({category}_advance_commit_kr-({commit4})) where ori_part_number=%s and prod_week={week} and prod_year={year}""".format(db_name=DB_NAME,category=demand_category,commit4=commit4,week=commit4_week,year=commit4_year)
            col=[ori_part_number]
            condb(sql,col)
            print('advance_commit is reduced from',ori_part_number,'for',demand_category,'by quantity',commit4,'for week and month',commit4_week,commit4_year)
            sql="""update {db_name}.advance_commit set commit4=null,commit4_week=null,commit4_year=null,commit4_date=null,commit4_old_value=null where id={id}""".format(db_name=DB_NAME,id=id)
            condb(sql) 
        if commit4_accepted==1:
            print("carrying forward this record for id",row.id)
            sql="""update {db_name}.advance_commit set active=1 where id={id}""".format(db_name=DB_NAME,id=id)
            condb(sql)
            sql1="""update {db_name}.forecast_plan set carry_forward=1,active=1 where ori_part_number=%s and prod_week={week} and prod_year={year}""".format(db_name=DB_NAME,week=prod_week,year=prod_year)
            col=[ori_part_number]
            condb(sql1,col)
        
    
    removing_the_NDMP_commits="""update {db_name}.forecast_plan
                    set
                    nongsa_advance_commit_kr=0,nongsa_advance_commit_irp=0,cumulative_nongsa_advance_commit_irp=0,nongsa_cause_code_remark=null,nongsa_cause_code=null,
                    gsa_advance_commit_kr=0,gsa_advance_commit_irp=0,cumulative_gsa_advance_commit_irp=0,gsa_cause_code_remark=null,gsa_cause_code=null,
                    forecast_advance_commit_kr=0,forecast_advance_commit_irp=0,cumulative_forecast_advance_commit_irp=0,forecast_cause_code_remark=null,forecast_cause_code=null,
                    system_advance_commit_kr=0,system_advance_commit_irp=0,cumulative_system_advance_commit_irp=0,system_cause_code_remark=null,system_cause_code=null,
                    nongsa_commit=0,gsa_commit=0,forecast_commit=0,system_commit=0,
                    nongsa_supply=0,gsa_supply=0,forecast_supply=0,system_supply=0,
                    total_commit=0,total_supply=0
                    where ((prod_week>{dmp_period} and prod_year>={dmp_year}) or (prod_week<={dmp_period} and prod_year>{dmp_year}));""".format(db_name=DB_NAME,dmp_period=last_dmp_period[-1],dmp_year=last_dmp_year[-1])
    condb(removing_the_NDMP_commits) 
    
    removing_the_all_zero_records="""update {db_name}.forecast_plan set active=0
                where
                ifnull(forecast_original,0)=0 and ifnull(forecast_adj,0)=0 and ifnull(forecast_supply,0)=0 and ifnull(forecast_advance_commit_irp,0)=0 and 
                ifnull(cumulative_forecast_advance_commit_irp,0)=0 and
                ifnull(system_original,0)=0 and ifnull(system_adj,0)=0 and ifnull(system_supply,0)=0 and ifnull(system_advance_commit_irp,0)=0 and 
                ifnull(cumulative_system_advance_commit_irp,0)=0 and 
                ifnull(gsa_original,0)=0 and ifnull(gsa_adj,0)=0 and ifnull(gsa_supply,0)=0 and ifnull(gsa_advance_commit_irp,0)=0 and 
                ifnull(cumulative_gsa_advance_commit_irp,0)=0 and 
                ifnull(nongsa_original,0)=0 and ifnull(nongsa_adj,0)=0 and ifnull(nongsa_supply,0)=0 and ifnull(nongsa_advance_commit_irp,0)=0 and 
                ifnull(cumulative_nongsa_advance_commit_irp,0)=0 and 
                ifnull(buffer,0)=0 and ifnull(buffer_opt_adj,0)=0 and ifnull(past_due_open_po,0)=0 and ifnull(current_open_po,0)=0 and
                ifnull(on_hand_nettable_si,0)=0 and ifnull(on_hand_total_cm,0)=0 and ifnull(received_qty,0)=0;""".format(db_name=DB_NAME)
    condb(removing_the_all_zero_records)
    
    moving_the_invalid_records="""insert into {db_name}.forecast_plan_inactive_records select * from {db_name}.forecast_plan where active=0""".format(db_name=DB_NAME)
    condb(moving_the_invalid_records)
    print("moved to forecast_plan_inactive_records table")
    
    deleting_inactive_records="""delete from {db_name}.forecast_plan where active=0""".format(db_name=DB_NAME)
    condb(deleting_inactive_records)
    print("deleting inactive records from forecast_plan")
    
    print(last_dmp_period[-1],last_dmp_year[-1],type(last_dmp_period))           
    s3_client = boto3.client('s3')
    df_json={
        "last_dmp_period":dmp_week,
        "last_dmp_year":dmp_year
            }
    json_body=json.dumps(df_json)
    s3_client.put_object(Body=json_body,Bucket='rapid-response-prod',Key='last_week_dmp_period/last_dmp_period.json')
    
    def archive_carry_forward_date(source_supplier,source_supplier_id):
        ############################
        ##  Carry Forward Record  ##
        ############################
        get_col_names="""SELECT column_name from information_schema.columns where table_schema='{db_name}' and table_name='forecast_plan';""".format(db_name=DB_NAME)
        col_names=list(condb(get_col_names))
        db_columns=list(sum(col_names,()))
        supplier_short_name = source_supplier.split()[0]
        carry_forward_query="""select * from {db_name}.forecast_plan where 
        (((updated_by!=1 and active=1)and((forecast_adj!=0 or nongsa_adj!=0 or gsa_adj!=0 or system_adj!=0 or forecast_supply!=0 or nongsa_supply!=0 or 
         gsa_supply!=0 or system_supply!=0 or forecast_commit!=0 or nongsa_commit!=0 or gsa_commit!=0 or system_commit!=0
         or gsa_advance_commit_irp!=0 or nongsa_advance_commit_irp!=0 or forecast_advance_commit_irp!=0 or system_advance_commit_irp!=0 or 
         cumulative_gsa_advance_commit_irp!=0 or cumulative_nongsa_advance_commit_irp!=0 or cumulative_forecast_advance_commit_irp!=0 or cumulative_system_advance_commit_irp!=0)))
        or 
        ((carry_forward=1 and active=1)and((forecast_adj!=0 or nongsa_adj!=0 or gsa_adj!=0 or system_adj!=0 or forecast_supply!=0 or nongsa_supply!=0 or 
         gsa_supply!=0 or system_supply!=0 or forecast_commit!=0 or nongsa_commit!=0 or gsa_commit!=0 or system_commit!=0
         or gsa_advance_commit_irp!=0 or nongsa_advance_commit_irp!=0 or forecast_advance_commit_irp!=0 or system_advance_commit_irp!=0 or 
         cumulative_gsa_advance_commit_irp!=0 or cumulative_nongsa_advance_commit_irp!=0 or cumulative_forecast_advance_commit_irp!=0 or cumulative_system_advance_commit_irp!=0)))
         or
         ((updated_by=1)and
         (forecast_adj!=0 or nongsa_adj!=0 or gsa_adj!=0 or system_adj!=0 or forecast_supply!=0 or nongsa_supply!=0 or 
         gsa_supply!=0 or system_supply!=0 or forecast_commit!=0 or nongsa_commit!=0 or gsa_commit!=0 or system_commit!=0 or 
         cumulative_gsa_advance_commit_irp!=0 or cumulative_nongsa_advance_commit_irp!=0 or cumulative_forecast_advance_commit_irp!=0 or cumulative_system_advance_commit_irp!=0)))
         and source_supplier_id={supplier_id};""".format(db_name=DB_NAME,supplier_id=source_supplier_id)
        
        carry_over_data=condb(carry_forward_query)
        carry_over_df=pd.DataFrame(data=carry_over_data,columns=db_columns)
        test1=carry_over_df.applymap(lambda x: x[0] if type(x) is bytes else x)
        test2=test1.fillna(0)
        carry_over_df1=test2.drop(['last_week_data','description'],axis=1)
        test_df=carry_over_df1.to_csv(index=False,header=True)
        s3=boto3.client("s3")
        s3.put_object(Body=test_df,Bucket=S3_BUCKET,Key="carry_forward_records/"+"carry_over_week_"+supplier_short_name+"_"+str(week_num)+".csv")
    
    
    # Get all suppliers and run through each of them to archive current week carry forwarded data
    suppliers=get_all_suppliers()
    for supplier in suppliers:
        if supplier:
            supp_id=supplier['source_supplier_id']
            supp_name=supplier['source_supplier']
            print(supp_id,"-",supp_name)
            archive_carry_forward_date(supp_name,supp_id)
        else:
            print("Individual supplier list empty") 