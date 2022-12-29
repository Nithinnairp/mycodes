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
DB_NAME = os.environ['DB_NAME']
month=condb_dict("select prod_month from {db_name}.keysight_calendar where prod_week={week} and prod_year={year};".format(db_name=DB_NAME,week=week_num,year=str(year)[2:]))[0]['prod_month']


def prepare_df(schema,bu,supp_id):
    #################################################################################
    # Planner Adj(N) vs (N-1)
    #################################################################################
    
    by_product_family_curr_year="""select source_supplier,bu,product_family,sum(fp.forecast_original)+
    +sum(fp.nongsa_original)+
    sum(fp.gsa_original)+sum(fp.system_original) from {table} as fp 
    where fp.source_supplier_id={supplier_id} and fp.prod_year=substring(year(curdate()),3) and bu='{bu}'
    group by product_family 
    order by bu,product_family;""".format(table=schema,supplier_id=supp_id,bu=bu)
    by_product_family_response_curr_year=condb(by_product_family_curr_year)
    
    sub_total_curr_year="""select source_supplier,bu,'' as product_family,(sum(fp.forecast_original)+
    +sum(fp.nongsa_original)+
    sum(fp.gsa_original)+sum(fp.system_original)) as total from {table} as fp 
    where fp.source_supplier_id={supplier_id} and fp.prod_year=substring(year(curdate()),3) and bu='{bu}';""".format(table=schema,supplier_id=supp_id,bu=bu)
    sub_total_response_curr_year=condb(sub_total_curr_year)
    
    Grand_total_curr_year="""select source_supplier,'Grand Total' as bu,'' as product_family,(sum(fp.forecast_original)+
    +sum(fp.nongsa_original)+
    sum(fp.gsa_original)+sum(fp.system_original)) as total from {table} as fp 
    where fp.source_supplier_id={supplier_id} and fp.prod_year=substring(year(curdate()),3);""".format(table=schema,supplier_id=supp_id)
    Grand_total_response_curr_year=condb(Grand_total_curr_year)
    
    by_product_family_curr_year_plus1="""select product_family,sum(fp.forecast_original)+
    +sum(fp.nongsa_original)+
    sum(fp.gsa_original)+sum(fp.system_original) from {table} as fp 
    where fp.source_supplier_id={supplier_id} and fp.prod_year=substring(year(curdate()),3)+1 and bu='{bu}'
    group by product_family;""".format(table=schema,supplier_id=supp_id,bu=bu)
    by_product_family_response_curr_year_plus1=condb(by_product_family_curr_year_plus1)
    
    sub_total_curr_year_plus1="""select '' as product_family,(sum(fp.forecast_original)+
    +sum(fp.nongsa_original)+
    sum(fp.gsa_original)+sum(fp.system_original)) as total from {table} as fp 
    where fp.source_supplier_id={supplier_id} and fp.prod_year=substring(year(curdate()),3)+1 and bu='{bu}';""".format(table=schema,supplier_id=supp_id,bu=bu)
    sub_total_response_curr_year_plus1=condb(sub_total_curr_year_plus1)
    
    Grand_total_curr_year_plus1="""select '' as product_family,(sum(fp.forecast_original)+
    +sum(fp.nongsa_original)+
    sum(fp.gsa_original)+sum(fp.system_original)) as total from {table} as fp 
    where fp.source_supplier_id={supplier_id} and fp.prod_year=substring(year(curdate()),3)+1;""".format(table=schema,supplier_id=supp_id)
    Grand_total_response_curr_year_plus1=condb(Grand_total_curr_year_plus1)
    
    by_product_family_curr_year_plus2="""select product_family,sum(fp.forecast_original)+
    +sum(fp.nongsa_original)+
    sum(fp.gsa_original)+sum(fp.system_original) from {table} as fp 
    where fp.source_supplier_id={supplier_id} and fp.prod_year=substring(year(curdate()),3)+2 and bu='{bu}'
    group by product_family;""".format(table=schema,supplier_id=supp_id,bu=bu)
    by_product_family_response_curr_year_plus2=condb(by_product_family_curr_year_plus2)
    
    sub_total_curr_year_plus2="""select '' as product_family,(sum(fp.forecast_original)+
    +sum(fp.nongsa_original)+
    sum(fp.gsa_original)+sum(fp.system_original)) as total from {table} as fp 
    where fp.source_supplier_id={supplier_id} and fp.prod_year=substring(year(curdate()),3)+2 and bu='{bu}';""".format(table=schema,supplier_id=supp_id,bu=bu)
    sub_total_response_curr_year_plus2=condb(sub_total_curr_year_plus2)
        
    Grand_total_curr_year_plus2="""select '' as product_family,(sum(fp.forecast_original)+
    +sum(fp.nongsa_original)+
    sum(fp.gsa_original)+sum(fp.system_original)) as total from {table} as fp 
    where fp.source_supplier_id={supplier_id} and fp.prod_year=substring(year(curdate()),3)+2;""".format(table=schema,supplier_id=supp_id)
    Grand_total_response_curr_year_plus2=condb(Grand_total_curr_year_plus2)
    
    By_pf_all_year_total="""select product_family,
    sum(fp.forecast_original)+sum(fp.nongsa_original)+sum(fp.gsa_original)+sum(fp.system_original)
    from {table} as fp where fp.source_supplier_id={supplier_id} and bu='{bu}'
    group by product_family;""".format(table=schema,supplier_id=supp_id,bu=bu)
    By_pf_all_year_total_response=condb(By_pf_all_year_total)
    
    sub_total_all_year_total="""select '' as product_family,
    sum(forecast_original+nongsa_original+gsa_original+system_original)
    from {table} as fp where fp.source_supplier_id={supplier_id} and bu='{bu}' 
    group by bu;""".format(table=schema,supplier_id=supp_id,bu=bu)
    sub_total_all_year_total_response=condb(sub_total_all_year_total)
    
    Grand_total_all_year="""select '' as product_family,(sum(fp.forecast_original)+
    +sum(fp.nongsa_original)+
    sum(fp.gsa_original)+sum(fp.system_original)) as total from {table} as fp 
    where fp.source_supplier_id={supplier_id};""".format(table=schema,supplier_id=supp_id)
    Grand_total_all_year_response=condb(Grand_total_all_year)
        
    by_product_family_curr_year_df=pd.DataFrame(data=by_product_family_response_curr_year,columns=['source_supplier','bu','product_family','21 Total'])
    sub_total_response_curr_year_df=pd.DataFrame(data=sub_total_response_curr_year,columns=['source_supplier','bu','product_family','21 Total'])
    Grand_total_response_curr_year_df=pd.DataFrame(data=Grand_total_response_curr_year,columns=['source_supplier','bu','product_family','21 Total'])
    final_total_21_df=pd.concat([sub_total_response_curr_year_df,by_product_family_curr_year_df])
    
    by_product_family_curr_year_plus1_df=pd.DataFrame(data=by_product_family_response_curr_year_plus1,columns=['product_family','22 Total'])
    sub_total_response_curr_year_plus1_df=pd.DataFrame(data=sub_total_response_curr_year_plus1,columns=['product_family','22 Total'])
    Grand_total_response_curr_year_plus1_df=pd.DataFrame(data=Grand_total_response_curr_year_plus1,columns=['product_family','22 Total'])
    final_total_curr_year_plus1_df=pd.concat([sub_total_response_curr_year_plus1_df,by_product_family_curr_year_plus1_df])
    
    by_product_family_curr_year_plus2_df=pd.DataFrame(data=by_product_family_response_curr_year_plus2,columns=['product_family','23 Total'])
    sub_total_response_curr_year_plus2_df=pd.DataFrame(data=sub_total_response_curr_year_plus2,columns=['product_family','23 Total'])
    Grand_total_response_curr_year_plus2_df=pd.DataFrame(data=Grand_total_response_curr_year_plus2,columns=['product_family','23 Total'])
    final_total_curr_year_plus2_df=pd.concat([sub_total_response_curr_year_plus2_df,by_product_family_curr_year_plus2_df])
    
    By_pf_all_year_total_df=pd.DataFrame(data=By_pf_all_year_total_response,columns=['product_family','Grand Total'])
    sub_total_all_year_total_df=pd.DataFrame(data=sub_total_all_year_total_response,columns=['product_family','Grand Total'])
    Grand_total_all_year_response_df=pd.DataFrame(data=Grand_total_all_year_response,columns=['product_family','Grand Total'])
    final_total_all_year_df=pd.concat([sub_total_all_year_total_df,By_pf_all_year_total_df])
    
    merging_sub_total_curr_year=pd.concat([sub_total_response_curr_year_df,by_product_family_curr_year_df])
    merging_sub_total_curr_year_plus1=pd.concat([sub_total_response_curr_year_plus1_df,by_product_family_curr_year_plus1_df])
    merging_sub_total_curr_year_plus2=pd.concat([sub_total_response_curr_year_plus2_df,by_product_family_curr_year_plus2_df])
    mergin_total_all_year=pd.concat([sub_total_all_year_total_df,By_pf_all_year_total_df])
    preparing_df1=pd.merge(merging_sub_total_curr_year,merging_sub_total_curr_year_plus1,how='left')
    preparing_df2=pd.merge(preparing_df1,merging_sub_total_curr_year_plus2,how='left')
    final_pf_df=pd.merge(preparing_df2,mergin_total_all_year,how='left')
    
    preparing_grand_total_df1=pd.merge(Grand_total_response_curr_year_df,Grand_total_response_curr_year_plus1_df,on='product_family')
    preparing_grand_total_df2=pd.merge(preparing_grand_total_df1,Grand_total_response_curr_year_plus2_df,on='product_family')
    Final_grand_df=pd.merge(preparing_grand_total_df2,Grand_total_all_year_response_df,on='product_family')
    print('prepare_df completed')
    return final_pf_df, Final_grand_df
    
    

def CMA_Validation(supp_id):
    ##################################
    # CMA Validation
    ##################################
    def color(x):
        c1 = 'color: #ffffff; background-color: #ba3018'
        c2 = 'color: #ffffff; background-color: #92D050'
        m=x[('Previous Week','Validation')]!=x[('Current Week','IRP Forecast Previous')]
        n=x[('Previous Week','Validation')]==x[('Current Week','IRP Forecast Previous')]
        # ('Delta',str(year+1)[2:]+' Total'),('Delta',str(year+2)[2:]+' Total'),('Delta','Grand Total')
        df1 = pd.DataFrame('', index=x.index, columns=x.columns)
        df1.loc[m, [('Previous Week','Validation')]] = c1
        df1.loc[n, [('Previous Week','Validation')]] = c2
        df1.loc[m, [('Current Week','IRP Forecast Previous')]] = c1
        df1.loc[n, [('Current Week','IRP Forecast Previous')]] = c2
        return df1

    sql1="""select ftp.org,ftp.source_supplier as "cm",
            (sum(ftp.forecast_adj)+sum(ftp.forecast_original)+sum(ftp.gsa_adj)+sum(ftp.gsa_original)+
            sum(ftp.nongsa_adj)+sum(ftp.nongsa_original)+sum(ftp.system_adj)+sum(ftp.system_original)) as "kr",

            ((sum(ftp.forecast_adj)+sum(ftp.forecast_original)+sum(ftp.gsa_adj)+sum(ftp.gsa_original)+
            sum(ftp.nongsa_adj)+sum(ftp.nongsa_original)+sum(ftp.system_adj)+sum(ftp.system_original))-
            (sum(ftp.forecast_supply)+sum(ftp.forecast_advance_commit_irp)+sum(ftp.cumulative_forecast_advance_commit_irp))) as "exec",

            ((sum(ftp.forecast_adj)+sum(ftp.forecast_original)+sum(ftp.gsa_adj)+sum(ftp.gsa_original)+
            sum(ftp.nongsa_adj)+sum(ftp.nongsa_original)+sum(ftp.system_adj)+sum(ftp.system_original))-
            ((sum(ftp.forecast_adj)+sum(ftp.forecast_original)+sum(ftp.gsa_adj)+sum(ftp.gsa_original)+
            sum(ftp.nongsa_adj)+sum(ftp.nongsa_original)+sum(ftp.system_adj)+sum(ftp.system_original))-
            (sum(ftp.forecast_supply)+sum(ftp.forecast_advance_commit_irp)+sum(ftp.cumulative_forecast_advance_commit_irp)+
            sum(ftp.nongsa_supply)+sum(ftp.nongsa_advance_commit_irp)+sum(ftp.cumulative_nongsa_advance_commit_irp)+
            sum(ftp.gsa_supply)+sum(ftp.gsa_advance_commit_irp)+sum(ftp.cumulative_gsa_advance_commit_irp)+
            sum(ftp.system_supply)+sum(ftp.system_advance_commit_irp)+sum(ftp.cumulative_system_advance_commit_irp)))) as "test_result_prev",

            (sum(fp.forecast_supply)+sum(fp.forecast_advance_commit_irp)+sum(fp.cumulative_forecast_advance_commit_irp)+
            sum(fp.nongsa_supply)+sum(fp.nongsa_advance_commit_irp)+sum(fp.cumulative_nongsa_advance_commit_irp)+
            sum(fp.gsa_supply)+sum(fp.gsa_advance_commit_irp)+sum(fp.cumulative_gsa_advance_commit_irp)+
            sum(fp.system_supply)+sum(fp.system_advance_commit_irp)+sum(fp.cumulative_system_advance_commit_irp)) as "test_result_curr"
             from {db_name}.forecast_plan as fp join {db_name}.forecast_plan_temp as ftp
            on fp.id=ftp.id where fp.source_supplier_id={supplier_id} group by fp.source_supplier,fp.org;""".format(db_name=DB_NAME,supplier_id=supp_id)
    result1=condb(sql1)
    f1=pd.DataFrame(data=result1,columns=[('Site'),'CM Name','kr','exec','validation','IRP forecast previous'])
    f1=f1.fillna(0)
    col=[('','Site'),('','CM Name'),('Previous Week','Final IRP Current'),('Previous Week','Exception'),('Previous Week','Validation'),('Current Week','IRP Forecast Previous')]
    f1.columns=pd.MultiIndex.from_tuples(col)
    f2=f1.style.apply(color,axis=None)
    print('CMA_Validation completed')
    return f2

def CM_COMMIT_CF(supp_id):
    sql1="""select 'last_week' as 'week',(sum(ifnuLL(forecast_supply,0))+sum(ifnull(forecast_advance_commit_irp,0))+sum(ifnull(cumulative_forecast_advance_commit_irp,0))) as 'forecast_commit',
    (sum(ifnull(nongsa_supply,0))+sum(ifnull(nongsa_advance_commit_irp,0))+sum(ifnull(cumulative_nongsa_advance_commit_irp,0))) as 'nongsa_commit',
    (sum(ifnull(gsa_supply,0))+sum(ifnull(gsa_advance_commit_irp,0))+sum(ifnull(cumulative_gsa_advance_commit_irp,0))) as 'gsa_commit',
    (sum(ifnull(system_supply,0))+sum(ifnull(system_advance_commit_irp,0))+sum(ifnull(cumulative_system_advance_commit_irp,0))) as 'system_commit'
    from {db_name}.forecast_plan_temp  
    where ((prod_week>{week} and prod_year={year}) or (prod_year>{year})) and source_supplier_id={supplier_id} and is_dmp=1
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
    print('CM_COMMIT_CF completed')
    return result,status

def Exception_validation(supp_id):
    sql1="""select count(*) as count from {db_name}.forecast_plan
    where active=1 and dmp_orndmp='DMP' and  process_type='Exception' and source_supplier_id={supplier_id} and
    (((ifnull(forecast_original,0)+ifnull(forecast_adj,0))-(ifnull(forecast_supply,0)+ifnull(forecast_advance_commit_irp,0)+ifnull(cumulative_forecast_advance_commit_irp,0)))=0 and
    ((ifnull(system_original,0)+ifnull(system_adj,0))-(ifnull(system_supply,0)+ifnull(system_advance_commit_irp,0)+ifnull(cumulative_system_advance_commit_irp,0)))=0 and
    ((ifnull(gsa_original,0)+ifnull(gsa_adj,0))-(ifnull(gsa_supply,0)+ifnull(gsa_advance_commit_irp,0)+ifnull(cumulative_gsa_advance_commit_irp,0)))=0 and
    ((ifnull(nongsa_original,0)+ifnull(nongsa_adj,0))-(ifnull(nongsa_supply,0)+ifnull(nongsa_advance_commit_irp,0)+ifnull(cumulative_nongsa_advance_commit_irp,0)))=0);""".format(db_name=DB_NAME,supplier_id=supp_id)
    
    result1=condb_dict(sql1)
    
    sql2="""select count(*) as count from {db_name}.forecast_plan  
    where dmp_orndmp='DMP' and active=1 and process_type is null and source_supplier_id={supplier_id} and 
    (((ifnull(forecast_original,0)+ifnull(forecast_adj,0))-(ifnull(forecast_supply,0)+ifnull(forecast_advance_commit_irp,0)+ifnull(cumulative_forecast_advance_commit_irp,0)))!=0 or
    ((ifnull(system_original,0)+ifnull(system_adj,0))-(ifnull(system_supply,0)+ifnull(system_advance_commit_irp,0)+ifnull(cumulative_system_advance_commit_irp,0)))!=0 or 
    ((ifnull(gsa_original,0)+ifnull(gsa_adj,0))-(ifnull(gsa_supply,0)+ifnull(gsa_advance_commit_irp,0)+ifnull(cumulative_gsa_advance_commit_irp,0)))!=0 or 
    ((ifnull(nongsa_original,0)+ifnull(nongsa_adj,0))-(ifnull(nongsa_supply,0)+ifnull(nongsa_advance_commit_irp,0)+ifnull(cumulative_nongsa_advance_commit_irp,0)))!=0);""".format(db_name=DB_NAME,supplier_id=supp_id)
    result2=condb_dict(sql2)
    
    sql3="""select count(*) as count from {db_name}.forecast_plan where active=1 and dmp_orndmp='NDMP' and process_type='Exception' and source_supplier_id={supplier_id};""".format(db_name=DB_NAME,supplier_id=supp_id)
    result3=condb_dict(sql3)
    status="OK" if result1[0]['count']==0 and result2[0]['count']==0 and result3[0]['count']==0 else "Fail"
    print('Exception_validation completed')
    return status
    
def irp_exception_cf(supp_id):
    sql1="""select ftp.source_supplier as "cm",ftp.org,
            (sum(ifnull(ftp.forecast_adj,0))+sum(ifnull(ftp.forecast_original,0))+sum(ifnull(ftp.gsa_adj,0))+sum(ifnull(ftp.gsa_original,0))+
            sum(ifnull(ftp.nongsa_adj,0))+sum(ifnull(ftp.nongsa_original,0))+sum(ifnull(ftp.system_adj,0))+sum(ifnull(ftp.system_original,0))) as "kr",
            (sum(ifnull(ftp.forecast_supply,0))+sum(ifnull(ftp.forecast_advance_commit_irp,0))+sum(ifnull(ftp.cumulative_forecast_advance_commit_irp,0))+
            sum(ifnull(ftp.nongsa_supply,0))+sum(ifnull(ftp.nongsa_advance_commit_irp,0))+sum(ifnull(ftp.cumulative_nongsa_advance_commit_irp,0))+
            sum(ifnull(ftp.gsa_supply,0))+sum(ifnull(ftp.gsa_advance_commit_irp,0))+sum(ifnull(ftp.cumulative_gsa_advance_commit_irp,0))+
            sum(ifnull(ftp.system_supply,0))+sum(ifnull(ftp.system_advance_commit_irp,0))+sum(ifnull(ftp.cumulative_system_advance_commit_irp,0))) as "commit",
            
            ((sum(ifnull(ftp.forecast_adj,0))+sum(ifnull(ftp.forecast_original,0))+sum(ifnull(ftp.gsa_adj,0))+sum(ifnull(ftp.gsa_original,0))+
            sum(ifnull(ftp.nongsa_adj,0))+sum(ifnull(ftp.nongsa_original,0))+sum(ifnull(ftp.system_adj,0))+sum(ifnull(ftp.system_original,0)))-
            (sum(ifnull(ftp.forecast_supply,0))+sum(ifnull(ftp.forecast_advance_commit_irp,0))+sum(ifnull(ftp.cumulative_forecast_advance_commit_irp,0)))) as "exc",
            
            (sum(ifnull(ftp.forecast_adj,0))+sum(ifnull(ftp.forecast_original,0))+sum(ifnull(ftp.gsa_adj,0))+sum(ifnull(ftp.gsa_original,0))+
            sum(ifnull(ftp.nongsa_adj,0))+sum(ifnull(ftp.nongsa_original,0))+sum(ifnull(ftp.system_adj,0))+sum(ifnull(ftp.system_original,0)))-
            ((sum(ifnull(ftp.forecast_adj,0))+sum(ifnull(ftp.forecast_original,0))+sum(ifnull(ftp.gsa_adj,0))+sum(ifnull(ftp.gsa_original,0))+
            sum(ifnull(ftp.nongsa_adj,0))+sum(ifnull(ftp.nongsa_original,0))+sum(ifnull(ftp.system_adj,0))+sum(ifnull(ftp.system_original,0)))-
            (sum(ifnull(ftp.forecast_supply,0))+sum(ifnull(ftp.forecast_advance_commit_irp,0))+sum(ifnull(ftp.cumulative_forecast_advance_commit_irp,0))+
            sum(ifnull(ftp.nongsa_supply,0))+sum(ifnull(ftp.nongsa_advance_commit_irp,0))+sum(ifnull(ftp.cumulative_nongsa_advance_commit_irp,0))+
            sum(ifnull(ftp.gsa_supply,0))+sum(ifnull(ftp.gsa_advance_commit_irp,0))+sum(ifnull(ftp.cumulative_gsa_advance_commit_irp,0))+
            sum(ifnull(ftp.system_supply,0))+sum(ifnull(ftp.system_advance_commit_irp,0))+sum(ifnull(ftp.cumulative_system_advance_commit_irp,0)))) 
            as "prev_exception",
            
            (sum(ifnull(fp.forecast_supply,0))+sum(ifnull(fp.forecast_advance_commit_irp,0))+sum(ifnull(fp.cumulative_forecast_advance_commit_irp,0))+
            sum(ifnull(fp.nongsa_supply,0))+sum(ifnull(fp.nongsa_advance_commit_irp,0))+sum(ifnull(fp.cumulative_nongsa_advance_commit_irp,0))+
            sum(ifnull(fp.gsa_supply,0))+sum(ifnull(fp.gsa_advance_commit_irp,0))+sum(ifnull(fp.cumulative_gsa_advance_commit_irp,0))+
            sum(ifnull(fp.system_supply,0))+sum(ifnull(fp.system_advance_commit_irp,0))+sum(ifnull(fp.cumulative_system_advance_commit_irp,0))) as "curr_exception"
             from {db_name}.forecast_plan as fp join {db_name}.forecast_plan_temp as ftp
            on fp.id=ftp.id 
            where fp.source_supplier_id={supplier_id} and ftp.is_dmp=1 group by fp.source_supplier,fp.org;""".format(db_name=DB_NAME,supplier_id=supp_id)
    result1=condb_dict(sql1)
    status_list=[]
    for i in range(0,len(result1)):
        print(result1[i]['prev_exception'],result1[i]['curr_exception'])
        status= "OK" if result1[i]['prev_exception']==result1[i]['curr_exception'] else "Fail"
        status_list.append(status)
    status="Fail" if 'Fail' in status_list else "OK"
    print('irp_exception_cf completed')
    return status
    
def forecast_plan_after_processing_validations(supp_id):
    validation={}
    validation1="""select count(*) as count from {db_name}.forecast_plan where process_week={week} and source_supplier_id={supplier_id};""".format(db_name=DB_NAME,week=week_num,supplier_id=supp_id)
    result=condb_dict(validation1)[0]
    validation['Total active records']=result['count']
    print(result)
    
    validation2="""select distinct prod_month,prod_year from {db_name}.forecast_plan where dmp_orndmp='DMP' and source_supplier_id={supplier_id};""".format(db_name=DB_NAME,supplier_id=supp_id)
    result=sum(condb(validation2),())
    prod_month=[result[0],result[2],result[4]]
    print(prod_month)
    prod_year=set([result[1],result[3],result[5]])
    print(prod_year)
    status='OK' if (len(prod_year)==1 and (min(prod_month)==month and max(prod_month)==month+2)) or \
    (len(prod_year)==2 and (min(prod_month)==1 and max(prod_month)==12)) else 'Fail'
    validation['dmp_validation']=status
    
    validation3="""select count(*) as count from {db_name}.forecast_plan  
        where dmp_orndmp='DMP' and active=1 and process_type='Exception' and source_supplier_id={supplier_id} and 
        (((ifnull(forecast_original,0)+ifnull(forecast_adj,0))-(ifnull(forecast_supply,0)+ifnull(forecast_advance_commit_irp,0)+ifnull(cumulative_forecast_advance_commit_irp,0)))!=0 or
        ((ifnull(system_original,0)+ifnull(system_adj,0))-(ifnull(system_supply,0)+ifnull(system_advance_commit_irp,0)+ifnull(cumulative_system_advance_commit_irp,0)))!=0 or 
        ((ifnull(gsa_original,0)+ifnull(gsa_adj,0))-(ifnull(gsa_supply,0)+ifnull(gsa_advance_commit_irp,0)+ifnull(cumulative_gsa_advance_commit_irp,0)))!=0 or 
        ((ifnull(nongsa_original,0)+ifnull(nongsa_adj,0))-(ifnull(nongsa_supply,0)+ifnull(nongsa_advance_commit_irp,0)+ifnull(cumulative_nongsa_advance_commit_irp,0)))!=0);""".format(db_name=DB_NAME,supplier_id=supp_id)
    result=condb_dict(validation3)[0]
    validation['Total exception records']=result['count']
    
    validation4="""select carry_forward, source_supplier,updated_by,count(1)
        from {db_name}.forecast_plan where source_supplier_id={supplier_id} group by carry_forward, source_supplier,updated_by;""".format(db_name=DB_NAME,supplier_id=supp_id)
    result=condb_dict(validation4)
    count1=0
    count2=0 
    for i in result:
        if i['updated_by']=='2':
            count1=count1+i['count(1)']
        else:
            count2=count2+i['count(1)']
    validation['RR records']=count2
    validation['Carry Forward records']=count1
    print('forecast_plan_after_processing_validations is completed')
    
    return validation
    