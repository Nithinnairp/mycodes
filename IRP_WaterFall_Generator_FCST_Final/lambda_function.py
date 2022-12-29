import pandas as pd
import datetime 
import boto3
import io
from DB_conn import condb,condb_dict
import os


DB_NAME=os.environ['DB_name']
S3_BUCKET = "so-interface-to-rr-prod"


def get_all_suppliers():
    get_suppliers_query="""select source_supplier_id,source_supplier from {db_name}.forecast_plan group by source_supplier;""".format(db_name=DB_NAME)
    suppliers_list=condb_dict(get_suppliers_query)
    return suppliers_list


def lambda_handler(event, context):
    current_time=datetime.datetime.now()
    sng_time=(current_time+datetime.timedelta(hours=8)).strftime("%Y-%m-%d %H_%M")
    my_date=datetime.datetime.today()
    year,week_num,day_of_week=my_date.isocalendar()
    
    catogory_name={}
    result=condb_dict("select * from {}.config where jhi_key in ('GSA','NON-GSA','FORECAST','SYSTEM');".format(DB_NAME))
    for i in result:
        print(i)
        if i['jhi_key']=='GSA':
            catogory_name['gsa']=i['value']
        elif i['jhi_key']=='NON-GSA':
            catogory_name['nongsa']=i['value']
        elif i['jhi_key']=='FORECAST':
            catogory_name['forecast']=i['value']
        elif i['jhi_key']=='SYSTEM':
            catogory_name['system']=i['value']
        else:
            None

    
    def waterfall_chart_generator(supp_name,source_supplier_id):
        query2="""select 'IODM_WF_FCSTFINAL' as 'Series Header BaseKey','IODM_WATERFALL_FCSTFINAL' as 'Series Header Category Value',
                    date_format(DATE_SUB(DATE(now()), INTERVAL DAYOFWEEK(now())-2 DAY),'%m%d%Y') as 'Series_AsOfDate',
                    concat('IODM_STD_IRP_',date_format(DATE_SUB(DATE(now()), INTERVAL DAYOFWEEK(now())-2 DAY),'%Y%m%d')) as 'Series_Id',
                    1 as 'Series_Sequence',null as 'Series Header PartSupplier BaseKey',source_supplier as 'Series Header PartSupplier_Supplier_Id',
                    null as 'Series Header PartSupplier_ToleranceProfile_Value',
                    test.ori_part_number as 'Series Header PartSupplier_Part_Name','PenangF03' as 'Series Header PartSupplier_Part_Site_Value',
                    date_format(test.date,'%m%d%Y') as 'BeginDate',date_format(test.date,'%m%d%Y') as 'EndDate',test.demand_category as 'Demand Category',
                    original as 'Quantity'
                    from 
                    (select source_supplier,ori_part_number,prod_week,prod_year,date,ifnull(forecast_adj,0) as adj,ifnull(forecast_original,0) as original,
                    ifnull(forecast_adj,0)+ifnull(forecast_original,0) as demand,
                    'Forecast' as demand_category,oripartnumber_week_year
                    from {3}.forecast_plan where source_supplier_id={0} and  (ifnull(forecast_original,0))!=0
                    union
                    select source_supplier,ori_part_number,prod_week,prod_year,date,
                    ifnull(gsa_adj,0) as adj,ifnull(gsa_original,0) as original,ifnull(gsa_adj,0)+ifnull(gsa_original,0) as demand,
                    '{1}' as demand_category,oripartnumber_week_year
                    from {3}.forecast_plan where source_supplier_id={0} and (ifnull(gsa_original,0))!=0
                    union
                    select source_supplier,ori_part_number,prod_week,prod_year,date,
                    ifnull(nongsa_adj,0) as adj,ifnull(nongsa_original,0) as original,
                    ifnull(nongsa_adj,0)+ifnull(nongsa_original,0) as demand,'{2}' as demand_category,oripartnumber_week_year
                    from {3}.forecast_plan where source_supplier_id={0} and (ifnull(nongsa_original,0))!=0
                    union
                    select source_supplier,ori_part_number,prod_week,prod_year,date,
                    ifnull(system_adj,0) as adj,ifnull(system_original,0) as original,ifnull(system_adj,0)+ifnull(system_original,0) as demand,
                    'System' as demand_category,oripartnumber_week_year
                    from {3}.forecast_plan where source_supplier_id={0} and (ifnull(system_original,0))!=0) as test
                    group by test.demand_category,test.oripartnumber_week_year 
                    order by test.ori_part_number,prod_year,prod_week;""".format(source_supplier_id,catogory_name['gsa'],catogory_name['nongsa'],DB_NAME)
        
        result2=condb(query2)
        column_names=['Series Header BaseKey','Series Header Category Value','Series_AsOfDate','Series_Id','Series_Sequence','Series Header PartSupplier BaseKey',
        'Series Header PartSupplier_Supplier_Id','Series Header PartSupplier_ToleranceProfile_Value','Series Header PartSupplier_Part_Name',
        'Series Header PartSupplier_Part_Site_Value','BeginDate','EndDate','Demand Category','Quantity']
        
        result_df2=pd.DataFrame(data=result2,columns=column_names)
        report_df2=result_df2.to_csv(index=False,header=True)
        s3=boto3.client("s3")
        s3_path = "waterfall_extract/{0}/{0}-".format(supp_name) + str(week_num) + "-Waterfall Chart-Final Quantity Weekly Extract with Category Original(" + str(sng_time) + ").csv" 
        # if source_supplier_id == 2 else \
        # "waterfall_extract/Venture/Venture-" + str(week_num) + "-Waterfall Chart-Final Quantity Weekly Extract with Category Original(" + str(sng_time) + ").csv" if source_supplier_id == 1 else \
        # "waterfall_extract/Jabil/Jabil-" + str(week_num) + "-Waterfall Chart-Final Quantity Weekly Extract with Category Original(" + str(sng_time) + ").csv"
        print(s3_path)
        s3.put_object(ACL='public-read',Body=report_df2,Bucket=S3_BUCKET,Key=s3_path)
        
        
        query3="""select 'IODM_WF_FCSTFINAL' as 'Series Header BaseKey','IODM_WATERFALL_FCSTFINAL' as 'Series Header Category Value',
                    date_format(DATE_SUB(DATE(now()), INTERVAL DAYOFWEEK(now())-2 DAY),'%m%d%Y') as 'Series_AsOfDate',
                    concat('IODM_FIN_IRP_',date_format(DATE_SUB(DATE(now()), INTERVAL DAYOFWEEK(now())-2 DAY),'%Y%m%d')) as 'Series_Id',
                    1 as 'Series_Sequence',null as 'Series Header PartSupplier BaseKey',source_supplier as 'Series Header PartSupplier_Supplier_Id',
                    null as 'Series Header PartSupplier_ToleranceProfile_Value',
                    test.ori_part_number as 'Series Header PartSupplier_Part_Name','PenangF03' as 'Series Header PartSupplier_Part_Site_Value',
                    date_format(test.date,'%m%d%Y') as 'BeginDate',date_format(test.date,'%m%d%Y') as 'EndDate',test.demand_category as 'Demand Category',
                    demand as 'Quantity'
                    from 
                    (select source_supplier,ori_part_number,prod_week,prod_year,date,ifnull(forecast_adj,0) as adj,ifnull(forecast_original,0) as original,
                    ifnull(forecast_adj,0)+ifnull(forecast_original,0) as demand,
                    'Forecast' as demand_category,oripartnumber_week_year
                    from {3}.forecast_plan where source_supplier_id={0} and  (ifnull(forecast_adj,0)+ifnull(forecast_original,0))!=0
                    union
                    select source_supplier,ori_part_number,prod_week,prod_year,date,
                    ifnull(gsa_adj,0) as adj,ifnull(gsa_original,0) as original,ifnull(gsa_adj,0)+ifnull(gsa_original,0) as demand,
                    '{1}' as demand_category,oripartnumber_week_year
                    from {3}.forecast_plan where source_supplier_id={0} and (ifnull(gsa_adj,0)+ifnull(gsa_original,0))!=0
                    union
                    select source_supplier,ori_part_number,prod_week,prod_year,date,
                    ifnull(nongsa_adj,0) as adj,ifnull(nongsa_original,0) as original,
                    ifnull(nongsa_adj,0)+ifnull(nongsa_original,0) as demand,'{2}' as demand_category,oripartnumber_week_year
                    from {3}.forecast_plan where source_supplier_id={0} and (ifnull(nongsa_adj,0)+ifnull(nongsa_original,0))!=0
                    union
                    select source_supplier,ori_part_number,prod_week,prod_year,date,
                    ifnull(system_adj,0) as adj,ifnull(system_original,0) as original,ifnull(system_adj,0)+ifnull(system_original,0) as demand,
                    'System' as demand_category,oripartnumber_week_year
                    from {3}.forecast_plan where source_supplier_id={0} and (ifnull(system_adj,0)+ifnull(system_original,0))!=0) as test
                    group by test.demand_category,test.oripartnumber_week_year 
                    order by test.ori_part_number,prod_year,prod_week;""".format(source_supplier_id,catogory_name['gsa'],catogory_name['nongsa'],DB_NAME)
        
        result3=condb(query3)
        column_names=['Series Header BaseKey','Series Header Category Value','Series_AsOfDate','Series_Id','Series_Sequence','Series Header PartSupplier BaseKey',
        'Series Header PartSupplier_Supplier_Id','Series Header PartSupplier_ToleranceProfile_Value','Series Header PartSupplier_Part_Name',
        'Series Header PartSupplier_Part_Site_Value','BeginDate','EndDate','Demand Category','Quantity']
        
        result_df3=pd.DataFrame(data=result3,columns=column_names)
        report_df3=result_df3.to_csv(index=False,header=True)
        s3=boto3.client("s3")
        s3_path = "waterfall_extract/{0}/{0}-".format(supp_name) + str(week_num) + "-Waterfall Chart-Final Quantity Weekly Extract with Category Final(" + str(sng_time) + ").csv"
        # if source_supplier_id == 2 else \
        # "waterfall_extract/Venture/Venture-" + str(week_num) + "-Waterfall Chart-Final Quantity Weekly Extract with Category Final(" + str(sng_time) + ").csv" if source_supplier_id == 1 else "waterfall_extract/Jabil/Jabil-" + str(week_num) + "-Waterfall Chart-Final Quantity Weekly Extract with Category Final(" + str(sng_time) + ").csv"
        print(s3_path)
        s3.put_object(ACL='public-read',Body=report_df3,Bucket=S3_BUCKET,Key=s3_path)
        
    
    suppliers=get_all_suppliers()
    for supplier in suppliers:
        if supplier:
            supp_id=supplier['source_supplier_id']
            supp_name=supplier['source_supplier']
            supplier_short_name = supp_name.split()[0]
            # print(supp_id,"-",supp_name) 
            waterfall_chart_generator(supplier_short_name,supp_id) 
            
        else:
            print("Individual supplier list empty")
        