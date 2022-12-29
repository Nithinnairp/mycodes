import pymysql
import json
import pandas as pd
import datetime
import boto3
import requests
import os
from DB_conn import condb,condb_dict
from urllib.parse import quote
import os

def monthly_report(source_supplier_name):
    my_date=datetime.date.today()
    current_time=datetime.datetime.now()
    year,week_num,day_of_week=my_date.isocalendar()
    year=str(year)[2:]
    encoded_supplier_name = quote(source_supplier_name)
    supplier_short_name = source_supplier_name.split()[0]
    DOMAIN=os.environ['DOMAIN']
    DB_NAME=os.environ['DB_NAME']
    S3_BUCKET = os.environ['S3_BUCKET']
    def trimming_dmp_period(number_of_week,col):
            to_be_trimmed=12-number_of_week
            while to_be_trimmed>=0:
                print(to_be_trimmed,col[9][to_be_trimmed])
                col[9].pop(to_be_trimmed)
                to_be_trimmed=to_be_trimmed-1
                print(to_be_trimmed)
                
    def rename_col(col,col_position,dmp_month):
        count=0
        for i in col[col_position]:
            print(count,i)
            i[0]='month'+str(dmp_month[count])+'_nongsa'
            i[1]='month'+str(dmp_month[count])+'_gsa'
            i[2]='month'+str(dmp_month[count])+'_system'
            i[3]='month'+str(dmp_month[count])+'_forecast'
            i[4]='month'+str(dmp_month[count])+'_total'
            count+=1
    def removeNestings(x,final_col):
        for i in x: 
            if type(i) == list: 
                removeNestings(i,final_col) 
            else: 
                final_col.append(i)
        return final_col
    
    date_str=current_time.strftime("%Y-%m-%d %H:%M")
    sng_time=(current_time+datetime.timedelta(hours=8)).strftime("%Y-%m-%d %H_%M")
    formated_time=(current_time+datetime.timedelta(hours=8)).strftime("%Y-%m-%d %H_%M").replace(' ','+')
    get_phase="""{}/api/irp-summary/monthly-data?page=0&size=20000&sort=division,asc&sort=productLine,asc&sort=productFamily,asc&isDecommitLine=false&sourceSupplier.in={}&filterType=irp-summary""".format(DOMAIN,encoded_supplier_name)
    response=requests.get(get_phase) 
    print(response.content)
    df=response.content
    phase_object=json.loads(response.content)
    try:
        del phase_object['months']
        del phase_object['deCommitLineDataDTOs']
    except:
        pass
    
    catogory_name={}
    config_query = "select * from {}.config where jhi_key in ('GSA','NON-GSA','FORECAST','SYSTEM');".format(DB_NAME)
    result=condb_dict(config_query)
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
    
    json_object=pd.DataFrame.from_dict(data=phase_object)
    df1=json_object["commitDataDTOs"]
    number_of_week=phase_object['numberOfWeeks']
    
    get_dmp_week="""select distinct kc.prod_month,kc.prod_year from {0}.keysight_calendar as kc,
                    (select prod_date,prod_month,prod_year from {0}.keysight_calendar where prod_week ={1} and prod_year ={2}) as kc_curr
                    where concat(kc.prod_year,kc.prod_month)
                    in (
                    concat(kc_curr.prod_year,kc_curr.prod_month),
                    if (kc_curr.prod_month=12,concat(kc_curr.prod_year+1,1),concat(kc_curr.prod_year,kc_curr.prod_month+1)),
                    if (kc_curr.prod_month=11,concat(kc_curr.prod_year+1,1),if (kc_curr.prod_month=12,concat(kc_curr.prod_year+1,2),concat(kc_curr.prod_year,kc_curr.prod_month+2)))
                    )
                    and kc.prod_date >= kc_curr.prod_date;""".format(DB_NAME,week_num,year)
    week_year=condb(get_dmp_week)
    df=pd.DataFrame(week_year,columns=['prod_month','prod_year'])
    dmp_month=[]
    dmp_year=[]
    for x,y in df.iterrows():
        dmp_month.append(y.prod_month)
        dmp_year.append(y.prod_year)
    dmp_year=set(dmp_year)
    
    source_supplier_list=[]
    coe_list=[]
    product_line_list=[]
    product_family_list=[]
    overall_nongsa_list=[]
    overall_gsa_list=[]
    overall_system_list=[]
    overall_forecast_list=[]
    overall_total_list=[]
    total_nongsa_demand_list=[]
    total_gsa_demand_list=[]
    total_forecast_demand_list=[]
    total_system_demand_list=[]
    total_nongsa_supply_list=[]
    total_gsa_supply_list=[]
    total_forecast_supply_list=[]
    total_system_supply_list=[]
    month_list=[[[],[],[],[],[],[],[],[],[],[],[],[],[]],
           [[],[],[],[],[],[],[],[],[],[],[],[],[]],
           [[],[],[],[],[],[],[],[],[],[],[],[],[]]]
    
    
    for y in df1:
        source_supplier=y['sourceSupplier'] if y['sourceSupplier'] is not None else None
        coe=y['coe'] if y['coe'] is not None else None
        product_line=y['productLine'] if y['productLine'] is not None else None
        product_family=y['productFamily'] if y['productFamily'] is not None else None
        overall_nongsa=round(y['totalNonGsaCommitPercentage']) if y['totalNonGsaCommitPercentage'] is not None else None
        overall_gsa=round(y['totalGsaCommitPercentage']) if y['totalGsaCommitPercentage'] is not None else None
        overall_system=round(y['totalSystemCommitPercentage']) if y['totalSystemCommitPercentage'] is not None else None
        overall_forecast=round(y['totalForecastCommitPercentage']) if y['totalForecastCommitPercentage'] is not None else None
        overall_total=round(y['overallTotalCommitPercentage']) if y['overallTotalCommitPercentage'] is not None else None
    
        weekly_data=y['commitMonthDataDTOs']
        source_supplier_list.append(source_supplier)
        coe_list.append(coe)
        product_line_list.append(product_line)
        product_family_list.append(product_family)
        overall_nongsa_list.append(overall_nongsa)
        overall_gsa_list.append(overall_gsa)
        overall_system_list.append(overall_system)
        overall_forecast_list.append(overall_forecast)
        overall_total_list.append(overall_total)
        json_object_counter=0
        for i in range(0,3):
            # print(len(weekly_data),i,json_object_counter)
            print(product_family)
            month=weekly_data[json_object_counter]['month']
    #         print('week',week,'week_counter',week_counter)
            row_counter=i
    
            if month==dmp_month[i]:
                nongsa_percentage=round(weekly_data[json_object_counter]['nonGsaCommitPercentage'],0) if weekly_data[json_object_counter]['nonGsaCommitPercentage'] is not None else None
                month_list[row_counter][0].append(nongsa_percentage)
                gsa_percentage=round(weekly_data[json_object_counter]['gsaCommitPercentage'],0) if weekly_data[json_object_counter]['gsaCommitPercentage'] is not None else None
                month_list[row_counter][1].append(gsa_percentage)
                system_percentage=round(weekly_data[json_object_counter]['systemCommitPercentage'],0) if weekly_data[json_object_counter]['systemCommitPercentage'] is not None else None
                month_list[row_counter][2].append(system_percentage)
                forecast_percentage=round(weekly_data[json_object_counter]['forecastCommitPercentage'],0) if weekly_data[json_object_counter]['forecastCommitPercentage'] is not None else None
                month_list[row_counter][3].append(forecast_percentage)
                total_percentage=round(weekly_data[json_object_counter]['totalCommitPercentage'],0) if weekly_data[json_object_counter]['totalCommitPercentage'] is not None else None
                month_list[row_counter][4].append(total_percentage)
                total_nongsa_demand=weekly_data[json_object_counter]['nonGsaDemand'] if weekly_data[json_object_counter]['nonGsaDemand'] is not None else 0
                total_gsa_demand=weekly_data[json_object_counter]['gsaDemand'] if weekly_data[json_object_counter]['gsaDemand'] is not None else 0
                total_system_demand=weekly_data[json_object_counter]['systemDemand'] if weekly_data[json_object_counter]['systemDemand'] is not None else 0
                total_forecast_demand=weekly_data[json_object_counter]['forecastDemand'] if weekly_data[json_object_counter]['forecastDemand'] is not None else 0
                total_nongsa_supply=weekly_data[json_object_counter]['nongsaSupplyPlusAdvanceCommitIrp'] if weekly_data[json_object_counter]['nongsaSupplyPlusAdvanceCommitIrp'] is not None else 0
                total_gsa_supply=weekly_data[json_object_counter]['gsaSupplyPlusAdvanceCommitIrp'] if weekly_data[json_object_counter]['gsaSupplyPlusAdvanceCommitIrp'] is not None else 0
                total_system_supply=weekly_data[json_object_counter]['systemSupplyPlusAdvanceCommitIrp'] if weekly_data[json_object_counter]['systemSupplyPlusAdvanceCommitIrp'] is not None else 0
                total_forecast_supply=weekly_data[json_object_counter]['forecastSupplyPlusAdvanceCommitIrp'] if weekly_data[json_object_counter]['forecastSupplyPlusAdvanceCommitIrp'] is not None else 0
                month_list[row_counter][5].append(round(total_nongsa_demand))
                month_list[row_counter][6].append(round(total_nongsa_supply))
                month_list[row_counter][7].append(round(total_gsa_demand))
                month_list[row_counter][8].append(round(total_gsa_supply))
                month_list[row_counter][9].append(round(total_system_demand))
                month_list[row_counter][10].append(round(total_system_supply))
                month_list[row_counter][11].append(round(total_forecast_demand))
                month_list[row_counter][12].append(round(total_forecast_supply))
    #             print('if executed for',i,weekly_data[json_object_counter]['week'],dmp_week[i],nongsa_percentage,gsa_percentage,forecast_percentage,system_percentage,total_percentage)
                if json_object_counter!=len(weekly_data)-1:
                    json_object_counter+=1
            else:
                nongsa_percentage=None
                month_list[row_counter][0].append(nongsa_percentage)
    #             print('else statement for',i)
                gsa_percentage=None
                month_list[row_counter][1].append(gsa_percentage)
                system_percentage=None
                month_list[row_counter][2].append(system_percentage)
                forecast_percentage=None
                month_list[row_counter][3].append(forecast_percentage)
                total_percentage=None
                month_list[row_counter][4].append(total_percentage)
                month_list[row_counter][5].append(0)
                month_list[row_counter][6].append(0)
                month_list[row_counter][7].append(0)
                month_list[row_counter][8].append(0)
                month_list[row_counter][9].append(0)
                month_list[row_counter][10].append(0)
                month_list[row_counter][11].append(0)
                month_list[row_counter][12].append(0)
    
        
    month_dict={'supplier':source_supplier_list,'coe':coe_list,'product_line':product_line_list,'product_family':product_family_list,
   'overall_nongsa':overall_nongsa_list,'overall_gsa':overall_gsa_list,'overall_system':overall_system_list,'overall_forecast':overall_forecast_list,'overall_total':overall_total_list,
   'month0_nongsa':month_list[0][0],'month0_gsa':month_list[0][1],'month0_system':month_list[0][2],'month0_forecast':month_list[0][3],'month0_total':month_list[0][4],
   'month1_nongsa':month_list[1][0],'month1_gsa':month_list[1][1],'month1_system':month_list[1][2],'month1_forecast':month_list[1][3],'month1_total':month_list[1][4],
   'month2_nongsa':month_list[2][0],'month2_gsa':month_list[2][1],'month2_system':month_list[2][2],'month2_forecast':month_list[2][3],'month2_total':month_list[2][4]}
    
    print(month_list[1][8],month_list[1][7])
    total_df={'supplier':[''],'coe':[''],'product_line':[''],'product_family':['Total'],
        'overall_nongsa':[round((sum(month_list[0][6])+sum(month_list[1][6])+sum(month_list[2][6]))*100/(sum(month_list[0][5])+sum(month_list[1][5])+sum(month_list[2][5])))] if (sum(month_list[0][5])+sum(month_list[1][5])+sum(month_list[2][5]))!=0 else None,
        'overall_gsa':[round((sum(month_list[0][8])+sum(month_list[1][8])+sum(month_list[2][8]))*100/(sum(month_list[0][7])+sum(month_list[1][7])+sum(month_list[2][7])))] if (sum(month_list[0][7])+sum(month_list[1][7])+sum(month_list[2][7]))!=0 else None,	
        'overall_system':[round((sum(month_list[0][10])+sum(month_list[1][10])+sum(month_list[2][10]))*100/(sum(month_list[0][9])+sum(month_list[1][9])+sum(month_list[2][9])))] if (sum(month_list[0][9])+sum(month_list[1][9])+sum(month_list[2][9]))!=0 else None,	
        'overall_forecast':[round((sum(month_list[0][12])+sum(month_list[1][12])+sum(month_list[2][12]))*100/(sum(month_list[0][11])+sum(month_list[1][11])+sum(month_list[2][11])))] if (sum(month_list[0][11])+sum(month_list[1][11])+sum(month_list[2][11]))!=0 else None,	
        'overall_total':[round((
        (sum(month_list[0][6])+sum(month_list[0][8])+sum(month_list[0][10])+sum(month_list[0][12]))+
        (sum(month_list[1][6])+sum(month_list[1][8])+sum(month_list[1][10])+sum(month_list[1][12]))+
        (sum(month_list[2][6])+sum(month_list[2][8])+sum(month_list[2][10])+sum(month_list[2][12])))*100/
        ((sum(month_list[0][5])+sum(month_list[0][7])+sum(month_list[0][9])+sum(month_list[0][11]))+
        (sum(month_list[1][5])+sum(month_list[1][7])+sum(month_list[1][9])+sum(month_list[1][11]))+
        (sum(month_list[2][5])+sum(month_list[2][7])+sum(month_list[2][9])+sum(month_list[2][11]))))] 
        if ((sum(month_list[0][5])+sum(month_list[0][7])+sum(month_list[0][9])+sum(month_list[0][11]))+
        (sum(month_list[1][5])+sum(month_list[1][7])+sum(month_list[1][9])+sum(month_list[1][11]))+
        (sum(month_list[2][5])+sum(month_list[2][7])+sum(month_list[2][9])+sum(month_list[2][11])))!=0 else None,
    
        'month0_nongsa':[round(sum(month_list[0][6])/sum(month_list[0][5])*100) if sum(month_list[0][5])!=0 else None],
        'month0_gsa':[round(sum(month_list[0][8])/sum(month_list[0][7])*100)if sum(month_list[0][7])!=0 else None],
        'month0_system':[round(sum(month_list[0][10])/sum(month_list[0][9])*100) if sum(month_list[0][9])!=0 else None],
        'month0_forecast':[round(sum(month_list[0][12])/sum(month_list[0][11])*100) if sum(month_list[0][11])!=0 else None],
        'month0_total':[round(((sum(month_list[0][6])+sum(month_list[0][8])+sum(month_list[0][10])+sum(month_list[0][12]))*100)/
          (sum(month_list[0][5])+sum(month_list[0][7])+sum(month_list[0][9])+sum(month_list[0][11])))] if 
          (sum(month_list[0][5])+sum(month_list[0][7])+sum(month_list[0][9])+sum(month_list[0][11]))!=0 else None,
        'month1_nongsa':[round(sum(month_list[1][6])/sum(month_list[1][5])*100) if sum(month_list[1][5])!=0 else None],
        'month1_gsa':[round(sum(month_list[1][8])/sum(month_list[1][7])*100)if sum(month_list[1][7])!=0 else None],
        'month1_system':[round(sum(month_list[1][10])/sum(month_list[1][9])*100) if sum(month_list[1][9])!=0 else None],
        'month1_forecast':[round(sum(month_list[1][12])/sum(month_list[1][11])*100) if sum(month_list[1][11])!=0 else None],
        'month1_total':[round(((sum(month_list[1][6])+sum(month_list[1][8])+sum(month_list[1][10])+sum(month_list[1][12]))*100)/
          (sum(month_list[1][5])+sum(month_list[1][7])+sum(month_list[1][9])+sum(month_list[1][11])))] if 
          (sum(month_list[1][5])+sum(month_list[1][7])+sum(month_list[1][9])+sum(month_list[1][11]))!=0 else None,
    
        'month2_nongsa':[round(sum(month_list[2][6])/sum(month_list[2][5])*100) if sum(month_list[2][5])!=0 else None],
        'month2_gsa':[round(sum(month_list[2][8])/sum(month_list[2][7])*100) if sum(month_list[2][7])!=0 else None],
        'month2_system':[round(sum(month_list[2][10])/sum(month_list[2][9])*100) if sum(month_list[2][9])!=0 else None],
        'month2_forecast':[round(sum(month_list[2][12])/sum(month_list[2][11])*100)if sum(month_list[2][11])!=0 else None],
        'month2_total':[round(((sum(month_list[2][6])+sum(month_list[2][8])+sum(month_list[2][10])+sum(month_list[2][12]))*100)/
          (sum(month_list[2][5])+sum(month_list[2][7])+sum(month_list[2][9])+sum(month_list[2][11])))] if 
          (sum(month_list[2][5])+sum(month_list[2][7])+sum(month_list[2][9])+sum(month_list[2][11]))!=0 else None}    
          
    
    df2=pd.DataFrame(data=month_dict)
    
    df4=pd.DataFrame(data=total_df)

    total_irp_df=pd.concat([df2,df4])

    col=['source_supplier','coe','product_line','product_family',
        'overall_nongsa','overall_gsa','overall_system','overall_forecast','overall_total',
        [['month1_nongsa','month1_gsa','month1_system','month1_forecast','month1_total'],
        ['month2_nongsa','month2_gsa','month2_system','month2_forecast','month2_total'],
        ['month3_nongsa','month3_gsa','month3_system','month3_forecast','month3_total']]]
    
    rename_col(col,9,dmp_month)
    renamed_final_col=[]
    final_col=[]
    renamed_final_col=removeNestings(col, final_col)
    total_irp_df.columns=renamed_final_col
    
    over_all_replacement=total_irp_df[['coe','product_line','product_family','overall_nongsa','overall_gsa','overall_system','overall_forecast','overall_total']]
    total_irp_df.drop(['overall_nongsa','overall_gsa','overall_system','overall_forecast','overall_total'],axis=1,inplace=True)
    final_irp_summary=total_irp_df.merge(over_all_replacement,on=['coe','product_line','product_family'])
    
    final_irp_summary.columns=list(map(lambda x: x.replace('gsa',catogory_name['gsa']), final_irp_summary))
    final_irp_summary.columns=list(map(lambda x: x.replace('non{}'.format(catogory_name['gsa']),catogory_name['nongsa']), final_irp_summary))
    
    final_df=final_irp_summary.to_csv(index=False,header=True)
    s3=boto3.client("s3")
    s3.put_object(ACL='public-read',Body=final_df,Bucket=S3_BUCKET,Key="Archival_Reports/"+supplier_short_name+"/IRP_summary_commit_extract/"+supplier_short_name+"_IRP_Summary_monthly("+sng_time+").csv")
    object_url="""https://{}.s3-ap-southeast-1.amazonaws.com/Archival_Reports/{}/IRP_summary_commit_extract/{}_IRP_Summary_monthly({}).csv""".format(S3_BUCKET,supplier_short_name,supplier_short_name,formated_time)
    
    
    return object_url
        