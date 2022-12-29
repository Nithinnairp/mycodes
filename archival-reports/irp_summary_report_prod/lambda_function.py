import pymysql
import json
import pandas as pd
import datetime
import boto3
import requests
from DB_conn import condb,condb_dict
from monthly_report import monthly_report 
from urllib.parse import quote
import os

my_date=datetime.date.today()
current_time=datetime.datetime.now()
curr_year,week_num,day_of_week=my_date.isocalendar()
curr_year=str(curr_year)[2:]
print(curr_year,week_num,day_of_week)
print(current_time)
DB_NAME=os.environ['DB_NAME']
S3_BUCKET = os.environ['S3_BUCKET']
SENDER=os.environ['SENDER']
DOMAIN=os.environ['DOMAIN']

def trimming_dmp_period(number_of_week,col):
        to_be_trimmed=12-number_of_week
        while to_be_trimmed>=0:
            print(to_be_trimmed,col[9][to_be_trimmed])
            col[9].pop(to_be_trimmed)
            to_be_trimmed=to_be_trimmed-1
            print(to_be_trimmed)
            
def rename_col(col,col_position,dmp_week):
    count=0
    for i in col[col_position]:
        print(count,i)
        i[0]='week'+str(dmp_week[count])+'_nongsa'
        i[1]='week'+str(dmp_week[count])+'_gsa'
        i[2]='week'+str(dmp_week[count])+'_system'
        i[3]='week'+str(dmp_week[count])+'_forecast'
        i[4]='week'+str(dmp_week[count])+'_total'
        count+=1

def removeNestings(x,final_col):
    for i in x: 
        if type(i) == list: 
            removeNestings(i,final_col) 
        else: 
            final_col.append(i)
    return final_col
    
def get_all_suppliers():
    get_suppliers_query="""select source_supplier_id,source_supplier from {db_name}.forecast_plan group by source_supplier;""".format(db_name=DB_NAME)
    suppliers_list=condb_dict(get_suppliers_query)
    return suppliers_list

def get_recipients(col, supplier_id):
    query="""select {column} from {db_name}.active_suppliers where id = {id};""".format(column=col, db_name=DB_NAME, id=supplier_id)
    recipient_result=condb(query)
    try:
        recipients = recipient_result[0][0]
    except:
        recipients = None
    # convert to list type
    if recipients is not None:
        list_of_recipients=recipients.split(',')
    else:
        list_of_recipients = []
    return list_of_recipients
    


def lambda_handler(event, context):
    date_str=current_time.strftime("%Y-%m-%d %H:%M")
    sng_time=(current_time+datetime.timedelta(hours=8)).strftime("%Y-%m-%d %H_%M")
    formated_time=(current_time+datetime.timedelta(hours=8)).strftime("%Y-%m-%d %H_%M").replace(' ','+')
    
    def process(source_supplier_name, supplier_id):
        encoded_supplier_name = quote(source_supplier_name)
        supplier_short_name = source_supplier_name.split()[0]
        object_url_list=[]
        get_phase="""{}/api/irp-summary/weekly-data?page=0&size=20000&sort=division,asc&sort=productLine,asc&sort=productFamily,asc&isDecommitLine=false&sourceSupplier.in={}&filterType=irp-summary""".format(DOMAIN,encoded_supplier_name)
        print(get_phase)
        response=requests.get(get_phase)
        df=response.content
        phase_object=json.loads(response.content)
        try:
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
        
        
        def send_mail(source_supplier, supplier_id_input):
            # Get email recipients and remove duplicates using set and convert to list back
            RECIPIENT = ['nithin.p@aspirenxt.com']
            # list(set(get_recipients('Archival_Validation_IRP_SUMM', supplier_id_input)) | set(get_recipients('Archival_Validation_common', supplier_id_input)))  
            client = boto3.client('ses',"us-east-1")
            response = client.send_email(Source=SENDER,Destination={'ToAddresses':RECIPIENT},
                            Message={
                                'Subject': {
                                    'Data': "IRP Summary Report for {} WW{}".format(source_supplier, week_num)
                                },
                                'Body': {
                                    'Text': {
                                        'Data': """Hello,\n\r\nPlease find the IRP Summary Report generated for all suppliers as of {}, download the file using below url.
                                        \n\n{}_weekly_report_URL- {} \n\n{}_monthly_report_URL- {}
                                        \n\nThanks & Regards,\nKeysightIT-Team\n\n\n""".format(sng_time,source_supplier,object_url_list[0],source_supplier,object_url_list[1])
                                    }
                                }
                            })
        
        get_dmp_week="""select distinct kc.prod_week,kc.prod_year from {0}.keysight_calendar as kc,
                    (select prod_date,prod_month,prod_year from {0}.keysight_calendar where prod_week ={1} and prod_year ={2}) as kc_curr
                    where concat(kc.prod_year,kc.prod_month)
                    in (
                    concat(kc_curr.prod_year,kc_curr.prod_month),
                    if (kc_curr.prod_month=12,concat(kc_curr.prod_year+1,1),concat(kc_curr.prod_year,kc_curr.prod_month+1)),
                    if (kc_curr.prod_month=11,concat(kc_curr.prod_year+1,1),if (kc_curr.prod_month=12,concat(kc_curr.prod_year+1,2),concat(kc_curr.prod_year,kc_curr.prod_month+2)))
                    )
                    and kc.prod_date >= kc_curr.prod_date;""".format(DB_NAME,week_num,curr_year)
        week_year=condb(get_dmp_week)
        df=pd.DataFrame(week_year,columns=['prod_week','prod_year'])
        dmp_week=[]
        dmp_year=[]
        for x,y in df.iterrows():
            dmp_week.append(y.prod_week)
            dmp_year.append(y.prod_year)
        dmp_year=list(set(dmp_year))
        # dmp_week=[13, 14, 15, 16, 17, 18, 19, 20, 21]
        
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
        week_list=[[[],[],[],[],[],[],[],[],[],[],[],[],[]],
                   [[],[],[],[],[],[],[],[],[],[],[],[],[]],
                   [[],[],[],[],[],[],[],[],[],[],[],[],[]],
                   [[],[],[],[],[],[],[],[],[],[],[],[],[]],
                   [[],[],[],[],[],[],[],[],[],[],[],[],[]],
                   [[],[],[],[],[],[],[],[],[],[],[],[],[]],
                   [[],[],[],[],[],[],[],[],[],[],[],[],[]],
                   [[],[],[],[],[],[],[],[],[],[],[],[],[]],
                   [[],[],[],[],[],[],[],[],[],[],[],[],[]],           
                   [[],[],[],[],[],[],[],[],[],[],[],[],[]],
                   [[],[],[],[],[],[],[],[],[],[],[],[],[]],
                   [[],[],[],[],[],[],[],[],[],[],[],[],[]],
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
            
            weekly_data=y['commitWeekDataDTOs']
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
            for i in range(0,number_of_week):
                # print(len(weekly_data),i,json_object_counter)
                # print(json_object_counter,source_supplier,coe,product_line,product_family)
                week=weekly_data[json_object_counter]['week']
                year=weekly_data[json_object_counter]['year']
        #         print('week',week,'week_counter',week_counter)
                row_counter=i
        
                if (week==dmp_week[i] and year==dmp_year[0]) or (week==dmp_week[i] and year==dmp_year[1]):
                    nongsa_percentage=round(weekly_data[json_object_counter]['nonGsaCommitPercentage'],0) if weekly_data[json_object_counter]['nonGsaCommitPercentage'] is not None else None
                    week_list[row_counter][0].append(nongsa_percentage)
                    gsa_percentage=round(weekly_data[json_object_counter]['gsaCommitPercentage'],0) if weekly_data[json_object_counter]['gsaCommitPercentage'] is not None else None
                    week_list[row_counter][1].append(gsa_percentage)
                    system_percentage=round(weekly_data[json_object_counter]['systemCommitPercentage'],0) if weekly_data[json_object_counter]['systemCommitPercentage'] is not None else None
                    week_list[row_counter][2].append(system_percentage)
                    forecast_percentage=round(weekly_data[json_object_counter]['forecastCommitPercentage'],0) if weekly_data[json_object_counter]['forecastCommitPercentage'] is not None else None
                    week_list[row_counter][3].append(forecast_percentage)
                    total_percentage=round(weekly_data[json_object_counter]['totalCommitPercentage'],0) if weekly_data[json_object_counter]['totalCommitPercentage'] is not None else None
                    week_list[row_counter][4].append(total_percentage)
                    total_nongsa_demand=weekly_data[json_object_counter]['nonGsaDemand'] if weekly_data[json_object_counter]['nonGsaDemand'] is not None else 0
                    total_gsa_demand=weekly_data[json_object_counter]['gsaDemand'] if weekly_data[json_object_counter]['gsaDemand'] is not None else 0
                    total_system_demand=weekly_data[json_object_counter]['systemDemand'] if weekly_data[json_object_counter]['systemDemand'] is not None else 0
                    total_forecast_demand=weekly_data[json_object_counter]['forecastDemand'] if weekly_data[json_object_counter]['forecastDemand'] is not None else 0
                    total_nongsa_supply=weekly_data[json_object_counter]['nongsaSupplyPlusAdvanceCommitIrp'] if weekly_data[json_object_counter]['nongsaSupplyPlusAdvanceCommitIrp'] is not None else 0
                    total_gsa_supply=weekly_data[json_object_counter]['gsaSupplyPlusAdvanceCommitIrp'] if weekly_data[json_object_counter]['gsaSupplyPlusAdvanceCommitIrp'] is not None else 0
                    total_system_supply=weekly_data[json_object_counter]['systemSupplyPlusAdvanceCommitIrp'] if weekly_data[json_object_counter]['systemSupplyPlusAdvanceCommitIrp'] is not None else 0
                    total_forecast_supply=weekly_data[json_object_counter]['forecastSupplyPlusAdvanceCommitIrp'] if weekly_data[json_object_counter]['forecastSupplyPlusAdvanceCommitIrp'] is not None else 0
                    week_list[row_counter][5].append(round(total_nongsa_demand))
                    week_list[row_counter][6].append(round(total_nongsa_supply))
                    week_list[row_counter][7].append(round(total_gsa_demand))
                    week_list[row_counter][8].append(round(total_gsa_supply))
                    week_list[row_counter][9].append(round(total_system_demand))
                    week_list[row_counter][10].append(round(total_system_supply))
                    week_list[row_counter][11].append(round(total_forecast_demand))
                    week_list[row_counter][12].append(round(total_forecast_supply))
                    # print('if executed for',i,weekly_data[json_object_counter]['week'],dmp_week[i],nongsa_percentage,gsa_percentage,forecast_percentage,system_percentage,total_percentage)
                    if json_object_counter!=len(weekly_data)-1:
                        json_object_counter+=1
                else:
                    nongsa_percentage=None
                    week_list[row_counter][0].append(nongsa_percentage)
                    print('else statement for',i)
                    gsa_percentage=None
                    week_list[row_counter][1].append(gsa_percentage)
                    system_percentage=None
                    week_list[row_counter][2].append(system_percentage)
                    forecast_percentage=None
                    week_list[row_counter][3].append(forecast_percentage)
                    total_percentage=None
                    week_list[row_counter][4].append(total_percentage)
                    week_list[row_counter][5].append(0)
                    week_list[row_counter][6].append(0)
                    week_list[row_counter][7].append(0)
                    week_list[row_counter][8].append(0)
                    week_list[row_counter][9].append(0)
                    week_list[row_counter][10].append(0)
                    week_list[row_counter][11].append(0)
                    week_list[row_counter][12].append(0)
                    
        invalid_weeks=13-number_of_week
        week_dict={'supplier':source_supplier_list,'coe':coe_list,'product_line':product_line_list,'product_family':product_family_list,
           'overall_nongsa':overall_nongsa_list,'overall_gsa':overall_gsa_list,'overall_system':overall_system_list,'overall_forecast':overall_forecast_list,'overall_total':overall_total_list,
           'week0_nongsa':week_list[0][0],'week0_gsa':week_list[0][1],'week0_system':week_list[0][2],'week0_forecast':week_list[0][3],'week0_total':week_list[0][4],
           'week1_nongsa':week_list[1][0],'week1_gsa':week_list[1][1],'week1_system':week_list[1][2],'week1_forecast':week_list[1][3],'week1_total':week_list[1][4],
           'week2_nongsa':week_list[2][0],'week2_gsa':week_list[2][1],'week2_system':week_list[2][2],'week2_forecast':week_list[2][3],'week2_total':week_list[2][4],
           'week3_nongsa':week_list[3][0],'week3_gsa':week_list[3][1],'week3_system':week_list[3][2],'week3_forecast':week_list[3][3],'week3_total':week_list[3][4],
           'week4_nongsa':week_list[4][0],'week4_gsa':week_list[4][1],'week4_system':week_list[4][2],'week4_forecast':week_list[4][3],'week4_total':week_list[4][4],
           'week5_nongsa':week_list[5][0],'week5_gsa':week_list[5][1],'week5_system':week_list[5][2],'week5_forecast':week_list[5][3],'week5_total':week_list[5][4],
           'week6_nongsa':week_list[6][0],'week6_gsa':week_list[6][1],'week6_system':week_list[6][2],'week6_forecast':week_list[6][3],'week6_total':week_list[6][4],
           'week7_nongsa':week_list[7][0],'week7_gsa':week_list[7][1],'week7_system':week_list[7][2],'week7_forecast':week_list[7][3],'week7_total':week_list[7][4],
           'week8_nongsa':week_list[8][0],'week8_gsa':week_list[8][1],'week8_system':week_list[8][2],'week8_forecast':week_list[8][3],'week8_total':week_list[8][4],
           'week9_nongsa':week_list[9][0],'week9_gsa':week_list[9][1],'week9_system':week_list[9][2],'week9_forecast':week_list[9][3],'week9_total':week_list[9][4],
           'week10_nongsa':week_list[10][0],'week10_gsa':week_list[10][1],'week10_system':week_list[10][2],'week10_forecast':week_list[10][3],'week10_total':week_list[10][4],
           'week11_nongsa':week_list[11][0],'week11_gsa':week_list[11][1],'week11_system':week_list[11][2],'week11_forecast':week_list[11][3],'week11_total':week_list[11][4],
           'week12_nongsa':week_list[12][0],'week12_gsa':week_list[12][1],'week12_system':week_list[12][2],'week12_forecast':week_list[12][3],'week12_total':week_list[12][4]}
        
        print("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$",week_list) 
        total_df={'supplier':[''],'coe':[''],'product_line':[''],'product_family':['Total'],
            'overall_nongsa':[round((sum(week_list[1][6])+sum(week_list[2][6])+sum(week_list[3][6])+sum(week_list[4][6])+sum(week_list[5][6])+
            sum(week_list[6][6])+sum(week_list[7][6])+sum(week_list[8][6])+sum(week_list[9][6])+sum(week_list[10][6])+
            sum(week_list[11][6])+sum(week_list[12][6]))*100/
            (sum(week_list[1][5])+sum(week_list[2][5])+sum(week_list[3][5])+sum(week_list[4][5])+sum(week_list[5][5])+
            sum(week_list[6][5])+sum(week_list[7][5])+sum(week_list[8][5])+sum(week_list[9][5])+sum(week_list[10][5])+
            sum(week_list[11][5])+sum(week_list[12][5])))] if (sum(week_list[1][5])+sum(week_list[2][5])+sum(week_list[3][5])+sum(week_list[4][5])+sum(week_list[5][5])+
            sum(week_list[6][5])+sum(week_list[7][5])+sum(week_list[8][5])+sum(week_list[9][5])+sum(week_list[10][5])+
            sum(week_list[11][5])+sum(week_list[12][5]))!=0 else None,
            'overall_gsa':[round((sum(week_list[1][8])+sum(week_list[2][8])+sum(week_list[3][8])+sum(week_list[4][8])+sum(week_list[5][8])+
            sum(week_list[6][8])+sum(week_list[7][8])+sum(week_list[8][8])+sum(week_list[9][8])+sum(week_list[10][8])+
            sum(week_list[11][8])+sum(week_list[12][8]))*100/
            (sum(week_list[1][7])+sum(week_list[2][7])+sum(week_list[3][7])+sum(week_list[4][7])+sum(week_list[5][7])+
            sum(week_list[6][7])+sum(week_list[7][7])+sum(week_list[8][7])+sum(week_list[9][7])+sum(week_list[10][7])+
            sum(week_list[11][7])+sum(week_list[12][7])))] if (sum(week_list[1][7])+sum(week_list[2][7])+sum(week_list[3][7])+sum(week_list[4][7])+sum(week_list[5][7])+
            sum(week_list[6][7])+sum(week_list[7][7])+sum(week_list[8][7])+sum(week_list[9][7])+sum(week_list[10][7])+
            sum(week_list[11][7])+sum(week_list[12][7]))!=0 else None,	 
            'overall_system':
            [round((sum(week_list[1][10])+sum(week_list[2][10])+sum(week_list[3][10])+sum(week_list[4][10])+sum(week_list[5][10])+
            sum(week_list[6][10])+sum(week_list[7][10])+sum(week_list[8][10])+sum(week_list[9][10])+sum(week_list[10][10])+
            sum(week_list[11][10])+sum(week_list[12][10]))*100/
            (sum(week_list[1][9])+sum(week_list[2][9])+sum(week_list[3][9])+sum(week_list[4][9])+sum(week_list[5][9])+
            sum(week_list[6][9])+sum(week_list[7][9])+sum(week_list[8][9])+sum(week_list[9][9])+sum(week_list[10][9])+
            sum(week_list[11][9])+sum(week_list[12][9])))] 
            if (sum(week_list[1][9])+sum(week_list[2][9])+sum(week_list[3][9])+sum(week_list[4][9])+sum(week_list[5][9])+
            sum(week_list[6][9])+sum(week_list[7][9])+sum(week_list[8][9])+sum(week_list[9][9])+sum(week_list[10][9])+
            sum(week_list[11][9])+sum(week_list[12][9]))!=0 else None,	 
            'overall_forecast':[round((sum(week_list[1][12])+sum(week_list[2][12])+sum(week_list[3][12])+sum(week_list[4][12])+sum(week_list[5][12])+
            sum(week_list[6][12])+sum(week_list[7][12])+sum(week_list[8][12])+sum(week_list[9][12])+sum(week_list[10][12])+
            sum(week_list[11][12])+sum(week_list[12][12]))*100/
            (sum(week_list[1][11])+sum(week_list[2][11])+sum(week_list[3][11])+sum(week_list[4][11])+sum(week_list[5][11])+
            sum(week_list[6][11])+sum(week_list[7][11])+sum(week_list[8][11])+sum(week_list[9][11])+sum(week_list[10][11])+
            sum(week_list[11][11])+sum(week_list[12][11])))] if (sum(week_list[1][11])+sum(week_list[2][11])+sum(week_list[3][11])+sum(week_list[4][11])+sum(week_list[5][11])+
            sum(week_list[6][11])+sum(week_list[7][11])+sum(week_list[8][11])+sum(week_list[9][11])+sum(week_list[10][11])+
            sum(week_list[11][11])+sum(week_list[12][11]))!=0 else None,
            'overall_total':[float(round((
            (sum(week_list[1][6])+sum(week_list[1][8])+sum(week_list[1][10])+sum(week_list[1][12]))+
            (sum(week_list[2][6])+sum(week_list[2][8])+sum(week_list[2][10])+sum(week_list[2][12]))+
            (sum(week_list[3][6])+sum(week_list[3][8])+sum(week_list[3][10])+sum(week_list[3][12]))+
            (sum(week_list[4][6])+sum(week_list[4][8])+sum(week_list[4][10])+sum(week_list[4][12]))+
            (sum(week_list[5][6])+sum(week_list[5][8])+sum(week_list[5][10])+sum(week_list[5][12]))+
            (sum(week_list[6][6])+sum(week_list[6][8])+sum(week_list[6][10])+sum(week_list[6][12]))+
            (sum(week_list[7][6])+sum(week_list[7][8])+sum(week_list[7][10])+sum(week_list[7][12]))+
            (sum(week_list[8][6])+sum(week_list[8][8])+sum(week_list[8][10])+sum(week_list[8][12]))+
            (sum(week_list[9][6])+sum(week_list[9][8])+sum(week_list[9][10])+sum(week_list[9][12]))+
            (sum(week_list[10][6])+sum(week_list[10][8])+sum(week_list[10][10])+sum(week_list[10][12]))+
            (sum(week_list[11][6])+sum(week_list[11][8])+sum(week_list[11][10])+sum(week_list[11][12]))+
            (sum(week_list[12][6])+sum(week_list[12][8])+sum(week_list[12][10])+sum(week_list[12][12])))*100/
            ((sum(week_list[1][5])+sum(week_list[1][7])+sum(week_list[1][9])+sum(week_list[1][11]))+
            (sum(week_list[2][5])+sum(week_list[2][7])+sum(week_list[2][9])+sum(week_list[2][11]))+
            (sum(week_list[3][5])+sum(week_list[3][7])+sum(week_list[3][9])+sum(week_list[3][11]))+
            (sum(week_list[4][5])+sum(week_list[4][7])+sum(week_list[4][9])+sum(week_list[4][11]))+
            (sum(week_list[5][5])+sum(week_list[5][7])+sum(week_list[5][9])+sum(week_list[5][11]))+
            (sum(week_list[6][5])+sum(week_list[6][7])+sum(week_list[6][9])+sum(week_list[6][11]))+
            (sum(week_list[7][5])+sum(week_list[7][7])+sum(week_list[7][9])+sum(week_list[7][11]))+
            (sum(week_list[8][5])+sum(week_list[8][7])+sum(week_list[8][9])+sum(week_list[8][11]))+
            (sum(week_list[9][5])+sum(week_list[9][7])+sum(week_list[9][9])+sum(week_list[9][11]))+
            (sum(week_list[10][5])+sum(week_list[10][7])+sum(week_list[10][9])+sum(week_list[10][11]))+
            (sum(week_list[11][5])+sum(week_list[11][7])+sum(week_list[11][9])+sum(week_list[11][11]))+
            (sum(week_list[12][5])+sum(week_list[12][7])+sum(week_list[12][9])+sum(week_list[12][11])))))] 
            if ((sum(week_list[1][5])+sum(week_list[1][7])+sum(week_list[1][9])+sum(week_list[1][11]))+
            (sum(week_list[2][5])+sum(week_list[2][7])+sum(week_list[2][9])+sum(week_list[2][11]))+
            (sum(week_list[3][5])+sum(week_list[3][7])+sum(week_list[3][9])+sum(week_list[3][11]))+
            (sum(week_list[4][5])+sum(week_list[4][7])+sum(week_list[4][9])+sum(week_list[4][11]))+
            (sum(week_list[5][5])+sum(week_list[5][7])+sum(week_list[5][9])+sum(week_list[5][11]))+
            (sum(week_list[6][5])+sum(week_list[6][7])+sum(week_list[6][9])+sum(week_list[6][11]))+
            (sum(week_list[7][5])+sum(week_list[7][7])+sum(week_list[7][9])+sum(week_list[7][11]))+
            (sum(week_list[8][5])+sum(week_list[8][7])+sum(week_list[8][9])+sum(week_list[8][11]))+
            (sum(week_list[9][5])+sum(week_list[9][7])+sum(week_list[9][9])+sum(week_list[9][11]))+
            (sum(week_list[10][5])+sum(week_list[10][7])+sum(week_list[10][9])+sum(week_list[10][11]))+
            (sum(week_list[11][5])+sum(week_list[11][7])+sum(week_list[11][9])+sum(week_list[11][11]))+
            (sum(week_list[12][5])+sum(week_list[12][7])+sum(week_list[12][9])+sum(week_list[12][11])))!=0 else None,
            
            'week0_nongsa':0,'week0_gsa':0,'week0_system':0,'week0_forecast':0,'week0_total':0,
            'week1_nongsa':[round(sum(week_list[1][6])/sum(week_list[1][5])*100) if sum(week_list[1][5])!=0 else None],
            'week1_gsa':[round(sum(week_list[1][8])/sum(week_list[1][7])*100)if sum(week_list[1][7])!=0 else None],
            'week1_system':[round(sum(week_list[1][10])/sum(week_list[1][9])*100) if sum(week_list[1][9])!=0 else None],
            'week1_forecast':[round(sum(week_list[1][12])/sum(week_list[1][11])*100) if sum(week_list[1][11])!=0 else None],
            'week1_total':[round(((sum(week_list[1][6])+sum(week_list[1][8])+sum(week_list[1][10])+sum(week_list[1][12]))*100)/
            (sum(week_list[1][5])+sum(week_list[1][7])+sum(week_list[1][9])+sum(week_list[1][11])))] if 
            (sum(week_list[1][5])+sum(week_list[1][7])+sum(week_list[1][9])+sum(week_list[1][11]))!=0 else None,
            
            'week2_nongsa':[round(sum(week_list[2][6])/sum(week_list[2][5])*100) if sum(week_list[2][5])!=0 else None],
            'week2_gsa':[round(sum(week_list[2][8])/sum(week_list[2][7])*100) if sum(week_list[2][7])!=0 else None],
            'week2_system':[round(sum(week_list[2][10])/sum(week_list[2][9])*100) if sum(week_list[2][9])!=0 else None],
            'week2_forecast':[round(sum(week_list[2][12])/sum(week_list[2][11])*100)if sum(week_list[2][11])!=0 else None],
            'week2_total':[round(((sum(week_list[2][6])+sum(week_list[2][8])+sum(week_list[2][10])+sum(week_list[2][12]))*100)/
            (sum(week_list[2][5])+sum(week_list[2][7])+sum(week_list[2][9])+sum(week_list[2][11])))] if 
            (sum(week_list[2][5])+sum(week_list[2][7])+sum(week_list[2][9])+sum(week_list[2][11]))!=0 else None,
            
            'week3_nongsa':[round(sum(week_list[3][6])/sum(week_list[3][5])*100) if sum(week_list[3][5])!=0 else None],
            'week3_gsa':[round(sum(week_list[3][8])/sum(week_list[3][7])*100) if sum(week_list[3][7])!=0 else None],
            'week3_system':[round(sum(week_list[3][10])/sum(week_list[3][9])*100) if sum(week_list[3][9])!=0 else None],
            'week3_forecast':[round(sum(week_list[3][12])/sum(week_list[3][11])*100)if sum(week_list[3][11])!=0 else None],
            'week3_total':[round(((sum(week_list[3][6])+sum(week_list[3][8])+sum(week_list[3][10])+sum(week_list[3][12]))*100)/
            (sum(week_list[3][5])+sum(week_list[3][7])+sum(week_list[3][9])+sum(week_list[3][11])))] if 
            (sum(week_list[3][5])+sum(week_list[3][7])+sum(week_list[3][9])+sum(week_list[3][11]))!=0 else None,
            
            'week4_nongsa':[round(sum(week_list[4][6])/sum(week_list[4][5])*100) if sum(week_list[4][5])!=0 else None],
            'week4_gsa':[round(sum(week_list[4][8])/sum(week_list[4][7])*100) if sum(week_list[4][7])!=0 else None],
            'week4_system':[round(sum(week_list[4][10])/sum(week_list[4][9])*100) if sum(week_list[4][9])!=0 else None],
            'week4_forecast':[round(sum(week_list[4][12])/sum(week_list[4][11])*100)if sum(week_list[4][11])!=0 else None],
            'week4_total':[round(((sum(week_list[4][6])+sum(week_list[4][8])+sum(week_list[4][10])+sum(week_list[4][12]))*100)/
            (sum(week_list[4][5])+sum(week_list[4][7])+sum(week_list[4][9])+sum(week_list[4][11])))] if 
            (sum(week_list[4][5])+sum(week_list[4][7])+sum(week_list[4][9])+sum(week_list[4][11]))!=0 else None,
            
            'week5_nongsa':[round(sum(week_list[5][6])/sum(week_list[5][5])*100) if sum(week_list[5][5])!=0 else None],
            'week5_gsa':[round(sum(week_list[5][8])/sum(week_list[5][7])*100) if sum(week_list[5][7])!=0 else None],
            'week5_system':[round(sum(week_list[5][10])/sum(week_list[5][9])*100) if sum(week_list[5][9])!=0 else None],
            'week5_forecast':[round(sum(week_list[5][12])/sum(week_list[5][11])*100)if sum(week_list[5][11])!=0 else None],
            'week5_total':[round(((sum(week_list[5][6])+sum(week_list[5][8])+sum(week_list[5][10])+sum(week_list[5][12]))*100)/
            (sum(week_list[5][5])+sum(week_list[5][7])+sum(week_list[5][9])+sum(week_list[5][11])))] if 
            (sum(week_list[5][5])+sum(week_list[5][7])+sum(week_list[5][9])+sum(week_list[5][11]))!=0 else None,
            
            'week6_nongsa':[round(sum(week_list[6][6])/sum(week_list[6][5])*100) if sum(week_list[6][5])!=0 else None],
            'week6_gsa':[round(sum(week_list[6][8])/sum(week_list[6][7])*100) if sum(week_list[6][7])!=0 else None],
            'week6_system':[round(sum(week_list[6][10])/sum(week_list[6][9])*100) if sum(week_list[6][9])!=0 else None],
            'week6_forecast':[round(sum(week_list[6][12])/sum(week_list[6][11])*100)if sum(week_list[6][11])!=0 else None],
            'week6_total':[round(((sum(week_list[6][6])+sum(week_list[6][8])+sum(week_list[6][10])+sum(week_list[6][12]))*100)/
            (sum(week_list[6][5])+sum(week_list[6][7])+sum(week_list[6][9])+sum(week_list[6][11])))] if 
            (sum(week_list[6][5])+sum(week_list[6][7])+sum(week_list[6][9])+sum(week_list[6][11]))!=0 else None,
            
            'week7_nongsa':[round(sum(week_list[7][6])/sum(week_list[7][5])*100) if sum(week_list[7][5])!=0 else None],
            'week7_gsa':[round(sum(week_list[7][8])/sum(week_list[7][7])*100) if sum(week_list[7][7])!=0 else None],
            'week7_system':[round(sum(week_list[7][10])/sum(week_list[7][9])*100) if sum(week_list[7][9])!=0 else None],
            'week7_forecast':[round(sum(week_list[7][12])/sum(week_list[7][11])*100)if sum(week_list[7][11])!=0 else None],
            'week7_total':[round(((sum(week_list[7][6])+sum(week_list[7][8])+sum(week_list[7][10])+sum(week_list[7][12]))*100)/
            (sum(week_list[7][5])+sum(week_list[7][7])+sum(week_list[7][9])+sum(week_list[7][11])))] if 
            (sum(week_list[7][5])+sum(week_list[7][7])+sum(week_list[7][9])+sum(week_list[7][11]))!=0 else None,
            
            'week8_nongsa':[round(sum(week_list[8][6])/sum(week_list[8][5])*100) if sum(week_list[8][5])!=0 else None],
            'week8_gsa':[round(sum(week_list[8][8])/sum(week_list[8][7])*100) if sum(week_list[8][7])!=0 else None],
            'week8_system':[round(sum(week_list[8][10])/sum(week_list[8][9])*100) if sum(week_list[8][9])!=0 else None],
            'week8_forecast':[round(sum(week_list[8][12])/sum(week_list[8][11])*100)if sum(week_list[8][11])!=0 else None],
            'week8_total':[round(((sum(week_list[8][6])+sum(week_list[8][8])+sum(week_list[8][10])+sum(week_list[8][12]))*100)/
            (sum(week_list[8][5])+sum(week_list[8][7])+sum(week_list[8][9])+sum(week_list[8][11])))] if 
            (sum(week_list[8][5])+sum(week_list[8][7])+sum(week_list[8][9])+sum(week_list[8][11]))!=0 else None,
            
            'week9_nongsa':[round(sum(week_list[9][6])/sum(week_list[9][5])*100) if sum(week_list[9][5])!=0 else None],
            'week9_gsa':[round(sum(week_list[9][8])/sum(week_list[9][7])*100) if sum(week_list[9][7])!=0 else None],
            'week9_system':[round(sum(week_list[9][10])/sum(week_list[9][9])*100) if sum(week_list[9][9])!=0 else None],
            'week9_forecast':[round(sum(week_list[9][12])/sum(week_list[9][11])*100)if sum(week_list[9][11])!=0 else None],
            'week9_total':[round(((sum(week_list[9][6])+sum(week_list[9][8])+sum(week_list[9][10])+sum(week_list[9][12]))*100)/
            (sum(week_list[9][5])+sum(week_list[9][7])+sum(week_list[9][9])+sum(week_list[9][11])))] if 
            (sum(week_list[9][5])+sum(week_list[9][7])+sum(week_list[9][9])+sum(week_list[9][11]))!=0 else None,
            
            'week10_nongsa':[round(sum(week_list[10][6])/sum(week_list[10][5])*100) if sum(week_list[10][5])!=0 else None],
            'week10_gsa':[round(sum(week_list[10][8])/sum(week_list[10][7])*100) if sum(week_list[10][7])!=0 else None],
            'week10_system':[round(sum(week_list[10][10])/sum(week_list[10][9])*100) if sum(week_list[10][9])!=0 else None],
            'week10_forecast':[round(sum(week_list[10][12])/sum(week_list[10][11])*100)if sum(week_list[10][11])!=0 else None],
            'week10_total':[round(((sum(week_list[10][6])+sum(week_list[10][8])+sum(week_list[10][10])+sum(week_list[10][12]))*100)/
            (sum(week_list[10][5])+sum(week_list[10][7])+sum(week_list[10][9])+sum(week_list[10][11])))] if 
            (sum(week_list[10][5])+sum(week_list[10][7])+sum(week_list[10][9])+sum(week_list[10][11]))!=0 else None,
            
            'week11_nongsa':[round(sum(week_list[11][6])/sum(week_list[11][5])*100) if sum(week_list[11][5])!=0 else None],
            'week11_gsa':[round(sum(week_list[11][8])/sum(week_list[11][7])*100) if sum(week_list[11][7])!=0 else None],
            'week11_system':[round(sum(week_list[11][10])/sum(week_list[11][9])*100) if sum(week_list[11][9])!=0 else None],
            'week11_forecast':[round(sum(week_list[11][12])/sum(week_list[11][11])*100)if sum(week_list[11][11])!=0 else None],
            'week11_total':[round(((sum(week_list[11][6])+sum(week_list[11][8])+sum(week_list[11][10])+sum(week_list[11][12]))*100)/
            (sum(week_list[11][5])+sum(week_list[11][7])+sum(week_list[11][9])+sum(week_list[11][11])))] if 
            (sum(week_list[11][5])+sum(week_list[11][7])+sum(week_list[11][9])+sum(week_list[11][11]))!=0 else None,
            
            'week12_nongsa':[round(sum(week_list[12][6])/sum(week_list[12][5])*100) if sum(week_list[12][5])!=0 else None],
            'week12_gsa':[round(sum(week_list[12][8])/sum(week_list[12][7])*100) if sum(week_list[12][7])!=0 else None],
            'week12_system':[round(sum(week_list[12][10])/sum(week_list[12][9])*100) if sum(week_list[12][9])!=0 else None],
            'week12_forecast':[round(sum(week_list[12][12])/sum(week_list[12][11])*100)if sum(week_list[12][11])!=0 else None],
            'week12_total':[round(((sum(week_list[12][6])+sum(week_list[12][8])+sum(week_list[12][10])+sum(week_list[12][12]))*100)/
            (sum(week_list[12][5])+sum(week_list[12][7])+sum(week_list[12][9])+sum(week_list[12][11])))] if 
            (sum(week_list[12][5])+sum(week_list[12][7])+sum(week_list[12][9])+sum(week_list[12][11]))!=0 else None}
        
           
        for i in range(0,(invalid_weeks*5)):
            print(list(week_dict)[-1])
            Last_key=list(week_dict)[-1]
            week_dict.pop(Last_key)
        
        for i in range(0,(invalid_weeks*5)):    
            print(list(total_df)[-1])
            Last_key=list(total_df)[-1]
            total_df.pop(Last_key)
                    
        df2=pd.DataFrame(data=week_dict)
        
        df4=pd.DataFrame(data=total_df)
        
        total_irp_df=pd.concat([df2,df4])
        
        col=['source_supplier','coe','product_line','product_family',
            'overall_nongsa','overall_gsa','overall_system','overall_forecast','overall_total',
            [['week1_nongsa','week1_gsa','week1_system','week1_forecast','week1_total'],
            ['week2_nongsa','week2_gsa','week2_system','week2_forecast','week2_total'],
            ['week3_nongsa','week3_gsa','week3_system','week3_forecast','week3_total'],
            ['week4_nongsa','week4_gsa','week4_system','week4_forecast','week4_total'],
            ['week5_nongsa','week5_gsa','week5_system','week5_forecast','week5_total'],
            ['week6_nongsa','week6_gsa','week6_system','week6_forecast','week6_total'],
            ['week7_nongsa','week7_gsa','week7_system','week7_forecast','week7_total'],
            ['week8_nongsa','week8_gsa','week8_system','week8_forecast','week8_total'],
            ['week9_nongsa','week9_gsa','week9_system','week9_forecast','week9_total'],
            ['week10_nongsa','week10_gsa','week10_system','week10_forecast','week10_total'],
            ['week11_nongsa','week11_gsa','week11_system','week11_forecast','week11_total'],
            ['week12_nongsa','week12_gsa','week12_system','week12_forecast','week12_total'],
            ['week13_nongsa','week13_gsa','week13_system','week13_forecast','week13_total']]]
            
        
        trimming_dmp_period(number_of_week,col)
        rename_col(col,9,dmp_week)
        final_col=[]
        renamed_final_col=removeNestings(col, final_col)
        total_irp_df.columns=renamed_final_col
        over_all_replacement=total_irp_df[['coe','product_line','product_family','overall_nongsa','overall_gsa','overall_system','overall_forecast','overall_total']]
        total_irp_df.drop(['overall_nongsa','overall_gsa','overall_system','overall_forecast','overall_total'],axis=1,inplace=True)
        final_irp_summary=pd.merge(total_irp_df,over_all_replacement,on=['product_family','product_line','coe'])
        
        final_irp_summary.columns=list(map(lambda x: x.replace('gsa',catogory_name['gsa']), final_irp_summary))
        final_irp_summary.columns=list(map(lambda x: x.replace('non{}'.format(catogory_name['gsa']),catogory_name['nongsa']), final_irp_summary))
        
        final_csv=final_irp_summary.to_csv(index=False)
        s3=boto3.client("s3")
        s3.put_object(ACL='public-read',Body=final_csv,Bucket=S3_BUCKET,Key="Archival_Reports/"+supplier_short_name+"/IRP_summary_commit_extract/"+supplier_short_name+"_IRP_Summary_weekly("+sng_time+").csv")
        object_url="""https://{}.s3-ap-southeast-1.amazonaws.com/Archival_Reports/{}/IRP_summary_commit_extract/{}_IRP_Summary_weekly({}).csv""".format(S3_BUCKET,supplier_short_name,supplier_short_name,formated_time)
        object_url_list.append(object_url)
        monthly_report_url=monthly_report(source_supplier_name)
        object_url_list.append(monthly_report_url)
        print(object_url_list)
        send_mail(supplier_short_name,supplier_id)
    
    # Get all suppliers and run through each of them
    suppliers=get_all_suppliers()
    for supplier in suppliers:
        if supplier:
            supp_id=supplier['source_supplier_id']
            supp_name=supplier['source_supplier']
            print(supp_id,"-",supp_name)
            process(supp_name,supp_id)  
        else:
            print("Individual supplier list empty")