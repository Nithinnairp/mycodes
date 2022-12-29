import sys
import json
import boto3 
import pandas as pd 
import numpy as np
import datetime
import pytz 
from DB_conn import condb,condb_dict
import requests
from urllib.parse import quote
import os

my_date=datetime.datetime.today()
year,week_num,day_of_week=my_date.isocalendar()
# print(week_num)
complete_current_year=str(year)
current_year=complete_current_year[2:]
DB_NAME = os.environ['DB_NAME']
SENDER = os.environ['SENDER']
S3_BUCKET = os.environ['S3_BUCKET']
DOMAIN = os.environ['DOMAIN']

def lambda_handler(event,context):
    current_time=datetime.datetime.now()
    date_str=current_time.strftime("%Y-%m-%d %H:%M")
    sng_time=(current_time+datetime.timedelta(hours=8)).strftime("%Y-%m-%d %H_%M")
    formated_time=(current_time+datetime.timedelta(hours=8)).strftime("%Y-%m-%d %H_%M").replace(' ','+')
    object_url_list=[]
    
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
    
    def get_all_suppliers():
        get_suppliers_query="""select source_supplier_id,source_supplier from {db_name}.forecast_plan group by source_supplier;""".format(db_name=DB_NAME)
        suppliers_list=condb_dict(get_suppliers_query)
        return suppliers_list

    def get_recipients(col, supplier_id):
        query="""select {} from {}.active_suppliers where id = {};""".format(col, DB_NAME, supplier_id)
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
    
    def irp_summary_percentage_validation(source_supplier_name, supplier_id_input):
        encoded_supplier_name = quote(source_supplier_name)
        supplier_short_name = source_supplier_name.split()[0]
        get_phase="{}/api/irp-summary/weekly-data?page=0&size=20000&sort=division,asc&sort=productLine,asc&sort=productFamily,asc&isDecommitLine=false&sourceSupplier.in={}&filterType=irp-summary".format(DOMAIN,encoded_supplier_name)
        response=requests.get(get_phase)
        df=response.content
        phase_object=json.loads(response.content)
        try:
            del phase_object['deCommitLineDataDTOs']
        except:
            pass
        
        main_df=pd.DataFrame.from_dict(data=phase_object)
        df1=main_df["commitDataDTOs"]
        Supplier_list=[]
        coe_list=[]
        product_line_list=[]
        product_family_list=[]
        week_list=[]
        year_list=[]
        nonGsaCommitPercentage_list=[]
        gsaCommitPercentage_list=[]
        forecastCommitPercentage_list=[]
        systemCommitPercentage_list=[]
        
        for i in df1:
            Supplier=i['sourceSupplier']
            COE=i['coe']
            product_line=i['productLine']
            product_family=i['productFamily']
            weekly_data=i['commitWeekDataDTOs']
            for x in weekly_data:
                # print(x)
                nonGsaCommitPercentage=0 if x['nonGsaCommitPercentage']==None else x['nonGsaCommitPercentage']
                gsaCommitPercentage=0 if x['gsaCommitPercentage']==None else x['gsaCommitPercentage']
                forecastCommitPercentage=0 if x['forecastCommitPercentage']==None else x['forecastCommitPercentage']
                systemCommitPercentage=0 if x['systemCommitPercentage']==None else x['systemCommitPercentage']
                week=x['week']
                year=x['year']
                if (nonGsaCommitPercentage<0)|(nonGsaCommitPercentage>100):
                    Supplier_list.append(Supplier)
                    coe_list.append(COE)
                    product_line_list.append(product_line)
                    product_family_list.append(product_family)
                    week_list.append(week)
                    year_list.append(year)
                    nonGsaCommitPercentage_list.append(nonGsaCommitPercentage)
                    gsaCommitPercentage_list.append(gsaCommitPercentage)
                    forecastCommitPercentage_list.append(forecastCommitPercentage)
                    systemCommitPercentage_list.append(systemCommitPercentage)
                if (gsaCommitPercentage<0)|(gsaCommitPercentage>100):
                    Supplier_list.append(Supplier)
                    coe_list.append(COE)
                    product_line_list.append(product_line)
                    product_family_list.append(product_family)
                    week_list.append(week)
                    year_list.append(year)
                    nonGsaCommitPercentage_list.append(nonGsaCommitPercentage)
                    gsaCommitPercentage_list.append(gsaCommitPercentage)
                    forecastCommitPercentage_list.append(forecastCommitPercentage)
                    systemCommitPercentage_list.append(systemCommitPercentage)
                if (forecastCommitPercentage<0)|(forecastCommitPercentage>100):
                    Supplier_list.append(Supplier)
                    coe_list.append(COE)
                    product_line_list.append(product_line)
                    product_family_list.append(product_family)
                    week_list.append(week)
                    year_list.append(year)
                    nonGsaCommitPercentage_list.append(nonGsaCommitPercentage)
                    gsaCommitPercentage_list.append(gsaCommitPercentage)
                    forecastCommitPercentage_list.append(forecastCommitPercentage)
                    systemCommitPercentage_list.append(systemCommitPercentage)
                if (systemCommitPercentage<0)|(systemCommitPercentage>100):
                    Supplier_list.append(Supplier)
                    coe_list.append(COE)
                    product_line_list.append(product_line)
                    product_family_list.append(product_family)
                    week_list.append(week)
                    year_list.append(year)
                    nonGsaCommitPercentage_list.append(nonGsaCommitPercentage)
                    gsaCommitPercentage_list.append(gsaCommitPercentage)
                    forecastCommitPercentage_list.append(forecastCommitPercentage)
                    systemCommitPercentage_list.append(systemCommitPercentage)
        
        error_records={'Supplier':Supplier_list,'COE':coe_list,'product_line':product_line_list,'product_family':product_family_list,
                   'week':week_list,'year':year_list,'nonGsaCommitPercentage':nonGsaCommitPercentage_list,
                  'gsaCommitPercentage':gsaCommitPercentage_list,'forecastCommitPercentage':forecastCommitPercentage_list,
                'systemCommitPercentage':systemCommitPercentage_list}
                
        result_df=pd.DataFrame.from_dict(error_records)
        
        result_df.columns=list(map(lambda x: x.replace('gsa',catogory_name['gsa']), result_df))
        result_df.columns=list(map(lambda x: x.replace('non{}'.format(catogory_name['gsa']),catogory_name['nongsa']), result_df))
        
        error_records_count=result_df.count()[0]
        # Get email recipients and remove duplicates using set and convert to list back
        RECIPIENT = list(set(get_recipients('Archival_Validation_IRP_SUMM', supplier_id_input)) | set(get_recipients('Archival_Validation_common', supplier_id_input)))  
        
        if error_records_count==0:
            result="No error found in the IRP commit summary percentages for {}".format(supplier_short_name)
            print(result)
            client = boto3.client('ses',"us-east-1")
            response = client.send_email(Source=SENDER,Destination={'ToAddresses':RECIPIENT},
                            Message={
                                'Subject': {
                                    'Data': "IRP Commit Summary , the Commit % {} - Validation Result for work week-{}".format(supplier_short_name, week_num)
                                },
                                'Body': {
                                    'Text': {
                                        'Data': "Hello,\n\r\nNo Records found where commit % <0 or >100 as of {}.\n\nThanks & Regards,\nKeysightIT-Team\n\n\n".format(sng_time)
                                    }
                                }
                            })
        else:
            result="errors found in the IRP commit summary percentages for {}".format(supplier_short_name)
            print(result)
            commit_per_df2=result_df.applymap(lambda x: x[0] if type(x) is bytes else x)
            final_df=commit_per_df2.to_csv(index=False,header=True,encoding='UTF-8')
            
            s3=boto3.client("s3")
            s3.put_object(ACL='public-read',Body=final_df,Bucket=S3_BUCKET,Key="Validation_reports/IRP_commit_summary_percentage_validation_records("+sng_time+").csv")
            object_url="""https://{}.s3-ap-southeast-1.amazonaws.com/Validation_reports/IRP_commit_summary_percentage_validation_records({}).csv""".format(S3_BUCKET, formated_time)
            client = boto3.client('ses',"us-east-1")
            response = client.send_email(Source=SENDER,Destination={'ToAddresses':RECIPIENT},
                            Message={
                                'Subject': {
                                    'Data': "IRP Commit Summary , the Commit % {} - Validation Result for work week-{}".format(supplier_short_name, week_num)
                                },
                                'Body': {
                                    'Text': {
                                        'Data': """Hello,\n\r\nRecords found where commit % <0 or >100 as of {}, download the file using below url.
                                        \n\ndownload-URL- {}
                                        \n\nThanks & Regards,\nKeysightIT-Team\n\n\n""".format(sng_time,object_url)
                                    }
                                }
                            })
        return "Completed"
        
    # Get all suppliers and run through each of them to validate for each suppliers
    suppliers=get_all_suppliers()
    for supplier in suppliers:
        if supplier:
            supp_id=supplier['source_supplier_id']
            supp_name=supplier['source_supplier']
            print(supp_id,"-",supp_name)
            irp_summary_percentage_validation(supp_name, supp_id)  
        else:
            print("Individual supplier list empty")        
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    # # print(phase_object)
    # df1=phase_object['commitDataDTOs'][0]['commitWeekDataDTOs']
    # abc=pd.DataFrame.from_dict(data=df1)
    # df3=abc.fillna({"nonGsaCommitPercentage":0,"gsaCommitPercentage":0,"forecastCommitPercentage":0,"systemCommitPercentage":0})
    # # print(abc)
    # for i in df3.iterrows():
    #     df2=i[1]
    #     print(df2)
    #     if (df2.nonGsaCommitPercentage<0)|(df2.nonGsaCommitPercentage>100):
    #         print("detected",df2.week,df2.nonGsaCommitPercentage)
    #     elif (df2.gsaCommitPercentage<0)|(df2.gsaCommitPercentage>100):
    #         print("detected",df2.week,df2.gsaCommitPercentage)
    #     elif (df2.forecastCommitPercentage<0)|(df2.forecastCommitPercentage>100):
    #         print("detected",df2.week,df2.forecastCommitPercentage)
    #     elif (df2.systemCommitPercentage<0)|(df2.systemCommitPercentage>100):
    #         print("detected",df2.week,df2.systemCommitPercentage)
    #     else:
    #         no_errors="No wrong percentages detected in any demand category"
    #         print("No wrong percentages detected in any demand category")
            
    # return no_errors   