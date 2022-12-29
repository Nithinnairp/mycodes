import sys
import json
import boto3
import pymysql
import pandas as pd
import logging
import datetime
from DB_conn import condb,condb_dict

my_date=datetime.datetime.today()
year,week_num,day_of_week=my_date.isocalendar()
# print(week_num)
complete_current_year=str(year)
current_year=complete_current_year[2:]
DB_NAME='User_App'

def lambda_handler(event, context):
    
    current_time=datetime.datetime.now()
    date_str=current_time.strftime("%Y-%m-%d %H:%M")
    sng_time=(current_time+datetime.timedelta(hours=8)).strftime("%Y-%m-%d %H_%M")
    formated_time=(current_time+datetime.timedelta(hours=8)).strftime("%Y-%m-%d %H_%M").replace(' ','+')
    
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

    
    SENDER = "pdl-noreply-cmcprod@keysight.com"
    RECIPIENT = ["nithin.p@aspirenxt.com","pallav.sharma@keysight.com","yew-chun_yeap@keysight.com","pdl-b2b-cls-da@keysight.com"]
    # RECIPIENT = ["nithin.p@aspirenxt.com","pallav.sharma@keysight.com","sheau-wei_ooi@keysight.com","yim-fong_lye@keysight.com",
    # "lee-yong_wong@keysight.com","jess_ang@keysight.com","wei-ghee_khaw@keysight.com","ee-ling_lee876@keysight.com","bee-lee_ong@keysight.com","yew-chun_yeap@keysight.com"]
     
    measurable_flag_validation="""select fp.bu,ip.bu,fp.build_type,ip.build_type,fp.dept_code,ip.dept,
        fp.division,ip.division,fp.planner_code,ip.part_planner_code,
        fp.planner_name,ip.part_planner_name,fp.product_family,ip.product_family,
        fp.product_line,ip.product_line,fp.instrument,ip.instrument,fp.ori_part_number,ip.part_name,
        fp.measurable,ip.measureable_flag,fp.countable,ip.countable_flag
        from User_App.forecast_plan as fp,User_App.iodm_part_list as ip
        where ((fp.measurable=1 and ip.measureable_flag='N')or(fp.measurable=0 and ip.measureable_flag='Y')or
        ((fp.measurable=1 or fp.measurable=0) and ip.measureable_flag is null)or
        (fp.measurable is null and (ip.measureable_flag='N' or ip.measureable_flag='Y' or ip.measureable_flag='y'))) and fp.ori_part_number=ip.part_name;"""
        
    countable_flag_validation="""select fp.bu,ip.bu,fp.build_type,ip.build_type,fp.dept_code,ip.dept,
        fp.division,ip.division,fp.planner_code,ip.part_planner_code,
        fp.planner_name,ip.part_planner_name,fp.product_family,ip.product_family,
        fp.product_line,ip.product_line,fp.instrument,ip.instrument,fp.ori_part_number,ip.part_name,
        fp.measurable,ip.measureable_flag,fp.countable,ip.countable_flag
        from User_App.forecast_plan as fp,User_App.iodm_part_list as ip
        where ((fp.countable=1 and ip.countable_flag='No')or(fp.countable=0 and ip.countable_flag='Yes')) and fp.ori_part_number=ip.part_name;"""
        
    other_attributes_validation="""select fp.bu,ip.bu,fp.build_type,ip.build_type,fp.dept_code,ip.dept,
        fp.division,ip.division,fp.planner_code,ip.part_planner_code,
        fp.planner_name,ip.part_planner_name,fp.product_family,ip.product_family,
        fp.product_line,ip.product_line,fp.instrument,ip.instrument,fp.ori_part_number,ip.part_name,
        fp.measurable,ip.measureable_flag,fp.countable,ip.countable_flag
        from User_App.forecast_plan as fp,User_App.iodm_part_list as ip
        where (fp.bu!=ip.bu or fp.build_type!=ip.build_type or fp.dept_code!=ip.dept or 
        fp.division!=ip.division or fp.planner_code!=ip.part_planner_code or
        fp.planner_name!=ip.part_planner_name or fp.product_family!=ip.product_family or
        fp.product_line!=ip.product_line or fp.instrument!=ip.instrument) and (fp.ori_part_number=ip.part_name);"""
    

    mdf1=condb(measurable_flag_validation)
    cdf1=condb(countable_flag_validation)
    adf1=condb(other_attributes_validation)
    # col=['ori_part_number','ip_part_name','fp_measurable','ip_measureable_flag','fp_countable','countable_flag']
    col=['fp.bu','ip.bu','fp.build_type','ip.build_type','fp.dept_code','ip.dept','fp.division','ip.division','fp.planner_code','ip.part_planner_code',
    'fp.planner_name','ip.part_planner_name','fp.product_family','ip.product_family',
    'fp.product_line','ip.product_line','fp.instrument','ip.instrument','fp.ori_part_number','ip.part_name',
    'fp_measurable','ip_measureable_flag','fp_countable','countable_flag']
    data3=pd.DataFrame(data=mdf1,columns=col)
    data4=pd.DataFrame(data=cdf1,columns=col)
    data5=pd.DataFrame(data=adf1,columns=col)
    
    measurable_errors=data3.count()[0]
    countable_errors=data4.count()[0]
    other_attributes_errors=data5.count()[0]
    
    print(measurable_errors,countable_errors,other_attributes_errors)
    if measurable_errors==0 and countable_errors==0 and other_attributes_errors==0:
        print("No errors found in measureable_flag/countable_flag stamping")
        client = boto3.client('ses',"us-east-1")
        response = client.send_email(Source=SENDER,Destination={'ToAddresses':RECIPIENT},
                        Message={
                            'Subject': {
                                'Data': "Validation of Download file and other attributes against Master list for work week-{}".format(week_num)
                            },
                            'Body': {
                                'Text': {
                                    'Data':"""Hello,\n\r\nNo errors were found in Validation of Download file and other attributes against Master list as of {}.\nValidation Details:- 'Download all file vs KR masters part list by Ori PN, map all attributes in Master list'\n\nThanks & Regards,\nKeysightIT-Team\n\n\n""".format(sng_time)
                                }
                            }
                        })
                        
    # else:
    #     print("error in measurable/countable_flag found")
    #     data5=pd.concat([data3,data4])
    #     final_df=data5.applymap(lambda x: x[0] if type(x) is bytes else x)
    #     save_csv=final_df.to_csv("s3://b2b-irp-analytics-prod/Validation_reports/Download_file_vs_master_list_validation_records("+sng_time+").csv",index=False,header=True,encoding='UTF-8')
            
    else:
        print("error in measurable/countable_flag found")
        data6=pd.concat([data3,data4,data5])
        final_df=data6.applymap(lambda x: x[0] if type(x) is bytes else x)
        final_df.columns=list(map(lambda x: x.replace('gsa',catogory_name['gsa']), final_df))
        final_df.columns=list(map(lambda x: x.replace('non{}'.format(catogory_name['gsa']),catogory_name['nongsa']), final_df))
        
        save_csv=final_df.to_csv("s3://b2b-irp-analytics-prod/Validation_reports/Download_file_vs_master_list_validation_records("+sng_time+").csv",index=False,header=True,encoding='UTF-8')
        object_url="https://b2b-irp-analytics-prod.s3.ap-southeast-1.amazonaws.com/Validation_reports/Download_file_vs_master_list_validation_records("+formated_time+").csv"
        client = boto3.client('ses',"us-east-1")
        response = client.send_email(Source=SENDER,Destination={'ToAddresses':RECIPIENT},
                        Message={
                            'Subject': {
                                'Data': "Validation of Download file and other attributes against Master list for work week-{}".format(week_num)
                            },
                            'Body': {
                                'Text': {
                                    'Data': """Hello,\n\r\nErrors were found in Measurable & Countable Validation as of {}.
                                    \n\nDownload report- {} \n\nThanks & Regards,\nKeysightIT-Team\n\n\n""".format(sng_time,object_url)
                                }
                            }
                        })
            
    