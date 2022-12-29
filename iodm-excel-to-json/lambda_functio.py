import sys
import boto3
import pandas as pd
import logging

def lambda_handler(event, context):
    print(str(event))
    bucket_name=event['Records'][0]['s3']['bucket']['name']
    key=event['Records'][0]['s3']['object']['key']
    
    o_key=key[11:-5].replace('+',' ').replace('%28','(').replace('%29',')')
    print(bucket_name,o_key)
    
    
    def error_handling():
        return '{},{} on Line:{}'.format(sys.exc_info()[0],sys.exc_info()[1],sys.exc_info()[2].tb_lineno)
    try:
        df=pd.read_excel('s3://'+ bucket_name +'/iodm_files/' + o_key +'.xlsx')
        # df=pd.read_excel('s3://rapid-response-prod/KR IRP Template (IODM).20201108T193919.xlsx')
        col=df.columns
        print('s3://'+ bucket_name +'/iodm_files/' + o_key +'.xlsx')
        print(col)
        new_col=[]
        
        
        for i in col:
            lower=i.lower()
            renamed=lower.replace(' ','_').replace('.','_').replace('-','_').replace(')','').replace('(','').replace('/','')
            new_col.append(renamed)
        
        df.columns=new_col
        print(df.columns,len(df.columns))
         
        
        # if len(df.columns)<=79:
        #     df_json=df.to_json(orient='records',date_format='iso',date_unit='s',lines=True)
        #     s3=boto3.client("s3")
        #     # s3.put_object(Body=df_json,Bucket='rapid-response-prod',Key="iodm_json_files/" + "KR IRP Template (IODM).20201108T193919.json")
        #     print("lambda Funtion completed successfully check for the json file in iodm_json_files folder",'\n')
        
        # else:
        #     print("Changes in fields observed, cross check the fields")
        
    except Exception as e:
        logging.error(error_handling())
        print("Lambda fuction failed for execution",'\n')
    
    
    
    
    # print(str(event))
    # # bucket_name=event['Records'][0]['s3']['bucket']['name']
    # # key=event['Records'][0]['s3']['object']['key']
    
    # # # key=O_key.replace('+',' ').replace('%28','(').replace('%29',')')
    
    # # file_name=key[5:-5]
    
    # # df=pd.read_excel('s3://rapid-response-test/' + key)
    # df=pd.read_excel('s3://rapid-response-prod/KR IRP Template (IODM).20200809T215350.xlsx')
    
    # col=['iodm','contract_manufacturer','ist_group','item_type','component_part','component_site','cm_part_number','bu','division','pl','dept_code','planner_code',
    #     'name','original_number','cid_mapped_item','instrument','product_family','description','type','build_type','date','production_week',
    #     'month','year','final_kr_current','total_exception','firmreq_nongsa_total_demand',
    #     'firmreq_nongsa_exception','firmreq_gsa_total_demand','firmreq_gsa_exception','forecast_total_demand','forecast_exception','calibration_option',
    #     'planner_remarks','process_type','dmp_ndmp','cm_inputs','firmreq_nongsa_commit_date','qty','firmreq_nongsa_commit_date_2',
    #     'qty_1','status','firmreq_nongsa_commit_date_3','qty_2','status_1','firmreq_nongsa_cause_code',
    #     'cause_code_remark','target_recovery','firmreq_gsa_commit_date','qty_3','firmreq_gsa_commit_date_2','qty_4',
    #     'status_2','firmreq_gsa_cause_code','cause_code_remark_1','target_recovery_1','forecast_commit_date','qty_5','forecast_commit_date_2',
    #     'qty_6','status_3','forecast_commit_date_3','qty_7','status_4','forecast_cause_code','cause_code_remark_2','target_recovery_2',
    #     'cm_remarks','cm_latest_commit','buffer','buffer_opt_adj','on_hand_total_cm','on_hand_nettable_si',
    #     'received_qty','current_open_po','past_due_open_po']
    
    # df.columns=col
    
    # # df_json=df.to_json(orient='records',indent=1,date_format='iso',date_unit='s')
    # df_json=df.to_json(orient='records',date_format='iso',date_unit='s',lines=True)
    
    # s3=boto3.client("s3")
    # s3.put_object(Body=df_json,Bucket='rapid-response-prod',Key="iodm_json_files/" + "KR IRP Template (IODM).20200809T215350.json")
