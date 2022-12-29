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
        col=df.columns
        print(col)
        new_col=[]
        
        for i in col:
            lower=i.lower()
            renamed=lower.replace(' ','_').replace('.','_').replace('-','_').replace(')','').replace('(','').replace('/','')
            new_col.append(renamed)
        
        df.columns=new_col
        print(df.columns,len(df.columns))
        
        if len(df.columns)<=79:
            df_json=df.to_json(orient='records',date_format='iso',date_unit='s',lines=True)
            s3=boto3.client("s3") 
            s3.put_object(Body=df_json,Bucket=bucket_name,Key="iodm_json_files/" + o_key + ".json")
            print("lambda Funtion completed successfully check for the json file in IODM_json folder",'\n')
        
        else:
            print("Changes in fields observed, cross check the fields")
        
    except Exception as e:
        logging.error(error_handling())
        print("Lambda fuction failed for execution",'\n')