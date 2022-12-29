import pandas as pd
import datetime 
import boto3
import io
from DB_conn import condb

def lambda_handler(event, context):
    current_time=datetime.datetime.now()
    sng_time=(current_time+datetime.timedelta(hours=8)).strftime("%Y-%m-%d %H_%M")
    DB_NAME='User_App'

    def generating_handling_code_xlsx():
        custom_columns = ['CODE', 'CREATED_AT (SEA)', 'CREATED_BY', 'CRITERIA', 'IS_ACTIVE', 'RECEIVING_SI', 'SEQ_INTERNAL_PRIORITY', 'SEQ_PRIORITY', 'SHIPPING_SI', 'UPDATED_AT (SEA)', 'UPDATED_BY', 'WAREHOUSE']
        
        primary_query = """SELECT CODE, 
                            CAST(CONVERT_TZ(CREATED_AT, 'America/Los_Angeles', 'Asia/Singapore') AS DATETIME) 'CREATED_AT', 
                            CREATED_BY, CRITERIA, export_set(IS_ACTIVE, 'TRUE', 'FALSE', '', 1) 'IS_ACTIVE', 
                            RECEIVING_SI, 
                            SEQ_INTERNAL_PRIORITY, 
                            SEQ_PRIORITY, 
                            SHIPPING_SI, 
                            CAST(CONVERT_TZ(UPDATED_AT, 'America/Los_Angeles', 'Asia/Singapore') AS DATETIME) 'UPDATED_AT', 
                            UPDATED_BY, 
                            WAREHOUSE 
                            FROM {}.preship_special_handling_code;""".format(DB_NAME)
        
        df=condb(primary_query)
        primary_query_data=pd.DataFrame(data=df,columns=custom_columns)
        
        with io.BytesIO() as output:
          with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            primary_query_data.to_excel(writer, index=False)
          data = output.getvalue()

        bucket = "b2b-irp-analytics-prod"
        subfolder = "special_handling_code"
        s3_file_path = "Archival_Reports/common/" +subfolder+ "/special_handling_code_archive" +"("+sng_time+").xlsx"
        s3 = boto3.client("s3")
        s3.put_object(ACL='public-read',Body=data,Bucket=bucket,Key=s3_file_path)
        
        return "special handling archive files generated"
        
        
    generating_handling_code_xlsx()
        
    
    