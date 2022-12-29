import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as f
from pyspark.sql.functions import to_date,to_timestamp,array,lit,col,when
import pymysql
import json
import pandas as pd
import numpy as np
import datetime
from awsglue.dynamicframe import DynamicFrame
import time
import logging

logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%d/%m/%Y %I:%M:%S %p',level=logging.INFO)
logging.info('This will get logged to a file')

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

#    Declaring date and Time Variables          
my_date=datetime.date.today()
current_time=datetime.datetime.now()
year,week_num,day_of_week=my_date.isocalendar()

client = boto3.client("secretsmanager", region_name='ap-southeast-1')
get_secret_value_response = client.get_secret_value(SecretId="cp-prod-aurora-serverless-01-Glue")

#Script for retreving the rds credentials
secret = get_secret_value_response['SecretString']
secret =json.loads(secret)
rds_dbname = 'User_App'
rds_host = secret.get("host")
rds_port = secret.get("port")
rds_username = secret.get("username")
rds_password = secret.get("password") 


logging.info('Credentials fetched for DB connection')

#Establishing connection between the Database
def condb(sql,col=None):
    conn=pymysql.connect(rds_host,rds_username,rds_password,rds_dbname)
    cursor=conn.cursor()
    cursor.execute(sql,col)
    df=cursor.fetchall()
    conn.commit()
    conn.close()
    return df

#Taking the backup of the tables
condb("drop table User_App.staging")
condb("create table User_App.staging like User_App.forecast_plan")

condb("drop table User_App.advance_commit_temp;")
condb("create table User_App.advance_commit_temp select * from User_App.advance_commit;")

condb("drop table User_App.escalation_temp;")
condb("create table User_App.escalation_temp select * from User_App.escalation;")

condb("drop table User_App.forecast_plan_temp;")
condb("create table User_App.forecast_plan_temp like User_App.forecast_plan")
condb("insert into User_App.forecast_plan_temp select * from User_App.forecast_plan;")

condb("drop table User_App.non_ascp_request_line_temp;")
condb("create table User_App.non_ascp_request_line_temp select * from User_App.non_ascp_request_line;")

condb("drop table User_App.non_ascp_request_header_temp;")
condb("create table User_App.non_ascp_request_header_temp select * from User_App.non_ascp_request_header;")


#Fetching the columns from database and preparing the DB dataframe

logging.info('Connection made successfully for DB')

logging.info('Job started')
get_col_names="""SELECT column_name from information_schema.columns where table_schema='User_App' and table_name='forecast_plan';"""
col_names=list(condb(get_col_names))
db_columns=list(sum(col_names,()))

sql="""SELECT * FROM User_App.forecast_plan;"""

df1=condb(sql)
sql_data=pd.DataFrame(data=np.array(df1),columns=db_columns)

##Preparing the last_week_data field
last_week_cols=sql_data[['forecast_original','forecast_exception','forecast_commit','forecast_adj','forecast_final','forecast_de_commit','forecast_supply',
                            'total_supply','prod_month','prod_year','prod_week','ori_part_number','de_commit','process_week',
                            'nongsa_original','nongsa_exception','nongsa_commit','nongsa_adj','nongsa_final','nongsa_de_commit','nongsa_supply',
                            'gsa_original','gsa_exception','gsa_commit','gsa_adj','gsa_final','gsa_de_commit','gsa_supply',
                            'system_original','system_exception','system_commit','system_adj','system_final','system_de_commit','system_supply',
                            'gsa_advance_commit_kr','nongsa_advance_commit_kr','forecast_advance_commit_kr','system_advance_commit_kr',
                            'gsa_advance_commit_irp','nongsa_advance_commit_irp','forecast_advance_commit_irp','system_advance_commit_irp',
                            'cumulative_gsa_advance_commit_irp','cumulative_nongsa_advance_commit_irp','cumulative_forecast_advance_commit_irp','cumulative_system_advance_commit_irp']]
                            
new_col=["forecastOriginal","forecastException","forecastCommit","forecastAdj","forecastFinal","forecastDeCommit","forecastSupply",
                            "totalSupply","prodMonth","prodYear","prodWeek","oriPartNumber","DeCommit","processWeek",
                            "nongsaOriginal","nongsaException","nongsaCommit","nongsaAdj","nongsaFinal","nongsaDeCommit","nongsaSupply",
                            "gsaOriginal","gsaException","gsaCommit","gsaAdj","gsaFinal", "gsaDeCommit","gsaSupply",
                            "systemOriginal",'systemException',"systemCommit","systemAdj","systemFinal","systemDeCommit","systemSupply",
                            "gsaAdvanceCommitKr","nongsaAdvanceCommitKr","forecastAdvanceCommitKr","systemAdvanceCommitKr",
                            "gsaAdvanceCommitIrp","nongsaAdvanceCommitIrp","forecastAdvanceCommitIrp","systemAdvanceCommitIrp",
                            "CummulativegsaAdvanceCommitIrp","CummulativenongsaAdvanceCommitIrp","CummulativeforecastAdvanceCommitIrp","CummulativesystemAdvanceCommitIrp"]
                            
last_week_cols.columns=new_col
           
test_json=last_week_cols.to_json(orient='records',lines=True).split('\n')
sql_data['last']=test_json
sql_data.drop('last_week_data',axis=1,inplace=True)
sql_data.rename(columns={'last':'last_week_data'},inplace=True)

#Defining the schema for the DB DataFrame in the spark##

myschema=StructType([StructField('id',LongType(), True)
                    ,StructField('bu',StringType(),True)
                    ,StructField('buffer',StringType(),True)
                    ,StructField('buffer_opt_adj',StringType(),True)
                    ,StructField('build_type',StringType(),True)
                    ,StructField('cal_option',StringType(),True)
                    ,StructField('cid_mapped_part_number',StringType(),True)
                    ,StructField('commit',DoubleType(),True)
                    ,StructField('current_open_po',IntegerType(),True)
                    ,StructField('date',DateType(),True)
                    ,StructField('dept_code',StringType(),True)
                    ,StructField('description',StringType(),True)
                    ,StructField('division',StringType(),True)
                    ,StructField('dmp_orndmp',StringType(),True)
                    ,StructField('is_dmp',StringType(),True)
                    ,StructField('exception',DoubleType(),True)
                    ,StructField('forecast_adj',DoubleType(),True)
                    ,StructField('forecast_adj_reason',StringType(),True)
                    ,StructField('forecast_commit',DoubleType(),True)
                    ,StructField('forecast_supply',DoubleType(),True)
                    ,StructField('forecast_final',DoubleType(),True)
                    ,StructField('forecast_original',DoubleType(),True)
                    ,StructField('gsa_adj',DoubleType(),True)
                    ,StructField('gsa_adj_reason',StringType(),True)
                    ,StructField('gsa_commit',DoubleType(),True)
                    ,StructField('gsa_supply',DoubleType(),True)
                    ,StructField('gsa_final',DoubleType(),True)
                    ,StructField('gsa_original',DoubleType(),True)
                    ,StructField('instrument',StringType(),True)
                    ,StructField('item_type',StringType(),True)
                    ,StructField('nongsa_adj',DoubleType(),True)
                    ,StructField('nongsa_adj_reason',StringType(),True)
                    ,StructField('nongsa_commit',DoubleType(),True)
                    ,StructField('nongsa_supply',DoubleType(),True)
                    ,StructField('nongsa_final',DoubleType(),True)
                    ,StructField('nongsa_original',DoubleType(),True)
                    ,StructField('on_hand_nettable_si',IntegerType(),True)
                    ,StructField('on_hand_total_cm',IntegerType(),True)
                    ,StructField('org',StringType(),True)
                    ,StructField('ori_part_number',StringType(),True)
                    ,StructField('past_due_open_po',IntegerType(),True)
                    ,StructField('planner_code',StringType(),True)
                    ,StructField('planner_name',StringType(),True)
                    ,StructField('planner_remark',StringType(),True)
                    ,StructField('process_type',StringType(),True)
                    ,StructField('prod_month',IntegerType(),True)
                    ,StructField('prod_quarter',StringType(),True)
                    ,StructField('prod_week',IntegerType(),True)
                    ,StructField('prod_year',IntegerType(),True)
                    ,StructField('prod_year_week',StringType(),True)
                    ,StructField('prod_year_month',StringType(),True)
                    ,StructField('product_family',StringType(),True)
                    ,StructField('received_qty',IntegerType(),True)
                    ,StructField('source_supplier',StringType(),True)
                    ,StructField('source_supplier_id',StringType(),True)
                    ,StructField('system_adj',DoubleType(),True)
                    ,StructField('system_adj_reason',StringType(),True)
                    ,StructField('system_commit',DoubleType(),True)
                    ,StructField('system_supply',DoubleType(),True)
                    ,StructField('system_final',DoubleType(),True)
                    ,StructField('system_original',DoubleType(),True)
                    ,StructField('total_adj',DoubleType(),True)
                    ,StructField('total_commit',DoubleType(),True)
                    ,StructField('total_supply',DoubleType(),True)
                    ,StructField('total_final',DoubleType(),True)
                    ,StructField('total_original',DoubleType(),True)
                    ,StructField('total_plassers_week',StringType(),True)
                    ,StructField('forecast_commit_reason',StringType(),True)
                    ,StructField('gsa_commit_reason',StringType(),True)
                    ,StructField('nongsa_commit_reason',StringType(),True)
                    ,StructField('system_commit_reason',StringType(),True)
                    ,StructField('is_escalated',StringType(),True)
                    ,StructField('advance_commit',StringType(),True)
                    ,StructField('process_week',IntegerType(),True)
                    ,StructField('process_year',IntegerType(),True)
                    ,StructField('forecast_commit1',DoubleType(),True)
                    ,StructField('forecast_commit1_date',DateType(),True)
                    ,StructField('forecast_commit1_accepted',StringType(),True)
                    ,StructField('forecast_commit2',DoubleType(),True)
                    ,StructField('forecast_commit2_date',DateType(),True)
                    ,StructField('forecast_commit2_accepted',StringType(),True)
                    ,StructField('forecast_commit3',DoubleType(),True)
                    ,StructField('forecast_commit3_date',DateType(),True)
                    ,StructField('forecast_commit3_accepted',StringType(),True)
                    ,StructField('forecast_commit4',DoubleType(),True)
                    ,StructField('forecast_commit4_date',DateType(),True)
                    ,StructField('forecast_commit4_accepted',StringType(),True)
                    ,StructField('forecast_exception',DoubleType(),True)
                    ,StructField('forecast_has_adjustment',IntegerType(),True)
                    ,StructField('forecast_has_commit',IntegerType(),True)
                    ,StructField('forecast_cause_code',StringType(),True)
                    ,StructField('forecast_cause_code_remark',StringType(),True)
                    ,StructField('forecast_target_recovery',StringType(),True)
                    ,StructField('gsa_commit1',DoubleType(),True)
                    ,StructField('gsa_commit1_date',DateType(),True)
                    ,StructField('gsa_commit1_accepted',StringType(),True)
                    ,StructField('gsa_commit2',DoubleType(),True)
                    ,StructField('gsa_commit2_date',DateType(),True)
                    ,StructField('gsa_commit2_accepted',StringType(),True)
                    ,StructField('gsa_commit3',DoubleType(),True)
                    ,StructField('gsa_commit3_date',DateType(),True)
                    ,StructField('gsa_commit3_accepted',StringType(),True)
                    ,StructField('gsa_commit4',DoubleType(),True)
                    ,StructField('gsa_commit4_date',DateType(),True)
                    ,StructField('gsa_commit4_accepted',StringType(),True)
                    ,StructField('gsa_exception',DoubleType(),True)
                    ,StructField('gsa_has_adjustment',IntegerType(),True)
                    ,StructField('gsa_has_commit',IntegerType(),True)
                    ,StructField('gsa_cause_code',StringType(),True)
                    ,StructField('gsa_cause_code_remark',StringType(),True)
                    ,StructField('gsa_target_recovery',StringType(),True)
                    ,StructField('nongsa_commit1',DoubleType(),True)
                    ,StructField('nongsa_commit1_date',DateType(),True)
                    ,StructField('nongsa_commit1_accepted',StringType(),True)
                    ,StructField('nongsa_commit2',DoubleType(),True)
                    ,StructField('nongsa_commit2_date',DateType(),True)
                    ,StructField('nongsa_commit2_accepted',StringType(),True)
                    ,StructField('nongsa_commit3',DoubleType(),True)
                    ,StructField('nongsa_commit3_date',DateType(),True)
                    ,StructField('nongsa_commit3_accepted',StringType(),True)
                    ,StructField('nongsa_commit4',DoubleType(),True)
                    ,StructField('nongsa_commit4_date',DateType(),True)
                    ,StructField('nongsa_commit4_accepted',StringType(),True)
                    ,StructField('nongsa_exception',DoubleType(),True)
                    ,StructField('nongsa_has_adjustment',IntegerType(),True)
                    ,StructField('nongsa_has_commit',IntegerType(),True)
                    ,StructField('nongsa_cause_code',StringType(),True)
                    ,StructField('nongsa_cause_code_remark',StringType(),True)
                    ,StructField('nongsa_target_recovery',StringType(),True)
                    ,StructField('system_commit1',DoubleType(),True)
                    ,StructField('system_commit1_date',DateType(),True)
                    ,StructField('system_commit1_accepted',StringType(),True)
                    ,StructField('system_commit2',DoubleType(),True)
                    ,StructField('system_commit2_date',DateType(),True)
                    ,StructField('system_commit2_accepted',StringType(),True)
                    ,StructField('system_commit3',DoubleType(),True)
                    ,StructField('system_commit3_date',DateType(),True)
                    ,StructField('system_commit3_accepted',StringType(),True)
                    ,StructField('system_commit4',DoubleType(),True)
                    ,StructField('system_commit4_date',DateType(),True)
                    ,StructField('system_commit4_accepted',StringType(),True)
                    ,StructField('system_exception',DoubleType(),True)
                    ,StructField('system_has_adjustment',IntegerType(),True)
                    ,StructField('system_has_commit',IntegerType(),True)
                    ,StructField('system_cause_code',StringType(),True)
                    ,StructField('system_cause_code_remark',StringType(),True)
                    ,StructField('system_target_recovery',StringType(),True)
                    ,StructField('de_commit',DoubleType(),True)
                    ,StructField('forecast_de_commit',DoubleType(),True)
                    ,StructField('gsa_de_commit',DoubleType(),True)
                    ,StructField('nongsa_de_commit',DoubleType(),True)
                    ,StructField('system_de_commit',DoubleType(),True)
                    ,StructField('created_at',TimestampType(),True)
                    ,StructField('created_by',StringType(),True)
                    ,StructField('updated_at',TimestampType(),True)
                    ,StructField('updated_by',StringType(),True)
                    ,StructField('product_line',StringType(),True)
                    ,StructField('forecast_advance_commit_reason',StringType(),True)
                    ,StructField('gsa_advance_commit_reason',StringType(),True)
                    ,StructField('nongsa_advance_commit_reason',StringType(),True)
                    ,StructField('system_advance_commit_reason',StringType(),True)
                    ,StructField('escalation_status',StringType(),True)
                    ,StructField('finished_goods',StringType(),True)
                    ,StructField('cm_part_number',StringType(),True)
                    ,StructField('active',StringType(),True)
                    ,StructField('old_data',StringType(),True)
                    ,StructField('gsa_remarks',StringType(),True)
                    ,StructField('nongsa_remarks',StringType(),True)
                    ,StructField('system_remarks',StringType(),True)
                    ,StructField('forecast_remarks',StringType(),True)
                    ,StructField('gsa_advance_commit_updated',StringType(),True)
                    ,StructField('nongsa_advance_commit_updated',StringType(),True)
                    ,StructField('system_advance_commit_updated',StringType(),True)
                    ,StructField('forecast_advance_commit_updated',StringType(),True)
                    ,StructField('forecast_advance_commit_irp',StringType(),True)
                    ,StructField('forecast_advance_commit_kr',StringType(),True)
                    ,StructField('gsa_advance_commit_irp',StringType(),True)
                    ,StructField('gsa_advance_commit_kr',StringType(),True)
                    ,StructField('nongsa_advance_commit_irp',StringType(),True)
                    ,StructField('nongsa_advance_commit_kr',StringType(),True)
                    ,StructField('system_advance_commit_irp',StringType(),True)
                    ,StructField('system_advance_commit_kr',StringType(),True)
                    ,StructField('latest_forecast_commit',StringType(),True)
                    ,StructField('latest_gsa_commit',StringType(),True)
                    ,StructField('latest_non_gsa_commit',StringType(),True)
                    ,StructField('latest_system_commit',StringType(),True)
                    ,StructField('is_processed',StringType(),True)
                    ,StructField('manually_added',StringType(),True)
                    ,StructField('measurable',StringType(),True)
                    ,StructField('countable',StringType(),True)
                    ,StructField('is_imported',StringType(),True)
                    ,StructField('via_advance_commit',StringType(),True)
                    ,StructField('carry_forward',StringType(),True)
                    ,StructField('ui_advance_commit',StringType(),True)
                    ,StructField('cumulative_forecast_advance_commit_irp',StringType(),True)
                    ,StructField('cumulative_gsa_advance_commit_irp',StringType(),True)
                    ,StructField('cumulative_nongsa_advance_commit_irp',StringType(),True)
                    ,StructField('cumulative_system_advance_commit_irp',StringType(),True)
                    ,StructField('oripartnumber_week_year',StringType(),True)
                    ,StructField('adj_updated',StringType(),True)
                    ,StructField('commit_advance_commit_updated',StringType(),True)
                    ,StructField('last_decommit_data',StringType(),True)
                    ,StructField('aging',StringType(),True)
                    ,StructField('assy',StringType(),True)
                    ,StructField('button_up',StringType(),True)
                    ,StructField('debug',StringType(),True)
                    ,StructField('fg_logistic',StringType(),True)
                    ,StructField('fg_qty',StringType(),True)
                    ,StructField('fvmi',StringType(),True)
                    ,StructField('in_transit_qty',StringType(),True)
                    ,StructField('pack',StringType(),True)
                    ,StructField('pending_assy',StringType(),True)
                    ,StructField('store_core',StringType(),True)
                    ,StructField('test',StringType(),True)
                    ,StructField('wip_gsa_nongsa_flag',StringType(),True)
                    ,StructField('wip_qty',StringType(),True)
                    ,StructField('forecast_commit_h',DoubleType(),True)
                    ,StructField('forecast_escalation_flag',StringType(),True)
                    ,StructField('gsa_commit_h',DoubleType(),True)
                    ,StructField('gsa_escalation_flag',StringType(),True)
                    ,StructField('nongsa_commit_h',DoubleType(),True)
                    ,StructField('nongsa_escalation_flag',StringType(),True)
                    ,StructField('system_commit_h',DoubleType(),True)
                    ,StructField('system_escalation_flag',StringType(),True)
                    ,StructField('last_week_data',StringType(),True)])
                    
DB_spark_df=spark.createDataFrame(sql_data,schema=myschema)

DB_final_df=DB_spark_df.withColumnRenamed('gsa_original','prev_gsa_original')\
                  .withColumnRenamed('nongsa_original','prev_nongsa_original')\
                  .withColumnRenamed('forecast_original','prev_forecast_original')\
                  .withColumnRenamed('system_original','prev_system_original')\
                  .withColumnRenamed('gsa_adj','prev_gsa_adj')\
                  .withColumnRenamed('nongsa_adj','prev_nongsa_adj')\
                  .withColumnRenamed('forecast_adj','prev_forecast_adj')\
                  .withColumnRenamed('system_adj','prev_system_adj')\
                  .withColumnRenamed('gsa_supply','prev_gsa_supply')\
                  .withColumnRenamed('nongsa_supply','prev_nongsa_supply')\
                  .withColumnRenamed('forecast_supply','prev_forecast_supply')\
                  .withColumnRenamed('system_supply','prev_system_supply')\
                  .withColumnRenamed('gsa_advance_commit_irp','prev_gsa_advance_commit_irp')\
                  .withColumnRenamed('nongsa_advance_commit_irp','prev_nongsa_advance_commit_irp')\
                  .withColumnRenamed('forecast_advance_commit_irp','prev_forecast_advance_commit_irp')\
                  .withColumnRenamed('system_advance_commit_irp','prev_system_advance_commit_irp')\
                  .withColumnRenamed('gsa_exception','prev_gsa_exception')\
                  .withColumnRenamed('nongsa_exception','prev_nongsa_exception')\
                  .withColumnRenamed('forecast_exception','prev_forecast_exception')\
                  .withColumnRenamed('system_exception','prev_system_exception')\
                  .withColumnRenamed('gsa_de_commit','prev_gsa_de_commit')\
                  .withColumnRenamed('nongsa_de_commit','prev_nongsa_de_commit')\
                  .withColumnRenamed('forecast_de_commit','prev_forecast_de_commit')\
                  .withColumnRenamed('system_de_commit','prev_system_de_commit')\
                  .withColumnRenamed('cumulative_gsa_advance_commit_irp','prev_cumulative_gsa_advance_commit_irp')\
                  .withColumnRenamed('cumulative_nongsa_advance_commit_irp','prev_cumulative_nongsa_advance_commit_irp')\
                  .withColumnRenamed('cumulative_forecast_advance_commit_irp','prev_cumulative_forecast_advance_commit_irp')\
                  .withColumnRenamed('cumulative_system_advance_commit_irp','prev_cumulative_system_advance_commit_irp')\

DB_final_df=DB_final_df.fillna({'prev_gsa_original':0,'prev_nongsa_original':0,'prev_forecast_original':0,'prev_system_original':0,
                  'prev_gsa_adj':0,'prev_nongsa_adj':0,'prev_forecast_adj':0,'prev_system_adj':0,
                  'prev_gsa_supply':0,'prev_nongsa_supply':0,'prev_forecast_supply':0,'prev_system_supply':0,
                  'prev_gsa_advance_commit_irp':0,'prev_nongsa_advance_commit_irp':0,'prev_forecast_advance_commit_irp':0,'prev_system_advance_commit_irp':0,
                  'prev_gsa_exception':0,'prev_nongsa_exception':0,'prev_forecast_exception':0,'prev_system_exception':0,
                  'prev_gsa_de_commit':0,'prev_nongsa_de_commit':0,'prev_forecast_de_commit':0,'prev_system_de_commit':0,
                  'prev_cumulative_gsa_advance_commit_irp':0,'prev_cumulative_nongsa_advance_commit_irp':0,'prev_cumulative_forecast_advance_commit_irp':0,'prev_cumulative_system_advance_commit_irp':0})

# datasource0=glueContext.create_dynamic_frame.from_options(connection_type = "s3", connection_options = {"paths": ["s3://rapid-response-prod/iodm_json_files/KR IRP Template (IODM).20220828T182347.json"]}, format = "json", transformation_ctx = "datasource0")

datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "multiple_json_files", table_name = "iodm_multiple_json_files", transformation_ctx = "datasource0")

resolvechoice_dyn1 = ResolveChoice.apply(frame = datasource0, specs=[('on_hand_nettable_si','cast:int')], transformation_ctx = "resolvechoice_dyn1")


json_file_df=resolvechoice_dyn1.toDF() 


json_schema=json_file_df.withColumnRenamed('calibration_option','cal_option')\
                    .withColumnRenamed('component_site','org')\
                    .withColumnRenamed('contract_manufacturer', 'source_supplier')\
                    .withColumnRenamed('dmp_ndmp', 'dmp_orndmp')\
                    .withColumnRenamed('forecast_total_demand','forecast_original')\
                    .withColumnRenamed('month','prod_month')\
                    .withColumnRenamed('name', 'planner_name')\
                    .withColumnRenamed('original_number', 'ori_part_number')\
                    .withColumnRenamed('production_week','prod_week')\
                    .withColumnRenamed('year','prod_year')\
                    .withColumnRenamed('cid_mapped_item','cid_mapped_part_number')\
                    .withColumnRenamed('cm_latest_commit','commit')\
                    .withColumnRenamed('planner_remarks','planner_remark')\
                    .withColumnRenamed('total_exception','exception')\
                    .withColumnRenamed('firmreq_gsa_total_demand','gsa_original')\
                    .withColumnRenamed('firmreq_nongsa_total_demand','nongsa_original')\
                    .withColumnRenamed('pl','product_line')\
                    .withColumnRenamed('kr_summary_yn','finished_goods')\
                    .withColumnRenamed('bucket_date','date')\
                    .withColumn('date',f.col('date').cast(DateType()))\
                    .withColumn('system_original',f.lit(0).cast(DoubleType()))\
                    .withColumn('process_week',f.lit(week_num).cast(IntegerType()))\
                    .withColumn('process_year',f.lit(year).cast(IntegerType()))\
                    .withColumn('created_at',f.lit(current_time).cast(TimestampType()))\
                    .withColumn('created_by',f.lit(1))\
                    .withColumn('updated_at',f.lit(current_time).cast(TimestampType()))\
                    .withColumn('updated_by',f.lit(1))\
                    .withColumn('is_escalated',f.lit(0))\
                    .withColumn('process_type',when((col('dmp_orndmp')=='DMP')&((col('forecast_original')!=0)|(col('system_original')!=0)|(col('gsa_original')!=0)|(col('nongsa_original')!=0)),'Exception'))\
                    .withColumn('active',f.lit(1))\
                    .withColumn('old_data',f.lit(0))\
                    .withColumn('prod_year_week',f.concat('prod_year','prod_week').cast(IntegerType()))\
                    .withColumn('oripartnumber_week_year',f.concat(col('ori_part_number'),lit('-'),col('prod_week'),lit('-'),col('prod_year')))\
                    .withColumn('prod_year_month',f.concat('prod_year','prod_month').cast(IntegerType()))\
                    .withColumn('is_dmp',when(col('dmp_orndmp')=='DMP',1).otherwise(0))\
                    .withColumn('measurable',when(col('measureable_yn')=='Y',1).when(col('measureable_yn')=='y',1).when(col('measureable_yn')=='N',0).otherwise(None))\
                    .withColumn('countable',when(col('countable_yn')=='Y',1).otherwise(0))

json_final_df=json_schema.drop('iodm','component_part','final_kr_current','firmreq_gsa_cause_code','cause_code_remark','cause_code_remark_1','cause_code_remark_2',
'cm_inputs','cm_remarks','firmreq_gsa_cause_code','firmreq_gsa_commit_date','firmreq_gsa_commit_date_2','firmreq_nongsa_cause_code','firmreq_nongsa_commit_date','firmreq_nongsa_commit_date_2',
'firmreq_nongsa_commit_date_3','forecast_cause_code','forecast_commit_date','forecast_commit_date_2','forecast_commit_date_3','ist_group','firmreq_gsa_exception','firmreq_nongsa_exception','forecast_exception',
'qty','qty_1','qty_2','qty_3','qty_4','qty_5','qty_6','qty_7','status','status_1','status_2','status_3','status_4','target_recovery','target_recovery_1','target_recovery_2','type','measureable_yn','countable_yn')


#applying Joins to find the ID to be updated and inserted
id_tobe_updated=DB_final_df.join(json_final_df,['prod_week','prod_year','ori_part_number'],how='left_semi')

updated_values=json_final_df.join(DB_final_df,['prod_week','prod_year','ori_part_number'],how='left_semi')

common_col=id_tobe_updated.select('ori_part_number','prod_week','prod_month','prod_year','id','last_week_data',
                                 'prev_gsa_original','prev_nongsa_original','prev_forecast_original','prev_system_original',
                                 'prev_gsa_exception','prev_nongsa_exception','prev_forecast_exception','prev_system_exception',
                                 'prev_gsa_adj','prev_nongsa_adj','prev_forecast_adj','prev_system_adj',
                                 'prev_gsa_supply','prev_nongsa_supply','prev_forecast_supply','prev_system_supply',
                                 'prev_gsa_advance_commit_irp','prev_nongsa_advance_commit_irp','prev_forecast_advance_commit_irp','prev_system_advance_commit_irp',
                                 'prev_cumulative_gsa_advance_commit_irp','prev_cumulative_nongsa_advance_commit_irp','prev_cumulative_forecast_advance_commit_irp','prev_cumulative_system_advance_commit_irp',
                                 'prev_gsa_de_commit','prev_nongsa_de_commit','prev_forecast_de_commit','prev_system_de_commit')

                                 
rows_tobe_updated=updated_values.join(common_col,['ori_part_number','prod_week','prod_year'],how='left')


dataframe1=rows_tobe_updated.withColumn('new_exception_gsa',(col('gsa_original')+col('prev_gsa_adj'))-(col('prev_gsa_supply')+col('prev_gsa_advance_commit_irp')))\
                            .withColumn('new_exception_nongsa',(col('nongsa_original')+col('prev_nongsa_adj'))-(col('prev_nongsa_supply')+col('prev_nongsa_advance_commit_irp')))\
                            .withColumn('new_exception_forecast',(col('forecast_original')+col('prev_forecast_adj'))-(col('prev_forecast_supply')+col('prev_forecast_advance_commit_irp')))\
                            .withColumn('new_exception_system',(col('system_original')+col('prev_system_adj'))-(col('prev_system_supply')+col('prev_system_advance_commit_irp')))\
                            .withColumn('new_gsa_de_commit',when(col('gsa_original')<col('prev_gsa_original'),(col('prev_gsa_de_commit')-(col('prev_gsa_original')-col('gsa_original')))))\
                            .withColumn('new_nongsa_de_commit',when(col('gsa_original')<col('prev_nongsa_original'),(col('prev_nongsa_de_commit')-(col('prev_nongsa_original')-col('nongsa_original')))))\
                            .withColumn('new_forecast_de_commit',when(col('forecast_original')<col('prev_forecast_original'),(col('prev_forecast_de_commit')-(col('prev_forecast_original')-col('forecast_original')))))\
                            .withColumn('new_system_de_commit',when(col('system_original')<col('prev_system_original'),(col('prev_system_de_commit')-(col('prev_system_original')-col('system_original')))))\
                            .withColumn('new_process_type',when((col('is_dmp')==1) & ((col('new_exception_gsa')!=0) | (col('new_exception_nongsa')!=0) | (col('new_exception_forecast')!=0) | (col('new_exception_system')!=0)),'Exception'))\
                            .withColumn('new_gsa_advance_commit_irp',when(col('prev_gsa_advance_commit_irp')>=0,col('prev_gsa_advance_commit_irp')+col('prev_cumulative_gsa_advance_commit_irp')))\
                            .withColumn('new_nongsa_advance_commit_irp',when(col('prev_nongsa_advance_commit_irp')>=0,col('prev_nongsa_advance_commit_irp')+col('prev_cumulative_nongsa_advance_commit_irp')))\
                            .withColumn('new_forecast_advance_commit_irp',when(col('prev_forecast_advance_commit_irp')>=0,col('prev_forecast_advance_commit_irp')+col('prev_cumulative_forecast_advance_commit_irp')))\
                            .withColumn('new_system_advance_commit_irp',when(col('prev_system_advance_commit_irp')>=0,col('prev_system_advance_commit_irp')+col('prev_cumulative_system_advance_commit_irp')))
                            

dataframe2=dataframe1.drop('prev_gsa_original','prev_nongsa_original','prev_forecast_original','prev_system_original',
                            'prev_gsa_exception','prev_nongsa_exception','prev_forecast_exception','prev_system_exception',
                            'prev_gsa_adj','prev_nongsa_adj','prev_forecast_adj','prev_system_adj',
                            'prev_gsa_supply','prev_nongsa_supply','prev_forecast_supply','prev_system_supply',
                            'new_exception_gsa','new_exception_nongsa','new_exception_forecast','new_exception_system',
                            'prev_gsa_advance_commit_irp','prev_nongsa_advance_commit_irp','prev_forecast_advance_commit_irp','prev_system_advance_commit_irp','process_type',
                            'cumulative_forecast_advance_commit_irp','cumulative_gsa_advance_commit_irp','cumulative_nongsa_advance_commit_irp','cumulative_system_advance_commit_irp',
                            'prev_cumulative_gsa_advance_commit_irp','prev_cumulative_nongsa_advance_commit_irp','prev_cumulative_forecast_advance_commit_irp','prev_cumulative_system_advance_commit_irp',
                            'prev_gsa_de_commit','prev_nongsa_de_commit','prev_forecast_de_commit','prev_system_de_commit')

final_rows_tobe_updated=dataframe2.withColumnRenamed('new_process_type','process_type')\
                                 .withColumnRenamed('new_gsa_de_commit','gsa_de_commit')\
                                 .withColumnRenamed('new_nongsa_de_commit','nongsa_de_commit')\
                                 .withColumnRenamed('new_forecast_de_commit','forecast_de_commit')\
                                 .withColumnRenamed('new_system_de_commit','system_de_commit')\
                                 .withColumnRenamed('new_gsa_advance_commit_irp','cumulative_gsa_advance_commit_irp')\
                                 .withColumnRenamed('new_nongsa_advance_commit_irp','cumulative_nongsa_advance_commit_irp')\
                                 .withColumnRenamed('new_forecast_advance_commit_irp','cumulative_forecast_advance_commit_irp')\
                                 .withColumnRenamed('new_system_advance_commit_irp','cumulative_system_advance_commit_irp')
                                 
                            
# final_rows

tobe_inserted=json_final_df.join(id_tobe_updated,['prod_week','prod_year','ori_part_number'],how='left_anti')

tobe_inserted_dyn=DynamicFrame.fromDF(tobe_inserted,glueContext,'tobe_inserted_dyn')

final_tobe_inserted_dyn = DropNullFields.apply(frame = tobe_inserted_dyn, transformation_ctx = "final_tobe_inserted_dyn")

final_rows_updated_dyn=DynamicFrame.fromDF(final_rows_tobe_updated,glueContext,'final_rows_updated_dyn')

final_rows_tobe_updated_dyn=DropNullFields.apply(frame = final_rows_updated_dyn, transformation_ctx = "final_rows_tobe_updated_dyn")

rr_rows_tobe_loaded_dyn=DropNullFields.apply(frame = resolvechoice_dyn1, transformation_ctx = "rr_rows_tobe_loaded_dyn")


# Updating Query

datasink3 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = final_tobe_inserted_dyn, catalog_connection = "rds-connection", connection_options = {"dbtable":"forecast_plan","database":"User_App"}, transformation_ctx = "datasink3")


datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = final_rows_tobe_updated_dyn, catalog_connection = "rds-connection", connection_options = {"dbtable":"staging","database":"User_App"}, transformation_ctx = "datasink4")

condb("drop table if exists User_App.forecast_RR")
datasink5 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = rr_rows_tobe_loaded_dyn, catalog_connection = "rds-connection", connection_options = {"dbtable":"forecast_RR","database":"User_App"}, transformation_ctx = "datasink5")

# datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = rr_file_load, catalog_connection = "rds-connection", connection_options = {"dbtable":"forecast_RR","database":"User_App"}, transformation_ctx = "datasink4")

pro1_call="""call User_App.Update_procedure"""

condb(pro1_call)

time.sleep(10)


###############################
###        Phase 2        #####
###############################

complete_current_year=str(year)
current_year=complete_current_year[2:]

# ###################################

updating_the_supplier_id_query="""update User_App.forecast_plan as a join User_App.active_suppliers as b 
                                    set a.source_supplier_id=b.id
                                    where upper(a.source_supplier) like upper(concat(b.glue_supplier_name,"%"));"""
                                    
condb(updating_the_supplier_id_query)

logging.info("Updated the Supplier Id ")


####################################

updating_the_escalation_flags_query="""update User_App.forecast_plan as a  
                                    set a.forecast_escalation_flag=null,a.gsa_escalation_flag=null,nongsa_escalation_flag=null,system_escalation_flag=null
                                    where process_week={}""".format(week_num)
                                    
condb(updating_the_escalation_flags_query)

logging.info("Reseting all the category Escalation Flags to null")


###################################

carry_forward_valid_records="""Update User_App.forecast_plan set 
                                forecast_original=0,forecast_commit=0,nongsa_original=0,nongsa_commit=0,gsa_original=0,gsa_commit=0,system_original=0,system_commit=0,
                                active=1,old_data=0,process_week={},process_year={},updated_by=2,updated_at=%s,buffer=0,buffer_opt_adj=0,on_hand_nettable_si=0,
                                on_hand_total_cm=0,past_due_open_po=0,current_open_po=0,received_qty=0,carry_forward=1,
                                cumulative_forecast_advance_commit_irp=(ifnull(forecast_advance_commit_irp,0)+ ifnull(cumulative_forecast_advance_commit_irp,0)),
                                cumulative_gsa_advance_commit_irp=(ifnull(gsa_advance_commit_irp,0)+ ifnull(cumulative_gsa_advance_commit_irp,0)),
                                cumulative_nongsa_advance_commit_irp=(ifnull(nongsa_advance_commit_irp,0)+ ifnull(cumulative_nongsa_advance_commit_irp,0)),
                                cumulative_system_advance_commit_irp=(ifnull(system_advance_commit_irp,0)+ ifnull(cumulative_system_advance_commit_irp,0)),
                                forecast_advance_commit_irp=0,gsa_advance_commit_irp=0,nongsa_advance_commit_irp=0,system_advance_commit_irp=0,
                                oripartnumber_week_year=concat(ori_part_number,'-',prod_week,'-',prod_year)
                                where
                                process_week!={} and (ifnull(forecast_adj,0)!=0 or ifnull(nongsa_adj,0)!=0 or ifnull(gsa_adj,0)!=0 or ifnull(system_adj,0)!=0 or 
                                ifnull(forecast_supply,0)!=0 or ifnull(nongsa_supply,0)!=0 or ifnull(gsa_supply,0)!=0 or ifnull(system_supply,0)!=0 or 
                                ifnull(gsa_advance_commit_irp,0)!=0 or ifnull(nongsa_advance_commit_irp,0)!=0 or ifnull(forecast_advance_commit_irp,0)!=0 or ifnull(system_advance_commit_irp,0)!=0 or
                                ifnull(cumulative_gsa_advance_commit_irp,0)!=0 or ifnull(cumulative_nongsa_advance_commit_irp,0)!=0 or ifnull(cumulative_forecast_advance_commit_irp,0)!=0 or 
                                ifnull(cumulative_system_advance_commit_irp,0)!=0)
                                and ((prod_year={} and prod_week>={}) or (prod_year>{}))""".format(week_num,complete_current_year,week_num,current_year,week_num,current_year)
col=[current_time]
condb(carry_forward_valid_records,col)
print("valid records carry forwarded")

####################################

making_invalid_records_inactive="""update User_App.forecast_plan set active=0,old_data=1 where ((prod_week<{} and prod_year={}) or (prod_year<{}));""".format(week_num,current_year,current_year)
condb(making_invalid_records_inactive)
print("invalid records made inactive")

making_invalid_records_inactive_prev_week="""update User_App.forecast_plan set active=0,old_data=1,carry_forward=0 where 
                                (ifnull(forecast_adj,0)=0 and ifnull(nongsa_adj,0)=0 and ifnull(gsa_adj,0)=0 and ifnull(system_adj,0)=0 and 
                                ifnull(forecast_supply,0)=0 and ifnull(nongsa_supply,0)=0 and ifnull(gsa_supply,0)=0 and ifnull(system_supply,0)=0 and 
                                ifnull(gsa_advance_commit_irp,0)=0 and ifnull(nongsa_advance_commit_irp,0)=0 and ifnull(forecast_advance_commit_irp,0)=0 and ifnull(system_advance_commit_irp,0)=0 and
                                ifnull(cumulative_gsa_advance_commit_irp,0)=0 and ifnull(cumulative_nongsa_advance_commit_irp,0)=0 and ifnull(cumulative_forecast_advance_commit_irp,0)=0 and 
                                ifnull(cumulative_system_advance_commit_irp,0)=0) and ((process_week<{} and process_year={}) or process_year<{}) and active=1;""".format(week_num,complete_current_year,complete_current_year)
                                
condb(making_invalid_records_inactive_prev_week)
print("invalid records made inactive from prev week")

moving_the_invalid_records="""insert into User_App.forecast_plan_inactive_records select * from User_App.forecast_plan where active=0"""
condb(moving_the_invalid_records)
print("moved to forecast_plan_inactive_records table")

deleting_inactive_records="""delete from User_App.forecast_plan where active=0"""
condb(deleting_inactive_records)
print("deleting inactive records from forecast_plan")


####################################

dmp_stamping="""update User_App.forecast_plan set dmp_orndmp='DMP',is_dmp=1
                where concat(prod_week,prod_month,prod_year) 
                in (
                	select concat(kc.prod_week,kc.prod_month,kc.prod_year) from User_App.keysight_calendar as kc,
                	(select prod_month,prod_year from User_App.keysight_calendar where prod_week ={}  and prod_year = {}) as kc_curr
                	where concat(kc.prod_year,kc.prod_month) 
                    in (
                		concat(kc_curr.prod_year,kc_curr.prod_month),
                		if (kc_curr.prod_month=12,concat(kc_curr.prod_year+1,1),concat(kc_curr.prod_year,kc_curr.prod_month+1)),
                		if (kc_curr.prod_month=11,concat(kc_curr.prod_year+1,1),if (kc_curr.prod_month=12,concat(kc_curr.prod_year+1,2),concat(kc_curr.prod_year,kc_curr.prod_month+2))) 
                		)
                	) and ((prod_year={} and prod_week>={}) or (prod_year>{}));""".format(week_num,current_year,current_year,week_num,current_year)
                
condb(dmp_stamping)
print("DMP stamping Done")

####################################

ndmp_stamping="""update User_App.forecast_plan set dmp_orndmp='NDMP',is_dmp=0  
                    where concat(prod_week,prod_month,prod_year) not in (
                    select concat(kc.prod_week,kc.prod_month,kc.prod_year)
                    from User_App.keysight_calendar as kc,
                    (select prod_month,prod_year from User_App.keysight_calendar where prod_week = {} and prod_year = {}) as kc_curr
                    where concat(kc.prod_year,kc.prod_month) in (
                    concat(kc_curr.prod_year,kc_curr.prod_month),
                        if (kc_curr.prod_month=12,concat(kc_curr.prod_year+1,1),concat(kc_curr.prod_year,kc_curr.prod_month+1)),
                    if (kc_curr.prod_month=11,concat(kc_curr.prod_year+1,1),   
                    if (kc_curr.prod_month=12,concat(kc_curr.prod_year+1,2),concat(kc_curr.prod_year,kc_curr.prod_month+2))
                    ) ));""".format(week_num,current_year)
                    
condb(ndmp_stamping)
print("NDMP stamping Done")


####################################

process_type_exception_stamping="""update User_App.forecast_plan set process_type='Exception'
                                where dmp_orndmp='DMP' and active=1 and 
                                (((ifnull(forecast_original,0)+ifnull(forecast_adj,0))-(ifnull(forecast_supply,0)+ifnull(forecast_advance_commit_irp,0)+ifnull(cumulative_forecast_advance_commit_irp,0)))!=0 or
                                ((ifnull(system_original,0)+ifnull(system_adj,0))-(ifnull(system_supply,0)+ifnull(system_advance_commit_irp,0)+ifnull(cumulative_system_advance_commit_irp,0)))!=0 or 
                                ((ifnull(gsa_original,0)+ifnull(gsa_adj,0))-(ifnull(gsa_supply,0)+ifnull(gsa_advance_commit_irp,0)+ifnull(cumulative_gsa_advance_commit_irp,0)))!=0 or 
                                ((ifnull(nongsa_original,0)+ifnull(nongsa_adj,0))-(ifnull(nongsa_supply,0)+ifnull(nongsa_advance_commit_irp,0)+ifnull(cumulative_nongsa_advance_commit_irp,0)))!=0);"""


condb(process_type_exception_stamping)
print("process type exception stamping done")


# #####################################

removing_exception_stamping="""update User_App.forecast_plan set process_type=null
                                where active=1 and dmp_orndmp='DMP' and 
                                (((ifnull(forecast_original,0)+ifnull(forecast_adj,0))-(ifnull(forecast_supply,0)+ifnull(forecast_advance_commit_irp,0)+ifnull(cumulative_forecast_advance_commit_irp,0)))=0 and
                                ((ifnull(system_original,0)+ifnull(system_adj,0))-(ifnull(system_supply,0)+ifnull(system_advance_commit_irp,0)+ifnull(cumulative_system_advance_commit_irp,0)))=0 and 
                                ((ifnull(gsa_original,0)+ifnull(gsa_adj,0))-(ifnull(gsa_supply,0)+ifnull(gsa_advance_commit_irp,0)+ifnull(cumulative_gsa_advance_commit_irp,0)))=0 and 
                                ((ifnull(nongsa_original,0)+ifnull(nongsa_adj,0))-(ifnull(nongsa_supply,0)+ifnull(nongsa_advance_commit_irp,0)+ifnull(cumulative_nongsa_advance_commit_irp,0)))=0);"""
                                
condb(removing_exception_stamping)
print("process type nullifying done")

removing_exception_stamping_ndmp="""update User_App.forecast_plan set process_type=null where active=1 and dmp_orndmp='NDMP';"""

condb(removing_exception_stamping_ndmp)
print("process type nullifying for NDMP done")



############################################

Nullfying_lastweek_advance_commits="""update User_App.forecast_plan set forecast_commit1=null,forecast_commit2=null,forecast_commit3=null,forecast_commit4=null,
                                    forecast_commit1_date=null,forecast_commit2_date=null,forecast_commit3_date=null,forecast_commit4_date=null,forecast_cause_code=null,forecast_cause_code_remark=null,
                                    forecast_commit1_accepted=null,forecast_commit2_accepted=null,forecast_commit3_accepted=null,forecast_commit4_accepted=null,
                                    gsa_commit1=null,gsa_commit2=null,gsa_commit3=null,gsa_commit4=null,
                                    gsa_commit1_date=null,gsa_commit2_date=null,gsa_commit3_date=null,gsa_commit4_date=null,gsa_cause_code=null,gsa_cause_code_remark=null,
                                    gsa_commit1_accepted=null,gsa_commit2_accepted=null,gsa_commit3_accepted=null,gsa_commit4_accepted=null,
                                    nongsa_commit1=null,nongsa_commit2=null,nongsa_commit3=null,nongsa_commit4=null,
                                    nongsa_commit1_date=null,nongsa_commit2_date=null,nongsa_commit3_date=null,nongsa_commit4_date=null,nongsa_cause_code=null,nongsa_cause_code_remark=null,
                                    nongsa_commit1_accepted=null,nongsa_commit2_accepted=null,nongsa_commit3_accepted=null,nongsa_commit4_accepted=null,
                                    system_commit1=null,system_commit2=null,system_commit3=null,system_commit4=null,
                                    system_commit1_date=null,system_commit2_date=null,system_commit3_date=null,system_commit4_date=null,system_cause_code=null,system_cause_code_remark=null,
                                    system_commit1_accepted=null,system_commit2_accepted=null,system_commit3_accepted=null,system_commit4_accepted=null
                                    where active=1 and process_week={}""".format(week_num)
                                    
condb(Nullfying_lastweek_advance_commits)
print("Nullifying the lastweek advance commits")

deleting_vj_records="""delete from User_App.forecast_plan where source_supplier not regexp '^(Celes|Ventu)';"""
condb(deleting_vj_records)

deleting_rr_records="""delete from User_App.forecast_RR where contract_manufacturer not regexp '^(Celes|Ventu)';"""
condb(deleting_rr_records)


###############################################
# Carry Over Past Need By Date
###############################################

update_carry_over_adj="""update User_App.forecast_plan as b join
                    	(select sum(ifnull(kr_qty,0)) as krqty,a.part_number,
                    	group_concat(optional_field1,'-Qty-',kr_qty) as remark from
                    	User_App.non_ascp_request_line as a 
                    	where a.add_adjustment_fnc=1 and (a.remove_adjustment_fnc!=1 or a.remove_adjustment_fnc is null)  
                    	and a.line_status not in('Cancelled','Completed','Open')
                    	and (a.need_by_date<sysdate()) and (a.carry_forward_fnc is null or a.carry_forward_fnc=0) group by a.part_number) as c
                     on b.ori_part_number=c.part_number
                        set b.updated_at=sysdate(),b.updated_by=concat('NAD_CARRY_Batch_',curdate()),b.nongsa_adj_reason='NAD',
                        b.nongsa_adj=ifnull(b.nongsa_adj,0)+(c.krqty),b.nongsa_remarks=if((b.nongsa_remarks is null or b.nongsa_remarks=''),c.remark,concat(b.nongsa_remarks,",",c.remark)),b.process_type=if(b.dmp_orndmp='DMP','Exception',null)
                     where b.prod_week=week(curdate()) and b.prod_year=substring(year(curdate()),3) and b.ori_part_number=c.part_number;"""
                     
condb(update_carry_over_adj)


logging.info("update_carry_over_adj")

insert_carry_over_adj="""insert into User_App.forecast_plan(bu,buffer,buffer_opt_adj,build_type,current_open_po,date,dept_code,division,
                            nongsa_adj,nongsa_commit,nongsa_final,nongsa_original,nongsa_adj_reason,
                            total_adj,total_commit,total_final,total_original,exception,is_escalated,
                            on_hand_nettable_si,on_hand_total_cm,ori_part_number,past_due_open_po,planner_code,
                            planner_name,prod_month,prod_week,prod_year,prod_year_week,
                            prod_year_month,product_family,source_supplier,source_supplier_id,
                            created_at,created_by,product_line,finished_goods,active,total_supply,
                            cid_mapped_part_number,dmp_orndmp,org,instrument,description,
                            item_type,process_week,process_year,is_dmp,old_data,manually_added,
                            cal_option,measurable,countable,oripartnumber_week_year,nongsa_remarks,updated_by,updated_at,process_type)
                        select ip.bu as bu,0 as buffer,0 as buffer_opt_adj,ip.build_type,0 as current_open_po,
                          (select prod_date 
                          from 
                          User_App.keysight_calendar 
                          where id=(select max(id) from User_App.keysight_calendar where prod_date<=date(curdate()))) as date,
                          ip.dept as dept_code,ip.division as division,
                        	ifnull(sum(ifnull(kr_qty,0)),0) as nongsa_adj,0 as nongsa_commit,ifnull(sum(ifnull(kr_qty,0)),0) as nongsa_final,0 as nongsa_original,
                        	'NAD' as nongsa_adj_reason,ifnull(sum(ifnull(kr_qty,0)),0) total_adj,0 as total_commit,ifnull(sum(ifnull(kr_qty,0)),0) as total_final,0 as total_original,0 as exception,
                        	0 as is_escalated,0 as on_hand_nettable_si,0 as on_hand_total_cm,a.part_number as ori_part_number,0 as past_due_open_po,ip.part_planner_code as planner_code,
                        	ip.part_planner_name as planner_name,
                        	(select prod_month from User_App.keysight_calendar where id=(select max(id) from User_App.keysight_calendar where prod_date<=date(curdate()))) as prod_month,
                        	(select prod_week from User_App.keysight_calendar where id=(select max(id) from User_App.keysight_calendar where prod_date<=date(curdate()))) as prod_week,
                        	(select prod_year from User_App.keysight_calendar where id=(select max(id) from User_App.keysight_calendar where prod_date<=date(curdate()))) as prod_year,
                        	(select concat(prod_year,prod_week) from User_App.keysight_calendar where id=(select max(id) from User_App.keysight_calendar 
                        	where prod_date<=date(curdate()))) as prod_year_week,
                        	(select concat(prod_year,prod_month) from User_App.keysight_calendar where id=(select max(id) from User_App.keysight_calendar 
                        	where prod_date<=date(curdate()))) as prod_year_month,
                        	ip.product_family,ip.supplier_name as source_supplier,
                        	(select id from User_App.active_suppliers where supplier_name=ip.supplier_name) as source_supplier_id,
                        	sysdate() as created_at,concat('NAD_CARRY_Batch_',curdate()) created_by,ip.product_line as product_line,'Y' as finished_goods,1 as active,0 as total_supply,
                        	ip.cid_map as cid_mapped_part_number,"DMP" as dmp_orndmp,ip.part_site as org,ip.instrument as instrument,
                        	ip.part_description as description,ip.part_item_type as item_type,week(curdate()) as process_week,year(curdate()) as process_year,
                        	1 as is_dmp,0 as old_data,0 as manually_added,ip.kr_special_opt_cal as cal_option,
                        	if(upper(ip.measureable_flag)='Y',1,
                        		if(upper(ip.measureable_flag)='N',0,Null)) as measurable,
                        	if(ip.countable_flag='Yes',1,0) as countable,
                        	concat(ip.part_name,'-',week(curdate()),'-',substring(year(curdate()),3)) oripartnumber_week_year,
                        	group_concat(optional_field1,'-Qty-',kr_qty) as nongsa_remarks,concat('NAD_CARRY_Batch_',curdate()) as updated_by,
                        	sysdate() as updated_at,'Exception' as process_type
                        	from User_App.non_ascp_request_line as a,User_App.iodm_part_list as ip
                        	where a.add_adjustment_fnc=1 and (a.remove_adjustment_fnc!=1 or a.remove_adjustment_fnc is null) 
                        	and a.line_status not in('Cancelled','Completed','Open')
                        	and (a.need_by_date<sysdate()) and (a.carry_forward_fnc is null or a.carry_forward_fnc=0) and a.part_number=ip.part_name
                        	and not exists
                        	(select 1 from User_App.forecast_plan as fp where fp.prod_week={} and fp.ori_part_number=a.part_number and 
                        	fp.prod_year=substring(year(curdate()),3))
                        	group by a.part_number;""".format(week_num)
                        	
condb(insert_carry_over_adj)


logging.info("insert_carry_over_adj")

insert_adj_notification_carry_over="""insert into User_App.adjustment_notification(build_type,business_unit,created_at,created_by,demand_category,dept_code,division,dmp_orndmp,
                            instrument,new_value,old_value,ori_part_number,planner_code,planner_name,process_week,process_year,prod_week,
                            prod_year,product_family,product_line,source_supplier,adjustment_reason,planner_remark,`status`)
                            select ip.build_type as build_type,ip.bu,sysdate() as created_at,concat('NAD_CARRY_Batch_',curdate()) as created_by,
                            'Non GSA' as demand_category,ip.dept as dept_code,ip.division as division,'DMP' as dmp_orndmp,
                            ip.instrument as instrument,pri.krqty as new_value,0 as old_value,pri.part_number as ori_part_number,
                            ip.part_planner_code as planner_code,ip.part_planner_name as planner_name,week(curdate()) as process_week,year(curdate()) as process_year,
                            week(curdate()) as prod_week,substring(year(curdate()),3) as prod_year,ip.product_family as product_family,
                            ip.product_line as product_line,pri.supplier_name as source_supplier,'NAD' as adjustment_reason,pri.remark as planner_remark,null as `status`
                             from User_App.iodm_part_list as ip join 
                            (select c.part_number,c.krqty,c.remark,c.supplier_name from User_App.forecast_plan as b join
                            	(select sum(ifnull(kr_qty,0)) as krqty,a.part_number,
                            	group_concat(optional_field1,'-Qty-',kr_qty) as remark,a.supplier_name from
                            	User_App.non_ascp_request_line as a 
                            	where a.add_adjustment_fnc=1 and (a.remove_adjustment_fnc!=1 or a.remove_adjustment_fnc is null)  
                            	and a.line_status not in('Cancelled','Completed','Open')
                            	and (a.need_by_date<sysdate()) and (a.carry_forward_fnc is null or a.carry_forward_fnc=0) group by a.part_number) as c
                             on b.ori_part_number=c.part_number
                             where b.prod_week=week(curdate()) and b.prod_year=substring(year(curdate()),3) and b.ori_part_number=c.part_number) as pri
                             on ip.part_name=pri.part_number;"""
                             
condb(insert_adj_notification_carry_over)


logging.info("insert_adj_notification_carry_over")

updating_carry_forward_fnc_flag="""update User_App.non_ascp_request_line as a set carry_forward_fnc=1 where 
                        a.add_adjustment_fnc=1 and (a.remove_adjustment_fnc!=1 or a.remove_adjustment_fnc is null)  
                    	and a.line_status not in('Cancelled','Completed','Open')
                    	and (a.need_by_date<sysdate()) 
                        and (a.carry_forward_fnc is null or a.carry_forward_fnc=0);"""
                        

condb(updating_carry_forward_fnc_flag)

logging.info("updating_carry_forward_fnc_flag")


###############################################
# Update Adj for Non Planning phase
###############################################

update_non_planning_adj_gt="""update User_App.forecast_plan as fp 
                            join 
                            	(select ifnull(sum(kr_qty),0) as kr_qty, narl.part_number as part_number, narl.ww as ww, substring(year(narl.need_by_date),3) as year,
                            	group_concat(narl.optional_field1,'-Qty-',narl.kr_qty) as remark
                            	from User_App.non_ascp_request_header as narh,User_App.non_ascp_request_line as narl
                            	where narh.request_id=narl.req_id and narh.approval_status in ('Approved','Order In Progress') and
                            	(narh.approved_phase is not null and narh.approved_phase not in ('PLANNING')) and 
                            	(narl.add_adjustment_fnc is null or add_adjustment_fnc=0) 
                            	and (narl.need_by_date>DATE_SUB(DATE(now()), INTERVAL DAYOFWEEK(now())+6 DAY)) 
                            	group by narl.part_number,narl.ww,substring(year(narl.need_by_date),3)) as nad 
                            on fp.ori_part_number=nad.part_number and 
                            fp.prod_week=if((nad.ww<week(curdate()) and nad.year=substring(year(curdate()),3)),if((nad.ww<week(curdate()) and nad.year>substring(year(curdate()),3)),nad.ww,week(curdate())),nad.ww) 		
							and fp.prod_year=nad.year
                            set fp.nongsa_adj=ifnull(fp.nongsa_adj,0)+ifnull(nad.kr_qty,0),fp.nongsa_remarks=if((fp.nongsa_remarks is null or fp.nongsa_remarks=''),nad.remark,concat(fp.nongsa_remarks,",",nad.remark)),fp.nongsa_adj_reason='NAD',
                            fp.process_type=if(fp.dmp_orndmp='DMP','Exception',null),updated_by=concat('NAD_NON_PLANNING_Batch_',curdate()),updated_at=sysdate();"""
                            
condb(update_non_planning_adj_gt)



logging.info("update_non_planning_adj")

insert_non_planning_adj="""insert into User_App.forecast_plan(bu,buffer,buffer_opt_adj,build_type,current_open_po,date,dept_code,division,
                                nongsa_adj,nongsa_commit,nongsa_final,nongsa_original,nongsa_adj_reason,
                                total_adj,total_commit,total_final,total_original,exception,is_escalated,
                                on_hand_nettable_si,on_hand_total_cm,ori_part_number,past_due_open_po,planner_code,
                                planner_name,prod_month,prod_week,prod_year,prod_year_week,
                                prod_year_month,product_family,source_supplier,source_supplier_id,
                                created_at,created_by,product_line,finished_goods,active,total_supply,
                                cid_mapped_part_number,dmp_orndmp,org,instrument,description,
                                item_type,process_week,process_year,is_dmp,old_data,manually_added,
                                cal_option,measurable,countable,oripartnumber_week_year,nongsa_remarks,updated_by,updated_at,process_type)                   
                        	select ip.bu as bu,0 as buffer,0 as buffer_opt_adj,ip.build_type,0 as current_open_po,
                        	(select prod_date from User_App.keysight_calendar where id=(select max(id) from User_App.keysight_calendar where prod_date<=date(if(date(a.need_by_date)<curdate(),curdate(),a.need_by_date)))) as date,
                        	ip.dept as dept_code,ip.division as division,
                        	ifnull(sum(ifnull(kr_qty,0)),0) as nongsa_adj,0 as nongsa_commit,
                        	ifnull(sum(ifnull(kr_qty,0)),0) as nongsa_final,0 as nongsa_original,'NAD' as nongsa_adj_reason,
                        	ifnull(sum(ifnull(kr_qty,0)),0) total_adj,0 as total_commit,ifnull(sum(ifnull(kr_qty,0)),0) as total_final,0 as total_original,0 as exception,
                        	0 as is_escalated,0 as on_hand_nettable_si,0 as on_hand_total_cm,a.part_number as ori_part_number,0 as past_due_open_po,
                        	ip.part_planner_code as planner_code,ip.part_planner_name as planner_name,
                        	(select prod_month from User_App.keysight_calendar where id=(select max(id) from User_App.keysight_calendar where prod_date<=date(a.need_by_date))) as prod_month,
							(select prod_week from User_App.keysight_calendar where id=(select max(id) from User_App.keysight_calendar where prod_date<=date(a.need_by_date))) as prod_week,
                        	(select prod_year from User_App.keysight_calendar where id=(select max(id) from User_App.keysight_calendar where prod_date<=date(a.need_by_date))) as prod_year,
                        	(select concat(prod_year,prod_week) from User_App.keysight_calendar where id=(select max(id) from User_App.keysight_calendar where 
                        	prod_date<=date(a.need_by_date))) as prod_year_week,
                        	(select concat(prod_year,prod_month) from User_App.keysight_calendar where id=(select max(id) from User_App.keysight_calendar where 
                        	prod_date<=date(a.need_by_date))) as prod_year_month,
                        	ip.product_family,ip.supplier_name as source_supplier,
                        	if(ip.supplier_name='Venture Electronics Services (Malaysia) Sdn. Bhd.',18,
                        	IF(ip.supplier_name='Celestica Electronics (M) Sdn. Bhd.',20,
                        		IF(ip.supplier_name='Jabil Circuit Sdn Bhd',17,4))) as source_supplier_id,
                        	sysdate() as created_at,concat('NAD_NON_PLANNING_Batch_',curdate()) as created_by,ip.product_line as product_line,'Y' as finished_goods,1 as active,0 as total_supply,ip.cid_map as cid_mapped_part_number,
                        	"DMP" as dmp_orndmp,ip.part_site as org,ip.instrument as instrument,
                        	ip.part_description as description,ip.part_item_type as item_type,week(curdate()) as process_week,year(curdate()) as process_year,
                        	1 as is_dmp,0 as old_data,0 as manually_added,ip.kr_special_opt_cal as cal_option,if(upper(ip.measureable_flag)='Y',1,
                        	if(upper(ip.measureable_flag)='N',0,Null)) as measurable,if(ip.countable_flag='Yes',1,0) as countable,
                        	concat(ip.part_name,'-',
                            (select prod_week from User_App.keysight_calendar 
                            where id=(select max(id) from User_App.keysight_calendar where prod_date<=date(a.need_by_date))),'-',
                            (select prod_year from User_App.keysight_calendar 
                            where id=(select max(id) from User_App.keysight_calendar where prod_date<=date(a.need_by_date)))) as oripartnumber_week_year,
                        	group_concat(a.optional_field1,'-Qty-',a.kr_qty) as nongsa_remarks,concat('NAD_NON_PLANNING_Batch_',curdate()) as updated_by,
                        	sysdate() as updated_at,'Exception' as process_type
                        	from 
                        	User_App.non_ascp_request_line as a 
                        	join 
                        	User_App.non_ascp_request_header as b on b.request_id=a.req_id
                        	join 
                        	User_App.iodm_part_list as ip on a.part_number=ip.part_name
                        	where 
                        	b.request_id=a.req_id and b.approval_status in ('Approved','Order In Progress') and
                        	(b.approved_phase is not null or b.approved_phase not in ('PLANNING')) and 
                        	(a.add_adjustment_fnc is null or a.add_adjustment_fnc =0) and 
                        	(a.need_by_date>DATE_SUB(DATE(now()), INTERVAL DAYOFWEEK(now())+6 DAY)) and
                        	a.part_number=ip.part_name 
                        	and not exists 
                        	(select 1 from User_App.forecast_plan as fp where fp.prod_week=if(a.ww<week(curdate()),week(curdate()),a.ww) and fp.ori_part_number=a.part_number and 
                        	fp.prod_year=substring(year(a.need_by_date),3))
                        	group by a.part_number,a.ww,substring(year(a.need_by_date),3);"""     
                        	
condb(insert_non_planning_adj)


logging.info("insert_non_planning_adj")

insert_into_adj_notification_non_planning="""insert into User_App.adjustment_notification(build_type,business_unit,created_at,created_by,demand_category,dept_code,division,dmp_orndmp,
                                instrument,new_value,old_value,ori_part_number,planner_code,planner_name,process_week,process_year,prod_week,
                                prod_year,product_family,product_line,source_supplier,adjustment_reason,planner_remark,`status`)
                                select ip.build_type as build_type,ip.bu,sysdate() as created_at,concat('NAD_NON_PLANNING_Batch_',curdate()) as created_by,
                                'Non GSA' as demand_category,ip.dept as dept_code,ip.division as division,'DMP' as dmp_orndmp,
                                ip.instrument as instrument,pri.krqty as new_value,0 as old_value,pri.part_number as ori_part_number,
                                ip.part_planner_code as planner_code,ip.part_planner_name as planner_name,week(curdate()) as process_week,year(curdate()) as process_year,
                                if((pri.ww<week(curdate()) and pri.year=substring(year(curdate()),3)),
                                if((pri.ww<week(curdate()) and pri.year>substring(year(curdate()),3)),pri.ww,week(curdate())),pri.ww) as prod_week,
                                pri.year as prod_year,ip.product_family as product_family,
                                ip.product_line as product_line,pri.source_supplier as source_supplier,'NAD' as adjustment_reason,pri.remark as planner_remark,null as `status`
                                from User_App.iodm_part_list as ip join 
                                (select c.part_number,c.krqty,c.remark,c.source_supplier,c.ww,c.year from User_App.forecast_plan as b join
                                (select ifnull(sum(kr_qty),0) as krqty, narl.part_number as part_number, narl.ww as ww, 
                                substring(year(narl.need_by_date),3) as year,narl.supplier_name as source_supplier,
                                group_concat(narl.optional_field1,'-Qty-',narl.kr_qty) as remark
                                from User_App.non_ascp_request_header  as narh,User_App.non_ascp_request_line as narl
                                where narh.request_id=narl.req_id and narh.approval_status in ('Approved','Order In Progress') and
                                (narh.approved_phase is not null and narh.approved_phase not in ('PLANNING')) and 
                                (narl.add_adjustment_fnc is null or add_adjustment_fnc=0) 
                                and (narl.need_by_date>DATE_SUB(DATE(now()), INTERVAL DAYOFWEEK(now())+6 DAY)) 
                                group by narl.part_number,narl.ww,substring(year(narl.need_by_date),3)) as c
                                on b.ori_part_number=c.part_number
                                where b.prod_week=if((c.ww<week(curdate()) and c.year=substring(year(curdate()),3)),
                                if((c.ww<week(curdate()) and c.year>substring(year(curdate()),3)),c.ww,week(curdate())),c.ww) and b.prod_year=c.year and b.ori_part_number=c.part_number) as pri
                                on ip.part_name=pri.part_number;"""
                                 
condb(insert_into_adj_notification_non_planning)


logging.info("insert_into_adj_notification_non_planning")


updating_add_fnc_flag="""update User_App.non_ascp_request_line as narl
                        join
                        User_App.non_ascp_request_header as narh
                        on
                        narh.request_id=narl.req_id
                        set narl.add_adjustment_fnc=1,narl.last_updated_date=sysdate()
                        	where narh.request_id=narl.req_id and narh.approval_status in ('Approved','Order In Progress') and
                        	(narh.approved_phase is not null and narh.approved_phase not in ('PLANNING')) and 
                        	(narl.add_adjustment_fnc is null or add_adjustment_fnc=0) 
                        	and (narl.need_by_date>DATE_SUB(DATE(now()), INTERVAL DAYOFWEEK(now())+6 DAY));"""

condb(updating_add_fnc_flag)


logging.info("updating_add_fnc_flag")

###############################################
# Remove Adj 
###############################################

remove_gt_curr_week="""update
                    	User_App.forecast_plan as fp 
                        join
                    		(select group_concat(optional_field1,'-Qty-',kr_qty) as old_remark,
                            group_concat('') as new_remark
                            ,part_number,sum(kr_qty) as qty,ww 
                            from 
                            User_App.non_ascp_request_line as narl 
                            where 
                    		narl.line_status in ('Completed','Cancelled') 
                            and
                    		(narl.add_adjustment_fnc is not null and narl.add_adjustment_fnc=1) 
                    		and (remove_adjustment_fnc is null  or remove_adjustment_fnc=0)
                    		and (need_by_date>DATE_SUB(DATE(now()), INTERVAL DAYOFWEEK(now())+6 DAY))
                            group by part_number,ww
                    		) as nad
                    	on fp.ori_part_number=nad.part_number
                        set fp.nongsa_adj=ifnull(fp.nongsa_adj,0)-ifnull(nad.qty,0),
                        fp.nongsa_remarks=nad.new_remark,fp.updated_at=sysdate(),fp.updated_by=concat('NAD_REMOVE_Batch_',curdate())
                        where fp.ori_part_number=nad.part_number and fp.prod_week=nad.ww;"""
                        
# condb(remove_gt_curr_week)


logging.info("remove_gt_curr_week")


remove_lt_curr_week="""update
                        User_App.forecast_plan as fp 
                            join
                        		(select group_concat(optional_field1,'-Qty-',kr_qty) as remark,
                                group_concat('') as new_remark,
                                part_number,sum(kr_qty) as qty,ww 
                                from 
                                User_App.non_ascp_request_line as narl 
                                where 
                        		narl.line_status in ('Completed','Cancelled') 
                                and
                        		(narl.add_adjustment_fnc is not null and narl.add_adjustment_fnc=1) 
                        		and (remove_adjustment_fnc is null  or remove_adjustment_fnc=0)
                        		and (need_by_date<DATE_SUB(DATE(now()), INTERVAL DAYOFWEEK(now())-2 DAY))
                                group by part_number,ww
                        		) as nad
                        	on fp.ori_part_number=nad.part_number
                            set fp.nongsa_adj=ifnull(fp.nongsa_adj,0)-(nad.qty),
                            fp.nongsa_remarks=nad.new_remark,fp.updated_at=sysdate(),fp.updated_by=concat('NAD_REMOVE_Batch_',curdate())
                            where fp.prod_week=week(curdate()) and fp.nongsa_adj_reason='NAD';"""
                            
# condb(remove_lt_curr_week)


logging.info("remove_lt_curr_week")

insert_into_adj_notification_remove_adj="""insert into User_App.adjustment_notification(build_type,business_unit,created_at,created_by,demand_category,dept_code,division,dmp_orndmp,
                                instrument,new_value,old_value,ori_part_number,planner_code,planner_name,process_week,process_year,prod_week,
                                prod_year,product_family,product_line,source_supplier,adjustment_reason,planner_remark,`status`)
                                select ip.build_type as build_type,ip.bu,sysdate() as created_at,concat('NAD_REMOVE_ADJ_Batch_',curdate()) as created_by,
                                'Non GSA' as demand_category,ip.dept as dept_code,ip.division as division,'DMP' as dmp_orndmp,
                                ip.instrument as instrument,pri.krqty as new_value,0 as old_value,pri.part_number as ori_part_number,
                                ip.part_planner_code as planner_code,ip.part_planner_name as planner_name,week(curdate()) as process_week,year(curdate()) as process_year,
                                week(curdate()) as prod_week,substring(year(curdate()),3) as prod_year,ip.product_family as product_family,
                                ip.product_line as product_line,pri.source_supplier as source_supplier,'NAD' as adjustment_reason,pri.remark as planner_remark,null as `status`
                                 from User_App.iodm_part_list as ip join 
                                (select c.part_number,(-1*c.krqty) as krqty,c.remark,c.source_supplier from User_App.forecast_plan as b join
                                	(select ifnull(sum(kr_qty),0) as krqty, narl.part_number as part_number, narl.ww as ww, 
                                	substring(year(narl.need_by_date),3) as year,narl.supplier_name as source_supplier,
                                	group_concat(narl.optional_field1,'-Qty-',narl.kr_qty) as remark
                                	from User_App.non_ascp_request_line as narl
                                	where narl.line_status in ('Completed','Cancelled') 
                                    and
                            		(narl.add_adjustment_fnc is not null and narl.add_adjustment_fnc=1) 
                            		and (remove_adjustment_fnc is null  or remove_adjustment_fnc=0)
                            		and (need_by_date>DATE_SUB(DATE(now()), INTERVAL DAYOFWEEK(now())+6 DAY))
                                    group by part_number,ww,substring(year(narl.need_by_date),3)) as c
                                 on b.ori_part_number=c.part_number
                                 where b.prod_week=if(c.ww<week(curdate()),week(curdate()),c.ww) and b.prod_year=substring(year(curdate()),3) and b.ori_part_number=c.part_number) as pri
                                 on ip.part_name=pri.part_number;"""
                                 
# condb(insert_into_adj_notification_remove_adj)


logging.info("insert_into_adj_notification_remove_adj")

update_remove_fnc_adj="""update User_App.non_ascp_request_line as narl
                    		set narl.remove_adjustment_fnc=1,narl.last_updated_date=sysdate()
                    			where narl.line_status in ('Completed','Cancelled') 
                                and
                        		(narl.add_adjustment_fnc is not null and narl.add_adjustment_fnc=1) 
                        		and (remove_adjustment_fnc is null  or remove_adjustment_fnc=0)
                        		and (need_by_date>DATE_SUB(DATE(now()), INTERVAL DAYOFWEEK(now())+6 DAY));"""
# condb(update_remove_fnc_adj)

condb("call User_App.fnc_nad_weekly_processing();")

logging.info("update_remove_fnc_adj")

logging.info("updating dmp_stamping 2")
condb(dmp_stamping)

logging.info("updating ndmp_stamping 2")
condb(ndmp_stamping)

logging.info("process_type_exception_stamping 2")
condb(process_type_exception_stamping)

logging.info("removing_exception_stamping 2")
condb(removing_exception_stamping)

logging.info("removing_exception_stamping_ndmp 2")
condb(removing_exception_stamping_ndmp)

logging.info("update_remove_fnc_adj")
condb(updating_the_escalation_flags_query)

condb(updating_the_supplier_id_query)

logging.info("Updated the Supplier Id ")


glue_client=boto3.client("glue",region_name='ap-southeast-1')

response = glue_client.start_job_run(JobName = 'snowflake_DB')

logging.info("CM wip raw data job started")


job.commit()
