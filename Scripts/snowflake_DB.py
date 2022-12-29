import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from py4j.java_gateway import java_import
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.types import *
import boto3
import json
import pymysql

## @params: [JOB_NAME, URL, ACCOUNT, WAREHOUSE, DB, SCHEMA, USERNAME, PASSWORD]
SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'URL', 'ACCOUNT', 'WAREHOUSE', 'DB', 'SCHEMA', 'USERNAME', 'PASSWORD'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
java_import(spark._jvm, SNOWFLAKE_SOURCE_NAME)
## uj = sc._jvm.net.snowflake.spark.snowflake
spark._jvm.net.snowflake.spark.snowflake.SnowflakeConnectorUtils.enablePushdownSession(spark._jvm.org.apache.spark.sql.SparkSession.builder().getOrCreate())
sfOptions = {
"sfURL" : args['URL'],
"sfAccount" : args['ACCOUNT'],
"sfUser" : args['USERNAME'],
"sfPassword" : args['PASSWORD'],
"sfDatabase" : args['DB'],
"sfSchema" : args['SCHEMA'],
"sfWarehouse" : args['WAREHOUSE'],
}

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

def condb(sql, col=None):
    conn = pymysql.connect(rds_host,rds_username,rds_password,rds_dbname)
    cursor = conn.cursor()
    cursor.execute(sql, col)
    df = cursor.fetchall()
    conn.commit()
    conn.close()
    cursor.close()
    return df    

condb('Truncate {}.cm_wip_raw_data'.format(rds_dbname))

query="""SELECT * FROM CM_WIPDATA_ALL_VIEW WHERE date(datecreated) = (SELECT max(date(DATECREATED)) FROM CM_WIPDATA_ALL_VIEW);"""

## Read from a Snowflake table into a Spark Data Frame
df = spark.read.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptions).option("query",query )\
    .option("header",True)\
    .option('partition_size_in_mb', 128)\
    .option('use_cached_result', 'true').option('use_copy_unload', 'false')\
    .option('parallelism', '30').option('sfCompress', 'on').load()


df1=df.withColumnRenamed('"SortOrder"','sort_order')\
    .withColumnRenamed('"ProcessName"','process_name')\
    .withColumnRenamed('SERIALNUMBERFROMFILE','serial_number_from_file')\
    .withColumnRenamed('SHOPORDER','shop_order')\
    .withColumnRenamed('FAMILY','family')\
    .withColumnRenamed('SUBFAMILY','sub_family')\
    .withColumnRenamed('PRODUCT','product')\
    .withColumnRenamed('ORIPARTNUMBER','ori_part_number')\
    .withColumnRenamed('MODEL','model')\
    .withColumnRenamed('REV','rev')\
    .withColumnRenamed('CURRENTSTATION','current_station')\
    .withColumnRenamed('REGISTDATE','regist_date')\
    .withColumnRenamed('LASTDATETIME','last_datetime')\
    .withColumnRenamed('LASTRESULT','last_result')\
    .withColumnRenamed('SERIALNUMBER','serial_number')\
    .withColumnRenamed('DATECREATED','date_created')\
    .withColumnRenamed('SOURCE','source')\
    .withColumnRenamed('PRODUCTNUMBER','product_number')\
    .withColumnRenamed('PRODUCTFAMILY','product_family')\
    .withColumnRenamed('MANUFACTURER','manufacturer')\
    .withColumnRenamed('SITE','site')\
    .withColumnRenamed('GEOLOCATION','geo_location')\
    .withColumnRenamed('DIVISION','division')\
    .withColumnRenamed('PRODUCTLINE','product_line')\
    .withColumnRenamed('FY','fy')\
    .withColumnRenamed('KEYSIGHTMONTH','keysight_month')\
    .withColumnRenamed('WORKWEEK','work_week')\
    .withColumnRenamed('"Category"','category')

data_dyn=DynamicFrame.fromDF(df1,glueContext,'data_dyn')


## Read from a Snowflake table into a Spark Data Frame
datasink3 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = data_dyn, catalog_connection = "rds-connection", connection_options = {"dbtable":"cm_wip_raw_data","database":rds_dbname}, transformation_ctx = "datasink3")

condb('Call User_App.cm_wip_mapping;')

client = boto3.client('lambda')

lambda_data=json.dumps({"DB":rds_dbname})


response = client.invoke(FunctionName='new_cm_wip_station_mapping',Payload=lambda_data)



job.commit()

## Perform any kind of transformations on your data and save as a new Data Frame: df1 = df.[Insert any filter, transformation, or other operation]
## Write the Data Frame contents back to Snowflake in a new table df1.write.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptions).option("dbtable", "[new_table_name]").mode("overwrite").save() job.commit()