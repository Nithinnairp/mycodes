
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3
import pymysql
import json

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "iodm_db", table_name = "latestkr_calendar_20210516t194322_csv", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []

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

#Establishing connection between the Database
def condb(sql,col=None):
    conn=pymysql.connect(rds_host,rds_username,rds_password,rds_dbname)
    cursor=conn.cursor()
    cursor.execute(sql,col)
    df=cursor.fetchall()
    conn.commit()
    conn.close()
    return df
    
truncate_existing_table="""truncate User_App.keysight_calendar;"""
condb(truncate_existing_table)


datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "keysight_calendar", table_name = "latest_converted_csv_file_csv", transformation_ctx = "datasource0")
## @type: ApplyMapping
## @args: [mapping = [("calendar value", "string", "calendar_value", "string"), ("value", "string", "prod_date", "string"), ("ww", "long", "prod_week", "long"), ("month", "long", "prod_month", "long"), ("year", "long", "prod_year", "long")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("calendar value", "string", "calendar_value", "string"), ("value", "string", "prod_date", "string"), 
("ww", "long", "prod_week", "long"), ("month", "long", "prod_month", "long"), ("year", "long", "prod_year", "long")], transformation_ctx = "applymapping1")
## @type: ResolveChoice
## @args: [choice = "make_cols", transformation_ctx = "resolvechoice2"]
## @return: resolvechoice2
## @inputs: [frame = applymapping1]
resolvechoice2 = ResolveChoice.apply(frame = applymapping1, choice = "make_cols", transformation_ctx = "resolvechoice2")
## @type: DropNullFields
## @args: [transformation_ctx = "dropnullfields3"]
## @return: dropnullfields3
## @inputs: [frame = resolvechoice2]
dropnullfields3 = DropNullFields.apply(frame = resolvechoice2, transformation_ctx = "dropnullfields3")
## @type: DataSink
## @args: [catalog_connection = "serverless_rds", connection_options = {"dbtable": "latest_kr_calendar_20201122t210856_csv", "database": "User_App"}, transformation_ctx = "datasink4"]
## @return: datasink4
## @inputs: [frame = dropnullfields3]
datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = dropnullfields3, catalog_connection = "rds-connection", connection_options = {"dbtable": "keysight_calendar", "database": "User_App"}, transformation_ctx = "datasink4")
job.commit()