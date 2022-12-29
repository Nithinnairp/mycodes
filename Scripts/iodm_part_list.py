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
## @args: [database = "part_list", table_name = "latest_kr_b2b_master_list_20210307t184531_csv", transformation_ctx = "datasource0"]
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
    
drop_iodm_release_table="""DROP table if exists User_App.iodm_part_list_release_backup;"""
condb(drop_iodm_release_table)

create_backup_table="""create table User_App.iodm_part_list_release_backup select * from User_App.iodm_part_list;"""
condb(create_backup_table)

    
condb("truncate User_App.iodm_part_list")

datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "part_list", table_name = "latest_iodm_part_list_csv_files", transformation_ctx = "datasource0")
## @type: ApplyMapping
## @args: [mapping = [("application", "string", "application", "string"), ("part name", "string", "part name", "string"), ("part site", "string", "part site", "string"), ("part planner code", "string", "part planner code", "string"), ("part planner name", "string", "part planner name", "string"), ("part pl", "string", "part pl", "string"), ("part pl bu", "string", "part pl bu", "string"), ("part pl division", "string", "part pl division", "string"), ("part solution", "string", "part solution", "string"), ("instrument", "string", "instrument", "string"), ("part dept wsf", "string", "part dept wsf", "string"), ("product family", "string", "product family", "string"), ("part description", "string", "part description", "string"), ("part item type", "string", "part item type", "string"), ("part item status", "string", "part item status", "string"), ("product group", "string", "product group", "string"), ("cid map", "string", "cid map", "string"), ("build type", "string", "build type", "string"), ("cto flag", "string", "cto flag", "string"), ("in testing flag", "string", "in testing flag", "string"), ("countable flag", "string", "countable flag", "string"), ("measureable flag", "string", "measureable flag", "string"), ("contract period", "long", "contract period", "long"), ("contract ratio", "double", "contract ratio", "double"), ("kanban lt", "long", "kanban lt", "long"), ("process lt", "long", "process lt", "long"), ("ship rel days", "long", "ship rel days", "long"), ("post proc lt", "long", "post proc lt", "long"), ("kr customization bufferbuildopt", "long", "kr customization bufferbuildopt", "long"), ("kr customization bufferrunrate", "long", "kr customization bufferrunrate", "long"), ("kr customization buffersrt", "long", "kr customization buffersrt", "long"), ("kr customization include kr pull up", "string", "kr customization include kr pull up", "string"), ("kr customization frozenweek", "long", "kr customization frozenweek", "long"), ("kr special opt cal", "string", "kr special opt cal", "string"), ("kr special opt rohs", "string", "kr special opt rohs", "string"), ("kr special opt sys/sol", "string", "kr special opt sys/sol", "string"), ("supplier name", "string", "supplier name", "string"), ("cma remarks", "string", "cma remarks", "string"), ("last update date", "string", "last update date", "string"), ("base key", "string", "base key", "string")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("application", "string", "application", "string"), ("part name", "string", "part_name", "string"), ("site", "string", "part_site", "string"), 
("part planner code", "string", "part_planner_code", "string"), ("part planner name", "string", "part_planner_name", "string"), ("part pl", "string", "product_line", "string"), ("part pl bu", "string", "bu", "string"), 
("part pl division", "string", "division", "string"), ("part solution", "string", "part_solution", "string"), ("instrument", "string", "instrument", "string"), ("part dept wsf", "string", "dept", "string"), 
("product family", "string", "product_family", "string"), ("part description", "string", "part_description", "string"), ("part item type", "string", "part_item_type", "string"), 
("part item status", "string", "part_item_status", "string"), ("product group", "string", "product_group", "string"), ("cid map", "string", "cid_map", "string"), ("build type", "string", "build_type", "string"), 
("cto flag", "string", "cto_flag", "string"), ("in testing flag", "string", "in_testing_flag", "string"), ("countable flag", "string", "countable_flag", "string"), ("measureable flag", "string", "measureable_flag", "string"), 
("contract period", "long", "contract_period", "long"), ("ratio", "double", "contract_ratio", "double"), ("kanban lt", "long", "kanban_lt", "long"), ("process lt", "long", "process_lt", "long"), 
("ship rel days", "long", "ship_rel_days", "long"), ("post proc lt", "long", "post_proc_lt", "long"), ("bufferbuildopt", "long", "kr_customization_bufferbuildopt", "long"), 
("bufferrunrate", "long", "kr_customization_bufferrunrate", "long"), ("buffersrt", "long", "kr_customization_buffer_srt", "long"), 
("include kr pull up", "string", "kr_customization_include_kr_pull_up", "string"), ("frozenweek", "long", "kr_customization_frozenweek", "long"), 
("kr special opt cal", "string", "kr_special_opt_cal", "string"), ("rohs", "string", "kr_special_opt_rohs", "string"), ("sys/sol", "string", "kr_special_opt_sys_sol", "string"), 
("supplier name", "string", "supplier_name", "string"), ("cma remarks", "string", "cma_remarks", "string"), ("last update date", "string", "last_update_date", "string"), ("base key", "string", "base_key", "string")], transformation_ctx = "applymapping1")
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
## @args: [catalog_connection = "rds-connection", connection_options = {"dbtable": "iodm_kr_b2b_master_list_20201011t212713_csv", "database": "User_App"}, transformation_ctx = "datasink4"]
## @return: datasink4
## @inputs: [frame = dropnullfields3]
datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = dropnullfields3, catalog_connection = "rds-connection", connection_options = {"dbtable": "iodm_part_list", "database": "User_App"}, transformation_ctx = "datasink4")

update_release_value="""update User_App.iodm_part_list as a, User_App.iodm_part_list_release_backup as b
                        set 
                        a.release_method=b.release_method,a.release_window=b.release_window
                        where 
                        a.part_name=b.part_name;"""
                        
condb(update_release_value)


job.commit()