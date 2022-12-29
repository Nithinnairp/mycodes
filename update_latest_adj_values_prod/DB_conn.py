import boto3
import json
import pymysql
import os

def condb(sql, col=None):
    DB_name=os.environ['DB_name']
    client = boto3.client("secretsmanager", region_name='ap-southeast-1')
    get_secret_value_response = client.get_secret_value(SecretId="cp-prod-aurora-serverless-01-Glue")
    
    #Script for retreving the rds credentials
    secret = get_secret_value_response['SecretString']
    secret =json.loads(secret)
    rds_dbname = DB_name
    rds_host = secret.get("host")
    rds_port = secret.get("port")
    rds_username = secret.get("username")
    rds_password = secret.get("password")
    
    conn = pymysql.connect(rds_host,rds_username,rds_password,rds_dbname)
    cursor = conn.cursor()
    cursor.execute(sql, col)
    df = cursor.fetchall()
    conn.commit()
    return df

def condb_dict(sql, col=None):
    DB_name=os.environ['DB_name']
    client = boto3.client("secretsmanager", region_name='ap-southeast-1')
    get_secret_value_response = client.get_secret_value(SecretId="cp-prod-aurora-serverless-01-Glue")
    
    #Script for retreving the rds credentials
    secret = get_secret_value_response['SecretString']
    secret =json.loads(secret)
    rds_dbname = DB_name
    rds_host = secret.get("host")
    rds_port = secret.get("port")
    rds_username = secret.get("username")
    rds_password = secret.get("password")
    
    conn = pymysql.connect(rds_host,rds_username,rds_password,rds_dbname)
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    cursor.execute(sql, col)
    df = cursor.fetchall()
    conn.commit()
    return df