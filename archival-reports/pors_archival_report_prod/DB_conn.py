import boto3
import json
import pymysql
import os

SECRECT_ID = os.environ['SECRET_ID']
DB_NAME = os.environ['DB_NAME']


def condb(sql, col=None):
    client = boto3.client("secretsmanager", region_name='ap-southeast-1')
    get_secret_value_response = client.get_secret_value(SecretId=SECRECT_ID)
    
    #Script for retreving the rds credentials
    secret = get_secret_value_response['SecretString']
    secret =json.loads(secret)
    rds_dbname = DB_NAME
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
    client = boto3.client("secretsmanager", region_name='ap-southeast-1')
    get_secret_value_response = client.get_secret_value(SecretId=SECRECT_ID)
    
    #Script for retreving the rds credentials
    secret = get_secret_value_response['SecretString']
    secret =json.loads(secret)
    rds_dbname = DB_NAME
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