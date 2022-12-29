import json
import pandas as pd
import datetime
from dateutil.relativedelta import relativedelta
import boto3
import pymysql
import io
import os
from DB_conn import condb,condb_dict

def lambda_handler(event, context):

    DB_NAME = os.environ['DB_NAME']
    S3_BUCKET = os.environ['S3_BUCKET']
    
    current_time=datetime.datetime.now()

    month_from = (current_time - relativedelta(years=1) - relativedelta(months=1) + datetime.timedelta(hours=8))
    month_from_str = month_from.strftime("%Y-%m")
    month_to = month_from + relativedelta(months=1)
    month_to_str = month_to.strftime("%Y-%m")
    archive_year = month_from.strftime("%Y")

    get_po_master_archive_data_query="""SELECT PROD_YEAR_WEEK AS "Work Week", CAST(B2B_CREATION_DATE AS DATE) AS "Last Refresh Date", KEYSIGHT_PART_NUMBER AS "Keysight Part Number", BUYER_SUGGESTED_DOCK_DATE AS "Buyer Suggested Dock Date", BUYER_ACTION AS "Buyer Action", FINAL_COMMIT_DOCK_DATE AS "Final Commit Dock Date", SUPPLIER_REMARK AS "Supplier Remark", ORGANIZATION AS "Org", DESCRIPTION AS "Item Description", LEAD_TIME AS "Lead Time", IBO, IOO,  PLANNER_CODE AS "Planner Code", BUYER_NAME AS "Buyer Name", MGR_NAME AS "Manager Name", SUPPLIER_NAME AS "Supplier Name", SUPPLIER_SITE AS "Supplier Site", CAST(PO_CREATION_DATE AS DATE) AS "PO Creation Date", PO_NUM AS "PO Num #", OLD_QTY AS "Qty", COST AS "Cost", EXT_VALUE AS "Extended Value", CAST(OLD_NEED_BY_DATE AS DATE) AS "Old Dock Date", CAST(NEW_DOCK_DATE AS DATE) AS "Suggested Dock Date", SUGGEST_ACTION AS "Suggested Action", CAST(BR_SUGGESTED_DATE AS DATE) AS "BR Suggested Dock Date", BR_SUGGESTED_ACTION AS "BR Suggested Action", BR_NAME AS "Business Rule", BUYER_REMARK AS "Buyer Remark", CAST(SUPPLIER_COMMIT_DELIVERY_DATE AS DATE) AS "Supplier Commit Delivery Date", CAST(SUPPLIER_COMMIT_DOCK_DATE AS DATE) AS "Supplier Commit Dock Date", SUPPLIER_REASON_CODE AS "Supplier Reason" FROM {}.po_master_archive WHERE B2B_CREATION_DATE >= \"{}\" AND B2B_CREATION_DATE < \"{}\";""".format(DB_NAME, (month_from_str+"-01 00:00:00"), (month_to_str+"-01 00:00:00"))
    
    print("Fetching Data For  ", month_from_str)

    po_master_archive_data = condb_dict(get_po_master_archive_data_query)
    
    if po_master_archive_data:
        primary_query_data=pd.DataFrame(data=po_master_archive_data)

        final_data = primary_query_data.to_csv(index=False,header=True) 

        bucket = S3_BUCKET
        subfolder = "Past_Week_Data" + "/" + archive_year
        s3_file_path = "Archival_Reports" + "/" +subfolder+ "/" + "Past_Week_Data_History(" + month_from_str + ").csv"
        s3 = boto3.client("s3")
        s3.put_object(ACL='public-read',Body=final_data,Bucket=bucket,Key=s3_file_path)

        print(s3_file_path + " created")
    
        delete_po_master_archive_query = """DELETE FROM {}.po_master_archive WHERE B2B_CREATION_DATE >= \"{}\" AND B2B_CREATION_DATE < \"{}\";""".format(DB_NAME, (month_from_str+"-01 00:00:00"), (month_to_str+"-01 00:00:00"))
        condb_dict(delete_po_master_archive_query)
        print("Deleted po_master_archive for ",month_from_str)
    
    else:
        print("No past week data found for month: ", month_from_str)

    get_po_audit_trial_data_query="""SELECT CAST(last_updated_date AS DATE) AS "Last Updated Date", action_taken_by AS "Action Taken By", action AS "Action", keysight_part AS "Keysight Part Number", po_num AS "PO", supplier_mass_update_flag AS "Supplier Mass Update Flag", buyer_mass_update_flag AS "Buyer Mass Update Flag", user, CAST(updated_buyer_suggested_dock_date AS DATE) AS "Updated Buyer Suggested Dock Date", updated_buyer_action AS "Updated Buyer Action", CAST(updated_buyer_final_commit_dock_date AS DATE) AS "Updated Final Commit Dock Date", CAST(org_buyer_suggested_dock_date AS DATE) AS "Original Buyer Suggested Dock Date", org_buyer_action AS "Original Buyer Action", CAST(org_buyer_final_commit_dock_date AS DATE) AS "Original Final Commit Dock Date", CAST(updated_supplier_final_commit_dock_date AS DATE) AS "Updated Supplier Commit Delivery Date", CAST(updated_supplier_suggested_dock_date AS DATE) AS "Updated Supplier Commit Dock Date", updated_supplier_action AS "Updated Supplier Reason", CAST(org_supplier_final_commit_dock_date AS DATE) AS "Original Supplier Commit Delivery Date", CAST(org_supplier_suggested_dock_date AS DATE) AS "Original Supplier Commit Dock Date", org_supplier_action AS "Original Supplier Reason" FROM {}.po_audit_trail where last_updated_date >= \"{}\" and last_updated_date < \"{}\";""".format(DB_NAME, (month_from_str+"-01 00:00:00"), (month_to_str+"-01 00:00:00"))
    
    print("Fetching po_audit_trail for  ", month_from_str)
    
    po_audit_trial_data = condb_dict(get_po_audit_trial_data_query)
    
    if po_audit_trial_data:
        po_audit_trial_query_data=pd.DataFrame(data=po_audit_trial_data)

        final_po_audit_trial_data = po_audit_trial_query_data.to_csv(index=False,header=True) 

        bucket = S3_BUCKET
        subfolder = "Transaction_History" + "/" + archive_year
        s3_file_path = "Archival_Reports" + "/" +subfolder+ "/" + "Transaction_History(" + month_from_str + ").csv"
        s3 = boto3.client("s3")
        s3.put_object(ACL='public-read',Body=final_po_audit_trial_data,Bucket=bucket,Key=s3_file_path)
    
        print(s3_file_path + " created")

        delete_po_master_archive_query = """DELETE FROM {}.po_audit_trail WHERE last_updated_date >= \"{}\" AND last_updated_date < \"{}\";""".format(DB_NAME, (month_from_str+"-01 00:00:00"), (month_to_str+"-01 00:00:00"))
        condb_dict(delete_po_master_archive_query)
        print("Deleted po_audit_trial for ",month_from_str)
    
    else:
        print("No transaction history found for month: ", month_from_str)