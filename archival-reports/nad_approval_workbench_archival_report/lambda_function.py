import pandas as pd
import datetime 
import boto3
import io
from DB_conn import condb,condb_dict

def lambda_handler(event, context):
    current_time=datetime.datetime.now()
    sng_time=(current_time+datetime.timedelta(hours=8)).strftime("%Y-%m-%d %H_%M")
    DB_NAME='User_App'

    def get_all_suppliers_prefix():
        query = """SELECT DISTINCT(supplier_name) FROM {}.iodm_part_list;""".format(DB_NAME)
        suppliers_query_list=list(condb(query))
        supplier_list=list(sum(suppliers_query_list,()))
        return supplier_list

    def generating_nad_approval_workbench_xlsx(source_supplier):
        custom_columns = ['Planner Manager/Approver',
                            'Requester',
                          	'Request ID',
                            'Request Type',
                          	'Request Sub Type',
                            'Approver Comments',
                          	'Planner Name',
                          	'Part Number',
                            'KR Quantity',
                          	'Request Header Status',
                            'Need By Date',
                            'Destination Locator']
        
        primary_query = """SELECT head.approver_email 'Planner Manager/Approver',
                              head.requestor_email 'Requester',
                            	CONCAT(head.request_id, '-', line.line_id) 'Request ID',
                              head.request_type 'Request Type',
                            	head.reason 'Request Sub Type',
                              head.approver_remarks 'Approver Comments',
                            	line.planner_name 'Planner Name',
                            	line.part_number 'Part Number',
                              line.kr_qty 'KR Quantity',
                            	head.approval_status 'Request Header Status',
                              DATE_FORMAT(DATE(line.need_by_date), '%m/%d/%Y') 'Need By Date',
                              line.destination_si_locator 'Destination Locator'
                              FROM {db_name}.non_ascp_request_header head 
                              INNER JOIN {db_name}.non_ascp_request_line line
                                ON head.request_id = line.req_id
                              LEFT OUTER JOIN {db_name}.keysight_user user
                                ON user.id = line.created_by OR user.id = line.last_updated_by
                              WHERE head.approval_status = 'Pending Approval'
                              AND UPPER(line.supplier_name) LIKE UPPER('{supplier_name}%');""".format(db_name=DB_NAME, supplier_name=source_supplier)
        
        df=condb(primary_query)
        primary_query_data=pd.DataFrame(data=df,columns=custom_columns)
        primary_query_data['Need By Date']=pd.to_datetime(primary_query_data['Need By Date'], format="%m/%d/%Y").dt.date
        
        with io.BytesIO() as output:
          with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            primary_query_data.to_excel(writer, index=False)
          data = output.getvalue()

        bucket = "b2b-irp-analytics-prod"
        subfolder = "NAD_Approval_Workbench"
        s3_file_path = "Archival_Reports/" + source_supplier + "/" +subfolder+ "/" +source_supplier+"_NAD_Approval_Workbench" +"("+sng_time+").xlsx"
        s3 = boto3.client("s3")
        s3.put_object(ACL='public-read',Body=data,Bucket=bucket,Key=s3_file_path)
        
        return "NAD Approval Workbench archive files generated"
        
        
    suppliers = get_all_suppliers_prefix()
    for supplier in suppliers:
      if supplier is not None and supplier != '':
        supplier_prefix = supplier.split()[0]
        print(supplier_prefix)
        generating_nad_approval_workbench_xlsx(supplier_prefix)
        
    
    