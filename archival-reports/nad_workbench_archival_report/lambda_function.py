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

    def generating_nad_workbench_xlsx(source_supplier):
        custom_columns = ['Request ID',
                            'Part Number',
                            'Request Header Status',
                            'Request Line Status',
                            'Approver Remark',
                            'Request Type',
                            'Request Sub Type',
                            'Requestor',
                            'WW',
                            'Need By Date',
                            'Destination SI Locator',
                            'KR Quantity',
                            'Requestor Remark',
                            'Commit Date 1',
                            'Commit Quantity 1',
                            'Commit Date 2',
                            'Commit Quantity 2',
                            'Commit Date 3',
                            'Commit Quantity 3',
                            'Commit Date 4',
                            'Commit Quantity 4',
                            'Commit Date 5',
                            'Commit Quantity 5',
                            'Cause Code',
                            'Cause Code Remark',
                            'PO#',
                            'PO Remaining Qty',
                            'PO Status',
                            'Submit For Approval',
                            'Approved',
                            'Cancelled', 
                            'Rejected',
                            'Closed',
                            'Commit1',
                            'Commit2',
                            'Commit3',
                            'Commit4',
                            'Commit5',
                            'Creation Date (SEA)',
                            'Created By',
                            'Last Updated Date (SEA)',
                            'Last Updated By']
        
        primary_query = """SELECT CONCAT(head.request_id, '-', line.line_id) 'Request ID',
                            line.part_number 'Part Number',
                            head.approval_status 'Request Header Status',
                            line.line_status 'Request Line Status',
                            head.approver_remarks 'Approver Remark',
                            head.request_type 'Request Type',
                            head.reason 'Request Sub Type',
                            head.requestor_email 'Requestor',
                            line.ww 'WW',
                            DATE_FORMAT(DATE(line.need_by_date), '%m/%d/%Y') 'Need By Date',
                            line.destination_si_locator 'Destination SI Locator',
                            line.kr_qty 'KR Quantity',
                            line.planner_remark 'Requestor Remark',
                            DATE_FORMAT(line.commit_date1, '%m/%d/%Y') 'Commit Date 1',
                            line.commit1qty 'Commit Quantity 1',
                            DATE_FORMAT(line.commit_date2, '%m/%d/%Y') 'Commit Date 2',
                            line.commit2qty 'Commit Quantity 2',
                            DATE_FORMAT(line.commit_date3, '%m/%d/%Y') 'Commit Date 3',
                            line.commit3qty 'Commit Quantity 3',
                            DATE_FORMAT(line.commit_date4, '%m/%d/%Y') 'Commit Date 4',
                            line.commit4qty 'Commit Quantity 4',
                            DATE_FORMAT(line.commit_date5, '%m/%d/%Y') 'Commit Date 5',
                            line.commit5qty 'Commit Quantity 5',
                            line.cause_code 'Cause Code',
                            line.cause_code_remark 'Cause Code Remark',
                            line.purchase_order 'PO#',
                            line.po_remaining_qty 'PO Remaining Qty',
                            line.po_status 'PO Status',
                            head.submit_for_approval_on 'Submit For Approval',
                            head.approved_on 'Approved',
                            head.cancelled_on 'Cancelled', 
                            head.rejected_on 'Rejected',
                            head.closed_on 'Closed',
                            line.commit1on 'Commit1',
                            line.commit2on 'Commit2',
                            line.commit3on 'Commit3',
                            line.commit4on 'Commit4',
                            line.commit5on 'Commit5',
                            DATE_FORMAT(CONVERT_TZ(line.creation_date, '-07:00','+08:00'), '%m/%d/%Y %H:%i:%s') 'Creation Date (SEA)',
                            user.email 'Created By',
                            DATE_FORMAT(CONVERT_TZ(line.last_updated_date, '-07:00','+08:00'), '%m/%d/%Y %H:%i:%s') 'Last Updated Date (SEA)',
                            user.email 'Last Updated By'
                            FROM {db_name}.non_ascp_request_header head 
                            INNER JOIN {db_name}.non_ascp_request_line line
                            ON head.request_id = line.req_id
                            LEFT OUTER JOIN {db_name}.keysight_user user
                            ON user.id = line.created_by OR user.id = line.last_updated_by
                            WHERE line.part_number IN (SELECT 
                                part_name
                            FROM
                                {db_name}.iodm_part_list
                            WHERE
                                UPPER(supplier_name) LIKE UPPER('{supplier_name}%'));""".format(db_name=DB_NAME, supplier_name=source_supplier)
        
        df=condb(primary_query)
        primary_query_data=pd.DataFrame(data=df,columns=custom_columns)
        primary_query_data['Need By Date']=pd.to_datetime(primary_query_data['Need By Date'], format="%m/%d/%Y").dt.date
        primary_query_data['Commit Date 1']=pd.to_datetime(primary_query_data['Commit Date 1'], format="%m/%d/%Y").dt.date
        primary_query_data['Commit Date 2']=pd.to_datetime(primary_query_data['Commit Date 2'], format="%m/%d/%Y").dt.date
        primary_query_data['Commit Date 3']=pd.to_datetime(primary_query_data['Commit Date 3'], format="%m/%d/%Y").dt.date
        primary_query_data['Commit Date 4']=pd.to_datetime(primary_query_data['Commit Date 4'], format="%m/%d/%Y").dt.date
        primary_query_data['Commit Date 5']=pd.to_datetime(primary_query_data['Commit Date 5'], format="%m/%d/%Y").dt.date
        primary_query_data['Creation Date (SEA)']=pd.to_datetime(primary_query_data['Creation Date (SEA)'], format="%m/%d/%Y %H:%M:%S")
        primary_query_data['Last Updated Date (SEA)']=pd.to_datetime(primary_query_data['Last Updated Date (SEA)'], format="%m/%d/%Y %H:%M:%S")
        
        with io.BytesIO() as output:
          with pd.ExcelWriter(output, engine='xlsxwriter', date_format='mm/dd/yyyy', datetime_format='mm/dd/yyyy hh:mm:ss') as writer:
            primary_query_data.to_excel(writer, index=False)
          data = output.getvalue()

        bucket = "b2b-irp-analytics-prod"
        subfolder = "NAD_Workbench"
        s3_file_path = "Archival_Reports/" + source_supplier + "/" + subfolder + "/" + source_supplier + "_NAD_Workbench" +"("+sng_time+").xlsx"
        s3 = boto3.client("s3")
        s3.put_object(ACL='public-read',Body=data,Bucket=bucket,Key=s3_file_path)
        
        return "NAD Workbench archive files generated"
        
    suppliers = get_all_suppliers_prefix()
    for supplier in suppliers:
      if supplier is not None and supplier != '':
        supplier_prefix = supplier.split()[0]
        print(supplier_prefix)
        generating_nad_workbench_xlsx(supplier_prefix)  
        
    
    