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

    def generating_so_view_xlsx(source_supplier):
        custom_columns = ['SO Status',
                            'SO',
                            'SO Line',
                            'Item Number',
                            'Top PTO Model',
                            'CON3',
                            'SO QTY',
                            'Allocated QTY',
                            'Reserved QTY',
                            'On Hand Qty',
                            'Set Order(Arrival Set/Ship Set)',
                            'Header Hold Status',
                            'Line Hold Status',
                            'Ship To Country/Region',
                            'Customer',
                            'Selling Price',
                            'WH',
                            'Shipping Warehouse',
                            'Order Type',
                            'SSD',
                            'KR Date',
                            'Planning Priority',
                            'Order Priority',
                            'Alloc Seq B2B',
                            'Auto Allocation',
                            'Special Handling',
                            'Override KR Date',
                            'Receiving SI',
                            'Shipping SI',
                            'Notes',
                            'FG Serial #',
                            'FOM Reason Code',
                            'PO #',
                            'PO Receipt Date',
                            'Urgent Flag',
                            'Flow Status(Order Status)',
                            'Allow Non RoHS',
                            'Ship Per CRD',
                            'CRDD Type',
                            'CRDD Minus TT',
                            'RCD',
                            'ESC Order',
                            'RE-ACK Date',
                            'RE-ACK Code',
                            'Earliest Ship Confirm Date',
                            'Must Ship My Unit',
                            'Planner Code',
                            'Planner Name',
                            'PO Status',
                            'DEPT Code',
                            'Sourcing Rule',
                            'Last Update By',
                            'Last Update At (SEA)']
        
        primary_query = """SELECT
                                SO_STATUS 'SO Status',
                                ORDER_NUMBER 'SO',
                                LINE_NUM 'SO Line',
                                ORDERED_ITEM 'Item Number',
                                TOP_MODEL 'Top PTO Model',
                                CON3 'CON3',
                                SO_QTY 'SO QTY',
                                IF(ALLOCATED_QTY IS NULL, 0, ALLOCATED_QTY) 'Allocated QTY',
                                IF(RESERVED_QTY IS NULL, 0, RESERVED_QTY) 'Reserved QTY',
                                IF(ON_HAND_QTY IS NULL, 0, ON_HAND_QTY) 'On Hand Qty',
                                CONCAT(IF(ARRIVAL_SET_INFO IS NULL, 'N', 'Y'), '/',IF(SHIP_SET_INFO IS NULL, 'N', 'Y')) 'Set Order(Arrival Set/Ship Set)',
                                HEADER_HOLD_STATUS 'Header Hold Status',
                                LINE_HOLD_STATUS 'Line Hold Status',
                                SHIP_TO_COUNTRY 'Ship To Country/Region',
                                CUSTOMER 'Customer',
                                UNIT_SELLING_PRICE 'Selling Price',
                                CROSS_DOCK_WAREHOUSE 'WH',
                                WAREHOUSE_CODE 'Shipping Warehouse',
                                ORDER_TYPE 'Order Type',
                                DATE_FORMAT(SCHEDULE_SHIP_DATE, '%d-%m-%Y') 'SSD',
                                DATE_FORMAT(KR_DATE, '%d-%m-%Y') 'KR Date',
                                PLANNING_PRIORITY 'Planning Priority',
                                ORDER_PRIORITY 'Order Priority',
                                SHIPPING_SEQUENCEB2B 'Alloc Seq B2B',
                                AUTO_ALLOCATION 'Auto Allocation',
                                SPECIAL_HANDLING_CODE 'Special Handling',
                                DATE_FORMAT(OVERRIDE_KR_DATE, '%d-%m-%Y') 'Override KR Date',
                                RECEIVING_SI 'Receiving SI',
                                SHIPPING_SI 'Shipping SI',
                                NOTES 'Notes',
                                FG_SERIAL 'FG Serial #',
                                FOM_REASON_CODE 'FOM Reason Code',
                                PO_DETAILS 'PO #',
                                DATE_FORMAT(PO_RECEIPT_DATE, '%d-%m-%Y') 'PO Receipt Date',
                                IF(URGENT_FLAG IS NULL OR URGENT_FLAG = 'N', 'NO', 'YES') 'Urgent Flag',
                                FLOW_STATUS_CODE 'Flow Status(Order Status)',
                                ALLOW_NON_ROHS 'Allow Non RoHS',
                                DATE_FORMAT(SPCRD, '%d-%m-%Y') 'Ship Per CRD',
                                CRDD_TYPE 'CRDD Type',
                                DATE_FORMAT(CRDD_TT, '%d-%m-%Y') 'CRDD Minus TT',
                                DATE_FORMAT(RCD, '%d-%m-%Y') 'RCD',
                                ESC_ORDER 'ESC Order',
                                DATE_FORMAT(REACK_DATE, '%d-%m-%Y') 'RE-ACK Date',
                                REACK_CODE 'RE-ACK Code',
                                DATE_FORMAT(EARLIEST_SHIP_CONFIRM_DATE, '%d-%m-%Y') 'Earliest Ship Confirm Date',
                                MUST_SHIP_MY_UNIT 'Must Ship My Unit',
                                PLANNER_CODE 'Planner Code',
                                PLANNER_NAME 'Planner Name',
                                PR_PO_STATUS 'PO Status',
                                DEPT_CODE 'DEPT Code',
                                SOURCING_RULE_VENDOR 'Sourcing Rule',
                                LAST_UPDATED_BY_EMAIL 'Last Update By',
                                CONVERT_TZ(LAST_UPDATED_DATE, '-07:00','+08:00') 'Last Update At (SEA)'
                                FROM
                                	(SELECT 
                                		sov.*,
                                		IF(sov.last_created_by_user_email IS NULL or sov.last_created_by_user_email = '', user.email, sov.last_created_by_user_email) AS LAST_UPDATED_BY_EMAIL
                                	FROM
                                		{db_name}.preship_so_view AS sov
                                	LEFT OUTER JOIN {db_name}.keysight_user AS user
                                		ON user.id = sov.last_updated_by
                                	WHERE
                                		sov.ORDERED_ITEM IN (SELECT part_name FROM {db_name}.iodm_part_list WHERE UPPER(supplier_name) LIKE UPPER('{supplier_name}%'))
                                			AND sov.flow_status_code NOT IN ('CLOSED' , 'FULFILLED', 'SHIPPED', 'AWAITING_RETURN', 'ENTERED', 'CANCELLED') 
                                	UNION 
                                	SELECT 
                                		sov.*,
                                		IF(sov.last_created_by_user_email IS NULL or sov.last_created_by_user_email = '', user.email, sov.last_created_by_user_email) AS LAST_UPDATED_BY_EMAIL
                                	FROM
                                		{db_name}.preship_so_view AS sov
                                	LEFT OUTER JOIN {db_name}.keysight_user AS user
                                		ON user.id = sov.last_updated_by
                                	WHERE
                                		sov.ORDERED_ITEM IN (SELECT part_name FROM {db_name}.iodm_part_list WHERE UPPER(supplier_name) LIKE UPPER('{supplier_name}%'))
                                			AND sov.flow_status_code = 'ENTERED'
                                			AND ORDER_CATEGORY = 'Prebuild' 
                                	UNION 
                                	SELECT 
                                		sov.*,
                                		IF(sov.last_created_by_user_email IS NULL or sov.last_created_by_user_email = '', user.email, sov.last_created_by_user_email) AS LAST_UPDATED_BY_EMAIL
                                	FROM
                                		{db_name}.preship_so_view AS sov
                                	LEFT OUTER JOIN {db_name}.keysight_user AS user
                                		ON user.id = sov.last_updated_by
                                	WHERE
                                		sov.ORDERED_ITEM IN (SELECT part_name FROM {db_name}.iodm_part_list WHERE UPPER(supplier_name) LIKE UPPER('{supplier_name}%'))
                                			AND sov.flow_status_code = 'CANCELLED'
                                			AND LINE_LAST_UPDATE_DATE > (DATE_SUB(CONVERT_TZ(SYSDATE(), '+00:00', '-07:00'), INTERVAL 5 DAY)) 
                                	UNION 
                                	SELECT 
                                		NULL AS ID,
                                		supplier_name AS SOURCING_RULE_VENDOR,
                                		req_id AS ORDER_NUMBER,
                                		line_id AS LINE_NUM,
                                		NULL AS TOP_MODEL,
                                		NULL AS TOP_ITEM_ID,
                                		part_number AS ORDERED_ITEM,
                                		NULL AS HEADER_ID,
                                		NULL AS LINE_ID,
                                		NULL AS TOP_MODEL_ID,
                                		part_description AS ITEM_DESC,
                                		NULL AS INVENTORY_ITEM_ID,
                                		CONCAT(req_id, '-', line_id, '-', part_number) AS CON3,
                                		kr_qty AS SO_QTY,
                                		line_status AS FLOW_STATUS_CODE,
                                		NULL AS ARRIVAL_SET_INFO,
                                		NULL AS SHIP_SET_INFO,
                                		NULL AS HEADER_HOLD_STATUS,
                                		NULL AS LINE_HOLD_STATUS,
                                		NULL AS SHIP_TO_ORG_ID,
                                		NULL AS SHIP_TO_COUNTRY,
                                		NULL AS SOLD_TO_ORG_ID,
                                		NULL AS CUSTOMER,
                                		NULL AS LOCAL_LANGUAGE_CUSTOMER,
                                		NULL AS UNIT_SELLING_PRICE,
                                		NULL AS UNIT_LIST_PRICE,
                                		NULL AS WAREHOUSE_CODE,
                                		request_type AS ORDER_TYPE,
                                		need_by_date AS SCHEDULE_SHIP_DATE,
                                		need_by_date AS KR_DATE,
                                		NULL AS PLANNING_PRIORITY,
                                		NULL AS ORDER_PRIORITY,
                                		'NO' AS AUTO_ALLOCATION,
                                		NULL AS OVERRIDE_KR_DATE,
                                		destination_si_locator AS RECEIVING_SI,
                                		planner_remark AS NOTES,
                                		purchase_order AS PO_DETAILS,
                                		NULL AS B2B_FLAG,
                                		NULL AS CROSS_DOCK_WAREHOUSE,
                                		creation_date AS CREATION_DATE,
                                		last_updated_date AS LAST_UPDATE_DATE,
                                		NULL AS ACTUAL_SHIPMENT_DATE,
                                		(kr_qty - po_remaining_qty) AS RECEIVED_QUANTITY,
                                		NULL AS LINE_LAST_UPDATE_DATE,
                                		NULL AS HOLD_LAST_UPDATE_DATE,
                                		NULL AS PO_LAST_UPDATE_DATE,
                                		NULL AS SHIPMENT_LAST_UPDATE_DATE,
                                		po_status AS PR_PO_STATUS,
                                		NULL AS LINE_NUMBER,
                                		NULL AS SHIPMENT_NUMBER,
                                		NULL AS OPTION_NUMBER,
                                		NULL AS COMPONENT_NUMBER,
                                		NULL AS RECORD_ID,
                                		NULL AS RECORD_TYPE,
                                		planner_code AS PLANNER_CODE,
                                		planner_name AS PLANNER_NAME,
                                		NULL AS BUYER_NAME,
                                		NULL AS PLANNER_ID,
                                		NULL AS CAL_OPTION,
                                		NULL AS SHIPMENT_STATUS,
                                		product_line AS PL,
                                		NULL AS RESERVED_QTY,
                                		NULL AS B2B_CREATION_DATE,
                                		last_updated_by AS LAST_UPDATED_BY,
                                		NULL AS LAST_UPDATED_DATE,
                                		creation_date AS LINE_CREATION_DATE,
                                		NULL AS FG_SERIAL,
                                		NULL AS ORDER_SITE,
                                		CONCAT(req_id, '-', line_id) AS CON2,
                                		NULL AS ORDER_CATEGORY,
                                		NULL AS BACKLOG_PRIORITY_BOOKED_DATE,
                                		NULL AS BACKLOG_PRIORITY_PREBUILD_DATE,
                                		NULL AS BACKLOG_PRIORITY_LSSD_DATE,
                                		NULL AS BACKLOG_PRIORITY_ESSD_DATE,
                                		NULL AS BACKLOG_PRIORITY_SSDNBD_DATE,
                                		NULL AS BACKLOG_PRIORITY_USD_NETT,
                                		NULL AS HEADER_HOLD_NAME,
                                		NULL AS LINE_HOLD_NAME,
                                		NULL AS WITH_HOLD,
                                		NULL AS RCD,
                                		NULL AS ESC_ORDER,
                                		NULL AS SPCRD,
                                		NULL AS CRDD_TYPE,
                                		NULL AS CRDD_TT,
                                		NULL AS EARLIEST_SHIP_CONFIRM_DATE,
                                		NULL AS REACK_CODE,
                                		NULL AS REACK_DATE,
                                		NULL AS MUST_SHIP_MY_UNIT,
                                		NULL AS ALLOW_NON_ROHS,
                                		NULL AS ORDER_CATEGORY_SEQ,
                                		NULL AS ORDER_PRIORITY_SEQ,
                                		NULL AS SHIPPING_SEQUENCEB2B,
                                		NULL AS LINE_UPDATE_FLAG,
                                		NULL AS UPDATE_MEDIA,
                                		line_status AS SO_STATUS,
                                		'N' AS SPECIAL_HANDLING_CODE,
                                		NULL AS SHIPPING_SI,
                                		NULL AS FOM_REASON_CODE,
                                		NULL AS PO_RECEIPT_DATE,
                                		NULL AS URGENT_FLAG,
                                		department_name AS DEPT_CODE,
                                		NULL AS OPTION_FLAG1,
                                		NULL AS OPTION_FLAG2,
                                		NULL AS OPTION_FLAG3,
                                		NULL AS NEW_FLAG1,
                                		NULL AS NEW_FLAG2,
                                		NULL AS NEW_FLAG3,
                                		NULL AS ALLOCATED_QTY,
                                		NULL AS ON_HAND_QTY,
                                		NULL AS CANCEL_FLAG_DATE,
                                		NULL AS ERROR_MSG_DETAIL,
                                		NULL AS LAST_CREATED_BY_USER_EMAIL,
                                		NULL AS SHIPPING_SI_OVERRIDE_FLAG,
                                		NULL AS SCRUB_UPDATE_DATE,
                                		NULL AS AUTO_ALLOCATION_OVERRIDE_FLAG,
                                		user.email AS LAST_UPDATED_BY_EMAIL
                                	FROM
                                		{db_name}.non_ascp_request_line line
                                	LEFT OUTER JOIN {db_name}.keysight_user AS user
                                		ON user.id = line.last_updated_by
                                	WHERE
                                		(line_status IN ('Pending Commit' , 'Commit Completed', 'In Progress')
                                			OR (line_status IN ('Cancelled')
                                			AND last_updated_date > (DATE_SUB(SYSDATE(), INTERVAL 5 DAY))))
                                			AND request_type != 'ADC'
                                			AND part_number IN (SELECT part_name FROM {db_name}.iodm_part_list WHERE UPPER(supplier_name) LIKE UPPER('{supplier_name}%'))) AS x""".format(db_name=DB_NAME, supplier_name=source_supplier)
        
        df=condb(primary_query)
        primary_query_data=pd.DataFrame(data=df,columns=custom_columns)
        primary_query_data['SSD']=pd.to_datetime(primary_query_data['SSD'], format="%d-%m-%Y").dt.date
        primary_query_data['KR Date']=pd.to_datetime(primary_query_data['KR Date'], format="%d-%m-%Y").dt.date
        primary_query_data['Override KR Date']=pd.to_datetime(primary_query_data['Override KR Date'], format="%d-%m-%Y").dt.date
        primary_query_data['PO Receipt Date']=pd.to_datetime(primary_query_data['PO Receipt Date'], format="%d-%m-%Y").dt.date
        primary_query_data['Ship Per CRD']=pd.to_datetime(primary_query_data['Ship Per CRD'], format="%d-%m-%Y").dt.date
        primary_query_data['CRDD Minus TT']=pd.to_datetime(primary_query_data['CRDD Minus TT'], format="%d-%m-%Y").dt.date
        primary_query_data['RCD']=pd.to_datetime(primary_query_data['RCD'], format="%d-%m-%Y").dt.date
        primary_query_data['RE-ACK Date']=pd.to_datetime(primary_query_data['RE-ACK Date'], format="%d-%m-%Y").dt.date
        primary_query_data['Earliest Ship Confirm Date']=pd.to_datetime(primary_query_data['Earliest Ship Confirm Date'], format="%d-%m-%Y").dt.date
        
        with io.BytesIO() as output:
          with pd.ExcelWriter(output, engine='xlsxwriter', date_format='dd-mm-yyyy') as writer:
            primary_query_data.to_excel(writer, index=False)
            workbook = writer.book
            worksheet = writer.sheets['Sheet1']
            cell_format = workbook.add_format({'bg_color': '#C6CCFF'})
            worksheet.write_row('A1', custom_columns, cell_format)
          data = output.getvalue()

        bucket = "b2b-irp-analytics-prod"
        subfolder = "SO_view"
        s3_file_path = "Archival_Reports/" + source_supplier + "/" +subfolder+ "/" +source_supplier+"_SO_view_archival_report" +"("+sng_time+").xlsx"
        s3 = boto3.client("s3")
        s3.put_object(ACL='public-read',Body=data,Bucket=bucket,Key=s3_file_path)
        
        return "SO View Archival Files generated"
        
    suppliers = get_all_suppliers_prefix()
    for supplier in suppliers:
      if supplier is not None and supplier != '':
        supplier_prefix = supplier.split()[0]
        generating_so_view_xlsx(supplier_prefix)    
        
    
    