import pandas as pd
import datetime 
import boto3
import io
from DB_conn import condb,condb_dict
import requests
import json
import os

def lambda_handler(event, context):
    current_time=datetime.datetime.now()
    year,week_num,day_of_week=current_time.isocalendar()
    short_year=year%100
    sng_time=(current_time+datetime.timedelta(hours=8)).strftime("%Y-%m-%d %H_%M")
    DB_NAME = os.environ['DB_NAME']
    URL_DOMAIN=os.environ['DOMAIN']
    # 'daily' or 'weekly'
    flag=event['flag']

    def get_phase(supplier_id):
        get_phase=URL_DOMAIN+'/api/forecast-cm-calendar-phases/supplier/{supplier_id}/week/{week}/year/{year}'.format(supplier_id=supplier_id,week=week_num,year=short_year)
        response=requests.get(get_phase)
        phase_object=json.loads(response.content)
        return phase_object["currentPhase"]

    def get_all_suppliers_prefix():
        query = """SELECT id, supplier_name FROM {}.active_suppliers;""".format(DB_NAME)
        suppliers_query_list=list(condb(query))
        # id = supplier_list[0][0]
        # supplier_name = supplier_list[0][1]
        return suppliers_query_list

    def generating_prpo_xlsx(source_supplier, current_phase):
        custom_columns = ['Keysight Part Number', 'CON3', 'Special Handling', 'WW', 'Need By Date', 'Open PO', 'Kr Qty', 'Planner Adj', 'PO Sugg Qty', 'Exception qty', 'Exception Reason', 'Planner Remarks', 'PO Final Qty', 'Status', 'Receiving SI', 'Release Window', 'Mapping Line Id', 'Last Update Date(SEA)', 'Last Update By']
        
        current_week_query = """SELECT prod_week, prod_year FROM {}.keysight_calendar WHERE prod_date = (SELECT DATE_ADD(CURDATE(), INTERVAL - WEEKDAY(CURDATE()) DAY));""".format(DB_NAME)
        current_prod_week_year = list(condb(current_week_query))[0]
        current_week = current_prod_week_year[0]
        current_year = current_prod_week_year[1]
        current_work_week = str(current_prod_week_year[0]) + str(current_prod_week_year[1])
        week_range_query = """SELECT 
                                    CONCAT(prod_year, prod_week) AS week_range
                                FROM
                                    {db_name}.keysight_calendar
                                WHERE
                                    id BETWEEN (SELECT 
                                            id
                                        FROM
                                            {db_name}.keysight_calendar
                                        WHERE
                                            prod_week = {week} AND prod_year = {year}) AND (SELECT 
                                            (id + 3)
                                        FROM
                                            {db_name}.keysight_calendar
                                        WHERE
                                            prod_week = {week} AND prod_year = {year});""".format(db_name=DB_NAME, week=str(current_week), year=str(current_year))
        week_range_result = list(condb(week_range_query))
        week_range_string = str(week_range_result[0][0]) + "," + str(week_range_result[1][0]) + "," + str(week_range_result[2][0]) + "," + str(week_range_result[3][0])

        if flag == 'daily':
            primary_query = """SELECT 
                                    plan.part_no 'Keysight Part Number', 
                                	plan.con3 'CON3', 
                                	plan.special_handling 'Special Handling', 
                                	CONCAT(plan.ww,'/',plan.wy) 'WW', 
                                	DATE_FORMAT(plan.need_by_date, '%m-%d-%Y') 'Need By Date', 
                                	plan.quantity 'Open PO', 
                                	plan.kr_qty 'Kr Qty', 
                                	plan.planner_adjustment 'Planner Adj.', 
                                	plan.po_suggested_qty 'PO Sugg Qty', 
                                	plan.exception_qty 'Exception qty', 
                                	adj.reason_code 'Exception Reason', 
                                	adj.planner_remark 'Planner Remarks', 
                                	plan.po_final_qty 'PO Final Qty', 
                                	plan.status 'Status', 
                                	plan.receiving_si 'Receiving SI', 
                                	CAST(if(plan.release_window = "", null, plan.release_window) as UNSIGNED) 'Release Window', 
                                	plan.prpoview_mapping_line_item 'Mapping Line Id', 
                                	CONVERT_TZ(plan.updated_at, '-07:00', '+08:00') 'Last Update Date', 
                                	user.email 'Last Update By'
                                FROM
                                    {db_name}.prpo_plan_view plan
                                LEFT OUTER JOIN
                                	{db_name}.prpo_adjustment adj
                                ON 
                                	plan.prpoview_mapping_line_item = adj.prpoview_mapping_line_item
                                LEFT OUTER JOIN 
                                	{db_name}.keysight_user user
                                ON 
                                	user.id = plan.updated_by
                                WHERE
                                    plan.part_no IN (SELECT 
                                            part_name
                                        FROM
                                            {db_name}.iodm_part_list
                                        WHERE
                                            UPPER(supplier_name) LIKE UPPER('{supplier_name}%'))
                                        AND plan.weekly_vs_daily_flag = 'daily'
                                        AND plan.process_week = {process_week}
                                        AND DATE(CONVERT_TZ(plan.created_at, '-07:00', '+08:00')) = DATE(CONVERT_TZ(SYSDATE(), '+00:00', '+08:00'))
                                        AND (plan.sales_order IS NULL
                                        OR plan.sales_order NOT LIKE 'NAD%') 
                                        AND CONCAT(plan.wy, plan.ww) IN ({week_range})
                                UNION SELECT 
                                    plan.part_no 'Keysight Part Number', 
                                	plan.con3 'CON3', 
                                	plan.special_handling 'Special Handling', 
                                	CONCAT(plan.ww,'/',plan.wy) 'WW', 
                                	DATE_FORMAT(plan.need_by_date, '%m-%d-%Y') 'Need By Date', 
                                	plan.quantity 'Open PO', 
                                	plan.kr_qty 'Kr Qty', 
                                	plan.planner_adjustment 'Planner Adj.', 
                                	plan.po_suggested_qty 'PO Sugg Qty', 
                                	plan.exception_qty 'Exception qty', 
                                	adj.reason_code 'Exception Reason', 
                                	adj.planner_remark 'Planner Remarks', 
                                	plan.po_final_qty 'PO Final Qty', 
                                	plan.status 'Status', 
                                	plan.receiving_si 'Receiving SI', 
                                	CAST(if(plan.release_window = "", null, plan.release_window) as UNSIGNED) 'Release Window', 
                                	plan.prpoview_mapping_line_item 'Mapping Line Id', 
                                	CONVERT_TZ(plan.updated_at, '-07:00', '+08:00') 'Last Update Date', 
                                	user.email 'Last Update By'
                                FROM
                                    {db_name}.prpo_plan_view plan
                                LEFT OUTER JOIN
                                	{db_name}.prpo_adjustment adj
                                ON 
                                	plan.prpoview_mapping_line_item = adj.prpoview_mapping_line_item
                                LEFT OUTER JOIN 
                                	{db_name}.keysight_user user
                                ON 
                                	user.id = plan.updated_by
                                WHERE
                                    plan.part_no IN (SELECT 
                                            part_name
                                        FROM
                                            {db_name}.iodm_part_list
                                        WHERE
                                            UPPER(supplier_name) LIKE UPPER('{supplier_name}%'))
                                        AND plan.sales_order LIKE 'NAD%';""".format(db_name=DB_NAME, supplier_name=source_supplier, process_week=current_work_week, week_range=week_range_string)
        else:
            primary_query = """SELECT 
                                    plan.part_no 'Keysight Part Number', 
                                	plan.con3 'CON3', 
                                	plan.special_handling 'Special Handling', 
                                	CONCAT(plan.ww,'/',plan.wy) 'WW', 
                                	DATE_FORMAT(plan.need_by_date, '%m-%d-%Y') 'Need By Date', 
                                	plan.quantity 'Open PO', 
                                	plan.kr_qty 'Kr Qty', 
                                	plan.planner_adjustment 'Planner Adj.', 
                                	plan.po_suggested_qty 'PO Sugg Qty', 
                                	plan.exception_qty 'Exception qty', 
                                	adj.reason_code 'Exception Reason', 
                                	adj.planner_remark 'Planner Remarks', 
                                	plan.po_final_qty 'PO Final Qty', 
                                	plan.status 'Status', 
                                	plan.receiving_si 'Receiving SI', 
                                	CAST(if(plan.release_window = "", null, plan.release_window) as UNSIGNED) 'Release Window', 
                                	plan.prpoview_mapping_line_item 'Mapping Line Id', 
                                	CONVERT_TZ(plan.updated_at, '-07:00', '+08:00') 'Last Update Date', 
                                	user.email 'Last Update By'
                                FROM
                                    {db_name}.prpo_plan_view plan
                                LEFT OUTER JOIN
                                	{db_name}.prpo_adjustment adj
                                ON 
                                	plan.prpoview_mapping_line_item = adj.prpoview_mapping_line_item
                                LEFT OUTER JOIN 
                                	{db_name}.keysight_user user
                                ON 
                                	user.id = plan.updated_by
                                WHERE
                                    plan.part_no IN (SELECT 
                                            part_name
                                        FROM
                                            {db_name}.iodm_part_list
                                        WHERE
                                            UPPER(supplier_name) LIKE UPPER('{supplier_name}%'))
                                        AND plan.weekly_vs_daily_flag = 'weekly'
                                        AND plan.process_week = {process_week}
                                        AND (plan.sales_order IS NULL
                                        OR plan.sales_order NOT LIKE 'NAD%') 
                                        AND CONCAT(plan.wy, plan.ww) IN ({week_range})
                                UNION SELECT 
                                    plan.part_no 'Keysight Part Number', 
                                	plan.con3 'CON3', 
                                	plan.special_handling 'Special Handling', 
                                	CONCAT(plan.ww,'/',plan.wy) 'WW', 
                                	DATE_FORMAT(plan.need_by_date, '%m-%d-%Y') 'Need By Date', 
                                	plan.quantity 'Open PO', 
                                	plan.kr_qty 'Kr Qty', 
                                	plan.planner_adjustment 'Planner Adj.', 
                                	plan.po_suggested_qty 'PO Sugg Qty', 
                                	plan.exception_qty 'Exception qty', 
                                	adj.reason_code 'Exception Reason', 
                                	adj.planner_remark 'Planner Remarks', 
                                	plan.po_final_qty 'PO Final Qty', 
                                	plan.status 'Status', 
                                	plan.receiving_si 'Receiving SI', 
                                	CAST(if(plan.release_window = "", null, plan.release_window) as UNSIGNED) 'Release Window', 
                                	plan.prpoview_mapping_line_item 'Mapping Line Id', 
                                	CONVERT_TZ(plan.updated_at, '-07:00', '+08:00') 'Last Update Date', 
                                	user.email 'Last Update By'
                                FROM
                                    {db_name}.prpo_plan_view plan
                                LEFT OUTER JOIN
                                	{db_name}.prpo_adjustment adj
                                ON 
                                	plan.prpoview_mapping_line_item = adj.prpoview_mapping_line_item
                                LEFT OUTER JOIN 
                                	{db_name}.keysight_user user
                                ON 
                                	user.id = plan.updated_by
                                WHERE
                                    plan.part_no IN (SELECT 
                                            part_name
                                        FROM
                                            {db_name}.iodm_part_list
                                        WHERE
                                            UPPER(supplier_name) LIKE UPPER('{supplier_name}%'))
                                        AND plan.sales_order LIKE 'NAD%';""".format(db_name=DB_NAME, supplier_name=source_supplier, process_week=current_work_week, week_range=week_range_string)
        if (flag == 'daily' and current_phase != 'PLANNING') or flag == 'weekly':
            df=condb(primary_query)
            primary_query_data=pd.DataFrame(data=df,columns=custom_columns)
            primary_query_data['Need By Date']=pd.to_datetime(primary_query_data['Need By Date'], format="%m-%d-%Y").dt.date
            
            with io.BytesIO() as output:
              with pd.ExcelWriter(output, engine='xlsxwriter', date_format='mm-dd-yyyy') as writer:
                primary_query_data.to_excel(writer, index=False)
              data = output.getvalue()
    
            bucket = "b2b-irp-analytics-prod"
            subfolder = "PRPO_" + flag + "_plan"
            s3_file_path = "Archival_Reports/" + source_supplier + "/" +subfolder+ "/" +source_supplier+"_PRPO_"+flag+"_plan" +"("+sng_time+").xlsx"
            s3 = boto3.client("s3")
            s3.put_object(ACL='public-read',Body=data,Bucket=bucket,Key=s3_file_path)
        
        return "PRPO Plan Files archival run completed"
        
    suppliers = get_all_suppliers_prefix()
    for supplier in suppliers:
        if supplier:
            current_phase = get_phase(supplier[0])
            supplier_prefix = supplier[1].split()[0]
            print(supplier_prefix + " is at " + current_phase)
            generating_prpo_xlsx(supplier_prefix, current_phase)
        else:
            print("Individual supplier list empty")
        
    
    