import sys
import json
import boto3
import pymysql
import pandas as pd 
import logging
import datetime
from DB_conn import condb 

my_date=datetime.datetime.today()
year,week_num,day_of_week=my_date.isocalendar()
print(week_num)
complete_current_year=str(year)
current_year=complete_current_year[2:]

def lambda_handler(event, context):
    current_time=datetime.datetime.now()
    date_str=current_time.strftime("%m%d%Y")
    print(date_str)
    sng_time=(current_time+datetime.timedelta(hours=8)).strftime("%m%d%Y")
    sql_query="""select
        ordered_item part_number,
        'PenangF03' as item_site,
        DATE(SCHEDULE_SHIP_DATE),
        so_qty, 
        ORDER_TYPE,
        'PenangF03' as order_site,
        order_number,
        concat(LINE_NUM ,'_',Line_id) as order_line,
        concat(order_number ,'-', line_number ,'.', shipment_number) con2,
        CON3,
        po_details, 
        option_flag2,
        override_kr_date,
        if ((flow_status_code = 'ENTERED' AND order_category = 'PREBUILD'), 
		  concat(ifnull(order_category,''),'-',ifnull(notes,'')),
		  notes),
        special_handling_code,
        shipping_si
        from User_App.preship_so_view
        where flow_status_code not in ('CANCELLED','CLOSED','SHIPPED','INVOICED')
        and so_qty != ifnull(reserved_qty,0)"""
    col=["Part Number","Part Site","SSD","Order Quantity","Order Type","Order Site","Order Number","Order Line","CON2","CON3","PO NUMBER","KR Date","Override KR Date","Comments",
    "Special Handling Code","Shipping Subinventory"]
        
    sql_query_v1="""select  nl.part_number,
                'PenangF03' as item_site,
                DATE(nl.need_by_date) as SCHEDULE_SHIP_DATE,
                nl.kr_qty as so_qty,
                nh.request_type as ORDER_TYPE,
                'PenangF03' as order_site,
                nh.request_id as order_number,
                concat(nl.LINE_id ,'_', nl.id) as order_line,
                concat(nh.request_id ,'-', nl.LINE_id ) as con2,
                concat(nh.request_id,'-',nl.LINE_id ,'-',nl.part_number) as CON3,
                nl.purchase_order as po_details,
                DATE(nl.need_by_date) as KR_DATE,
                null as override_kr_date,
                null as notes,
                'N' as special_handling_code,
                destination_si_locator as shipping_si
        from User_App.non_ascp_request_header as nh,User_App.non_ascp_request_line as nl, User_App.iodm_part_list ipl
        where nh.request_id = nl.req_id
        and nh.approval_status in ('Approved','Order In Progress')
        and ipl.part_name = nl.part_number;"""
    col=["Part Number","Part Site","SSD","Order Quantity","Order Type","Order Site","Order Number","Order Line","CON2","CON3","PO NUMBER","KR Date","Override KR Date","Comments",
    "Special Handling Code","Shipping Subinventory"]
    result=condb(sql_query)
    result_v1=condb(sql_query_v1)
    result_df=pd.DataFrame(data=result,columns=col)
    result_df_v1=pd.DataFrame(data=result_v1,columns=col)
    csv_result_df=result_df.to_csv(index=False,header=True)
    csv_result_df_v1=result_df_v1.to_csv(index=False,header=True)
    s3=boto3.client("s3")
    s3.put_object(Body=csv_result_df,Bucket='so-interface-to-rr-prod',Key="B2B_SO_NAD_Data_Export/"+"B2B_SO_Data_Export.csv")
    s3.put_object(Body=csv_result_df_v1,Bucket='so-interface-to-rr-prod',Key="B2B_SO_NAD_Data_Export/"+"B2B_NAD_Data_Export.csv")
     
    sql_query1="""select x.Pool_id, x.ordered_item,x.part_site,x.order_number,x.Order_Line,x.PO_Number,x.po_line_number,x.shipping_si,
    x.serial_number,x.balance_to_receive,x.so_qty from
    (select distinct concat('B2B_',psv.line_id) as Pool_id,psv.ordered_item,'PenangF03' part_site,psv.order_number,
        concat(psv.line_number,'.', psv.shipment_number,'_',psv.line_id) as Order_Line,
        SUBSTRING(po_details,1,instr(po_details,'-')-1) PO_Number,
        replace(SUBSTRING(po_details,instr(po_details,'-')+1,20),'-','.') po_line_number,
        psv.shipping_si,
        SUBSTRING_INDEX(SUBSTRING_INDEX(fg_serial, ',', kc.id), ',', -1) serial_number, 
		(psv.so_qty - ifnull(psv.received_quantity,0)) as balance_to_receive, psv.so_qty
        from User_App.preship_so_view as psv, User_App.keysight_calendar as kc
        where special_handling_code not in ( 'V','Common') 
		and kc.id <=so_qty
		and (po_details is not null or fg_serial is not null)
		and psv.so_qty != ifnull(psv.reserved_qty,0)
		and psv.flow_status_code not in ('CANCELLED','CLOSED','SHIPPED','INVOICED')
        order by psv.order_number, psv.ordered_item,psv.line_id,kc.id) as x
        union
        select concat(req_id,'-',line_id), part_number,'PenangF03',req_id,concat(LINE_id ,'_', id),SUBSTRING(purchase_order,1,instr(purchase_order,'-')-1) PO_Number,
        replace(SUBSTRING(purchase_order,instr(purchase_order,'-')+1,20),'-','.') po_line_number,destination_si_locator,null,po_remaining_qty,kr_qty
        from User_App.non_ascp_request_line
        where purchase_order is not null
        and line_status not in ('Cancelled','Completed');"""
    col1=["Pool_Id","ordered_item","part_site","order_number","Order_Line","PO_Number","po_line_number","shipping_si","serial_number"
    ,"Balance_to_receive","SO_Qty"]
    result1=condb(sql_query1)
    result1_df=pd.DataFrame(data=result1,columns=col1)
    csv_result1_df=result1_df.to_csv(index=False,header=True)
    s3.put_object(Body=csv_result1_df,Bucket='so-interface-to-rr-prod',Key="B2B_SO_PO_Serial_Extract/"+"B2B_SO_PO_Serial_Extract.csv")
    
