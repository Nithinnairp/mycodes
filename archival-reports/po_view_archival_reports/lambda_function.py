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

    def generating_po_view_xlsx(source_supplier):
        custom_columns = ['Supplier Name', 
                            'Purchase Order Number (Release Line + Shipment Line)',
                            'Item Number',
                            'Requested Quantity',
                            'Receipt Quantity',
                            'Subinventory',
                            'Balance Quantity',
                            'Planner Name',
                            'Need By Date',
                            'Promise Date',
                            'Created Date',
                            'Approval Status',
                            'Line Status',
                            'SO/NAD Info']
        
        primary_query = """SELECT po.vendor_name 'Supplier Name', 
                            CONCAT(po_number, IF(release_number IS NOT NULL OR release_number <> '', CONCAT('-', release_number, '-', shipment_num), CONCAT('-', shipment_num) )) 'Purchase Order Number (Release Line + Shipment Line)',
                            item_number 'Item Number',
                            quantity 'Requested Quantity',
                            quantity_received 'Receipt Quantity',
                            destination_subinventory 'Subinventory',
                            quantity - quantity_received 'Balance Quantity',
                            part_planner_name 'Planner Name',
                            DATE_FORMAT(need_by_date, '%d-%m-%Y') 'Need By Date',
                            DATE_FORMAT(promised_date, '%d-%m-%Y') 'Promise Date',
                            DATE_FORMAT(creation_date, '%d-%m-%Y') 'Created Date',
                            IF(rel_approval_status IS NULL, po_approval_status, rel_approval_status) 'Approval Status',
                            closed_code 'Line Status',
                            reference_num 'SO/NAD Info'
                            FROM {db_name}.purchase_order po
                            INNER JOIN {db_name}.iodm_part_list pl
                            ON po.item_number = pl.part_name
                            WHERE (po_approval_status NOT IN ('CLOSED','FINALLY CLOSED', 'CANCELLED')
                            OR rel_approval_status NOT IN ('CLOSED','FINALLY CLOSED', 'CANCELLED'))
                            AND UPPER(pl.supplier_name) LIKE UPPER('{supplier_name}%');""".format(db_name=DB_NAME, supplier_name=source_supplier)
        
        df=condb(primary_query)
        primary_query_data=pd.DataFrame(data=df,columns=custom_columns)
        primary_query_data['Need By Date']=pd.to_datetime(primary_query_data['Need By Date'], format="%d-%m-%Y").dt.date
        primary_query_data['Promise Date']=pd.to_datetime(primary_query_data['Promise Date'], format="%d-%m-%Y").dt.date
        primary_query_data['Created Date']=pd.to_datetime(primary_query_data['Created Date'], format="%d-%m-%Y").dt.date
        
        with io.BytesIO() as output:
          with pd.ExcelWriter(output, engine='xlsxwriter', date_format='dd-mm-yyyy') as writer:
            primary_query_data.to_excel(writer, index=False)
            workbook = writer.book
            worksheet = writer.sheets['Sheet1']
            cell_format = workbook.add_format({'bg_color': '#C6CCFF'})
            worksheet.write_row('A1', custom_columns, cell_format)
          data = output.getvalue()

        bucket = "b2b-irp-analytics-prod"
        subfolder = "PO_View"
        s3_file_path = "Archival_Reports/" + source_supplier + "/" +subfolder+ "/" +source_supplier+"_PO_View" +"("+sng_time+").xlsx"
        s3 = boto3.client("s3")
        s3.put_object(ACL='public-read',Body=data,Bucket=bucket,Key=s3_file_path)
        
        return "purchase order archive files generated"
        
    suppliers = get_all_suppliers_prefix()
    for supplier in suppliers:
      if supplier is not None and supplier != '':
        supplier_prefix = supplier.split()[0]
        generating_po_view_xlsx(supplier_prefix)
        
    
    