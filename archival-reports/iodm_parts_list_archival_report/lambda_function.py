import pandas as pd
import datetime 
import boto3
import io
from DB_conn import condb

def lambda_handler(event, context):
    current_time=datetime.datetime.now()
    sng_time=(current_time+datetime.timedelta(hours=8)).strftime("%Y-%m-%d %H_%M")
    DB_NAME='User_App'

    def generating_iodm_part_list_xlsx():
        custom_columns = ['Application',
                            'Part Name',
                            'Part Site',
                            'Product Family',
                            'Product Group',
                            'CID Map',
                            'Build Type',
                            'CTO Flag',
                            'In Testing Flag',
                            'Countable Flag',
                            'Measureable Flag',
                            'Contract Period',
                            'Contract Ratio',
                            'Kanban LT',
                            'Process LT',
                            'Ship Rel Days',
                            'Post Proc LT',
                            'BUFF OPT',
                            'BUFF QTY',
                            'BUFF SRT',
                            'Include KR Pull up',
                            'KRFROZENWK',
                            'KR Special Opt Cal',
                            'Last Update Date(SEA)',
                            'Last Update By',
                            'Supplier Name',
                            'CMA Remarks',
                            'BU',
                            'Dept',
                            'Base Key',
                            'Release Method',
                            'Release Window',
                            'Last Update Release Date(SEA)',
                            'Last Update Release By']
                            
        
        custom_columns_row = ('', 'Part', '', 'Product', '', 'CID', 
                            'Build', 'CTO', 'In Testing', 'Countable', 'Measureable', 'Contract', '', 
                            'Kanban', 'Process', 'Ship Rel', 'Post Proc', '', '', 
                            '', '', '', '', 
                            'Last Update', 'Last Update', 'Supplier', '', 
                            '', '', '', '', '', 'Last Update', 'Last Update')
                            
        custom_columns_row2 = ('Application', 'Name', 'Site', 'Family', 'Group', 'Map', 
                            'Type', 'Flag', 'Flag', 'Flag', 'Flag', 'Period', 'Ratio', 
                            'LT', 'LT', 'Days', 'LT', 'BUFF OPTS', 'BUFF QTY', 'BUFF SRT', 
                            'Include KR Pull Up', 'KRFROZENWK', 'KR Special Opt CAL', 'Date(SEA)', 'By', 'Name', 'CMA Remarks', 
                            'BU', 'Dept', 'Base Key','Release Method','Release Window','Release Date(SEA)', 'Release By')
                            
                            
        primary_query = """SELECT
                            application 'Application',
                            part_name 'Part Name',
                            part_site 'Part Site',
                            product_family 'Product Family',
                            product_group 'Product Group',
                            cid_map 'CID Map',
                            build_type 'Build Type',
                            cto_flag 'CTO Flag',
                            in_testing_flag 'In Testing Flag',
                            countable_flag 'Countable Flag',
                            measureable_flag 'Measureable Flag',
                            contract_period 'Contract Period',
                            contract_ratio 'Contract Ratio',
                            kanban_lt 'Kanban LT',
                            process_lt 'Process LT',
                            ship_rel_days 'Ship Rel Days',
                            post_proc_lt 'Post Proc LT',
                            kr_customization_bufferbuildopt 'BUFF OPT',
                            kr_customization_bufferrunrate 'BUFF QTY',
                            kr_customization_buffer_srt 'BUFF SRT',
                            kr_customization_include_kr_pull_up 'Include KR Pull up',
                            kr_customization_frozenweek 'KRFROZENWK',
                            kr_special_opt_cal 'KR Special Opt Cal',
                            CONVERT_TZ(last_update_date, '+00:00', '+08:00') 'Last Update Date',
                            last_update_by 'Last Update By',
                            supplier_name 'Supplier Name',
                            cma_remarks 'CMA Remarks',
                            bu 'BU',
                            dept 'Dept',
                            base_key 'Base Key',
                            release_method 'Release Method',
                            CAST(if(release_window = "", null, release_window) as UNSIGNED) 'Release Window',
                            CONVERT_TZ(last_update_release_date, '+00:00', '+08:00') 'Last Update Release Date',
                            last_update_release_by 'Last Update Release By'
                            FROM {}.iodm_part_list;""".format(DB_NAME)
        
        df=condb(primary_query)
        primary_query_data=pd.DataFrame(data=df, columns=custom_columns)
        
        with io.BytesIO() as output:
          with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            primary_query_data.to_excel(writer, index=False, header=False, startrow=2)
            workbook = writer.book
            worksheet = writer.sheets['Sheet1']
            cell_format = workbook.add_format({'bg_color': '#A9C4E9', 'align': 'center'})
            worksheet.write_row('A1', custom_columns_row, cell_format)
            worksheet.write_row('A2', custom_columns_row2, cell_format)
            worksheet.merge_range('B1:C1', custom_columns_row[1], cell_format)
            worksheet.merge_range('D1:E1', custom_columns_row[3], cell_format)
            worksheet.merge_range('L1:M1', custom_columns_row[11], cell_format)
          data = output.getvalue()
        
        bucket = "b2b-irp-analytics-prod"
        subfolder = "iodm_part_list"
        s3_file_path = "Archival_Reports/common/" +subfolder+ "/iodm_part_list_archive" + "("+sng_time+").xlsx"
        s3 = boto3.client("s3")
        s3.put_object(ACL='public-read',Body=data,Bucket=bucket,Key=s3_file_path)
        
        return "IODM part list archive file generated"
        
        
    generating_iodm_part_list_xlsx()
        
    
    