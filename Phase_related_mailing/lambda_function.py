import io
import json
import pandas as pd
import datetime 
import sys
import boto3
import pymysql
import logging
from DB_conn import condb,condb_dict
from mail import send_mail
from before_planning_phase import prepare_df,CMA_Validation,CM_COMMIT_CF,Exception_validation,irp_exception_cf,forecast_plan_after_processing_validations
from before_commit_phase import cid_map_part_number_validation,kr_qty_validation
from before_review_phase import CM_full_commmit_validation
import openpyxl
import os

my_date=datetime.date.today()
current_time=datetime.datetime.now()
year,week_num,day_of_week=my_date.isocalendar()
sng_time=(current_time+datetime.timedelta(hours=8)).strftime("%Y-%m-%d %H_%M")
formated_time=(current_time+datetime.timedelta(hours=8)).strftime("%Y-%m-%d %H_%M").replace(' ','+')
DB_NAME = os.environ['DB_NAME']
S3_BUCKET = os.environ['S3_BUCKET']
 
def lambda_handler(event, context):
    mail_data={}
    print(year,week_num,day_of_week)
    print(current_time)
    print(event) 
    phase=event['Phase']
    try:
        # Get supplier id from event data
        supplier_id=event['supplier_id']
    except:
        # If no supplier id in event data, assign to 0 as an indicator that no supplier id being passed in. We will get list of suppliers to loop through instead.
        supplier_id=0
        pass
    def color(x):
        c1 = 'color: #ffffff; background-color: #FF0000'
        c2 = 'color: #000000; background-color: #66FF33'
        # m = x['21 Total'] != x['22 Total']
        m=x[('Delta ',str(year)[2:]+' Total')].str.split(' ').apply(lambda x:x[0]).astype(float)<(0)
        a=x[('Delta ',str(year)[2:]+' Total')].str.split(' ').apply(lambda x:x[0]).astype(float)>(0) 
        n=x[('Delta ',str(year+1)[2:]+' Total')].str.split(' ').apply(lambda x:x[0]).astype(float)<(0)
        b=x[('Delta ',str(year+1)[2:]+' Total')].str.split(' ').apply(lambda x:x[0]).astype(float)>0
        o=x[('Delta ',str(year+2)[2:]+' Total')].str.split(' ').apply(lambda x:x[0]).astype(float)<0
        c=x[('Delta ',str(year+2)[2:]+' Total')].str.split(' ').apply(lambda x:x[0]).astype(float)>0
        p=x[('Delta ','Grand Total')].str.split(' ').apply(lambda x:x[0]).astype(float)<0
        d=x[('Delta ','Grand Total')].str.split(' ').apply(lambda x:x[0]).astype(float)>0
        # ('Delta',str(year+1)[2:]+' Total'),('Delta',str(year+2)[2:]+' Total'),('Delta','Grand Total')
        df1 = pd.DataFrame('', index=x.index, columns=x.columns)
        df1.loc[m, [('Delta ',str(year)[2:]+' Total')]] = c1
        df1.loc[a, [('Delta ',str(year)[2:]+' Total')]] = c2
        df1.loc[n, [('Delta ',str(year+1)[2:]+' Total')]] = c1
        df1.loc[b, [('Delta ',str(year+1)[2:]+' Total')]] = c2
        df1.loc[o, [('Delta ',str(year+2)[2:]+' Total')]] = c1
        df1.loc[c, [('Delta ',str(year+2)[2:]+' Total')]] = c2
        df1.loc[p, [('Delta ','Grand Total')]] = c1
        df1.loc[d, [('Delta ','Grand Total')]] = c2
        return df1
    
    def get_all_suppliers():
        get_supplier_id="""select source_supplier_id,source_supplier from {db_name}.forecast_plan group by source_supplier;""".format(db_name=DB_NAME)
        supplier_id_list=condb_dict(get_supplier_id)
        print('##################source_supplier#############',supplier_id_list)
        return supplier_id_list
        
    def get_supplier_name(supp_id):
        get_supplier_name="""select distinct(source_supplier) from {db_name}.forecast_plan where source_supplier_id={supplier_id};""".format(db_name=DB_NAME,supplier_id=supp_id)
        supp_name=condb(get_supplier_name)
        return supp_name[0][0]
    
    def get_recipients(col, supplier_id):
        query="""select {} from {}.active_suppliers where id = {};""".format(col, DB_NAME, supplier_id)
        result=condb(query)
        recipients = result[0][0]
        # convert to list type
        if recipients is not None:
            list_of_recipients=recipients.split(',')
        else:
            list_of_recipients = []
        return list_of_recipients
    
    def process(supplier_id_input,supplier_name_input):
        supplier_short_name = supplier_name_input.split()[0]
        bu_list="""select bu from {db_name}.forecast_plan where source_supplier_id in ({supplier_id}) group by bu;""".format(db_name=DB_NAME, supplier_id=supplier_id_input)
        bu_result=list(sum(condb(bu_list),()))
        print(bu_result)
        
        N_list=[]
        N1_list=[]
        for x,y in enumerate(bu_result):
            print('before',y)
            if (y is not None) and (y!=''):    
                print('after',y)
                df, grand_df =prepare_df(DB_NAME+".forecast_plan",y,supplier_id_input)
                N_list.append(df)
                if len(bu_result)-1==x:
                    N_list.append(grand_df)
                
        for x,y in enumerate(bu_result):  
            if (y is not None) and (y!=''):
                df, grand_df =prepare_df(DB_NAME+".forecast_plan_temp",y,supplier_id_input)
                N1_list.append(df)
                if len(bu_result)-1==x:
                    N1_list.append(grand_df)
        
        N_df=pd.concat(N_list)
        N1_df=pd.concat(N1_list)
        final_df=pd.merge(N_df,N1_df,on=['source_supplier','bu','product_family'],how='left')
            
        # df1=prepare_df("forecast_plan")
        # df2=prepare_df("forecast_plan_temp")
        # df2.drop('bu',axis=1,inplace=True)
        # final_df=pd.merge(df1,df2,on='product_family')
        # col=[('','bu'),('','product_family'),('N',str(year)[2:]+' Total'),('N',str(year+1)[2:]+' Total'),('N',str(year+2)[2:]+' Total'),('N','Grand Total'),
        # ('N-1',str(year)[2:]+' Total'),('N-1',str(year+1)[2:]+' Total'),('N-1',str(year+2)[2:]+' Total'),('N-1','Grand Total')]
        

        # col1=((final_df['21 Total_x']-final_df['21 Total_y'])/final_df['21 Total_y'])*100
        # col2=((final_df['22 Total_x']-final_df['22 Total_y'])/final_df['22 Total_y'])*100
        # col3=((final_df['23 Total_x']-final_df['23 Total_y'])/final_df['23 Total_y'])*100
        # col4=((final_df['Grand Total_x']-final_df['Grand Total_y'])/final_df['Grand Total_y'])*100
        
        col1=((final_df['21 Total_x']-final_df['21 Total_y'])/final_df['21 Total_y'])
        col2=((final_df['22 Total_x']-final_df['22 Total_y'])/final_df['22 Total_y'])
        col3=((final_df['23 Total_x']-final_df['23 Total_y'])/final_df['23 Total_y'])
        col4=((final_df['Grand Total_x']-final_df['Grand Total_y'])/final_df['Grand Total_y'])
        
        final_df['col1']=col1
        final_df['col2']=col2
        final_df['col3']=col3
        final_df['col4']=col4
        # final_df['col1']=col1.apply(lambda x:str(int(x))+' %' if str(x)+'%'!='nan%' else '0 %')
        # final_df['col2']=col2.apply(lambda x:str(int(x))+' %' if str(x)+'%'!='nan%' else '0 %')
        # final_df['col3']=col3.apply(lambda x:str(int(x))+' %' if str(x)+'%'!='nan%' else '0 %')
        # final_df['col4']=col4.apply(lambda x:str(int(x))+' %' if str(x)+'%'!='nan%' else '0 %')
        
        # final_df['col1'],final_df['col2'],final_df['col3'],final_df['col4']=round(col1),round(col2),round(col3),round(col4)
        col=[('','CM'),('','BU'),('','PRODUCT_FAMILY'),('N',str(year)[2:]+' Total'),('N',str(year+1)[2:]+' Total'),('N',str(year+2)[2:]+' Total'),('N','Grand Total'),
        ('N-1',str(year)[2:]+' Total'),('N-1',str(year+1)[2:]+' Total'),('N-1',str(year+2)[2:]+' Total'),('N-1','Grand Total'),
        ('Delta ',str(year)[2:]+' Total'),('Delta ',str(year+1)[2:]+' Total'),('Delta ',str(year+2)[2:]+' Total'),('Delta ','Grand Total')]
        
        final_df.columns=pd.MultiIndex.from_tuples(col)
        final_df1=final_df.style.apply(color,axis=None)
        final_df2=CMA_Validation(supplier_id_input)
        
        index=list(final_df.index)
        print(index)
        
        # first_row=2
        # last_row=100
        first_col=12
        last_col=15
        
        
        with io.BytesIO() as output:
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                # writer= pd.ExcelWriter('testing.xlsx',engine='xlsxwriter')
                print('passed1')
                final_df.to_excel(writer,sheet_name='Forecast (N) vs (N-1) ')
                final_df2.to_excel(writer,sheet_name='IODM CMA Validation Report ')
                workbook  = writer.book
                worksheet = writer.sheets['Forecast (N) vs (N-1) ']
                background = workbook.add_format({'bg_color': '#BFBFBF','font_color': '#000000'})
                # format3 = workbook.add_format({'bg_color': '#66FF33','font_color': '#000000'})
                format1=workbook.add_format({'num_format':'##,###'})
                format2= workbook.add_format({'num_format': '0%'})
                bold=workbook.add_format({'bold': True})
                worksheet.set_column('E:L',None,format1)
                worksheet.set_column('M:P', None, format2)
                worksheet.set_column('A:A', None, None,{'hidden':True}) 
                print('passed2')
                
                worksheet.conditional_format(index[0], first_col, index[-1]+4, last_col, {'type': '3_color_scale',
                                             'min_color': "#FA4444",
                                             'mid_color': "#FFFFFF",
                                             'max_color': "#1CB06A"})
                
                
                worksheet.conditional_format('E3:G{}'.format(index[-1]+4),{'type':'cell','criteria':'>=','value':0,'format':background})
                worksheet.conditional_format('I3:K{}'.format(index[-1]+4),{'type':'cell','criteria':'>=','value':0,'format':background})
                print('passed3')
                
                
            data=output.getvalue()
            print(data)
        s3=boto3.client("s3") 
        s3.put_object(ACL='public-read',Body=data,Bucket=S3_BUCKET,Key="phase_validations/Validation_results_"+supplier_short_name+"("+ sng_time +").xlsx")
        object_url="""https://{}.s3-ap-southeast-1.amazonaws.com/phase_validations/Validation_results_{}({}).xlsx""".format(S3_BUCKET, supplier_short_name, formated_time)
        
        
        get_col_names="""SELECT column_name from information_schema.columns where table_schema='{db_name}' and table_name='advance_commit';""".format(db_name=DB_NAME)
        col_names=list(condb(get_col_names))
        db_columns=list(sum(col_names,()))
        advance_commit_records_query="select * from {db_name}.advance_commit_temp where process_week={week} and process_year={year} and source_supplier='{supplier_name}'".format(db_name=DB_NAME,week=week_num-1,year=year,supplier_name=supplier_name_input)
        advance_commit_data=condb(advance_commit_records_query)
        advance_commit_df=pd.DataFrame(data=advance_commit_data,columns=db_columns)
        test1=advance_commit_df.applymap(lambda x: x[0] if type(x) is bytes else x)
        test2=test1.fillna(0)
        test_df=test2.to_csv(index=False,header=True)
        s3=boto3.client("s3")
        s3.put_object(Body=test_df,Bucket='rapid-response-prod',Key="advance_commit_records/"+"advance_commit_records_" + supplier_short_name + "_"+str(week_num-1)+".csv")
        
        
        
        if phase=='Planning':
            status1=Exception_validation(supplier_id_input)
            print('Exception_validation complete')
            status2=irp_exception_cf(supplier_id_input)
            print('irp_exception_cf complete')
            cm_commit_cf,status3=CM_COMMIT_CF(supplier_id_input)
            print('CM_COMMIT_CF complete')
            validation=forecast_plan_after_processing_validations(supplier_id_input)
            print('forecast_plan_after_processing_validations complete')
            # Get email recipients and remove duplicates using set and convert to list back
            mail_data['RECIPIENT'] = list(set(get_recipients('Validations_Planning_Phase', supplier_id_input)) | set(get_recipients('Archival_Validation_common', supplier_id_input)))
            mail_data['subject']="Before Planning Phase Validation - " + supplier_short_name
            mail_data['current phase']="Planning"
            mail_data['object_url']=object_url
            mail_data['status']={'Exception status':status1,'Irp exception status c/f':status2,'IRP CM commit c/f':status3}
            mail_data['validation']=validation
            mail_data['cm_commit_cf']=cm_commit_cf
            mail_status=send_mail(mail_data,supplier_short_name)
        
        if phase=='Commit':
            status1=cid_map_part_number_validation(supplier_id_input)
            status2=kr_qty_validation(supplier_id_input)
            status3=Exception_validation(supplier_id_input)
            cm_commit_cf,status4=CM_COMMIT_CF(supplier_id_input)
            mail_data['object_url']=object_url
            # Get email recipients and remove duplicates using set and convert to list back
            mail_data['RECIPIENT'] = list(set(get_recipients('Validations_Commit_Phase', supplier_id_input)) | set(get_recipients('Archival_Validation_common', supplier_id_input)))
            mail_data['subject']="Before Commit Phase Validation - " + supplier_short_name
            mail_data['current phase']="Commit"
            mail_data['status']={'CID_mapping':status1,'kty_qty':status2,'Exception status':status3,'IRP CM commit c/f':status4}
            mail_data['cm_commit_cf']=cm_commit_cf
            mail_status=send_mail(mail_data,supplier_short_name)
            
        if phase=='Review':
            status1=CM_full_commmit_validation(supplier_id_input)
            mail_data['object_url']=object_url
            # Get email recipients and remove duplicates using set and convert to list back
            mail_data['RECIPIENT'] = list(set(get_recipients('Validations_Commit_Phase', supplier_id_input)) | set(get_recipients('Archival_Validation_common', supplier_id_input)))
            mail_data['subject']="Before Review Phase Validation - " + supplier_short_name
            mail_data['current phase']="Review"
            status2=Exception_validation(supplier_id_input)
            mail_data['status']={'CM commit is Full':status1,'Exception status':status2}
            mail_status=send_mail(mail_data,supplier_short_name)
        
        print(mail_data)

    if supplier_id is not None:
        if supplier_id == 0:
            # supplier id is not provided in event data, we loop through the list of suppliers and process all suppliers available
            suppliers=get_all_suppliers()
            for supplier in suppliers:
                if supplier:
                    supp_id=supplier['source_supplier_id']
                    supp_name=supplier['source_supplier']
                    print(supp_id,"-",supp_name)
                    process(supp_id,supp_name)  
                else:
                    print("Individual supplier list empty")
        else:
            # supplier id provided from event data, only process that supplier
            supp_name = get_supplier_name(supplier_id)
            print(supplier_id,"-",supp_name)
            process(supplier_id,supp_name)
    else:
        print("No supplier info to run!")