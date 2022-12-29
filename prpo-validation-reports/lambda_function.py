from pickle import TRUE
import pandas as pd
import datetime 
import boto3
import io
import json
import os
from db_connection import condb,condb_dict
from mail import send_mail
def lambda_handler(event, context):
    DB_NAME= os.environ['DB_NAME'] 
    past_work_weeks = []
    past_week_numbers = []
    past_work_years = []
    email_table_columns = []

    # For getting planner of the specific supplier and current week
    def get_planner_list(supplier_data, process_week):
        supplier_name = supplier_data['supplier_name']
        threshold = int(supplier_data['prpo_threshold'])
        email_list = supplier_data['prpo_emails'].split(",")
        subject = supplier_data['prpo_email_subject'] + '('+ supplier_data['supplier_short_name'] + ')' 
        custom_columns=['part_planner_name','current_release_count']

        query = """
        SELECT part_planner_name, SUM(current_release_count) AS 'current_release_count' FROM
        (
            SELECT b.part_planner_name AS 'part_planner_name', COUNT(*) AS 'current_release_count'
            FROM {db_name}.prpo_plan_view AS c, {db_name}.iodm_part_list AS b
            WHERE b.part_name = c.part_no AND c.process_week = {pw} AND b.supplier_name = '{sn}' AND c.status IN ('Approved','Released') AND c.con3 LIKE 'nad%'
            GROUP BY b.part_planner_name
            union
            SELECT b.part_planner_name  AS 'part_planner_name', COUNT(*) AS 'current_release_count'
            FROM {db_name}.prpo_plan_view AS c, {db_name}.iodm_part_list AS b
            WHERE b.part_name = c.part_no AND c.process_week = {pw} AND b.supplier_name = '{sn}' AND c.status IN ('Approved','Released') AND (c.con3 IS NULL OR c.con3 NOT LIKE 'nad%')
            GROUP BY b.part_planner_name
        ) t
        GROUP BY part_planner_name desc
        """.format(sn=supplier_name, db_name=DB_NAME, pw=process_week)

        df = condb(query)
        print(supplier_name)
        print(query)
        query_data = pd.DataFrame(data=df, columns=custom_columns)
        deviation = []
        for index,data in query_data.iterrows():
            past_release_info = get_past_release_counts(supplier_name, data['part_planner_name'])
            if past_release_info:
                change_in_release = (data['current_release_count'] / ((past_release_info[3] + past_release_info[2] + past_release_info[1] + past_release_info[0])/4))*100
                if change_in_release < 100 - threshold or change_in_release > 100 + threshold:
                    temp_list =[data['part_planner_name'], past_release_info[3], past_release_info[2], past_release_info[1], past_release_info[0], data['current_release_count'], str(format(change_in_release,".0f")) + '%']
                    deviation.append(temp_list)
            else:
                temp_list =[data['part_planner_name'], 0, 0, 0, 0, data['current_release_count'], str(format((data['current_release_count'] / 0.001),".0f")) + '%']
                deviation.append(temp_list)
        if deviation:
            send_mail(supplier_name,email_table_columns, deviation, subject, threshold, email_list)
        return 1

    # Get release counts of a specific planner
    def get_past_release_counts(supplier_name, planner_name):
        query = """
        SELECT SUM(a.process_week = {pastweek_1}) AS 'current_week_1',SUM(a.process_week = {pastweek_2}) AS 'current_week_2',SUM(a.process_week = {pastweek_3}) AS 'current_week_3',SUM(a.process_week = {pastweek_4}) AS 'current_week_4'
        FROM {db_name}.prpo_plan_view_archive AS a, {db_name}.iodm_part_list AS b
        WHERE b.part_name = a.part_no AND b.part_planner_name = '{pn}' AND b.supplier_name = '{sn}' AND a.status IN ('Released') AND a.process_week IN ({pastweek_1},{pastweek_2},{pastweek_3},{pastweek_4})
        GROUP BY b.part_planner_name
        ORDER By b.supplier_name desc;
        """.format(sn=supplier_name, db_name=DB_NAME, pn=planner_name, pastweek_1=past_work_weeks[0], pastweek_2=past_work_weeks[1], pastweek_3=past_work_weeks[2], pastweek_4=past_work_weeks[3])
        supplier_list=list(sum(list(condb(query)),()))
        return supplier_list

    # Process Current email table header
    def process_email_header_data(list_of_past_weeks, past_work_years, current_week, current_year):
        email_table_columns.append('Planner')
        for number in range(3, -1, -1):
            email_table_columns.append('Past Release (Week ' + str(list_of_past_weeks[number]) + '/' + str(past_work_years[number])+') count')
        email_table_columns.append('Curr Release (Week '+str(current_week)+'/'+str(current_year)+') count')
        email_table_columns.append("% Current Release vs Past count")

    # Get current -week- -year- and -work_week- data
    def get_current_week_data():
        current_week_query = """SELECT prod_week, prod_year FROM {}.keysight_calendar WHERE prod_date = (SELECT DATE_ADD(CURDATE(), INTERVAL - WEEKDAY(CURDATE()) DAY));""".format(DB_NAME);
        current_prod_week_year = list(condb(current_week_query))[0]
        return current_prod_week_year

    # All the active supplier and there informations
    def get_active_suppliers():
        custom_columns = ['supplier_name','supplier_short_name','supplier_type','prpo_threshold','prpo_emails','prpo_email_subject','description','supplier_email','supplier_manager_email','supplier_planner_list','category','status']
        query = """
        SELECT supplier_name,supplier_short_name,supplier_type,prpo_threshold,prpo_emails,prpo_email_subject,description,supplier_email,supplier_manager_email,supplier_planner_list,category,status
        FROM {db_name}.active_suppliers
        WHERE status = 1""".format(db_name=DB_NAME)
        df = condb(query)
        query_data = pd.DataFrame(data=df, columns=custom_columns)
        return query_data

    def process_data():
        suppliers = get_active_suppliers()
        week_data = get_current_week_data() # List of values
        work_week = str(week_data[0]) + str(week_data[1])
        currnet_week = week_data[0]
        current_year = week_data[1]

        for number in range(4):
            if currnet_week != 1:
                past_work_weeks.append(str(currnet_week-1)+str(current_year))
                past_week_numbers.append(currnet_week-1)
                past_work_years.append(current_year)
                currnet_week -= 1
            else:
                current_year-=1
                currnet_week = 52
                past_work_years.append(current_year)
                past_work_weeks.append(str(currnet_week)+str(current_year))
                past_week_numbers.append(currnet_week)

        process_email_header_data(past_week_numbers,past_work_years,week_data[0], current_year)
        
        for index, supplier in suppliers.iterrows():
            get_planner_list(supplier, work_week)

    process_data()

    # TODO implement
    return {
        'statusCode': 200,
        'body': 1
    }
