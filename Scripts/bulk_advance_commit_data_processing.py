import sys
import json
import boto3
import pandas as pd
import numpy as np
import datetime
import pytz
from DB_conn import condb


def lambda_handler(event, context):
    current_time = datetime.datetime.now()
    my_date = datetime.datetime.today()
    year, week_num, week_day = my_date.isocalendar()

    # return the columns in the DB table
    get_col_names = """SELECT column_name from information_schema.columns where table_schema='User_App' and table_name='forecast_plan_test';"""
    col_names = list(condb(get_col_names))
    db_columns = list(sum(col_names, ()))

    get_records = """select * from User_App.forecast_plan_test where is_processed=0"""
    df1 = condb(sql=get_records)
    db_data = pd.DataFrame(data=df1, columns=db_columns)

    def get_date_format(date=None):
        if date is None:
            year, week, day = None, None, None
            return year, week, day
        else:
            year, week, day = str(date).split('-')
            return int(year), int(week), int(day)

    for index, row in db_data.iterrows():
        business_unit = row.bu
        ori_part_number = row.ori_part_number
        process_week = row.process_week
        process_year = row.process_year
        prod_week = row.prod_week
        prod_year = row.prod_year
        source_supplier = row.source_supplier
        division = row.division
        product_family = row.product_family
        product_line = row.product_line
        dept_code = row.dept_code
        planner_name = row.planner_name
        planner_code = row.planner_code
        dmp_orndmp = row.dmp_orndmp
        build_type = row.build_type
        prod_year_week = row.prod_year_week

        if (row.forecast_commit1 != 0 and row.forecast_commit1_date is not None) or (
                row.forecast_commit2 != 0 and row.forecast_commit2_date is not None) or \
                (row.forecast_commit3 != 0 and row.forecast_commit3_date is not None) or (
                row.forecast_commit4 != 0 and row.forecast_commit4_date is not None):
            demand_category = 'Forecast'
            original = row.forecast_original
            adjustment = row.forecast_adj
            commit = row.forecast_commit
            commit1 = row.forecast_commit1
            commit1_date = row.forecast_commit1_date
            try:
                x, y, z = get_date_format(commit1_date)
                commit1_week = datetime.date(x, y, z).isocalendar()[1]
                commit1_year = datetime.date(x, y, z).isocalendar()[0]
            except:
                commit1_week = None
                commit1_year = None

            commit2 = row.forecast_commit2
            commit2_date = row.forecast_commit2_date
            try:
                x, y, z = get_date_format(commit2_date)
                commit2_week = datetime.date(x, y, z).isocalendar()[1]
                commit2_year = datetime.date(x, y, z).isocalendar()[0]
            except:
                commit2_week = None
                commit2_year = None

            commit3 = row.forecast_commit3
            commit3_date = row.forecast_commit3_date
            try:
                x, y, z = get_date_format(commit3_date)
                commit3_week = datetime.date(x, y, z).isocalendar()[1]
                commit3_year = datetime.date(x, y, z).isocalendar()[0]
            except:
                commit3_week = None
                commit3_year = None

            commit4 = row.forecast_commit4
            commit4_date = row.forecast_commit4_date
            try:
                x, y, z = get_date_format(commit4_date)
                commit4_week = datetime.date(x, y, z).isocalendar()[1]
                commit4_year = datetime.date(x, y, z).isocalendar()[0]
            except:
                commit4_week = None
                commit4_year = None

            sql = """insert into User_App.advance_commit_test(business_unit,ori_part_number,process_week,process_year,prod_week,
                    prod_year,source_supplier,division,product_family,product_line,
                    dept_code,planner_name,planner_code,dmp_orndmp,build_type,
                    prod_year_week,demand_category,original,adjustment,commit,
                    commit1,commit1_date,commit1_week,commit1_year,commit1_accepted,
                    commit2,commit2_date,commit2_week,commit2_year,commit2_accepted,
                    commit3,commit3_date,commit3_week,commit3_year,commit3_accepted,
                    commit4,commit4_date,commit4_week,commit4_year,commit4_accepted,
                    kr_processed,irp_processed)
                    values(%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,
                    %s,%s,%s,%s,0,
                    %s,%s,%s,%s,0,
                    %s,%s,%s,%s,0,
                    %s,%s,%s,%s,0,
                    0,0)"""
            col = [business_unit, ori_part_number, process_week, process_year, prod_week,
                   prod_year, source_supplier, division, product_family, product_line,
                   dept_code, planner_name, planner_code, dmp_orndmp, build_type,
                   prod_year_week, demand_category, original, adjustment, commit,
                   commit1, commit1_date, commit1_week, commit1_year,
                   commit2, commit2_date, commit2_week, commit2_year,
                   commit3, commit3_date, commit3_week, commit3_year,
                   commit4, commit4_date, commit4_week, commit4_year]
            condb(sql, col)

        if (row.gsa_commit1 != 0 and row.gsa_commit1_date is not None) or (
                row.gsa_commit2 != 0 and row.gsa_commit2_date is not None) or \
                (row.gsa_commit3 != 0 and row.gsa_commit3_date is not None) or (
                row.gsa_commit4 != 0 and row.gsa_commit4_date is not None):
            demand_category = 'GSA'
            original = row.gsa_original
            adjustment = row.gsa_adj
            commit = row.gsa_commit
            commit1 = row.gsa_commit1
            commit1_date = row.gsa_commit1_date
            try:
                x, y, z = get_date_format(commit1_date)
                commit1_week = datetime.date(x, y, z).isocalendar()[1]
                commit1_year = datetime.date(x, y, z).isocalendar()[0]
            except:
                commit1_week = None
                commit1_year = None

            commit2 = row.gsa_commit2
            commit2_date = row.gsa_commit2_date
            try:
                x, y, z = get_date_format(commit2_date)
                commit2_week = datetime.date(x, y, z).isocalendar()[1]
                commit2_year = datetime.date(x, y, z).isocalendar()[0]
            except:
                commit2_week = None
                commit2_year = None

            commit3 = row.gsa_commit3
            commit3_date = row.gsa_commit3_date
            try:
                x, y, z = get_date_format(commit3_date)
                commit3_week = datetime.date(x, y, z).isocalendar()[1]
                commit3_year = datetime.date(x, y, z).isocalendar()[0]
            except:
                commit3_week = None
                commit3_year = None

            commit4 = row.gsa_commit4
            commit4_date = row.gsa_commit4_date
            try:
                x, y, z = get_date_format(commit4_date)
                commit4_week = datetime.date(x, y, z).isocalendar()[1]
                commit4_year = datetime.date(x, y, z).isocalendar()[0]
            except:
                commit4_week = None
                commit4_year = None

            sql = """insert into advance_commit_test(business_unit,ori_part_number,process_week,process_year,prod_week,
                    prod_year,source_supplier,division,product_family,product_line,
                    dept_code,planner_name,planner_code,dmp_orndmp,build_type,
                    prod_year_week,demand_category,original,adjustment,commit,
                    commit1,commit1_date,commit1_week,commit1_year,commit1_accepted,
                    commit2,commit2_date,commit2_week,commit2_year,commit2_accepted,
                    commit3,commit3_date,commit3_week,commit3_year,commit3_accepted,
                    commit4,commit4_date,commit4_week,commit4_year,commit4_accepted,
                    kr_processed,irp_processed)
                    values(%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,
                    %s,%s,%s,%s,0,
                    %s,%s,%s,%s,0,
                    %s,%s,%s,%s,0,
                    %s,%s,%s,%s,0,
                    0,0)"""
            col = [business_unit, ori_part_number, process_week, process_year, prod_week,
                   prod_year, source_supplier, division, product_family, product_line,
                   dept_code, planner_name, planner_code, dmp_orndmp, build_type,
                   prod_year_week, demand_category, original, adjustment, commit,
                   commit1, commit1_date, commit1_week, commit1_year,
                   commit2, commit2_date, commit2_week, commit2_year,
                   commit3, commit3_date, commit3_week, commit3_year,
                   commit4, commit4_date, commit4_week, commit4_year]
            condb(sql, col)

        if (row.non_gsa_commit1 != 0 and row.non_gsa_commit1_date is not None) or (
                row.non_gsa_commit2 != 0 and row.non_gsa_commit2_date is not None) or \
                (row.non_gsa_commit3 != 0 and row.non_gsa_commit3_date is not None) or (
                row.non_gsa_commit4 != 0 and row.non_gsa_commit4_date is not None):
            demand_category = 'Non GSA'
            original = row.nongsa_original
            adjustment = row.nongsa_adj
            commit = row.nongsa_commit
            commit1 = row.non_gsa_commit1
            commit1_date = row.non_gsa_commit1_date
            try:
                x, y, z = get_date_format(commit1_date)
                commit1_week = datetime.date(x, y, z).isocalendar()[1]
                commit1_year = datetime.date(x, y, z).isocalendar()[0]
            except:
                commit1_week = None
                commit1_year = None

            commit2 = row.non_gsa_commit2
            commit2_date = row.non_gsa_commit2_date
            try:
                x, y, z = get_date_format(commit2_date)
                commit2_week = datetime.date(x, y, z).isocalendar()[1]
                commit2_year = datetime.date(x, y, z).isocalendar()[0]
            except:
                commit2_week = None
                commit2_year = None

            commit3 = row.non_gsa_commit3
            commit3_date = row.non_gsa_commit3_date
            try:
                x, y, z = get_date_format(commit3_date)
                commit3_week = datetime.date(x, y, z).isocalendar()[1]
                commit3_year = datetime.date(x, y, z).isocalendar()[0]
            except:
                commit3_week = None
                commit3_year = None

            commit4 = row.non_gsa_commit4
            commit4_date = row.non_gsa_commit4_date
            try:
                x, y, z = get_date_format(commit4_date)
                commit4_week = datetime.date(x, y, z).isocalendar()[1]
                commit4_year = datetime.date(x, y, z).isocalendar()[0]
            except:
                commit4_week = None
                commit4_year = None

            sql = """insert into advance_commit_test(business_unit,ori_part_number,process_week,process_year,prod_week,
                    prod_year,source_supplier,division,product_family,product_line,
                    dept_code,planner_name,planner_code,dmp_orndmp,build_type,
                    prod_year_week,demand_category,original,adjustment,commit,
                    commit1,commit1_date,commit1_week,commit1_year,commit1_accepted,
                    commit2,commit2_date,commit2_week,commit2_year,commit2_accepted,
                    commit3,commit3_date,commit3_week,commit3_year,commit3_accepted,
                    commit4,commit4_date,commit4_week,commit4_year,commit4_accepted,
                    kr_processed,irp_processed)
                    values(%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,
                    %s,%s,%s,%s,0,
                    %s,%s,%s,%s,0,
                    %s,%s,%s,%s,0,
                    %s,%s,%s,%s,0,
                    0,0)"""
            col = [business_unit, ori_part_number, process_week, process_year, prod_week,
                   prod_year, source_supplier, division, product_family, product_line,
                   dept_code, planner_name, planner_code, dmp_orndmp, build_type,
                   prod_year_week, demand_category, original, adjustment, commit,
                   commit1, commit1_date, commit1_week, commit1_year,
                   commit2, commit2_date, commit2_week, commit2_year,
                   commit3, commit3_date, commit3_week, commit3_year,
                   commit4, commit4_date, commit4_week, commit4_year]
            condb(sql, col)

        if (row.system_commit1 != 0 and row.system_commit1_date is not None) or (
                row.system_commit2 != 0 and row.system_commit2_date is not None) or \
                (row.system_commit3 != 0 and row.system_commit3_date is not None) or (
                row.system_commit4 != 0 and row.system_commit4_date is not None):
            demand_category = 'System'
            original = row.system_original
            adjustment = row.system_adj
            commit = row.system_commit
            commit1 = row.system_commit1
            commit1_date = row.system_commit1_date
            try:
                x, y, z = get_date_format(commit1_date)
                commit1_week = datetime.date(x, y, z).isocalendar()[1]
                commit1_year = datetime.date(x, y, z).isocalendar()[0]
            except:
                commit1_week = None
                commit1_year = None

            commit2 = row.system_commit2
            commit2_date = row.system_commit2_date
            try:
                x, y, z = get_date_format(commit2_date)
                commit2_week = datetime.date(x, y, z).isocalendar()[1]
                commit2_year = datetime.date(x, y, z).isocalendar()[0]
            except:
                commit2_week = None
                commit2_year = None

            commit3 = row.system_commit3
            commit3_date = row.system_commit3_date
            try:
                x, y, z = get_date_format(commit3_date)
                commit3_week = datetime.date(x, y, z).isocalendar()[1]
                commit3_year = datetime.date(x, y, z).isocalendar()[0]
            except:
                commit3_week = None
                commit3_year = None

            commit4 = row.system_commit4
            commit4_date = row.system_commit4_date
            try:
                x, y, z = get_date_format(commit4_date)
                commit4_week = datetime.date(x, y, z).isocalendar()[1]
                commit4_year = datetime.date(x, y, z).isocalendar()[0]
            except:
                commit4_week = None
                commit4_year = None

            sql = """insert into advance_commit_test(business_unit,ori_part_number,process_week,process_year,prod_week,
                    prod_year,source_supplier,division,product_family,product_line,
                    dept_code,planner_name,planner_code,dmp_orndmp,build_type,
                    prod_year_week,demand_category,original,adjustment,commit,
                    commit1,commit1_date,commit1_week,commit1_year,commit1_accepted,
                    commit2,commit2_date,commit2_week,commit2_year,commit2_accepted,
                    commit3,commit3_date,commit3_week,commit3_year,commit3_accepted,
                    commit4,commit4_date,commit4_week,commit4_year,commit4_accepted,
                    kr_processed,irp_processed)
                    values(%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,
                    %s,%s,%s,%s,0,
                    %s,%s,%s,%s,0,
                    %s,%s,%s,%s,0,
                    %s,%s,%s,%s,0,
                    0,0)"""
            col = [business_unit, ori_part_number, process_week, process_year, prod_week,
                   prod_year, source_supplier, division, product_family, product_line,
                   dept_code, planner_name, planner_code, dmp_orndmp, build_type,
                   prod_year_week, demand_category, original, adjustment, commit,
                   commit1, commit1_date, commit1_week, commit1_year,
                   commit2, commit2_date, commit2_week, commit2_year,
                   commit3, commit3_date, commit3_week, commit3_year,
                   commit4, commit4_date, commit4_week, commit4_year]
            condb(sql, col)
