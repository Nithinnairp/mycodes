import json
import datetime 
import sys
import boto3
import json
# import pymysql
import calendar
import requests
from DB_conn import condb
import os


my_date=datetime.datetime.today()
year,week_num,day_of_week=my_date.isocalendar()
current_year=str(year)
current_year=current_year[2:]
DB_NAME = os.environ['DB_NAME']
URL_DOMAIN = os.environ['DOMAIN']
ARN = os.environ['ARN']

#Connecting to the DB
def lambda_handler(event, context):
    current_time=datetime.datetime.now()
    cloudwatch_events = boto3.client('events')
    lambda_client = boto3.client('lambda')
    
    irp_without_commit_report_targets = ['phase_related_mailing', 'irp_cm_commit_report', 'total_requirement_validation_before_commit_phase']
    irp_with_commit_report_targets = ['irp_cm_commit_report']
    commit_after_commit_phase_targets = ['phase_related_mailing', 'Planners_adjustment_vs_KEYS_KR_summary_prod', 'cm_commit_validation_after_commit_phase']
    
    def get_phase(supplier_id):
        get_phase=URL_DOMAIN+'/api/forecast-cm-calendar-phases/current/supplier/{supplier_id}'.format(supplier_id=supplier_id)
        response=requests.get(get_phase)
        phase_object=json.loads(response.content)
        print("considered",phase_object)
        return phase_object
    
    def findDaytime(phase_date,phase_time):
        year,month,date=(phase_date).split('-')
        timef=datetime.datetime.strptime(phase_time,"%H:%M:%S")
        # convert the date/time to UTC before putting the scheduler
        next_time = (timef - datetime.timedelta(hours=8)).strftime("%H:%M:%S")
        hour,minute,second=next_time.split(':')
        day=calendar.day_name[datetime.date(int(year),int(month),int(date)).weekday()].upper()
        print("from def",day[:3])
        return day[:3],hour,minute
    
    def get_all_suppliers_prefix():
        # Get all suppliers' id and name from forecast plan to process all available suppliers
        query = """SELECT source_supplier_id, source_supplier FROM {}.forecast_plan group by source_supplier;""".format(DB_NAME)
        suppliers_list=list(condb(query))
        # id = supplier_list[0][0]
        # supplier_name = supplier_list[0][1]
        return suppliers_list
    
    def schedule_event_rules(phase_object, rule_name, phase, supplier_id, supplier_name):
        # Schedule EventBridge rule day and time
        if phase == 'COMMIT':
            day,hour,minute=findDaytime(phase_object['commitDate'],phase_object['commitTime'])
        elif phase == 'REVIEW':
            day,hour,minute=findDaytime(phase_object['reviewDate'],phase_object['reviewTime'])
        elif phase == 'CLOSED':
            day,hour,minute=findDaytime(phase_object['closedDate'],phase_object['closedTime'])
            
        response=cloudwatch_events.put_rule(
            Name=rule_name,
            ScheduleExpression='cron({} {} ? * {} *)'.format(minute,hour,day),
            State='ENABLED')
        
        print("###############",hour,minute,day,"#############")
        return response
        
    def add_target(rule_name, target_function, json_input):
        # Put Target with input
        if json_input is not None:
            passing_input=json.dumps(json_input)
            scheduled_lambda = [
                {
                    'Id': target_function, # lambda name
                    'Arn': ARN + target_function,
                    'Input':passing_input
                }
            ]
        else:
            scheduled_lambda = [
                {
                    'Id': target_function, # lambda name
                    'Arn': ARN + target_function
                }
            ]
        response=cloudwatch_events.put_targets(
            Rule=rule_name,Targets=scheduled_lambda)
                    
    def add_lambda_trigger(rule_res, rule_name, lambda_name):
        # Add lambda trigger permissions if not exist
        try:
            lambda_client.add_permission(
              FunctionName=lambda_name,
              StatementId=rule_name,
              Action='lambda:InvokeFunction',
              Principal='events.amazonaws.com',
              SourceArn=rule_res['RuleArn'],
            )
        except:
            # Ignore if permission added previously
            print(rule_name + " created previously")
            pass
            
    
    def schedule_events(supplier_id, supplier_short_name):
        phase_object = get_phase(supplier_id)
        # report-IRP without commit report scheduling
        
        # irp_without_commit_report event
        rule_name_1 = 'irp_without_commit_report_'+supplier_short_name
        rule_response = schedule_event_rules(phase_object, rule_name_1, 'COMMIT', supplier_id, supplier_short_name)
        
        add_lambda_trigger(rule_response, rule_name_1+'-'+irp_without_commit_report_targets[0], irp_without_commit_report_targets[0])
        json_input = {"Phase":"Commit","supplier_id":supplier_id}
        add_target(rule_name_1, irp_without_commit_report_targets[0], json_input)
        
        add_lambda_trigger(rule_response, rule_name_1+'-'+irp_without_commit_report_targets[1], irp_without_commit_report_targets[1])
        without_commit_json_input = {"Subject":"without_commit","test": 2,"supplier_id":supplier_id}
        add_target(rule_name_1, irp_without_commit_report_targets[1], without_commit_json_input)
        
        add_lambda_trigger(rule_response, rule_name_1+'-'+irp_without_commit_report_targets[2], irp_without_commit_report_targets[2])
        total_requirement_validation_input = {"supplier_id":supplier_id}
        add_target(rule_name_1, irp_without_commit_report_targets[2], total_requirement_validation_input)

        # irp_with_commit_report event
        rule_name_2 = 'irp_with_commit_report_'+supplier_short_name
        rule_response_2 = schedule_event_rules(phase_object, rule_name_2, 'CLOSED', supplier_id, supplier_short_name)
        add_lambda_trigger(rule_response_2, rule_name_2+'-'+irp_with_commit_report_targets[0], irp_with_commit_report_targets[0])
        with_commit_json_input = {"Subject":"with_commit","test": 2,"supplier_id":supplier_id}
        add_target(rule_name_2, irp_with_commit_report_targets[0], with_commit_json_input)   

        # commit_after_commit_phase event
        rule_name_3 = 'commit_after_commit_phase_'+supplier_short_name
        rule_response_3 = schedule_event_rules(phase_object, rule_name_3, 'REVIEW', supplier_id, supplier_short_name)
        
        add_lambda_trigger(rule_response_3, rule_name_3+'-'+commit_after_commit_phase_targets[0], commit_after_commit_phase_targets[0])
        json_input_2 = {"Phase":"Review","supplier_id":supplier_id}
        add_target(rule_name_3, commit_after_commit_phase_targets[0], json_input_2)  
        
        add_lambda_trigger(rule_response_3, rule_name_3+'-'+commit_after_commit_phase_targets[1], commit_after_commit_phase_targets[1])
        adj_summary_json_input = {"test": 1,"supplier_id":supplier_id}
        add_target(rule_name_3, commit_after_commit_phase_targets[1], adj_summary_json_input)  
        
        add_lambda_trigger(rule_response_3, rule_name_3+'-'+commit_after_commit_phase_targets[2], commit_after_commit_phase_targets[2])
        cm_commit_validation_json_input = {"test": 1,"supplier_id":supplier_id}
        add_target(rule_name_3, commit_after_commit_phase_targets[2], cm_commit_validation_json_input)  
        
    
    # Loop through all suppliers to schedule based on their phase timings
    suppliers=get_all_suppliers_prefix()
    for supplier in suppliers:
        if supplier:
            supplier_id = supplier[0]
            supplier_prefix = supplier[1].split()[0]
            print(supplier_prefix)
            schedule_events(supplier_id, supplier_prefix)   
        else:
            print("Individual supplier list empty")
    
    
    