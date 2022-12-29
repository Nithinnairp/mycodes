import json
import datetime
import boto3
import os

my_date=datetime.date.today()
current_time=datetime.datetime.now()
year,week_num,day_of_week=my_date.isocalendar()
sng_time=(current_time+datetime.timedelta(hours=8)).strftime("%Y-%m-%d %H_%M")
DB_NAME = os.environ['DB_NAME']
S3_BUCKET_EXTERNAL = os.environ['S3_BUCKET_EXTERNAL']
SENDER = os.environ['SENDER']
  
def send_mail(mail_data,supplier_short_name):
  print(mail_data)
  phase=mail_data['current phase']
  
  if phase=='Planning':
    RECIPIENT = mail_data["RECIPIENT"]
    object_url_list=[]
    object_url_list.append(mail_data['object_url'])
    SUBJECT=mail_data['subject']
    phase=mail_data['current phase']
    status1=mail_data['status']['Exception status']
    status2=mail_data['status']['Irp exception status c/f']
    status3=mail_data['status']['IRP CM commit c/f']
    Total_active_records=mail_data['validation']['Total active records']
    RR_records=mail_data['validation']['RR records']
    Carry_Forward_records=mail_data['validation']['Carry Forward records']
    dmp_validation=mail_data['validation']['dmp_validation']
    Total_exception_records=mail_data['validation']['Total exception records']
    print(object_url_list)
    data=mail_data['cm_commit_cf']
    print("data",data)
    last_forecast_commit=int(data[0]['forecast_commit']) if data[0]['forecast_commit'] is not None else 0
    last_nongsa_commit=int(data[0]['nongsa_commit']) if data[0]['forecast_commit'] is not None else 0
    last_gsa_commit=int(data[0]['gsa_commit']) if data[0]['forecast_commit'] is not None else 0
    last_system_commit=int(data[0]['system_commit']) if data[0]['forecast_commit'] is not None else 0
    curr_forecast_commit=int(data[1]['forecast_commit']) if data[1]['forecast_commit'] is not None else 0
    curr_nongsa_commit=int(data[1]['nongsa_commit']) if data[1]['forecast_commit'] is not None else 0
    curr_gsa_commit=int(data[1]['gsa_commit']) if data[1]['forecast_commit'] is not None else 0
    curr_system_commit=int(data[1]['system_commit']) if data[1]['forecast_commit'] is not None else 0
    c1='lime' if last_nongsa_commit==curr_nongsa_commit else 'Red'
    c2='lime' if last_gsa_commit==curr_gsa_commit else 'Red'
    c3='lime' if last_system_commit==curr_system_commit else 'Red'
    c4='lime' if last_forecast_commit==curr_forecast_commit else 'Red'
    carry_forward_link="https://"+S3_BUCKET_EXTERNAL+".s3.ap-southeast-1.amazonaws.com/carry_forward_records/carry_over_week_"+supplier_short_name+"_"+str(week_num)+".csv"
    advance_commit_link="https://"+S3_BUCKET_EXTERNAL+".s3.ap-southeast-1.amazonaws.com/advance_commit_records/advance_commit_records_"+supplier_short_name+"_"+str(week_num-1)+".csv"
    
    weekly_record_details="""<b>{}</b> (Total records from IODM file-<b>{}</b>, Carry Forwarded records- <b>{}</b>)""".format(Total_active_records,RR_records,Carry_Forward_records)
    # The HTML body of the email.
    
    BODY_HTML="""<html>
               <body>"""+"""<p>Hello,<br>
                <br>The current week data has been loaded to prod. Below are validation details,
                <br>Please find the validation status and details before the planning phase as of {},<a href={}> click here </a>to download the validation file.</p>
                </body>""".format(sng_time,object_url_list[0])+"""<h4>Pre-Adj Validation</h4>
                <style>
                  table, th, td {border: 1px solid black;border-collapse: collapse;}
                  th, td {padding: 5px;text-align: left;}
                  </style>
                  <table style="width:100%">
                  """+"""<tr> 
                      <td>Total number of active records</td> 
                      <td COLSPAN="10";style= border:none;>{}</td> 
                    </tr>""".format(weekly_record_details)+"""<tr>
                      <td>Total number of Exception records</td> 
                      <td COLSPAN="10";style= border:none;><b>{}</b></td> 
                    </tr>""".format(Total_exception_records)+"""<tr>
                      <td>DMP/NDMP Validation</td>
                      <td bgcolor={}><b>{}</b></td>
                      <tdstyle= border:none;></td>
                    </tr>""".format('Red' if dmp_validation=='Fail' else 'lime',dmp_validation)+"""
                      <td>Exception Stamp</td>
                      <td bgcolor={}><b>{}</b></td>
                      <td style= border:none;></td>
                    </tr>""".format('Red' if status1=='Fail' else 'lime',status1)+"""<tr>
                      <td>IRP exception C/F</td>
                      <td bgcolor={}><b>{}</b></td>
                      <tdstyle= border:none;></td>
                    </tr>""".format('Red' if status2=='Fail' else 'lime',status2)+"""<tr>
                      <td>IRP CM Commit C/F</td>
                      <td bgcolor={}><b>{}</b></td>
                      <td>(N-1) Final</td><td>FirmReq(NONTAA)</td><td bgcolor={}>{}</td><td>FirmReq(TAA)</td><td bgcolor={}>{}</td><td>System</td><td bgcolor={}>{}</td><td>Forecast</td>
                      <td bgcolor={}>{}</td>
                    </tr>""".format('Red' if status3=='Fail' else 'lime',status3,c1,last_nongsa_commit,c2,last_gsa_commit,c3,last_system_commit,c4,last_forecast_commit)+"""<tr>
                      <td COLSPAN="2";style= border:none;></td>
                      <td>(N)</td><td>FirmReq(NONTAA)</td><td bgcolor={}>{}</td><td>FirmReq(TAA)</td><td bgcolor={}>{}</td><td>System</td><td bgcolor={}>{}</td><td>Forecast</td>
                      <td bgcolor={}>{}</td>
                    </tr>
                  </table><br>""".format(c1,curr_nongsa_commit,c2,curr_gsa_commit,c3,curr_system_commit,c4,curr_forecast_commit)+"""
                  <p><br><a href={}> click here </a> to download the carry forwarded records file.
                  <br><a href={}> click here </a> to download the advance commit records file.</p>""".format(carry_forward_link,advance_commit_link)+"""
                  </body>
                  </html>
                  <br><br>Thanks & Regards,<br>KeysightIT-Team.</p>"""
              

  if phase=="Commit":
    RECIPIENT = mail_data["RECIPIENT"]
    object_url_list=[]
    object_url_list.append(mail_data['object_url'])
    SUBJECT=mail_data['subject']
    phase=mail_data['current phase']
    status1=mail_data['status']['CID_mapping']
    status2=mail_data['status']['kty_qty']
    status3=mail_data['status']['Exception status']
    status4=mail_data['status']['IRP CM commit c/f']
    data=mail_data['cm_commit_cf']
    print("data",data)
    last_forecast_commit=data[0]['forecast_commit']
    last_nongsa_commit=data[0]['nongsa_commit']
    last_gsa_commit=data[0]['gsa_commit']
    last_system_commit=data[0]['system_commit']
    curr_forecast_commit=data[1]['forecast_commit']
    curr_nongsa_commit=data[1]['nongsa_commit']
    curr_gsa_commit=data[1]['gsa_commit']
    curr_system_commit=data[1]['system_commit']
    c1='lime' if last_nongsa_commit==curr_nongsa_commit else 'Red'
    c2='lime' if last_gsa_commit==curr_gsa_commit else 'Red'
    c3='lime' if last_system_commit==curr_system_commit else 'Red'
    c4='lime' if last_forecast_commit==curr_forecast_commit else 'Red'
    
    BODY_HTML="""<html>
             <body>"""+"""<p>Hello,<br>
              <br>Please find the validation status and details before the commit phase as of {},<a href={}> click here </a>to download the validation file.</p>
              </body>""".format(sng_time,object_url_list[0])+"""<h4>Pre-Rel Validation</h4>
              <style>
                table, th, td {border: 1px solid black;border-collapse: collapse;}
                th, td {padding: 5px;text-align: left;}
                </style>
                <table style="width:100%">
                """+"""<tr>
                    <td>CID Mapping</td>
                    <td bgcolor={}><b>{}</b></td>
                    <td style= border:none;></td>
                  </tr>""".format('Red' if status1=='Fail' else 'lime',status1)+"""<tr>
                    <td>Total KR not 0</td>
                    <td bgcolor={}><b>{}</b></td>
                    <td style= border:none;></td>
                  </tr>""".format('Red' if status2=='Fail' else 'lime',status2)+"""<tr>
                    <td>Exception Stamp</td>
                    <td bgcolor={}><b>{}</b></td>
                  </tr>""".format('Red' if status3=='Fail' else 'lime',status3)+"""<tr>
                    <td>IRP CM Commit C/F</td>
                    <td bgcolor={}><b>{}</b></td>
                    <td>(N-1) Final</td><td>FirmReq(NONTAA)</td><td bgcolor={}>{}</td><td>FirmReq(TAA)</td><td bgcolor={}>{}</td><td>System</td><td bgcolor={}>{}</td><td>Forecast</td>
                    <td bgcolor={}>{}</td>
                  </tr>""".format('Red' if status4=='Fail' else 'lime',status4,c1,last_nongsa_commit,c2,last_gsa_commit,c3,last_system_commit,c4,last_forecast_commit)+"""<tr>
                    <td COLSPAN="2";style= border:none;></td>
                    <td>(N)</td><td>FirmReq(NONTAA)</td><td bgcolor={}>{}</td><td>FirmReq(TAA)</td><td bgcolor={}>{}</td><td>System</td><td bgcolor={}>{}</td><td>Forecast</td>
                    <td bgcolor={}>{}</td>
                  </tr>
                </table><br>""".format(c1,curr_nongsa_commit,c2,curr_gsa_commit,c3,curr_system_commit,c4,curr_forecast_commit)+"""
                </body>
                </html>
                <br><br>Thanks & Regards,<br>KeysightIT-Team.</p>"""
                
  if phase=="Review":
    RECIPIENT = mail_data["RECIPIENT"]
    object_url_list=[]
    object_url_list.append(mail_data['object_url'])
    SUBJECT=mail_data['subject']
    phase=mail_data['current phase']
    status1=mail_data['status']['CM commit is Full']
    status2=mail_data['status']['Exception status']
    
    BODY_HTML = """<html>
      <head></head>
      <body>
          <p>Hello,<br>
          <br>Please find the validation status and details before the Review phase as of {}.
          <li >CM commit in Full commit VS Ori Exception status- <b>{}</b></li>
          <li>Exception Stamping status- <b>{}</b></li>
          <br><br>Thanks & Regards,<br>KeysightIT-Team.</p>
      </body>
      </html>""".format(sng_time,status1,status2)            
    
    
    
    
    
  CHARSET = "UTF-8"
  client = boto3.client('ses',"us-east-1")
  response = client.send_email(Source=SENDER,Destination={'ToAddresses':RECIPIENT},
                  Message={
          'Body': {
              'Html': {
                  'Charset': CHARSET,
                  'Data': BODY_HTML
              },
          },
          'Subject': {
              'Charset': CHARSET,
              'Data': SUBJECT
          }})
  
  return "Mail sent"
    