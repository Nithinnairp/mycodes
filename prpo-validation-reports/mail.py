import json
import datetime
import boto3

my_date=datetime.date.today()
current_time=datetime.datetime.now()
year,week_num,day_of_week=my_date.isocalendar()
sng_time=(current_time+datetime.timedelta(hours=8)).strftime("%Y-%m-%d %H_%M")

SENDER = 'pdl-noreply-cmcdev@keysight.com'
RECIPIENT = ['aruna.kumar@keysight.com']
BODY_HTML="""<html>
               <body>"""+"""<p>Hi,<br>
                <br>Your count of PO release lines is more than Average past release lines. Please check.
<br><br>PO Release Threshold is 
                """.format(sng_time)

CALCULATION_FORMULA = """<br><strong>Calculation:</strong><br>
% Current Release vs Past count = Curr Release count / Average of Past Release count<br>"""
              

def send_mail(supplier_name, columns, table_data, subject, threshold, emai_list):
    print('Mail Send to - ')
    print(supplier_name)
    output_table ='<table role="presentation" border="1" width="100%" style="border: 1px solid black;"><tr style="background-color:#b4c6e7">'
    for column in columns:
        output_table+= '<th>' + str(column) + '</th>'
    output_table +='</tr>'
    for row_data in table_data:
        output_table +='<tr>'
        count =0
        for data in row_data:
            if count == 0:
                output_table +='<td>'+str(data)+'</td>'
                count+=1
            else:
                output_table +='<td style="text-align:right">'+str(data)+'</td>'

        output_table +='</tr>'
    output_table +='</table>'

    final_email_body = BODY_HTML + str(threshold) + '% <br>' + CALCULATION_FORMULA + output_table + '</body></html>'
    print(final_email_body)
    CHARSET = "UTF-8"
    client = boto3.client('ses',"us-east-1")
    response = client.send_email(Source=SENDER,Destination={'ToAddresses':emai_list},
                  Message={
          'Body': {
              'Html': {
                  'Charset': CHARSET,
                  'Data': final_email_body
              },
          },
          'Subject': {
              'Charset': CHARSET,
              'Data': subject
          }})
    return "Mail sent"