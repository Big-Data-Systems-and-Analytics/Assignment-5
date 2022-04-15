from google.cloud import bigquery
from google.oauth2 import service_account
import json



with open('premium-strata-340618-745287f8fd66.json') as source:
    info = json.load(source)

credentials = service_account.Credentials.from_service_account_info(info)
projectid = "premium-strata-340618"
client = bigquery.Client(credentials= credentials,project=projectid)
table = client.get_table("premium-strata-340618.nowcast_user_logs.userlogs")

def numreq(user):
    
    query = """SELECT Fullname,Email,Number_of_Requests  FROM `premium-strata-340618.nowcast_user_logs.userlogs`  where Email = "{}" order by Number_of_Requests desc limit 1""".format(user)
    query_job = client.query(query)
    result = query_job.result()
    for row in result:
     return row.Fullname,row.Email,row.Number_of_Requests

def increq(fullname,email,user_requests,time,location):
    #print(fullname+email+user_requests+time+location)
    rows_to_insert = [{u"Fullname": fullname, u"Email": email, u"Number_of_Requests": user_requests,u"Requested_Time": time,u"Location":location }]
    errors = client.insert_rows_json(table, rows_to_insert)
    print(rows_to_insert)

def addUser(fullname,email):
    rows_to_insert = [{u"Fullname": fullname, u"Email": email, u"Number_of_Requests": 0 }]
    errors = client.insert_rows_json(table, rows_to_insert)
    #assert errors
    