
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import os
import json
import sys
from geopy.geocoders import Nominatim
from nearbyloc import locfinder
sys.path.append(os.path.realpath(os.path.join(os.path.dirname(__file__), '..')))

with open('premium-strata-340618-745287f8fd66.json') as source:
    info = json.load(source)

credentials = service_account.Credentials.from_service_account_info(info)
projectid = "premium-strata-340618"
client = bigquery.Client(credentials= credentials,project=projectid)

def query_nearlocfile(nearloc):
     query = """ SELECT file_name, file_index,EPISODE_NARRATIVE FROM `premium-strata-340618.CATSTORM.CATSTORM` WHERE Begin_location = '{}'  AND  file_name like 'vil%' LIMIT 1""".format(nearloc)
     query_job = client.query(query)
     result = query_job.result()
     for row in result:
       return row.file_name, row.file_index,row.EPISODE_NARRATIVE

def query_filename(location,begintime,endtime):
  query = """ SELECT file_name, file_index, EPISODE_NARRATIVE FROM `premium-strata-340618.CATSTORM.CATSTORM` WHERE Begin_location = '{}' AND Begin_date_time = '{}' AND End_date_time = '{}' AND  file_name like 'vil%'""".format(location,begintime,endtime)
  query_job = client.query(query)
  result = query_job.result()
  res_num = result.total_rows
  print(res_num)
  if(res_num == 0):
      nearloc = locfinder(location)
      print(nearloc+"nearloc")
      if(nearloc != 'NoLoc'):
        nearloc_filename = query_nearlocfile(nearloc)
        return nearloc_filename
      else:
        return 'nofile'
  else:
    for row in result:
        return row.file_name, row.file_index, row.EPISODE_NARRATIVE

  


