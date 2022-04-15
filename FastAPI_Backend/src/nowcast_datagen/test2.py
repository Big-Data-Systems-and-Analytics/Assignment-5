from google.cloud import storage
from google.oauth2 import service_account
import json
import gcsfs
import h5py
import os
import numpy as np
# initializing a random numpy array
arr = np.random.randn(1000)
with open(r"C:\Users\16178\api_appengine\nowcast_datagen\premium-strata-340618-745287f8fd66.json") as source:
    info = json.load(source)
    #print(info)
credentials =  service_account.Credentials.from_service_account_info(info)
token = r"C:\Users\16178\api_appengine\nowcast_datagen\premium-strata-340618-745287f8fd66.json"
projectid = "premium-strata-340618"
FS = gcsfs.GCSFileSystem(project=projectid,
                          token=credentials)
path = "gs://nowcast_interim/requirements2.h5"

# client = storage.Client(credentials=credentials, project=projectid)
# bucket = client.get_bucket('nowcast_interim')
# blob = bucket.blob('nowcast_testing.h5')
# file_url = blob.public_url

# data_file = FS.open(file_url, 'w')
# data_file.write()
# print("d") gs://nowcast_interim/nowcast_testing.h5
#os.makedirs("../data2/interim/")
#FS.open(path, 'a')
client = storage.Client(credentials=credentials, project=projectid)
bucket = client.get_bucket('nowcast_interim')
blob = bucket.blob('requirements2.h5')
#with blob.open(mode='w') as model_file:
with FS.open(path, 'w') as model_file:
    file =  h5py.File(model_file, 'a')
    dset = file.create_dataset("default", data = arr)
#print(hf)
    
