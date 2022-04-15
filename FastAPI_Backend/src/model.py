import h5py
import gcsfs
import json
from google.oauth2 import service_account
import h5py
import tensorflow as tf



#with open('premium-strata-340618-745287f8fd66.json') as source:
#    info = json.load(source)
info = r"premium-strata-340618-745287f8fd66.json"
#credentials = service_account.Credentials.from_service_account_info(info)
projectid = "premium-strata-340618"
path = "gs://model_sevir/mse_model.h5"

FS = gcsfs.GCSFileSystem(project=projectid,
                         token=info)

with FS.open(path, 'rb') as model_file:
     model_gcs = h5py.File(model_file, 'r')
     model = tf.keras.models.load_model(model_gcs,compile=False,custom_objects={"tf": tf})
   
