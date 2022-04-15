import boto3
from botocore.handlers import disable_signing
import h5py
from google.cloud import storage
from google.oauth2 import service_account
import json
import gcsfs
#from visualize import model, visualize_result, norm,hmf_colors
import matplotlib.pyplot as plt
import numpy as np
import tensorflow as tf


with open(r'C:\Users\16178\api_appengine\nowcast_datagen\premium-strata-340618-745287f8fd66.json') as source:
    info = json.load(source)

credentials = service_account.Credentials.from_service_account_info(info)
projectid = "premium-strata-340618"

# Initialise a client
storage_client = storage.Client(credentials= credentials,project=projectid)
# Create a bucket object for our bucket
bucket = storage_client.get_bucket("model_sevir")
# Create a blob object from the filepath
blob = bucket.blob("mse_model.h5")
# Download the file to a destination
blob.download_to_filename("mse_model.h5")
model = tf.keras.models.load_model('mse_model.h5',compile=False,custom_objects={"tf": tf})


norm = {'scale':47.54,'shift':33.44}
hmf_colors = np.array( [
    [82,82,82], 
    [252,141,89],
    [255,255,191],
    [145,191,219]
])/255

def visualize_result(models,x_test,y_test,idx,ax,labels):
    fs=10

    for i in range(1,13,3):
        xt = x_test[idx,:,:,i]*norm['scale']+norm['shift']
        ax[(i-1)//3][0].imshow(xt)
    ax[0][0].set_title('Inputs',fontsize=fs)
    
 
    x_test = x_test[idx:idx+1]
    y_test = y_test[idx:idx+1]*norm['scale']+norm['shift']
    y_preds=[]
    for i,m in enumerate(models):
        yp = m.predict(x_test)
        if isinstance(yp,(list,)):
            yp=yp[0]
        y_preds.append(yp*norm['scale']+norm['shift'])
    
    
    
    for k,m in enumerate(models):
        for i in range(0,12,3):
            ax[i//3][2+2*k].imshow(y_preds[k][0,:,:,i])
            

        ax[0][2+2*k].set_title(labels[k],fontsize=fs)
        
        
    for j in range(len(ax)):
        for i in range(len(ax[j])):
            ax[j][i].xaxis.set_ticks([])
            ax[j][i].yaxis.set_ticks([])
    for i in range(4):
        ax[i][1].set_visible(False)
    for i in range(4):
        ax[i][3].set_visible(False)
    ax[0][0].set_ylabel('-45 Minutes')
    ax[1][0].set_ylabel('-30 Minutes')
    ax[2][0].set_ylabel('-15 Minutes')
    ax[3][0].set_ylabel('  0 Minutes')
    ax[0][2].set_ylabel('+15 Minutes')
    ax[1][2].set_ylabel('+30 Minutes')
    ax[2][2].set_ylabel('+45 Minutes')
    ax[3][2].set_ylabel('+60 Minutes')
    

    plt.subplots_adjust(hspace=0.05, wspace=0.05)

def read_write_chunks( generator, n_chunks ):
   
    chunksize = len(generator)//n_chunks
  
    print('Gathering chunks  0/%s:' % n_chunks)
    X,Y=generator.load_batches(n_batches=chunksize,offset=0,progress_bar=True)
    print(len(X))
    print(len(X[0]))
 


import s3fs
from nowcast_generator import get_nowcast_test_generator


tst_generator = get_nowcast_test_generator(sevir_catalog='../CATALOG.csv',
                                               sevir_location="s3://sevir/data")

read_write_chunks(tst_generator,5)









# https://sevir.s3.us-west-2.amazonaws.com/data/vil/2019/SEVIR_VIL_STORMEVENTS_2019_0701_1231.h5
# 'https://storage.googleapis.com/nowcast_interim/nowcast_testing.h5'