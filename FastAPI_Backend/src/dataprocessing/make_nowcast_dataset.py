import h5py
import numpy as np
import os
import s3fs

#data = "s3://sevir/data/vil/2019/SEVIR_VIL_STORMEVENTS_2019_0701_1231.h5"
s3_path = "s3://sevir/data/"

def make_nowcast_dataset(filename,idx):

    fs = s3fs.S3FileSystem(anon=True)
    data_file = fs.open(s3_path+filename, 'rb')
    h5_file = h5py.File(data_file,'r')
    s = np.s_[idx-1:idx:idx+1]
    vil = h5_file['vil'][s] 

    input = {'IN':[],"OUT":[]}
    for i in vil:
        #print(i)
        #print(i.shape)
        splitting(i, input)

    input["IN"] = np.array(input["IN"])
    input["OUT"] = np.array(input["OUT"])
    return input


def splitting(array, input):
        for i in range(25):
            temp = np.dsplit(array, np.array([i, i + 13, i + 25]))
            input['IN'].append(temp[1])
            input['OUT'].append(temp[2])

   




