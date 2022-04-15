import json
import pandas as pd
from nearbyloc import locfinder
def findepnarr(location,begintime,endtime):
    df = pd.read_csv("Streamlit_Frontend/StormEvents_details.csv")
    if df.loc[(df['BEGIN_LOCATION']==location) & (df['BEGIN_DATE_TIME'] == begintime) & (df['END_DATE_TIME'] == endtime)].shape[0] >= 1:
        int_df = df[(df['BEGIN_LOCATION'] == location) & (df['BEGIN_DATE_TIME'] == begintime) & (df['END_DATE_TIME'] == endtime)]
        #print(int_df["EPISODE_NARRATIVE"].loc[int_df.index[0]])
        epnarrative = str(int_df["EPISODE_NARRATIVE"].loc[int_df.index[0]]) #this
        evnarrative = str(int_df["EVENT_NARRATIVE"].loc[int_df.index[0]])
        epnrr = {"episode_narrative": epnarrative}
        evnrr = {"episode_narrative": evnarrative}
        return epnrr,evnrr
    else:
        nearloc = locfinder(location)
        int_df = df[(df['BEGIN_LOCATION'] == nearloc) & (df['BEGIN_DATE_TIME'] == begintime) & (df['END_DATE_TIME'] == endtime)]
        epnarrative = str(int_df["EPISODE_NARRATIVE"].loc[int_df.index[0]])
        evnarrative = str(int_df["EVENT_NARRATIVE"].loc[int_df.index[0]])
        epnrr = {"episode_narrative": epnarrative}
        evnrr = {"episode_narrative": evnarrative}
        return epnrr,evnrr

        #EVENT_NARRATIVE

#print(event)