
#export const COMPONENT_READY_WARNING_TIME_MS = 3000
import streamlit as st
#import tensorflow as tf
from PIL import Image
import requests
from cached_data import locator
from locepnarr import findepnarr
from englishtospanish import en_to_esp
import gcsfs
from google.cloud import storage
import matplotlib.pyplot as plt
import matplotlib.image as mpimg
import json
source= "Streamlit_Frontend/premium-strata-340618-745287f8fd66.json"
projectid = 'premium-strata-340618'
gcs = gcsfs.GCSFileSystem(project=projectid, token=source)


def predict_weather(location,begintime,endtime):

    data ={
    "location": location,
    "begintime": begintime,
    "endtime": endtime
    }
    response = requests.post('https://nowcastserviceapi-dot-premium-strata-340618.uk.r.appspot.com/predict',json=data)
    return response
    
def predict():      
    st.title("STORM PREDICTION FOR FARMERS")
    html_temp = """
    <div style="background-color:green;text-align:center;padding:15px">
    <h3 style="color:white;text-align:center;">This app shows storm prediction for farmers </h3>
    </div>
    """
    st.markdown(html_temp,unsafe_allow_html=True)
    location = st.text_input("LOCATION")
    begintime = st.text_input("BEGIN Time")
    endtime = st.text_input("END TIME")

    result=""
    r = st.radio("Pick one", ('Fresh', 'Cache'),index =1)
    lang = st.selectbox('Select a Language:',('English','Spanish'))
    
    #if st.selectbox('Fresh'):
    if st.button("Predict"):
        
        epi,event = findepnarr(location,begintime,endtime)
        narrsummary= requests.post('https://mim72xi7ra.execute-api.us-east-1.amazonaws.com/dev/qa',json=epi)
        answer = json.loads(narrsummary.content.decode())
        ner = requests.post('https://08xistdo89.execute-api.us-east-1.amazonaws.com/dev/qa',json=epi)
        answer2 = json.loads(ner.content.decode())
        eventsummary= requests.post('https://mim72xi7ra.execute-api.us-east-1.amazonaws.com/dev/qa',json=event)
        evanswer = json.loads(eventsummary.content.decode())
        ner = requests.post('https://08xistdo89.execute-api.us-east-1.amazonaws.com/dev/qa',json=event)
        answer2 = json.loads(ner.content.decode())
        episodesumm = str(answer['answer'][0]['summary_text'])
        epner = str(answer2['answer'][0:])
        eventsumm = str(evanswer['answer'][0]['summary_text'])
        eventner = str(answer2['answer'][0:])
        if(lang == "English"):
            st.info("Episode Summary:"+" "+ episodesumm)
            
            st.info("Episode Named Entities:"+" "+epner)
            
            st.info("Event Summary:"+" "+ eventsumm)
            
            st.info("Event Named Entities:"+" "+eventner)#
        else:
            
            episodesumm_es,epner_es,eventsumm_es,eventner_es = en_to_esp(episodesumm,epner,eventsumm,eventner)
            st.info("Resumen del Episodio:"+" "+ episodesumm_es)
            
            st.info("Episodio llamado Entidades:"+" "+epner_es)
            
            st.info("Resumen del evento:"+" "+ eventsumm_es)
            
            st.info("Entidades con nombre de evento:"+" "+eventner_es)
        
        if(r == "Fresh"):
            img = predict_weather(location,begintime,endtime)
            #print(img.content)
            if(str(img.content)[3:23]=="Enter all the fields"):
                st.warning('Enter all the details')
            elif(str(img.content)[3]=="L"):
                 st.warning('Location not found. Please try different location')
            elif(str(img.content)[3:25]=="Request Limit Exceeded"):
                 st.warning('Request Limit Exceeded')
            else:
                st.success('The output is {}'.format(result))
                st.image(img.content)
        if(r == "Cache"):
            loc = locator(location)
            if(loc=="hitapi"):
                img = predict_weather(location,begintime,endtime)
                if(str(img.content)[3:23]=="Enter all the fields"):
                    st.warning('Enter all the details')
                elif(str(img.content)[3]=="L"):
                    st.warning('Location not found. Please try different location')
                else:
                    st.success('The output is {}'.format(result))
                    st.image(img.content)
            else:
                #get loc image from gcs
                filename = 'gcs://cache_loc_images/' +str(loc)+".png"
                with gcs.open(filename,'rb') as f:
                    st.image(f.read())
                        
def main():  
   
    st.title("Welcome to Weather Nowcasting")
    

    menu = ["Home","Login","SignUp"]
    choice = st.sidebar.selectbox("Menu",menu)

    if choice == "Home":
        st.subheader('Login to Continue. New User? Signup first')

    elif choice == "Login":
        st.subheader("Login Section")

        email = st.sidebar.text_input("Email")
        password = st.sidebar.text_input("Password",type='password')
        
        if st.sidebar.checkbox("Login"):
            login_data = {"email": email,"password": password}
            response = requests.post('https://nowcastserviceapi-dot-premium-strata-340618.uk.r.appspot.com/user/login',json=login_data)
            
            if(response.content.decode()[2:7] == "error"):
                st.warning("Incorrect Username/Password")
            elif(response.content.decode()[2:8] == "detail"):
                st.warning("Invalid Email")
            
            else:
                st.success("Logged In as {}".format(email))
                task = st.selectbox("Task",["Nowcasting"])
                if task == "Nowcasting":
                    predict()

        
    elif choice == "SignUp":
        st.subheader("Create New Account")
        new_fullname = st.text_input("Full Name")
        new_email= st.text_input("Email")
        new_password = st.text_input("Password",type='password')

        if st.button("Signup"):
            signup_data = {
            "fullname": new_fullname,
            "email":new_email,
            "password": new_password
        }
            response = requests.post('https://nowcastserviceapi-dot-premium-strata-340618.uk.r.appspot.com/user/signup',json=signup_data)
            if (response.content.decode()[2:8] == "access"):
                st.success("You have successfully created a valid Account")
                st.info("Go to Login Menu to login")
            else:
                st.warning("Value is not a valid email address")

if __name__ == '__main__':
    main()

    


