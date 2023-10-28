import pandas as pd
import streamlit as st 
from streamlit import stop, error, warning
import plotly.express as px
from PIL import Image 
import numpy as np
import matplotlib.pyplot as plt
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta 
import mysql.connector
from Fetch_Data import Fetch_Data, Fetch_Download_Data  #From own module
from Data_Transformation import Data_Transformation  #From own module
import datetime
import streamlit_authenticator as stauth
import yaml 
from yaml.loader import SafeLoader
from pathlib import Path
from authentication import authenticate_user

#---------------------Custom function----------------------------------------------------------------

#Create function to convert dataframe to csv file
@st.cache_data
def convert_df(df):
    # IMPORTANT: Cache the conversion to prevent computation on every rerun
    return df.to_csv().encode('utf-8')

#---------------------check whether logged user have access to the page---------------------------------------------

# Check if the user is not an admin, and stop the app execution if so
if st.session_state.get("role") != "admin":
    error("You do not have access to this page.")
    stop()

#----------------------------set the page config---------------------------------------------------------
st.set_page_config(page_title='Philly Project Dashboard',page_icon='MIDF_favicon.png'   ,layout='wide')

#------------------------------Render HTML template------------------------------------------------------------------------------------------------------------------

# Define the HTML template with CSS to position the image and title
html_template = """
<div style="display: flex; align-items: center;">
    <img src="https://seeklogo.com/images/M/midf-logo-44DC64A7C7-seeklogo.com.png" alt="MIDF Logo" style="width: 200px; height: 72px; margin-right: 10px;">
    <h1>Download Section</h1>
</div>
"""
# Render the HTML template
st.markdown(html_template, unsafe_allow_html=True)

 #---------------------------Fetch data from production database-----------------------------------------------------
Fetched_Data = Fetch_Data() 
Approved_Cases_DF = Fetched_Data['df6']   #Approval List
ccris_DF = Fetched_Data['df7']   

Fetched_Download_Data = Fetch_Download_Data()
Cancellation_Reject_ReasonDF = Fetched_Download_Data['df1']  
Tasks_at_LeadsDF = Fetched_Download_Data['df2']  
Tasks_at_ProspectsDF = Fetched_Download_Data['df3']  
Tasks_at_MandatedDF = Fetched_Download_Data['df4']  
Tasks_at_CPDF = Fetched_Download_Data['df5']  

#----------------------------User input for filtering----------------------------------------------------------------------------------------------------------------------

# Create a sidebar section for user input
st.sidebar.title('Dashboard Filters')

# Add a date input component (Default if user does not input any date)
min_date = datetime.date(2022,12,13)
max_date = datetime.date(2023,4,30)

Date_Range = st.sidebar.date_input("Select Date Range:", (min_date, max_date))
#Convert the selected date into date time (otherwise, dataframe cannot be filtered since dataframe date column is in datetime)

try:
    selected_date = pd.to_datetime(Date_Range[0])
    selected_date2 = pd.to_datetime(Date_Range[1])
    st.sidebar.error("Date Range Selected: " + selected_date.strftime('%Y/%m/%d') + " to " +selected_date2.strftime('%Y/%m/%d') )
    st.sidebar.write('Data in JOMCOM only available from 13 Dec 2022 onwards')
        
     

    #----------------------------------Filter data--------------------------------------------------------
    Approved_CasesDF_Filtered = Approved_Cases_DF[(Approved_Cases_DF['APPROVAL DATE'] <= selected_date2) & (Approved_Cases_DF['APPROVAL DATE'] >= selected_date)]
    ccris_FilteredDF = ccris_DF[(ccris_DF['CCR_RESPONSE_DATE'] <= selected_date2) & (ccris_DF['CCR_RESPONSE_DATE'] >= selected_date)] 
    Cancellation_Reason_FilteredDF = Cancellation_Reject_ReasonDF[(Cancellation_Reject_ReasonDF['Cancellation_Reject_Date'] <= selected_date2) & (Cancellation_Reject_ReasonDF['Cancellation_Reject_Date'] >= selected_date)] 
    #----------------------------------Render webpage-------------------------------------------
    st.subheader("List of Downloadable Data")
    st.write("1.CCRIS Data")

    with st.expander('Expand this for CCRIS Data:'):
        st.write('Data for CCRIS Reporting')
        st.dataframe(ccris_FilteredDF)
        csv_ccris_DF = convert_df(ccris_FilteredDF)
        st.download_button(
            label="Download data as CSV",
            data=csv_ccris_DF,
            file_name='CCRIS_Data.csv',
            mime='text/csv',
        )

    st.write("2.CIC Meeting Data")

    with st.expander('Expand this for CIC Data:'):
        st.write('List of Approved Cases')
        st.dataframe(Approved_CasesDF_Filtered)
        csv_List_Approved_CasesDF = convert_df(Approved_CasesDF_Filtered)
        st.download_button(
            label="Download data as CSV",
            data=csv_List_Approved_CasesDF,
            file_name='List_of_Approved_Cases.csv',
            mime='text/csv',
        )

    st.write("3.Cancellation/Reject Reason Data")

    with st.expander('Expand this for Cancellation/reject Reason Data:'):
        st.write('List of Cancellation/Reject Reason')
        st.dataframe(Cancellation_Reason_FilteredDF)
        csv_Cancellation_ReasonDF = convert_df(Cancellation_Reason_FilteredDF)
        st.download_button(
            label="Download data as CSV",
            data=csv_Cancellation_ReasonDF,
            file_name='Cancellation_Reason.csv',
            mime='text/csv',
        )

    st.write("4.Pending Tasks by Stage")

    with st.expander('Expand this for List of Pending Tasks:'):
        st.write('List of Pending Tasks at Leads')
        st.dataframe(Tasks_at_LeadsDF)
        csv_Tasks_at_LeadsDF = convert_df(Tasks_at_LeadsDF)
        st.download_button(
            label="Download data as CSV",
            data=csv_Tasks_at_LeadsDF,
            file_name='Tasks_at_Leads.csv',
            mime='text/csv',
        )

        st.write('List of Pending Tasks at Prospects')
        st.dataframe(Tasks_at_ProspectsDF)
        csv_Tasks_at_ProspectsDF = convert_df(Tasks_at_ProspectsDF)
        st.download_button(
            label="Download data as CSV",
            data=csv_Tasks_at_ProspectsDF,
            file_name='Tasks_at_Prospects.csv',
            mime='text/csv',
        )


        st.write('List of Pending Tasks at Mandated')
        st.dataframe(Tasks_at_MandatedDF)
        csv_Tasks_at_MandatedDF = convert_df(Tasks_at_MandatedDF)
        st.download_button(
            label="Download data as CSV",
            data=csv_Tasks_at_MandatedDF,
            file_name='Tasks_at_Mandated.csv',
            mime='text/csv',
        )

        st.write('List of Pending Tasks at CP Full Data Entry')
        st.dataframe(Tasks_at_CPDF)
        csv_Tasks_at_CPDF = convert_df(Tasks_at_CPDF)
        st.download_button(
            label="Download data as CSV",
            data=csv_Tasks_at_CPDF,
            file_name='Tasks_at_CP_Full_Data_Entry.csv',
            mime='text/csv',
        )

       



except IndexError:
    st.error("Please select both start and end dates")
