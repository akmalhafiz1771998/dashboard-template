import pandas as pd
import streamlit as st 
import plotly.express as px
from PIL import Image 
import numpy as np
import matplotlib.pyplot as plt
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta 
import mysql.connector

@st.cache_data(show_spinner="Performing Data Transformation...",ttl=3600, max_entries=500)
#This is changed data transformation code (can handle 3 parameters instead of 5 parameters)
def Data_Transformation(QueueDF, AmtAppliedDF, Customer_HistoryDF, selected_date=None, selected_date2=None):
    # Load stagelookup for data transformation
    address_StageLookup = "./StageLookup.csv"
    StageLookupDF = pd.read_csv(address_StageLookup)

    # Convert start and end dt to datetime
    QueueDF['START_DT'] = pd.to_datetime(QueueDF['START_DT'])
    QueueDF['END_DT'] = pd.to_datetime(QueueDF['END_DT'])

    # Merged queue df and amt applied df
    QueueDF_Merged = pd.merge(QueueDF, AmtAppliedDF, left_on='CASEID', right_on='CASEID', how='left')
    QueueDF_Merged['Amount Applied'] = QueueDF_Merged['Amount Applied'].fillna(QueueDF_Merged['TOTAL FACILITY AMT'])
    QueueDF_Merged['Region'] = QueueDF_Merged['REGION (Originator)'].str.extract(r'-\s*([A-Za-z]+)\s+Office')
    QueueDF_Merged['Start_Month'] = QueueDF_Merged['START_DT'].dt.month

    FirstCondition = QueueDF_Merged['Amount Applied'] > 500000
    SecondCondition = (QueueDF_Merged['Amount Applied'] <= 500000) & (QueueDF_Merged['Amount Applied'] > 0)
    conditions = [FirstCondition, SecondCondition]
    values = [">500k", "<=500k"]
    QueueDF_Merged['Amt_Grouping'] = np.select(conditions, values, default="not specified")

    QueueDF_Merged['Lead_Date'] = QueueDF_Merged['START_DT'].dt.date
    QueueDF_Merged['Closed_Date'] = QueueDF_Merged['END_DT'].dt.date

    if selected_date is None:
        reporting_date = pd.to_datetime(QueueDF_Merged['Lead_Date'].min())  # Use the earliest Lead_Date as the reporting_date
    else:
        reporting_date = pd.to_datetime(selected_date)

    if selected_date2 is None:
        reporting_date2 = pd.to_datetime(QueueDF_Merged['Lead_Date'].max())  # Use the latest Lead_Date as the reporting_date2
    else:
        reporting_date2 = pd.to_datetime(selected_date2)

    QueueDF_Merged['Closed_Date'] = QueueDF_Merged['Closed_Date'].fillna(reporting_date2)

    # Convert to datetime format before subtracting
    QueueDF_Merged['Closed_Date'] = pd.to_datetime(QueueDF_Merged['Closed_Date'])
    QueueDF_Merged['Lead_Date'] = pd.to_datetime(QueueDF_Merged['Lead_Date'])

    QueueDF_Merged['Duration'] = QueueDF_Merged['Closed_Date'] - QueueDF_Merged['Lead_Date']
    QueueDF_Merged['Duration'] = QueueDF_Merged['Duration'].dt.days  # To solve timedelta bug, need to convert type

    QueueDF_Filtered = QueueDF_Merged[(QueueDF_Merged['Lead_Date'] <= reporting_date2) & (QueueDF_Merged['Lead_Date'] >= reporting_date)] 
    #QueueDF_Filtered in DF after filter up until reporting date
    QueueDF_Filtered['Duration'] = QueueDF_Filtered['Closed_Date'] - QueueDF_Filtered['Lead_Date']
    QueueDF_Filtered['Duration'] = QueueDF_Filtered['Duration'].dt.days

    #add reporting week column function
    def calculate_iso_week_start_date(date):
        first_day_of_year = datetime(date.year, 1, 1)
        iso_week_start_date = (
            max(first_day_of_year, first_day_of_year - timedelta(days=first_day_of_year.weekday())) +
            timedelta(days=(date.isocalendar()[1] - 1) * 7 + 1)
        )
        return iso_week_start_date

    QueueDF_Filtered['Reporting_Week'] = QueueDF_Filtered['Lead_Date'].apply(calculate_iso_week_start_date) #add reporting week column
    QueueDF_Filtered['Reporting_Month'] = QueueDF_Filtered['Reporting_Week'].apply(lambda x: x.strftime('%m/%Y')) # Add Reporting_Month column

    #Merge with stage lookup table
    QueueDF_MergedFiltered = pd.merge(QueueDF_Filtered,StageLookupDF,left_on='STAGE',right_on='Stage',how='left')
    QueueDF_MergedFiltered =  QueueDF_MergedFiltered.drop(columns = 'Stage')

    #ADD leads new
    QueueDF_MergedFiltered['Leads_New'] = ((QueueDF_MergedFiltered['CASEID'] != QueueDF_MergedFiltered['CASEID'].shift()) 
    & (QueueDF_MergedFiltered['Stage_Banded']=='Leads')).astype(int)

    #add leads progressed
    QueueDF_MergedFiltered['Leads_Progressed'] = ((QueueDF_MergedFiltered['CASEID'] == QueueDF_MergedFiltered['CASEID'].shift())  #check case id current equal to previous row
    & (QueueDF_MergedFiltered['Stage_Banded'].isin(['Prospects','Mandated']))                       #check current stage is prospects or mandated
    & (QueueDF_MergedFiltered['Stage_Banded'].shift() == 'Leads')).astype(int)       #check previous row stage is leads

    #Leads_Declined
    QueueDF_MergedFiltered['Leads_Declined'] = ((QueueDF_MergedFiltered['CASEID'] == QueueDF_MergedFiltered['CASEID'].shift())  #check case id current equal to previous row
    & (QueueDF_MergedFiltered['Stage_Banded'] == 'Declined')                       #check current stage is declined
    & (QueueDF_MergedFiltered['Stage_Banded'].shift() == 'Leads')).astype(int)       #check previous row stage is leads

    #Prospects New
    QueueDF_MergedFiltered['Prospects_New'] = ((QueueDF_MergedFiltered['CASEID'] == QueueDF_MergedFiltered['CASEID'].shift())  #check case id current equal to previous row
    & (QueueDF_MergedFiltered['Stage_Banded'] == 'Prospects')                       #check current stage is prospects
    & (QueueDF_MergedFiltered['Stage_Banded'].shift().isin(['Leads','Group Shariah','Mandated']))).astype(int)       #check previous row stage is either 'Leads','Group Shariah','Mandated'

    #Prospects progressed
    QueueDF_MergedFiltered['Prospects_Progressed'] = ((QueueDF_MergedFiltered['CASEID'] == QueueDF_MergedFiltered['CASEID'].shift())  #check case id current equal to previous row
    & (QueueDF_MergedFiltered['Stage_Banded'].isin(['Mandated','Group Shariah']))                       #check current stage is mandated or g.shariah
    & (QueueDF_MergedFiltered['Stage_Banded'].shift() == 'Prospects')).astype(int)       #check previous row stage is either 'Prospects'

    #Prospects Declined
    QueueDF_MergedFiltered['Prospects_Declined'] = ((QueueDF_MergedFiltered['CASEID'] == QueueDF_MergedFiltered['CASEID'].shift())  #check case id current equal to previous row
    & (QueueDF_MergedFiltered['Stage_Banded'] == 'Declined')                       #check current stage is Declined
    & (QueueDF_MergedFiltered['Stage_Banded'].shift() == 'Prospects')).astype(int)       #check previous row stage is'Prospects'

    #Mandated New
    QueueDF_MergedFiltered['Mandated_New'] = ((QueueDF_MergedFiltered['CASEID'] == QueueDF_MergedFiltered['CASEID'].shift())  #check case id current equal to previous row
    & (QueueDF_MergedFiltered['Stage_Banded'] == 'Mandated')                       #check current stage is Mandated
    & (QueueDF_MergedFiltered['Stage_Banded'].shift().isin(['Prospects','Group Shariah','CIC Approval','Leads','Fast Track Approval','RMD']))).astype(int)      #check previous row stage is rmd,'Prospects',group shariah, cic approval, leads, and fast track approval

    #Mandated Progressed
    QueueDF_MergedFiltered['Mandated_Progressed'] = ((QueueDF_MergedFiltered['CASEID'] == QueueDF_MergedFiltered['CASEID'].shift())  #check case id current equal to previous row
    & (QueueDF_MergedFiltered['Stage_Banded'].isin(['Prospects','Group Shariah','RMD','Fast Track Approval','CIC Approval']))                       #check current stage is 'Prospects','Group Shariah','RMD','Fast Track Approval','CIC approval'
    & (QueueDF_MergedFiltered['Stage_Banded'].shift() == 'Mandated')).astype(int)       #check if previous row stage is 'Mandated'

    #Mandated Declined
    QueueDF_MergedFiltered['Mandated_Declined'] = ((QueueDF_MergedFiltered['CASEID'] == QueueDF_MergedFiltered['CASEID'].shift())  #check case id current equal to previous row
    & (QueueDF_MergedFiltered['Stage_Banded'] == 'Declined')                       #check current stage is Declined
    & (QueueDF_MergedFiltered['Stage_Banded'].shift() == 'Mandated')).astype(int)       #check previous row stage is'Mandated'

    #G.Shariah New
    QueueDF_MergedFiltered['Group Shariah_New'] = ((QueueDF_MergedFiltered['CASEID'] == QueueDF_MergedFiltered['CASEID'].shift())  #check case id current equal to previous row
    & (QueueDF_MergedFiltered['Stage_Banded'] == 'Group Shariah')                       #check current stage is G.Shariah
    & (QueueDF_MergedFiltered['Stage_Banded'].shift().isin(['Mandated','Prospects']))).astype(int)       #check previous row stage is either 'Mandated' or 'Prospects'

    #Group Shariah Progressed
    QueueDF_MergedFiltered['Group Shariah_Progressed'] = ((QueueDF_MergedFiltered['CASEID'] == QueueDF_MergedFiltered['CASEID'].shift())  #check case id current equal to previous row
    & (QueueDF_MergedFiltered['Stage_Banded'].isin(['Mandated','Prospects','RMD']))                       #check current stage is 'Mandated','Prospects','RMD'
    & (QueueDF_MergedFiltered['Stage_Banded'].shift() == 'Group Shariah')).astype(int)       #check if previous row stage is 'Group Shariah'

    #Group Shariah Declined
    QueueDF_MergedFiltered['Group Shariah_Declined'] = ((QueueDF_MergedFiltered['CASEID'] == QueueDF_MergedFiltered['CASEID'].shift())  #check case id current equal to previous row
    & (QueueDF_MergedFiltered['Stage_Banded'] == 'Declined')                       #check current stage is Declined
    & (QueueDF_MergedFiltered['Stage_Banded'].shift() == 'Group Shariah')).astype(int)       #check previous row stage is'Group Shariah'

    #RMD New
    QueueDF_MergedFiltered['RMD_New'] = ((QueueDF_MergedFiltered['CASEID'] == QueueDF_MergedFiltered['CASEID'].shift())  #check case id current equal to previous row
    & (QueueDF_MergedFiltered['Stage_Banded'] == 'RMD')                       #check current stage is RMD
    & (QueueDF_MergedFiltered['Stage_Banded'].shift().isin(['Mandated','Group Shariah']))).astype(int)       #check previous row stage is either 'Mandated' or 'Group Shariah'

    #RMD Progressed
    QueueDF_MergedFiltered['RMD_Progressed'] = ((QueueDF_MergedFiltered['CASEID'] == QueueDF_MergedFiltered['CASEID'].shift())  #check case id current equal to previous row
    & (QueueDF_MergedFiltered['Stage_Banded'].isin(['CIC Approval','Fast Track Approval','Mandated']))                       #check current stage is 'CIC Approval','Fast Track Approval','Mandated'
    & (QueueDF_MergedFiltered['Stage_Banded'].shift() == 'RMD')).astype(int)       #check if previous row stage is 'RMD'

    #RMD Declined
    QueueDF_MergedFiltered['RMD_Declined'] = ((QueueDF_MergedFiltered['CASEID'] == QueueDF_MergedFiltered['CASEID'].shift())  #check case id current equal to previous row
    & (QueueDF_MergedFiltered['Stage_Banded'] == 'Declined')                       #check current stage is Declined
    & (QueueDF_MergedFiltered['Stage_Banded'].shift() == 'RMD')).astype(int)       #check previous row stage is'RMD'

    #CIC Approval New
    QueueDF_MergedFiltered['CIC_Approval_New'] = ((QueueDF_MergedFiltered['CASEID'] == QueueDF_MergedFiltered['CASEID'].shift())  #check case id current equal to previous row
    & (QueueDF_MergedFiltered['Stage_Banded'] == 'CIC Approval')                       #check current stage is CIC Approval
    & (QueueDF_MergedFiltered['Stage_Banded'].shift().isin(['RMD','Fast Track Approval','Mandated']))).astype(int)       #check if previous row stage is RMD, Fast Track Approval, Mandated

    #CIC Approval Progressed
    QueueDF_MergedFiltered['CIC_Approval_Progressed'] = ((QueueDF_MergedFiltered['CASEID'] == QueueDF_MergedFiltered['CASEID'].shift())  #check case id current equal to previous row
    & (QueueDF_MergedFiltered['Stage_Banded'].isin(['Acceptance','Mandated']))                       #check current stage is 'Acceptance' or 'Mandated'
    & (QueueDF_MergedFiltered['Stage_Banded'].shift() == 'CIC Approval')).astype(int)       #check if previous row stage is 'CIC Approval'

    #CIC Approval Declined
    QueueDF_MergedFiltered['CIC_Approval_Declined'] = ((QueueDF_MergedFiltered['CASEID'] == QueueDF_MergedFiltered['CASEID'].shift())  #check case id current equal to previous row
    & (QueueDF_MergedFiltered['Stage_Banded'] == 'Declined')                       #check current stage is Declined
    & (QueueDF_MergedFiltered['Stage_Banded'].shift() == 'CIC Approval')).astype(int)       #check previous row stage is'CIC Approval'

    #Fast Track Approval New
    QueueDF_MergedFiltered['FT_Approval_New'] = ((QueueDF_MergedFiltered['CASEID'] == QueueDF_MergedFiltered['CASEID'].shift())  #check case id current equal to previous row
    & (QueueDF_MergedFiltered['Stage_Banded'] == 'Fast Track Approval')                       #check current stage is Fast Track Approval
    & (QueueDF_MergedFiltered['Stage_Banded'].shift().isin(['RMD','Mandated']))).astype(int)       #check if previous row stage is 'RMD' or 'Mandated'

    #Fast Track Approval Progressed
    QueueDF_MergedFiltered['FT_Approval_Progressed'] = ((QueueDF_MergedFiltered['CASEID'] == QueueDF_MergedFiltered['CASEID'].shift())  #check case id current equal to previous row
    & (QueueDF_MergedFiltered['Stage_Banded'].isin(['Acceptance','Mandated','CIC Approval']))                       #check current stage is 'Acceptance','Mandated','CIC Approval'
    & (QueueDF_MergedFiltered['Stage_Banded'].shift() == 'Fast Track Approval')).astype(int)       #check if previous row stage is 'Fast track approval Approval'

    #Fast Track Approval Declined
    QueueDF_MergedFiltered['FT_Approval_Declined'] = ((QueueDF_MergedFiltered['CASEID'] == QueueDF_MergedFiltered['CASEID'].shift())  #check case id current equal to previous row
    & (QueueDF_MergedFiltered['Stage_Banded'] == 'Declined')                       #check current stage is Declined
    & (QueueDF_MergedFiltered['Stage_Banded'].shift() == 'Fast Track Approval')).astype(int)       #check previous row stage is'Fast Track Approval'

    #Acceptance 
    QueueDF_MergedFiltered['Acceptance'] = ((QueueDF_MergedFiltered['CASEID'] == QueueDF_MergedFiltered['CASEID'].shift())  #check case id current equal to previous row
    & (QueueDF_MergedFiltered['Stage_Banded'] == 'Acceptance')                       #check current stage is Acceptance
    & (QueueDF_MergedFiltered['Stage_Banded'].shift().isin(['CIC Approval','Fast Track Approval']))).astype(int)       #check if previous row stage is 'CIC Approval' or 'Fast Track Approval'

    #Add a column for customer history (N=new customer,y=existing customer,F=former customer)
    QueueDF_MergedFiltered = pd.merge(QueueDF_MergedFiltered,Customer_HistoryDF,left_on='CASEID',right_on='CASEID',how='left')
    QueueDF_MergedFiltered.drop(['CIF_ID'] ,axis=1, inplace=True)

     
    #create status 2 column
    QueueDF_MergedFiltered['STATUS_2'] = (QueueDF_MergedFiltered['CASEID'] != QueueDF_MergedFiltered['CASEID'].shift(-1)).astype(int) #check case id current is not equal to previous row
    QueueDF_MergedFiltered['STATUS_2'] = QueueDF_MergedFiltered.apply(lambda row:'Actv' if row['STATUS_2']==1 else 'del', axis=1)
    
    return QueueDF_MergedFiltered


