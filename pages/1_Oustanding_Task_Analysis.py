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
from Fetch_Data import Fetch_Data  #From own module
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
    <h1>Outstanding Task Trend</h1>
</div>
"""

# Render the HTML template
st.markdown(html_template, unsafe_allow_html=True)

#----------------------------User input for filtering----------------------------------------------------------------------------------------------------------------------

# Create a sidebar section for user input
st.sidebar.title('Dashboard Filters')

# Add a date input component
min_date = datetime.date(2022,12,13)
max_date = datetime.date(2023,4,30)

Date_Range = st.sidebar.date_input("Select Date Range:", (min_date, max_date))
#Convert the selected date into date time (otherwise, dataframe cannot be filtered since dataframe date column is in datetime)

try:
    selected_date = pd.to_datetime(Date_Range[0])
    selected_date2 = pd.to_datetime(Date_Range[1])
    st.sidebar.error("Date Range Selected: " + selected_date.strftime('%Y/%m/%d') + " to " +selected_date2.strftime('%Y/%m/%d') )
    st.sidebar.write('Data in JOMCOM only available from 13 Dec 2022 onwards')
        
    #---------------------------Fetch data from production database-----------------------------------------------------
    Fetched_Data = Fetch_Data()  

    #Access individual dataframe
    df1 = Fetched_Data['df1']   #queue table
    df2 = Fetched_Data['df2']   #amount applied table
    df3 = Fetched_Data['df3'] #customer History table
    df4 = Fetched_Data['df4'] #List of approved Cases table
    df5 = Fetched_Data['df5'] #dataframe for TAT to approved calculation

    QueueDF_MergedFiltered = Data_Transformation(df1,df2,df3)

    #---------------------------Data Analysis-----------------------------------------------------------------------

    #Create pivot table from QueueDF_MergedFiltered dataframe, aggregate function is sum
    Weekly_PivotDF = pd.pivot_table(QueueDF_MergedFiltered, values = ['Leads_New','Leads_Progressed','Leads_Declined','Prospects_New','Prospects_Progressed','Prospects_Declined','Mandated_New','Mandated_Progressed','Mandated_Declined','Group Shariah_New','Group Shariah_Progressed','Group Shariah_Declined','RMD_New','RMD_Progressed','RMD_Declined','FT_Approval_New','FT_Approval_Progressed','FT_Approval_Declined','CIC_Approval_New','CIC_Approval_Progressed','CIC_Approval_Declined'], columns = 'Reporting_Week', aggfunc=np.sum)
    # Specify the desired column order
    index_order = ['Leads_New', 'Leads_Progressed', 'Leads_Declined', 'Prospects_New','Prospects_Progressed','Prospects_Declined','Mandated_New','Mandated_Progressed','Mandated_Declined','Group Shariah_New','Group Shariah_Progressed','Group Shariah_Declined','RMD_New','RMD_Progressed','RMD_Declined','FT_Approval_New','FT_Approval_Progressed','FT_Approval_Declined','CIC_Approval_New','CIC_Approval_Progressed','CIC_Approval_Declined']
    Weekly_PivotDF = Weekly_PivotDF.reindex(index_order)
    Reporting_Week= pd.to_datetime(Weekly_PivotDF.columns).tolist()
    #Reporting_Week = pd.to_datetime(Reporting_Week).strftime('%Y-%m-%d')   -- this line is used in columns parameter to convert datetime to string

    #write a function for bucket analysis
    def Bucket_Analysis(New, Progressed, Declined):
        #Create a DF to check for outstanding balance at b.o.p ad e.o.p
        #1) create empty dataframe
        Outstanding_StockDF = pd.DataFrame(columns=Reporting_Week, index=['Stock_Beginning','+New','-Progressed','-Declined','Stock_Ending'])

        #Code below is for Leads outstanding table
        #2) set initial value of 'Outstanding_BOP ' to 0 and calculate for first week column 
        Outstanding_StockDF.loc['Stock_Beginning', Reporting_Week[0]] = 0   #first week stock beginning
        Outstanding_StockDF.loc['+New',Reporting_Week[0]] = Weekly_PivotDF.loc[New,Reporting_Week[0]]
        Outstanding_StockDF.loc['-Progressed',Reporting_Week[0]] =  Weekly_PivotDF.loc[Progressed,Reporting_Week[0]]
        Outstanding_StockDF.loc['-Declined', Reporting_Week[0]] = Weekly_PivotDF.loc[Declined, Reporting_Week[0]]
        Outstanding_StockDF.loc['Stock_Ending', Reporting_Week[0]] = 0 +  Weekly_PivotDF.loc[New,Reporting_Week[0]] - Weekly_PivotDF.loc[Progressed ,Reporting_Week[0]] - Weekly_PivotDF.loc[Declined,Reporting_Week[0]]  

        #iterate over remaining weeks:
        for i in range(1, len(Reporting_Week)):
            #get previous week's stock ending value
            prev_week_stock_ending = Outstanding_StockDF.loc['Stock_Ending', Reporting_Week[i-1]]
            
            Outstanding_StockDF.loc['Stock_Beginning', Reporting_Week[i]] = prev_week_stock_ending
            Outstanding_StockDF.loc['+New',Reporting_Week[i]] = Weekly_PivotDF.loc[New,Reporting_Week[i]]
            Outstanding_StockDF.loc['-Progressed',Reporting_Week[i]] =  Weekly_PivotDF.loc[Progressed,Reporting_Week[i]]
            Outstanding_StockDF.loc['-Declined', Reporting_Week[i]] = Weekly_PivotDF.loc[Declined, Reporting_Week[i]]
            Outstanding_StockDF.loc['Stock_Ending', Reporting_Week[i]] = prev_week_stock_ending +  Weekly_PivotDF.loc[New,Reporting_Week[i]] - Weekly_PivotDF.loc[Progressed,Reporting_Week[i]] - Weekly_PivotDF.loc[Declined,Reporting_Week[i]] 
        
        return Outstanding_StockDF

    def Transposed_Filter_Retransposed(Outstanding_StockDF): #first transposed to reshape the df prior to filtering (reporting week needs to be in single column for filter), after filtering, perform transposed again into final format
        Outstanding_TransposedDF = Outstanding_StockDF.transpose().reset_index(level=0)
        Outstanding_TransposedDF = Outstanding_TransposedDF.rename(columns={'index':'Reporting_Week'})
        Outstanding_TransposedDF = Outstanding_TransposedDF[(Outstanding_TransposedDF['Reporting_Week'] <= selected_date2) & (Outstanding_TransposedDF['Reporting_Week'] >= selected_date - pd.Timedelta(days=7))]
        Outstanding_TransposedDF['Reporting_Week'] = Outstanding_TransposedDF['Reporting_Week'].dt.strftime('%Y-%m-%d')
        Outstanding_FinalDF = Outstanding_TransposedDF.transpose()
        Outstanding_FinalDF.columns = Outstanding_FinalDF.iloc[0]  #let first row to become column header
        Outstanding_FinalDF = Outstanding_FinalDF.drop(Outstanding_FinalDF.index[0])  #drop first row since it is now column header

        return Outstanding_FinalDF

    #Create an empty data frame for progressed/declined by week
    Progressed_ByWeekDF = pd.DataFrame(columns=Reporting_Week, index=['Leads','Prospects','Mandated','Group Shariah','RMD','Fast Track Approval','CIC Approval'])
    #this code is to generate summary table for progressed and declined number by week:
    for i in range(0, len(Reporting_Week)):
        Progressed_ByWeekDF.loc['Leads',Reporting_Week[i]] = Weekly_PivotDF.loc['Leads_Progressed',Reporting_Week[i]] + Weekly_PivotDF.loc['Leads_Declined', Reporting_Week[i]]
        Progressed_ByWeekDF.loc['Prospects',Reporting_Week[i]] = Weekly_PivotDF.loc['Prospects_Progressed',Reporting_Week[i]] + Weekly_PivotDF.loc['Prospects_Declined', Reporting_Week[i]]
        Progressed_ByWeekDF.loc['Mandated',Reporting_Week[i]] = Weekly_PivotDF.loc['Mandated_Progressed',Reporting_Week[i]] + Weekly_PivotDF.loc['Mandated_Declined', Reporting_Week[i]]
        Progressed_ByWeekDF.loc['Group Shariah',Reporting_Week[i]] = Weekly_PivotDF.loc['Group Shariah_Progressed',Reporting_Week[i]] + Weekly_PivotDF.loc['Group Shariah_Declined', Reporting_Week[i]]
        Progressed_ByWeekDF.loc['RMD',Reporting_Week[i]] = Weekly_PivotDF.loc['RMD_Progressed',Reporting_Week[i]] + Weekly_PivotDF.loc['RMD_Declined', Reporting_Week[i]]
        Progressed_ByWeekDF.loc['Fast Track Approval',Reporting_Week[i]] = Weekly_PivotDF.loc['FT_Approval_Progressed',Reporting_Week[i]] + Weekly_PivotDF.loc['FT_Approval_Declined', Reporting_Week[i]]
        Progressed_ByWeekDF.loc['CIC Approval',Reporting_Week[i]] = Weekly_PivotDF.loc['CIC_Approval_Progressed',Reporting_Week[i]] + Weekly_PivotDF.loc['CIC_Approval_Declined', Reporting_Week[i]]
    
    Progressed_ByWeekFinalDF = Transposed_Filter_Retransposed(Progressed_ByWeekDF)

    #call out the bucket analysis function for each stage bucket, then transposed (to reshape prior to filtering), filter, then retransposed again
    Leads_Outstanding_StockDF = Bucket_Analysis('Leads_New','Leads_Progressed','Leads_Declined')
    Leads_Stock_FinalDF = Transposed_Filter_Retransposed(Leads_Outstanding_StockDF)

    Prospects_Outstanding_StockDF = Bucket_Analysis('Prospects_New','Prospects_Progressed','Prospects_Declined')
    Prospects_Stock_FinalDF = Transposed_Filter_Retransposed(Prospects_Outstanding_StockDF)

    Mandated_Outstanding_StockDF = Bucket_Analysis('Mandated_New','Mandated_Progressed','Mandated_Declined')
    Mandated_Stock_FinalDF = Transposed_Filter_Retransposed(Mandated_Outstanding_StockDF)

    Shariah_Outstanding_StockDF = Bucket_Analysis('Group Shariah_New','Group Shariah_Progressed','Group Shariah_Declined')
    Shariah_Stock_FinalDF = Transposed_Filter_Retransposed(Shariah_Outstanding_StockDF)
    
    RMD_Outstanding_StockDF = Bucket_Analysis('RMD_New','RMD_Progressed','RMD_Declined')
    RMD_Stock_FinalDF = Transposed_Filter_Retransposed(RMD_Outstanding_StockDF)

    FTA_Outstanding_StockDF = Bucket_Analysis('FT_Approval_New','FT_Approval_Progressed','FT_Approval_Declined')
    FTA_Stock_FinalDF = Transposed_Filter_Retransposed(FTA_Outstanding_StockDF)

    CIC_Outstanding_StockDF = Bucket_Analysis('CIC_Approval_New','CIC_Approval_Progressed','CIC_Approval_Declined')
    CIC_Stock_FinalDF = Transposed_Filter_Retransposed(CIC_Outstanding_StockDF)    

    #Code below is to create table used for chart (Outstanding stock at end of period)
    #1) create empty dataframe
    Stock_EndingDF = pd.DataFrame(columns=Reporting_Week, index=['Leads','Prospects','Mandated','RMD','FT_Approval','CIC_Approval','Group_Shariah','Approvals(Cummulative)','Declined(Cummulative)'])

    #2) Fill in the Stock_EndingDF values
    for i in range(0,len(Reporting_Week)):
        Stock_EndingDF.loc['Leads',Reporting_Week[i]] = Leads_Outstanding_StockDF.loc['Stock_Ending',Reporting_Week[i]]
        Stock_EndingDF.loc['Prospects',Reporting_Week[i]] = Prospects_Outstanding_StockDF.loc['Stock_Ending',Reporting_Week[i]]
        Stock_EndingDF.loc['Mandated',Reporting_Week[i]] = Mandated_Outstanding_StockDF.loc['Stock_Ending',Reporting_Week[i]]
        Stock_EndingDF.loc['RMD',Reporting_Week[i]] = RMD_Outstanding_StockDF.loc['Stock_Ending',Reporting_Week[i]]
        Stock_EndingDF.loc['FT_Approval',Reporting_Week[i]] = FTA_Outstanding_StockDF.loc['Stock_Ending',Reporting_Week[i]]
        Stock_EndingDF.loc['CIC_Approval',Reporting_Week[i]] = CIC_Outstanding_StockDF.loc['Stock_Ending',Reporting_Week[i]]
        Stock_EndingDF.loc['Group_Shariah',Reporting_Week[i]] = Shariah_Outstanding_StockDF.loc['Stock_Ending',Reporting_Week[i]]

    # Remove the timestamp from column names  (Try this, if doesnt work, remove it)
    #Stock_EndingDF.columns = Stock_EndingDF.columns.str.split().str[0]



    #Calculate for declined(cummulative) row
    Stock_EndingDF.loc['Declined(Cummulative)',:] = Leads_Outstanding_StockDF.loc['-Declined',:].cumsum() + Prospects_Outstanding_StockDF.loc['-Declined',:].cumsum() + Mandated_Outstanding_StockDF.loc['-Declined',:].cumsum() + RMD_Outstanding_StockDF.loc['-Declined',:].cumsum() + FTA_Outstanding_StockDF.loc['-Declined',:].cumsum() + CIC_Outstanding_StockDF.loc['-Declined',:].cumsum() + Shariah_Outstanding_StockDF.loc['-Declined',:].cumsum()

    #Calculate for Approvals (cummulative) row
    Stock_EndingDF.loc['Approvals(Cummulative)',:] = FTA_Outstanding_StockDF.loc['-Progressed',:].cumsum() + CIC_Outstanding_StockDF.loc['-Progressed',:].cumsum()
    #Stock_EndingDF.columns = pd.to_datetime(Stock_EndingDF.columns, format = '%Y-%m').strftime('%Y-%m-%d')

    #Reset index so that reporting week becomes column, then transposed, prerequisite before plotting charts
    Stock_Ending_TransposedDF = Stock_EndingDF.transpose().reset_index(level=0)
    Stock_Ending_TransposedDF = Stock_Ending_TransposedDF.rename(columns={'index':'Reporting_Week'})
    Stock_Ending_TransposedDF = Stock_Ending_TransposedDF[(Stock_Ending_TransposedDF['Reporting_Week'] <= selected_date2) & (Stock_Ending_TransposedDF['Reporting_Week'] >= selected_date - pd.Timedelta(days=7))]
    Stock_Ending_TransposedDF['Reporting_Week'] = Stock_Ending_TransposedDF['Reporting_Week'].dt.strftime('%Y-%m-%d')
    stock_Ending_FinalDF = Stock_Ending_TransposedDF.transpose()
    stock_Ending_FinalDF.columns = stock_Ending_FinalDF.iloc[0]  #let first row to become column header
    stock_Ending_FinalDF = stock_Ending_FinalDF.drop(stock_Ending_FinalDF.index[0])  #drop first row since it is now column header

    #Create table to check New Leads based on region
    New_Leads_PivotDF = pd.pivot_table(QueueDF_MergedFiltered, values = 'Leads_New', index= 'Region', columns = 'Reporting_Week', aggfunc=np.sum, fill_value=0, margins=True)

    #Code below is to drop New_leads_PivotDF 'Total' column and row, prerequisite for percentage calculation
    New_Leads_Pivot_Remove_Total = New_Leads_PivotDF.drop('All')
    New_Leads_Pivot_Remove_Total.drop('All', axis=1,inplace=True)

    #Create table for New Leads Percentage by region
    column_totals = New_Leads_Pivot_Remove_Total.sum()
    New_Leads_PercentageDF = New_Leads_Pivot_Remove_Total.div(column_totals) * 100
    New_Leads_PercentageDF = New_Leads_PercentageDF.applymap(lambda x: '{:.0f}%'.format(x))

    #remove the timestamp for all column (under bucket analysis) / Warning: this code needs to be placed after stock ending calculation to avoid key error when converting the column to string
    Leads_Outstanding_StockDF.columns = pd.to_datetime(Leads_Outstanding_StockDF.columns, format = '%Y-%m').strftime('%Y-%m-%d')
    Prospects_Outstanding_StockDF.columns = pd.to_datetime(Prospects_Outstanding_StockDF.columns, format = '%Y-%m').strftime('%Y-%m-%d')
    Mandated_Outstanding_StockDF.columns = pd.to_datetime(Mandated_Outstanding_StockDF.columns, format = '%Y-%m').strftime('%Y-%m-%d')
    Shariah_Outstanding_StockDF.columns = pd.to_datetime(Shariah_Outstanding_StockDF.columns, format = '%Y-%m').strftime('%Y-%m-%d')
    RMD_Outstanding_StockDF.columns = pd.to_datetime(RMD_Outstanding_StockDF.columns, format = '%Y-%m').strftime('%Y-%m-%d')
    FTA_Outstanding_StockDF.columns = pd.to_datetime(FTA_Outstanding_StockDF.columns, format = '%Y-%m').strftime('%Y-%m-%d')
    CIC_Outstanding_StockDF.columns = pd.to_datetime(CIC_Outstanding_StockDF.columns, format = '%Y-%m').strftime('%Y-%m-%d')

    #----------------------Display Results/ Render Results in Web App----------------------------------------------------------------------------------

    # Create a container 
    chart_container = st.container()

    #1)Create chart for Stock_Ending Dataframe   :  Stock_Ending_TransposedDF
    with chart_container:
        fig = make_subplots(specs=[[{"secondary_y": True}]])

        fig.add_trace(
            go.Bar(
                x = Stock_Ending_TransposedDF['Reporting_Week'],
                y =Stock_Ending_TransposedDF['Declined(Cummulative)'],
            name = 'Declined(Cummulative)',
            marker = dict(color='#EFCC00'), opacity =1
            ) ,secondary_y=False,
            row=1, col=1
        )
        fig.add_trace(
            go.Scatter(
                x = Stock_Ending_TransposedDF['Reporting_Week'],
                y =Stock_Ending_TransposedDF['Leads'],
            name = 'Leads Outstanding',
            line=dict(color='blue')
            ),
            secondary_y=True,
            row=1, col=1
        )
        fig.add_trace(
            go.Scatter(
                x = Stock_Ending_TransposedDF['Reporting_Week'],
                y =Stock_Ending_TransposedDF['Prospects'],
            name = 'Prospects Outstanding',
            line=dict(color='green')
            ),secondary_y=True,
            row=1, col=1
        )
        fig.add_trace(
            go.Scatter(
                x = Stock_Ending_TransposedDF['Reporting_Week'],
                y =Stock_Ending_TransposedDF['Mandated'],
            name = 'Mandated Outstanding',
            line=dict(color='red')
            ), secondary_y=True,
            row=1, col=1
        )
        fig.add_trace(
            go.Scatter(
                x = Stock_Ending_TransposedDF['Reporting_Week'],
                y =Stock_Ending_TransposedDF['RMD'],
            name = 'RMD Outstanding', line=dict(color='gray')
            ), secondary_y=True,
            row=1, col=1
        )
        fig.add_trace(
            go.Scatter(
                x = Stock_Ending_TransposedDF['Reporting_Week'],
                y =Stock_Ending_TransposedDF['FT_Approval'],
            name = 'FT Approval Outstanding'
            ), secondary_y=True,
            row=1, col=1
        )
        fig.add_trace(
            go.Scatter(
                x = Stock_Ending_TransposedDF['Reporting_Week'],
                y =Stock_Ending_TransposedDF['CIC_Approval'],
            name = 'CIC Approval Outstanding', line=dict(color='black')
            ) ,secondary_y=True,
            row=1, col=1
        )
        fig.add_trace(
            go.Scatter(
                x = Stock_Ending_TransposedDF['Reporting_Week'],
                y =Stock_Ending_TransposedDF['Group_Shariah'],
            name = 'G.Shariah Outstanding', line=dict(color='brown')
            ) ,secondary_y=True,
            row=1, col=1
        )
        fig.add_trace(
            go.Scatter(
                x = Stock_Ending_TransposedDF['Reporting_Week'],
                y =Stock_Ending_TransposedDF['Approvals(Cummulative)'],
            name = 'Approvals(Cummulative)', line=dict(color='purple')
            ) ,secondary_y=True,
            row=1, col=1
        )


        fig.update_layout(
            width = 1200,
            height = 400,
            yaxis=dict(title='# of Applications',side='right',showgrid=False),
            yaxis2=dict( side='left'),
            xaxis=dict(
                ticktext=Stock_Ending_TransposedDF['Reporting_Week'],  # Set the ticktext to the exact data
                tickmode='array',
                tickvals=Stock_Ending_TransposedDF['Reporting_Week']  # Set the tickvals to the exact data
            ),
            title=dict(text='Weekly Application Status Trend in JOMCOM', font=dict(size=20), x=0.4, y=0.95 )  # Set the chart title and styling
        )

        st.plotly_chart(fig, use_container_width=True)

    # Display the filtered DataFrame as a table
    st.dataframe(stock_Ending_FinalDF, use_container_width=True)

    #Create download button for stock ending dataframe
    csv_Stock_Ending = convert_df(Stock_EndingDF)

    st.download_button(
        label="Download data as CSV",
        data=csv_Stock_Ending,
        file_name='Stock_Ending_df.csv',
        mime='text/csv',
    )

    with st.expander('Expand this for details by stage:'):
        st.write('Leads Outstanding')
        st.dataframe( Leads_Stock_FinalDF, use_container_width=True)
        csv_Leads_Outstanding_StockDF = convert_df( Leads_Stock_FinalDF)
        st.download_button(
            label="Download data as CSV",
            data=csv_Leads_Outstanding_StockDF,
            file_name='Leads_Outstanding_df.csv',
            mime='text/csv',
        )

        st.write('Prospects Outstanding')
        st.dataframe( Prospects_Stock_FinalDF, use_container_width=True)
        csv_Prospects_Outstanding_StockDF = convert_df(Prospects_Stock_FinalDF)
        st.download_button(
            label="Download data as CSV",
            data=csv_Prospects_Outstanding_StockDF,
            file_name='Prospects_Outstanding_df.csv',
            mime='text/csv',
        )
        
        st.write('Mandated Outstanding')
        st.dataframe(Mandated_Stock_FinalDF, use_container_width=True)
        csv_Mandated_Outstanding_StockDF = convert_df(Mandated_Stock_FinalDF)
        st.download_button(
            label="Download data as CSV",
            data=csv_Mandated_Outstanding_StockDF,
            file_name='Mandated_Outstanding_df.csv',
            mime='text/csv',
        )

        st.write('Shariah Outstanding')
        st.dataframe(Shariah_Stock_FinalDF, use_container_width=True)
        csv_Shariah_Outstanding_StockDF = convert_df(Shariah_Stock_FinalDF)
        st.download_button(
            label="Download data as CSV",
            data=csv_Shariah_Outstanding_StockDF,
            file_name='Shariah_outstanding_df.csv',
            mime='text/csv',
        )

        st.write('RMD Outstanding')
        st.dataframe(RMD_Stock_FinalDF, use_container_width=True)
        csv_RMD_Outstanding_StockDF = convert_df(RMD_Stock_FinalDF)
        st.download_button(
            label="Download data as CSV",
            data=csv_RMD_Outstanding_StockDF,
            file_name='RMD_Outstanding_df.csv',
            mime='text/csv',
        )

        st.write('Fast Track Approval Oustanding')
        st.dataframe(FTA_Stock_FinalDF, use_container_width=True)
        csv_FTA_Outstanding_StockDF = convert_df(FTA_Stock_FinalDF)
        st.download_button(
            label="Download data as CSV",
            data=csv_FTA_Outstanding_StockDF,
            file_name='Fast_Track_Approval_Outstanding_df.csv',
            mime='text/csv',
        )

        st.write('CIC Approval Outstanding')
        st.dataframe(CIC_Stock_FinalDF, use_container_width=True)
        csv_CIC_Outstanding_StockDF = convert_df(CIC_Stock_FinalDF)
        st.download_button(
            label="Download data as CSV",
            data=csv_CIC_Outstanding_StockDF,
            file_name='CIC_Approval_Outstanding_df.csv',
            mime='text/csv',
        )

        st.write('Summary of Progressed/Declined by Week')
        st.dataframe(Progressed_ByWeekFinalDF, use_container_width=True)
        csv_Progressed_ByWeekDF = convert_df(Progressed_ByWeekFinalDF)
        st.download_button(
            label="Download data as CSV",
            data=csv_Progressed_ByWeekDF,
            file_name='summary of progressed and declined by week.csv',
            mime='text/csv',
        )

except IndexError:
    st.error("Please select both start and end dates")