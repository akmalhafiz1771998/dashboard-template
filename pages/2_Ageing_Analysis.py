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


#---------------------Custom function--------------------------------------------------------------------------------

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

# check localStorage if loginSession == null || !loginSession, error("You do not have access to this page.")

#----------------------------set the page config--------------------------------------------------------------------
st.set_page_config(page_title='Philly Project Dashboard',page_icon='MIDF_favicon.png'   ,layout='wide')

# Define the HTML template with CSS to position the image and title
html_template = """
<div style="display: flex; align-items: center;">
    <img src="https://seeklogo.com/images/M/midf-logo-44DC64A7C7-seeklogo.com.png" alt="MIDF Logo" style="width: 200px; height: 72px; margin-right: 10px;">
    <h1>Ageing Analysis</h1>
</div>
"""


# Render the HTML template
st.markdown(html_template, unsafe_allow_html=True)

#------------------User input for filtering------------------------------------------------------------------------
# Create a sidebar section for user input
st.sidebar.title('Dashboard Filters')

# Add a date input component
min_date = datetime.date(2022,12,13)
max_date = datetime.date(2023,4,30)

Date_Range = st.sidebar.date_input("Select Date Range:", (min_date, max_date))


Selected_Stage = st.sidebar.selectbox(
    'Stage to Filter:',
    ('Leads', 'Prospects', 'Mandated','Group Shariah','RMD','CIC Approval','Acceptance','Fast Track Approval','Declined','All'))

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

    #--------------------------Data Analysis---------------------------------------------------------------------------

    #Code below is to calculate ageing.
    #1) pergorm groupby on caseid, reporting week, stage banded and sum aggregate duration to get ageing
    AgeingDF = QueueDF_MergedFiltered.groupby(['CASEID','Reporting_Week','Stage_Banded'])['Duration'].sum().reset_index().rename(columns = {'Duration':'Ageing'})

    #For filtering using user's input
    if Selected_Stage != 'All':
        AgeingDF = AgeingDF[AgeingDF['Stage_Banded']==Selected_Stage]
    else:
        pass

    Ageing_TransformedDF = AgeingDF.pivot_table(values='Ageing', index='Reporting_Week', aggfunc={'Ageing': ['mean', 'median', 'max', 'sum','count']}).reset_index()

    # Rename the columns
    Ageing_TransformedDF = Ageing_TransformedDF.rename(columns={'mean': 'Average of Ageing', 'median': 'Median of Ageing', 'max': 'Max of Ageing', 'sum': 'Sum of Ageing', 'count':'Count of CASEID'})

    # Convert Reporting_Week column to string
    #Ageing_TransformedDF['Reporting_Week'] = Ageing_TransformedDF['Reporting_Week'].astype(str)

    # Round off mean and median to days only
    Ageing_TransformedDF['Average of Ageing'] = Ageing_TransformedDF['Average of Ageing'].round()
    Ageing_TransformedDF['Median of Ageing'] = Ageing_TransformedDF['Median of Ageing'].round()

    #Filter dataframe, change to string then transposed
    Ageing_TransformedDF = Ageing_TransformedDF[(Ageing_TransformedDF['Reporting_Week'] <= selected_date2) & (Ageing_TransformedDF['Reporting_Week'] >= selected_date - pd.Timedelta(days=7))]
    Ageing_TransformedDF['Reporting_Week'] = Ageing_TransformedDF['Reporting_Week'].dt.strftime('%Y-%m-%d')
    Transposed_AgeingDF = Ageing_TransformedDF.set_index('Reporting_Week').transpose()

    Transposed_AgeingDF = Ageing_TransformedDF.set_index('Reporting_Week').transpose()
    Transposed_AgeingDF_1 = Transposed_AgeingDF.drop('Count of CASEID')   #this table is for ageing without bar chart
    Transposed_AgeingDF_2 = Transposed_AgeingDF.drop(['Max of Ageing','Median of Ageing'])  #this table is for ageing with bar chart
    
    #---------------------Display result/ Render page----------------------------------------------------
    #---------------------Metrics Card-------------------------------------------------------------------

    #Setup the variable for metrics card
    Last_Reporting_Week = Ageing_TransformedDF.iloc[-1,0]   #to get the last reporting week
    Last_Average_Ageing = Ageing_TransformedDF.iloc[-1,3] #to get the final week average of ageing
    Second_Last_Average = Ageing_TransformedDF.iloc[-2,3] #to get the 2nd final week average of ageing
    Overall_Average_Ageing =  AgeingDF[(AgeingDF['Reporting_Week'] <= selected_date2) & (AgeingDF['Reporting_Week'] >= selected_date)]['Ageing'].mean().round()
    Overall_Median_Ageing = AgeingDF[(AgeingDF['Reporting_Week'] <= selected_date2) & (AgeingDF['Reporting_Week'] >= selected_date)]['Ageing'].median()
    Delta_Average_Ageing = Last_Average_Ageing - Second_Last_Average

    #Render the metric card section
    col1, col2, col3 = st.columns(3)

    # Create a container for the metric boxes
    metric_container = st.container()

    # Define a CSS class for the metric container
    metric_container_css = """
    .metric-container {
        background-color: #FFFFFF;
        border: 1px solid #CCCCCC;
        padding: 1rem;
        border-radius: 5px;
        box-shadow: 0 0.15rem 1.75rem 0 rgba(58, 59, 69, 0.15);
        border-left: 4px solid red; /* Add a left border color here */
        width: 300px; /* Adjust the width here */
        height: 120px; /* Adjust the height here */
    }
    """

    # Add the CSS styling using st.markdown()
    st.markdown(f"<style>{metric_container_css}</style>", unsafe_allow_html=True)

    # Use the metric_container to display the metrics
    with metric_container:
        col1.markdown(
            """
            <div class="metric-container">
                <div>
                    <p style="font-size: 1.5rem; margin-bottom: 0;">Average of Ageing</p>
                    <p style="font-size: 2.0rem; margin-top: 0;">{}</p>
                </div>
            </div>
            """.format(Overall_Average_Ageing),
            unsafe_allow_html=True,
        )

        col2.markdown(
            """
            <div class="metric-container">
                <div>
                    <p style="font-size: 1.5rem; margin-bottom: 0;">Median of Ageing</p>
                    <p style="font-size: 2.0rem; margin-top: 0;">{}</p>
                </div>
            </div>
            """.format(Overall_Median_Ageing),
            unsafe_allow_html=True,
        )

        col3.markdown(
            """
            <div class="metric-container">
                <div>
                    <p style="font-size: 1.5rem; margin-bottom: 0;">Delta</p>
                    <p style="font-size: 2.0rem; margin-top: 0;">{}</p>
                </div>
            </div>
            """.format(Delta_Average_Ageing),
            unsafe_allow_html=True,
        )
    #----------------------Create container for charts----------------------------------------------------------------------------------

    # Create a container 
    #chart_container = st.container()
    tab1, tab2 = st.tabs(["Ageing Trend", "No of Case against Ageing"])

    #1)Create chart for ageing trend
    with tab1:
        fig3 = make_subplots(specs=[[{"secondary_y": True}]])

        fig3.add_trace(      
        go.Scatter(
            x = Ageing_TransformedDF['Reporting_Week'],
            y =Ageing_TransformedDF['Average of Ageing'],
        name = 'Average of Ageing',
        mode = 'markers',
        marker=dict(color='green')
        ),
        secondary_y=False,
        row=1, col=1
        )
        fig3.add_trace(
        go.Scatter(
            x = Ageing_TransformedDF['Reporting_Week'],
            y =Ageing_TransformedDF['Median of Ageing'],
        name = 'Median of Ageing',
        mode = 'markers',
        marker=dict(color='#DE970B')
        ),secondary_y=False,
        row=1, col=1
        )
        fig3.add_trace(
        go.Scatter(
            x = Ageing_TransformedDF['Reporting_Week'],
            y =Ageing_TransformedDF['Max of Ageing'],
        name = 'Max of Ageing',
        mode = 'markers',
        marker=dict(color='black')
        ), secondary_y=False,
        row=1, col=1
        )
        fig3.add_trace(
        go.Scatter(
            x = Ageing_TransformedDF['Reporting_Week'],
            y =Ageing_TransformedDF['Sum of Ageing'],
        name = 'Sum of Ageing', line=dict(color='red')
        ), secondary_y=True,
        row=1, col=1
        )
        
        fig3.update_layout(
        width = 1000,
        height = 400,
        yaxis=dict(title='Ageing (Days)',side='left',dtick=5, tickmode = 'auto'),
        yaxis2=dict(title='Sum of Ageing (Days)', side='right' , showgrid = False),
        xaxis=dict(
            ticktext=Ageing_TransformedDF['Reporting_Week'],  # Set the ticktext to the exact data
            tickmode='array',
            tickvals=Ageing_TransformedDF['Reporting_Week'] , # Set the tickvals to the exact data
            showgrid = False
        ),
        title=dict(text='Ageing Trend for ' + Selected_Stage, font=dict(size=20), x=0.4, y=0.95 ),  # Set the chart title and styling
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
        )

        st.plotly_chart(fig3, use_container_width=True)
        st.dataframe(Transposed_AgeingDF_1, use_container_width=True)

    #2 create ageing chart against count bar plot

    with tab2:
        #Create chart for ageing against bar plot
        fig4 = make_subplots(specs=[[{"secondary_y": True}]])

        fig4.add_trace(
            go.Bar(
                x = Ageing_TransformedDF['Reporting_Week'],
                y = Ageing_TransformedDF['Count of CASEID'],
            name = 'Count of CASEID',
            marker = dict(color='#Ff8c00'), opacity =1
            ) ,secondary_y=False,
            row=1, col=1
        )
        fig4.add_trace(
            go.Scatter(
                x = Ageing_TransformedDF['Reporting_Week'],
                y = Ageing_TransformedDF['Sum of Ageing'],
            name = 'Sum of Ageing',
            line=dict(color='red')
            ),
            secondary_y=True,
            row=1, col=1
        )
        fig4.add_trace(
            go.Scatter(
                x = Ageing_TransformedDF['Reporting_Week'],
                y = Ageing_TransformedDF['Average of Ageing'],
            name = 'Average of Ageing',
            line=dict(color='blue')
            ),secondary_y=True,
            row=1, col=1
        )


        fig4.update_layout(
            width = 1000,
            height = 400,
            yaxis=dict(title='Count of Cases',side='left', dtick=10),
            yaxis2=dict(title= 'Ageing (Days)', side='right',showgrid = False),
            xaxis=dict(
                ticktext=Ageing_TransformedDF['Reporting_Week'],  # Set the ticktext to the exact data
                tickmode='array',
                tickvals=Ageing_TransformedDF['Reporting_Week']  # Set the tickvals to the exact data
            ),
            title=dict(text='Ageing trend for ' + Selected_Stage + ' against # of cases', font=dict(size=20), x=0.4, y=0.95 )  # Set the chart title and styling
        )


        st.plotly_chart(fig4, use_container_width=True)
        st.dataframe(Transposed_AgeingDF_2, use_container_width=True)
    

except IndexError:
    st.error("Please select both start and end dates")