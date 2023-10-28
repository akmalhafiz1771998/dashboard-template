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


#---------------------Custom function--------------------------------------------------------------------------------

#Create function to convert dataframe to csv file
@st.cache_data
def convert_df(df):
    # IMPORTANT: Cache the conversion to prevent computation on every rerun
    return df.to_csv().encode('utf-8')

#----------------------------set the page config--------------------------------------------------------------------
st.set_page_config(page_title='Philly Project Dashboard',page_icon='MIDF_favicon.png'   ,layout='wide')

#------------------Login/ Credentials function------------------------------------------------------------
#Import YAML file to script
file_path = Path(__file__).parent/"config.yaml"
with file_path.open("rb") as file:
    config = yaml.load(file, Loader=SafeLoader)

#create authenticator object
authenticator = stauth.Authenticate(
    config['credentials'],
    config['cookie']['name'],
    config['cookie']['key'], 
    config['cookie']['expiry_days']
)

name, authentication_status, username = authenticator.login('Login','main')   #render login page

if authentication_status == False:
    st.error("Username/Passwords is incorrect")
    stop()

if authentication_status == None:
    st.warning("Please enter your username and password")
    stop()

# Retrieve the user's role from the config file
role = config['credentials']['usernames'][username]['role']
st.session_state["role"] = role


if authentication_status:
    # Define the HTML template with CSS to position the image and title
    html_template = """
    <div style="display: flex; align-items: center;">
        <img src="https://seeklogo.com/images/M/midf-logo-44DC64A7C7-seeklogo.com.png" alt="MIDF Logo" style="width: 200px; height: 72px; margin-right: 10px;">
        <h1>Philly Project Dashboard</h1>
    </div>
    """
    # Render the HTML template
    st.markdown(html_template, unsafe_allow_html=True)

    # store loginSession = true in localStorage
    # Python command for setItem in localStorage

    #------------------User input for filtering------------------------------------------------------------------------
    # Create a sidebar section for user input
    st.sidebar.title('Dashboard Filters')

    # Add a date input component
    min_date = datetime.date(2022,12,13)
    max_date = datetime.date(2023,4,30)

    Date_Range = st.sidebar.date_input("Select Date Range:", (min_date, max_date))
    

    #Convert the selected date into date time (otherwise, dataframe cannot be filtered since dataframe date column is in datetime)
    try: #this try-except wrap to handle error from filtering using selected date
        selected_date = pd.to_datetime(Date_Range[0])
        selected_date2 = pd.to_datetime(Date_Range[1])
        st.sidebar.error("Date Range Selected: " + selected_date.strftime('%Y/%m/%d') + " to " +selected_date2.strftime('%Y/%m/%d') )
        st.sidebar.write('Data in JOMCOM only available from 13 Dec 2022 onwards')

        #logout sidebar
        def logout():
            if authenticator.logout("Logout", "sidebar"):
                st.session_state['role'] = 'None'        
                st.experimental_rerun()
                # localStorage delete / set loginSession = false

        logout()
        # Add footer and code version
        footer = """
        <style>
        .footer {
            position: relative;
        }

        @media (max-width: 600px) {
            .footer {
                position: static;
                margin-top: 20px;
            }
        }
        </style>

        <div class="footer">
            <hr style="margin-top: 20px;">
            <p style="font-size: 12px; color: gray;">Code version: 1.0.9</p>
        </div>
        """
        st.sidebar.markdown(footer, unsafe_allow_html=True)
        #---------------------------Fetch data from production database-----------------------------------------------------
        Fetched_Data = Fetch_Data()  

        #Access individual dataframe
        df1 = Fetched_Data['df1']   #queue table
        df2 = Fetched_Data['df2']   #amount applied table
        df3 = Fetched_Data['df3'] #customer History table
        df4 = Fetched_Data['df4'] #List of approved Cases table
        df5 = Fetched_Data['df5'] #dataframe for TAT to approved calculation

        QueueDF_MergedFiltered = Data_Transformation(df1,df2,df3,selected_date,selected_date2)
        TAT_DF = df5 #rename df5, this is TAT to approved dataframe
        Approval_ListDF = df4[(df4['APPROVAL DATE']<=selected_date2) & (df4['APPROVAL DATE'] >= selected_date)] #rename df4 for approval list and filter based on selected date range
        #---------------------------Data Analysis-----------------------------------------------------------------------

        #1) New full document submission 
        # Create pivot table from QueueDF_MergedFiltered dataframe, aggregate function is sum
        Monthly_PivotDF = pd.pivot_table(QueueDF_MergedFiltered, values = ['Leads_New','Leads_Progressed','Leads_Declined','Prospects_New','Prospects_Progressed','Prospects_Declined','Mandated_New','Mandated_Progressed','Mandated_Declined','Group Shariah_New','Group Shariah_Progressed','Group Shariah_Declined','RMD_New','RMD_Progressed','RMD_Declined','FT_Approval_New','FT_Approval_Progressed','FT_Approval_Declined','CIC_Approval_New','CIC_Approval_Progressed','CIC_Approval_Declined'], columns = 'Reporting_Month', aggfunc=np.sum  )
        # Specify the desired column order
        index_order = ['Leads_New', 'Leads_Progressed', 'Leads_Declined', 'Prospects_New','Prospects_Progressed','Prospects_Declined','Mandated_New','Mandated_Progressed','Mandated_Declined','Group Shariah_New','Group Shariah_Progressed','Group Shariah_Declined','RMD_New','RMD_Progressed','RMD_Declined','FT_Approval_New','FT_Approval_Progressed','FT_Approval_Declined','CIC_Approval_New','CIC_Approval_Progressed','CIC_Approval_Declined']
        Monthly_PivotDF.columns = pd.to_datetime(Monthly_PivotDF.columns, format='%m/%Y').strftime('%Y-%m')  #This is to convert each column to datetime then take the string-time to allow the dataframe to be sorted correctly
        Monthly_PivotDF = Monthly_PivotDF.reindex(index_order).sort_index(axis=1)


        #Create pivot table from Monthly mandated new dataframe, aggregate function is sum
        Monthly_Mandated_NewDF = pd.pivot_table(QueueDF_MergedFiltered, values = ['Mandated_New','CASEID'], index = ['Reporting_Month','EH_FLAG'], aggfunc={'Mandated_New': 'sum' , 'CASEID':'count' } )

        #The reporting month is sorted by changing to datetime value first, sort the values and only then the datetime is converted to string
        Monthly_Mandated_NewDF = Monthly_Mandated_NewDF.reset_index()
        Monthly_Mandated_NewDF.loc[Monthly_Mandated_NewDF.EH_FLAG == 'C', 'EH_FLAG'] = 'BAU'
        Monthly_Mandated_NewDF.loc[Monthly_Mandated_NewDF.EH_FLAG == 'N', 'EH_FLAG'] = 'BAU'
        Monthly_Mandated_NewDF.loc[Monthly_Mandated_NewDF.EH_FLAG == 'Y', 'EH_FLAG'] = 'Lane 5'
        Monthly_Mandated_NewDF['Reporting_Month'] = pd.to_datetime(Monthly_Mandated_NewDF['Reporting_Month'], format='%m/%Y')
        Monthly_Mandated_NewDF['Reporting_Month'] = Monthly_Mandated_NewDF['Reporting_Month'].dt.strftime('%Y-%m')
        Monthly_Mandated_NewDF = Monthly_Mandated_NewDF.sort_values('Reporting_Month')
        Sum_of_Mandated = Monthly_Mandated_NewDF['Mandated_New'].sum()

        #2) TAT to Approval/ TAT to closure - Data cleaning for TAT to approved/closure dataframe
        TAT_DF['TAT to Approved'] = (TAT_DF['Approved_Date'] - TAT_DF['Created_Date']).dt.round('d').dt.days
        
        TAT_DF['TAT to Closure'] = (TAT_DF['Closed_Date'] - TAT_DF['Created_Date']).dt.round('d').dt.days

        try:
            #Replace eh_flag with corresponding meaning, and change date time to date, and create month created date, then filter all null value 
            TAT_DF.loc[TAT_DF.EH_FLAG == 'C', 'EH_FLAG'] = 'BAU'
            TAT_DF.loc[TAT_DF.EH_FLAG == 'N', 'EH_FLAG'] = 'BAU'
            TAT_DF.loc[TAT_DF.EH_FLAG == 'Y', 'EH_FLAG'] = 'Lane 5'
            TAT_DF['Created_Date'] = pd.to_datetime(TAT_DF['Created_Date']).dt.date
            TAT_DF['Closed_Date'] = pd.to_datetime(TAT_DF['Closed_Date']).dt.date
            TAT_DF['Cancelled_Date'] = pd.to_datetime(TAT_DF['Cancelled_Date']).dt.date
            TAT_DF['Month_Created'] = pd.to_datetime(TAT_DF['Created_Date'], format='%Y/%m') 
            TAT_DF['Month_Approved'] = pd.to_datetime(TAT_DF['Approved_Date'],format='%Y/%m')
            TAT_DF['Month_Created'] = TAT_DF['Month_Created'].dt.strftime('%Y-%m')
            TAT_DF['Month_Approved'] = TAT_DF['Month_Approved'].dt.strftime('%Y-%m')

            #dataframe for TAT to approved
            TAT_DF_Filtered = TAT_DF[TAT_DF['TAT to Approved'].notnull()] 
            TAT_DF_Filtered = TAT_DF_Filtered[(TAT_DF_Filtered['Approved_Date'] <= selected_date2) & (TAT_DF_Filtered['Approved_Date'] >= selected_date)]
            Overall_TAT_to_Approved = TAT_DF_Filtered['TAT to Approved'].quantile(q=0.8).round() #calculate overall 80th percentile for TAT to Approved

            #dataframe for TAT to closure
            TAT_DF_Closed = TAT_DF[TAT_DF['TAT to Closure'].notnull()] 
            TAT_DF_Closed = TAT_DF_Closed[(TAT_DF_Closed['Created_Date'] <= selected_date2) & (TAT_DF_Closed['Created_Date'] >= selected_date)]
            Overall_TAT_to_Closure = TAT_DF_Closed['TAT to Closure'].quantile(q=0.8).round() #calculate overall 80th percentile for TAT to Approved
            
            #Create a Pivot table from TAT_DF dataframe, aggregate by 80th percentile for TAT to approved
            TAT_DF_Pivot = pd.pivot_table(TAT_DF_Filtered, values = ['TAT to Approved'], index = ['Month_Approved','EH_FLAG'], aggfunc={'TAT to Approved':lambda x: np.percentile(x, 80) } )
            TAT_DF_Pivot = TAT_DF_Pivot.reset_index()

            #Create a Pivot table from TAT_DF dataframe, aggregate by 80th percentile for TAT to closure
            TAT_Closed_PivotDF = pd.pivot_table(TAT_DF_Closed, values = ['TAT to Closure'], index = ['Month_Created','EH_FLAG'], aggfunc={'TAT to Closure':lambda x: np.percentile(x, 80) } )
            TAT_Closed_PivotDF = TAT_Closed_PivotDF.reset_index()

            
            TAT_DF_Pivot['TAT to Approved'] = TAT_DF_Pivot['TAT to Approved'].round() #round off the TAT 
            TAT_Closed_PivotDF['TAT to Closure'] = TAT_Closed_PivotDF['TAT to Closure'].round() #round off the TAT 

            
            #3)Approval List KPI
            #Convert Month Approved to format Year-Month
            Approval_ListDF['Month Approved'] = pd.to_datetime(Approval_ListDF['APPROVAL DATE'], format='%Y/%m')
            Approval_ListDF['Month Approved'] = Approval_ListDF['Month Approved'].dt.strftime('%Y-%m')

            #Create pivot table from Approval_List dataframe, aggregate function is sum
            Approval_ListPivot = pd.pivot_table(Approval_ListDF, values = ['AMOUNT APPROVED','CASEID'], index = ['Month Approved','EH_FLAG'], aggfunc={'AMOUNT APPROVED': 'sum' , 'CASEID':'count' } )
            Approval_ListPivot = Approval_ListPivot.reset_index()
            Approval_ListPivot.loc[Approval_ListPivot.EH_FLAG == 'C', 'EH_FLAG'] = 'BAU'
            Approval_ListPivot.loc[Approval_ListPivot.EH_FLAG == 'N', 'EH_FLAG'] = 'BAU'
            Approval_ListPivot.loc[Approval_ListPivot.EH_FLAG == 'Y', 'EH_FLAG'] = 'Lane 5'
            Approval_ListPivot['AMOUNT APPROVED MILLION'] = np.round((Approval_ListPivot['AMOUNT APPROVED'].astype(float)/1000000),decimals=2)
            Total_Amount_Approved = Approval_ListPivot['AMOUNT APPROVED'].sum() / 1000000 #calculate total amount approved for key metric
            #get total number of approved cases (to use in metric card)
            Total_Approved = Approval_ListPivot['CASEID'].sum()
                 
            #get the total number of leads (to use in metric card)
            Total_Leads = QueueDF_MergedFiltered['CASEID'].nunique()
            #get the total declined (to use in metric card)
            Total_Declined = QueueDF_MergedFiltered[QueueDF_MergedFiltered['Stage_Banded']=='Declined']['CASEID'].nunique()
            #-----------------------------Display result/ Render page--------------------------------------------------------------------------

            #Metric Card section
            #Render the metric card section
            col1, col2, col3 = st.columns(3)

            # Create a container for the metric boxes
            metric_container = st.container()

            # Define a CSS class for the metric container
            metric_container_css = """
            .metric-container {
                background-color: #ffe6e6;
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
                            <p style="font-size: 1.5rem; margin-bottom: 0;">Total Leads</p>
                            <p style="font-size: 2.0rem; margin-top: 0;">{}</p>
                        </div>
                    </div>
                    """.format(Total_Leads),
                    unsafe_allow_html=True,
                )

                col2.markdown(
                    """
                    <div class="metric-container">
                        <div>
                            <p style="font-size: 1.5rem; margin-bottom: 0;">Total Approved Cases</p>
                            <p style="font-size: 2.0rem; margin-top: 0;">{}</p>
                        </div>
                    </div>
                    """.format(Total_Approved),
                    unsafe_allow_html=True,
                )

                col3.markdown(
                    """
                    <div class="metric-container">
                        <div>
                            <p style="font-size: 1.5rem; margin-bottom: 0;">Total Declined Cases</p>
                            <p style="font-size: 2.0rem; margin-top: 0;">{}</p>
                        </div>
                    </div>
                    """.format(Total_Declined),
                    unsafe_allow_html=True,
                )
            

            # Define a CSS class for the metric container
            metric_container_css2 = """
            .metric-container2 {
                background-color: #ffffff;
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
            st.markdown(f"<style>{metric_container_css2}</style>", unsafe_allow_html=True)
            
            #Render the metric card section
            col1, col2, col3 = st.columns(3)

            # Use the metric_container to display the metrics
            with metric_container:
                col1.markdown(
                    """
                    <div class="metric-container2">
                        <div>
                            <p style="font-size: 1.5rem; margin-bottom: 0;">Total Full Submission</p>
                            <p style="font-size: 2.0rem; margin-top: 0;">{}</p>
                        </div>
                    </div>
                    """.format(Sum_of_Mandated),
                    unsafe_allow_html=True,
                )

                col2.markdown(
                    """
                    <div class="metric-container2">
                        <div>
                            <p style="font-size: 1.5rem; margin-bottom: 0;">Overall TAT to Closure</p>
                            <p style="font-size: 2.0rem; margin-top: 0;">{}</p>
                        </div>
                    </div>
                    """.format(Overall_TAT_to_Closure),
                    unsafe_allow_html=True,
                )
                
                col3.markdown(
                    """
                    <div class="metric-container2">
                        <div>
                            <p style="font-size: 1.5rem; margin-bottom: 0;">Total Amt Approved(Mil)</p>
                            <p style="font-size: 2.0rem; margin-top: 0;">{}</p>
                        </div>
                    </div>
                    """.format(Total_Amount_Approved),
                    unsafe_allow_html=True,
                )

            col1, col2, col3 = st.columns(3)
            with col1:
                fig_Mandated = go.Figure()
                colors = {'BAU': '#e60000', 'Lane 5': '#999999'}  # Specify the colors for each 'EH_FLAG'
                for flag in Monthly_Mandated_NewDF['EH_FLAG'].unique():
                    filtered_data_Mandated = Monthly_Mandated_NewDF[Monthly_Mandated_NewDF['EH_FLAG'] == flag]
                    
                    fig_Mandated.add_trace(go.Bar(
                        x=filtered_data_Mandated['Reporting_Month'],
                        y=filtered_data_Mandated['Mandated_New'],
                        text=filtered_data_Mandated['Mandated_New'],  # Use 'Mandated_New' for text labels
                        textposition='outside',
                        name=flag,
                        marker=dict(color=colors.get(flag, '#ff8080'))  # Set the color based on the 'EH_FLAG' value
                    ))

                fig_Mandated.update_layout(
                    title= dict(text = '# of Full Submission vs Reporting Month'),
                    xaxis=dict(
                        title='Reporting Month',
                        tickmode='array',
                        tickvals=Monthly_Mandated_NewDF['Reporting_Month'],
                        ticktext=Monthly_Mandated_NewDF['Reporting_Month']
                    ),
                    yaxis=dict(title='# of Full Submission'),
                    barmode='group',
                    height = 450,
                    width = 370,
                    legend=dict(yanchor='top', y=1.15, xanchor='left', x=-0.1, orientation ='h')
                    #showlegend=False
                )

                st.plotly_chart(fig_Mandated, use_container_width=True)

            with col2:
                #Create figure chart for TAT to approved
                fig_TAT = go.Figure()
                colors = {'BAU': '#e60000', 'Lane 5': '#999999'}  # Specify the colors for each 'EH_FLAG'
                for flag in TAT_Closed_PivotDF['EH_FLAG'].unique():
                    filtered_data2 = TAT_Closed_PivotDF[TAT_Closed_PivotDF['EH_FLAG'] == flag]
                    
                    fig_TAT.add_trace(go.Bar(
                        x=filtered_data2['Month_Created'],  
                        y=filtered_data2['TAT to Closure'],
                        text=filtered_data2['TAT to Closure'],  # Use string format for text labels
                        textposition='outside',
                        name=flag,
                        marker=dict(color=colors.get(flag, 'gray'))  # Set the color based on the 'EH_FLAG' value
                    ))

                fig_TAT.update_layout(
                    title='80th Percentile of TAT to Closure',
                    xaxis=dict(
                        title='Created Month',
                        tickmode='array',
                        tickvals=TAT_Closed_PivotDF['Month_Created'],
                        ticktext=TAT_Closed_PivotDF['Month_Created']
                    ),
                    yaxis=dict(title='TAT to Closure'
                    ),
                    barmode='group',
                    height=450,
                    width=370,
                    legend=dict(yanchor='top', y=1.15, xanchor='left', x=-0.1, orientation ='h'),   
                    #showlegend=False
                )
                st.plotly_chart(fig_TAT, use_container_width=True)

            with col3:
                #Create figure chart for Approval Amount by month
                fig_ApprovalAmt = go.Figure()
                colors = {'BAU': '#e60000', 'Lane 5': '#999999'}  # Specify the colors for each 'EH_FLAG'
                for flag in Approval_ListPivot['EH_FLAG'].unique():
                    filtered_data = Approval_ListPivot[Approval_ListPivot['EH_FLAG'] == flag]
                    
                    fig_ApprovalAmt.add_trace(go.Bar(
                        x=filtered_data['Month Approved'],
                        y=filtered_data['AMOUNT APPROVED'],
                        text=filtered_data['AMOUNT APPROVED MILLION'],  # Use 'AMOUNT APPROVED' for text labels
                        textposition='outside',
                        name=flag,
                        marker=dict(color=colors.get(flag, 'gray'))  # Set the color based on the 'EH_FLAG' value
                        
                    ))

                fig_ApprovalAmt.update_layout(
                    title='Approval Amount by Month',
                    xaxis=dict(
                        title='Approval Month',
                        tickmode='array',
                        tickvals=Approval_ListPivot['Month Approved'],
                        ticktext=Approval_ListPivot['Month Approved']
                    ),
                    yaxis=dict(title='Amount Approved'),
                    barmode='group',
                    height = 450,
                    width = 370,
                    legend=dict(yanchor='top', y=1.15, xanchor='left', x=-0.1, orientation ='h')
                    #showlegend=False
                )

                st.plotly_chart(fig_ApprovalAmt, use_container_width=True)

        except(KeyError):
            st.error("No approved case within timeframe selected")

            #set variables for key metric cards to 0
            Total_Approved = 0
            Total_Amount_Approved = 0
            Overall_TAT_to_Approved = 0

            #get the total number of leads (to use in metric card)
            Total_Leads = QueueDF_MergedFiltered['CASEID'].nunique()
            #get the total declined (to use in metric card)
            Total_Declined = QueueDF_MergedFiltered[QueueDF_MergedFiltered['Stage_Banded']=='Declined']['CASEID'].nunique()

            #Metric Card section
            #Render the metric card section
            col1, col2, col3 = st.columns(3)

            # Create a container for the metric boxes
            metric_container = st.container()

            # Define a CSS class for the metric container
            metric_container_css = """
            .metric-container {
                background-color: #ffe6e6;
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
                            <p style="font-size: 1.5rem; margin-bottom: 0;">Total Leads</p>
                            <p style="font-size: 2.0rem; margin-top: 0;">{}</p>
                        </div>
                    </div>
                    """.format(Total_Leads),
                    unsafe_allow_html=True,
                )

                col2.markdown(
                    """
                    <div class="metric-container">
                        <div>
                            <p style="font-size: 1.5rem; margin-bottom: 0;">Total Approved Cases</p>
                            <p style="font-size: 2.0rem; margin-top: 0;">{}</p>
                        </div>
                    </div>
                    """.format(Total_Approved),
                    unsafe_allow_html=True,
                )

                col3.markdown(
                    """
                    <div class="metric-container">
                        <div>
                            <p style="font-size: 1.5rem; margin-bottom: 0;">Total Declined Cases</p>
                            <p style="font-size: 2.0rem; margin-top: 0;">{}</p>
                        </div>
                    </div>
                    """.format(Total_Declined),
                    unsafe_allow_html=True,
                )
            

            # Define a CSS class for the metric container
            metric_container_css2 = """
            .metric-container2 {
                background-color: #ffffff;
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
            st.markdown(f"<style>{metric_container_css2}</style>", unsafe_allow_html=True)
            
            #Render the metric card section
            col1, col2, col3 = st.columns(3)

            # Use the metric_container to display the metrics
            with metric_container:
                col1.markdown(
                    """
                    <div class="metric-container2">
                        <div>
                            <p style="font-size: 1.5rem; margin-bottom: 0;">Total Full Submission</p>
                            <p style="font-size: 2.0rem; margin-top: 0;">{}</p>
                        </div>
                    </div>
                    """.format(Sum_of_Mandated),
                    unsafe_allow_html=True,
                )

                col2.markdown(
                    """
                    <div class="metric-container2">
                        <div>
                            <p style="font-size: 1.5rem; margin-bottom: 0;">Overall TAT to Approved</p>
                            <p style="font-size: 2.0rem; margin-top: 0;">{}</p>
                        </div>
                    </div>
                    """.format(Overall_TAT_to_Approved),
                    unsafe_allow_html=True,
                )

                col3.markdown(
                    """
                    <div class="metric-container2">
                        <div>
                            <p style="font-size: 1.5rem; margin-bottom: 0;">Total Amt Approved(Mil)</p>
                            <p style="font-size: 2.0rem; margin-top: 0;">{}</p>
                        </div>
                    </div>
                    """.format(Total_Amount_Approved),
                    unsafe_allow_html=True,
                )
        

        #Below is outside try-except wrap (error no approved cases)
        #Create pivot table of new leads based on region for each week
        New_Leads_PivotDF = pd.pivot_table(QueueDF_MergedFiltered, values = 'Leads_New', index= 'Region', columns = 'Reporting_Week', aggfunc=np.sum, fill_value=0, margins=True)
        New_Leads_PivotDF.drop('All', axis=1,inplace=True) #remove total column
        New_Leads_PivotDF.columns = pd.to_datetime(New_Leads_PivotDF.columns, format='%Y/%m').strftime('%Y-%m-%d')
        New_Leads_Pivot_Remove_Total = New_Leads_PivotDF.drop('All') #remove total row
        #Create table for New Leads Percentage by region
        column_totals = New_Leads_Pivot_Remove_Total.sum()
        New_Leads_PercentageDF = New_Leads_Pivot_Remove_Total.div(column_totals) * 100
        New_Leads_PercentageDF = New_Leads_PercentageDF.applymap(lambda x: '{:.0f}%'.format(x))

        #Create declined by stage for each week dataframe
        Declined_ByStage_DF = pd.pivot_table(QueueDF_MergedFiltered, values = ['Leads_Declined','Prospects_Declined','Mandated_Declined','Group Shariah_Declined','RMD_Declined','FT_Approval_Declined','CIC_Approval_Declined'], index= 'Reporting_Week', aggfunc=np.sum)
        # Specify the desired column order
        column_order = ['Leads_Declined','Prospects_Declined','Mandated_Declined','Group Shariah_Declined','RMD_Declined','CIC_Approval_Declined','FT_Approval_Declined']
        Declined_ByStage_DF = Declined_ByStage_DF.reindex(columns=column_order)
        Declined_ByStage_DF = Declined_ByStage_DF.reset_index(level=0)
        Declined_ByStage_DF = Declined_ByStage_DF.rename(columns={'index':'Reporting_Week'})
        Melted_DeclinedDF = pd.melt(Declined_ByStage_DF, id_vars=['Reporting_Week'])
        Melted_DeclinedDF.rename(columns={'variable':'Stage'}, inplace=True)
        Melted_Declined_FilteredDF = Melted_DeclinedDF[Melted_DeclinedDF['Stage'].isin(['Leads_Declined','Prospects_Declined','Mandated_Declined']) ]

        
        #get the total number of leads (to use in metric card)
        Total_Leads = QueueDF_MergedFiltered['CASEID'].nunique()
        #get the total declined (to use in metric card)
        Total_Declined = QueueDF_MergedFiltered[QueueDF_MergedFiltered['Stage_Banded']=='Declined']['CASEID'].nunique()

        #Render dataframe for new leads by region
        cutoff_Date = pd.to_datetime(Date_Range[1]).strftime('%Y-%m-%d')
        st.subheader("New Leads Based on Region as of " + cutoff_Date )

        tab1, tab2 = st.tabs(["Absolute Number", "Percentage"])
        with tab1:
            st.dataframe(New_Leads_PivotDF, use_container_width=True)

        with tab2:
            st.dataframe(New_Leads_PercentageDF, use_container_width=True)


        #Render chart for declined by stage for each week
        fig_DeclinedByStage = go.Figure()
        fig_DeclinedByStage.update_layout(
            xaxis=dict(title_text='Stages'),
            yaxis=dict(title_text='# of Declined Cases')
        )

        fig_DeclinedByStage.add_trace(go.Bar(x=[Melted_Declined_FilteredDF['Stage'],Melted_Declined_FilteredDF['Reporting_Week'].dt.strftime('%Y-%m-%d')],y=Melted_Declined_FilteredDF['value'], text=Melted_Declined_FilteredDF['value'],
                textposition='outside', marker = dict(color='#e60000'))
        )


        fig_DeclinedByStage.update_layout(
            width = 1200,
            height = 600,
        )
        st.subheader("Number of Declined Cases by Week as of " + cutoff_Date)
        st.plotly_chart(fig_DeclinedByStage, use_container_width=True)

    except IndexError:
            st.error("Please select both start and end dates")