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
from st_aggrid import GridOptionsBuilder, AgGrid, GridUpdateMode, DataReturnMode


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

# Define the HTML template with CSS to position the image and title
html_template = """
<div style="display: flex; align-items: center;">
    <img src="https://seeklogo.com/images/M/midf-logo-44DC64A7C7-seeklogo.com.png" alt="MIDF Logo" style="width: 200px; height: 72px; margin-right: 10px;">
    <h1>Task Balance Snapshot</h1>
</div>
"""
#testing for git
# Render the HTML template
st.markdown(html_template, unsafe_allow_html=True)

#------------------User input for filtering------------------------------------------------------------------------
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


    QueueDF_MergedFiltered = Data_Transformation(df1,df2, df3, selected_date,selected_date2)


    #---------------------------Data Analysis---------------------------------------------------------------------------

    #write function to create column that convert y,N,F flag to existing, New, and former customer flag
    def flag_convert(df):
        if(df['Customer_History'] == 'Y'):
            return "Existing to Bank"
        if(df['Customer_History'] == 'N'):
            return "New to Bank"
        if(df['Customer_History']=='F'):
            return "Former to Bank"
        
    #create status 2 column
    QueueDF_MergedFiltered['STATUS_2'] = (QueueDF_MergedFiltered['CASEID'] != QueueDF_MergedFiltered['CASEID'].shift(-1)).astype(int) #check case id current is not equal to previous row
    QueueDF_MergedFiltered['STATUS_2'] = QueueDF_MergedFiltered.apply(lambda row:'Actv' if row['STATUS_2']==1 else 'del', axis=1)
    Queue_ActiveDF = QueueDF_MergedFiltered[QueueDF_MergedFiltered['STATUS_2']=='Actv']

    Queue_ActiveDF['Customer History'] = Queue_ActiveDF.apply(flag_convert, axis=1)

    #data for pie chart
    Cust_Hist_Pie = Queue_ActiveDF['Customer History'].value_counts()

    Total_Leads = len(Queue_ActiveDF['CASEID'])
    Total_Manual_Application = len(Queue_ActiveDF[Queue_ActiveDF['C_CHANNEL'] == 'Manual'])
    Total_Jaccess_Application = len(Queue_ActiveDF[Queue_ActiveDF['C_CHANNEL'] == 'JAccess'])

    #Create snapshot dataframe
    SnapshotDF = pd.pivot_table(Queue_ActiveDF,values = ['CASEID'] ,index=['C_CHANNEL','Amt_Grouping'], columns = 'Stage_Banded' ,aggfunc = len, margins = True,margins_name = 'Grand Total')
    SnapshotDF = SnapshotDF.fillna(0)

    #specify desired column order for snapshot dataframe
    columns_order_snapshot = ['Declined','Leads','Prospects','Mandated','Group Shariah','RMD','CIC Approval','Fast Track Approval','Acceptance','Grand Total']
    SnapshotDF.columns = SnapshotDF.columns.droplevel(0)
    SnapshotDF = SnapshotDF.reindex(columns = columns_order_snapshot).fillna(0)

    Snapshot_PercentageDF = SnapshotDF.drop('Grand Total',axis=1)
    Snapshot_PercentageDF.iloc[:,:] = Snapshot_PercentageDF.iloc[:,:].apply(lambda x: x.div(x.sum()).mul(100),axis=1)
    Snapshot_PercentageDF['Grand Total'] = Snapshot_PercentageDF.sum(axis=1)
    Snapshot_PercentageDF = Snapshot_PercentageDF.round().astype(int).astype(str)+'%'

    #Create Customer History Snapshot 
    Customer_SnapshotDF = pd.pivot_table(Queue_ActiveDF,values = ['CASEID'] ,index=['C_CHANNEL','Amt_Grouping'], columns = 'Customer History' , aggfunc=len, margins = True, margins_name = 'Grand Total')
    Customer_SnapshotDF.columns = Customer_SnapshotDF.columns.droplevel(0)  #Drop multilevel column 
    Customer_SnapshotDF = Customer_SnapshotDF.fillna(0)

    Customer_PercentageDF = Customer_SnapshotDF.drop('Grand Total',axis=1)
    Customer_PercentageDF.iloc[:,:] = Customer_PercentageDF.iloc[:,:].apply(lambda x: x.div(x.sum()).mul(100),axis=1)
    Customer_PercentageDF['Grand Total'] = Customer_PercentageDF.sum(axis=1)
    # Round the values to zero decimal places and add '%' sign
    Customer_PercentageDF = Customer_PercentageDF.round(0).astype(int).astype(str) + '%'

    #-----------------------------create pie chart-------------------------------------------------------------

    # Create the figure and axis
    labels = ['New to Bank','Existing to Bank','Former to Bank']
    # Calculate the total amount
    total = sum(Cust_Hist_Pie)
    fig, ax = plt.subplots()

    # Create the donut chart
    wedges, texts, autotexts = ax.pie(Cust_Hist_Pie, autopct='%1.1f%%', startangle=90, wedgeprops=dict(width=0.4))

    # Add a circle in the middle to create the donut effect
    center_circle = plt.Circle((0, 0), 0.5, color='white')
    ax.add_artist(center_circle)

    # Set the aspect ratio to make the chart circular
    ax.set(aspect="equal", title='Customer History')

    # Create the legend
    ax.legend(wedges, labels, loc='upper left')

    # Add the total amount in the middle of the donut chart
    ax.text(0, 0, f'Total: {total}', fontsize=12, weight='bold', va='center', ha='center')


    #---------------------------Render dashboard-----------------------------------------------------------------

    st.write("")

    tab1, tab2 = st.tabs(["Task Outstanding by Stage", "Task Outstanding by Customer History"])

    with tab1:
        # Render the layout with two columns
        col1, col2 = st.columns([1, 3])

        # Create a container for the metric cards
        metric_container = col1.container()

        # Define a CSS class for the metric container
        metric_container_css = """
        .metric-container {
            background-color: #FFFFFF;
            border: 1px solid #CCCCCC;
            padding: 1rem;
            border-radius: 5px;
            box-shadow: 0 0.15rem 1.75rem 0 rgba(58, 59, 69, 0.15);
            border-left: 4px solid red; /* Add a left border color here */
            width: 100%; /* Adjust the width here */
            height: 181px; /* Adjust the height here */
            margin-bottom: 1rem; /* Add some margin between the metric containers */
        }
        """

        # Add the CSS styling using st.markdown()
        st.markdown(f"<style>{metric_container_css}</style>", unsafe_allow_html=True)

        # Use the metric_container to display the metric cards
        with metric_container:
            st.write("")
            st.write("")
            st.write("")
            st.write("")
            st.markdown(
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

            st.markdown(
                """
                <div class="metric-container">
                    <div>
                        <p style="font-size: 1.5rem; margin-bottom: 0;"># of Application through Jaccess</p>
                        <p style="font-size: 2.0rem; margin-top: 0;">{}</p>
                    </div>
                </div>
                """.format(Total_Jaccess_Application),
                unsafe_allow_html=True,
            )

            st.markdown(
                """
                <div class="metric-container">
                    <div>
                        <p style="font-size: 1.5rem; margin-bottom: 0;"># of Application through DFD Sales</p>
                        <p style="font-size: 2.0rem; margin-top: 0;">{}</p>
                    </div>
                </div>
                """.format(Total_Manual_Application),
                unsafe_allow_html=True,
            )

        cutoff_Date = pd.to_datetime(Date_Range[1]).strftime('%Y-%m-%d')
        # Display the data frames in the second column
        col2.subheader("Task Outstanding as of " + cutoff_Date )
        col2.dataframe(SnapshotDF, use_container_width=True)
        col2.subheader("Task Outstanding (Percentage) as of " + cutoff_Date)
        col2.dataframe(Snapshot_PercentageDF, use_container_width=True)

    with tab2:
        # Render the layout with two columns
        col1, col2, col3 = st.columns([1.5, 3 , 1.5])

        # Create a container for the metric cards
        metric_container = col1.container()

        # Define a CSS class for the metric container
        metric_container_css = """
        .metric-container {
            background-color: #FFFFFF;
            border: 1px solid #CCCCCC;
            padding: 1rem;
            border-radius: 5px;
            box-shadow: 0 0.15rem 1.75rem 0 rgba(58, 59, 69, 0.15);
            border-left: 4px solid red; /* Add a left border color here */
            width: 100%; /* Adjust the width here */
            height: 181px; /* Adjust the height here */
            margin-bottom: 1rem; /* Add some margin between the metric containers */
        }
        """

        # Add the CSS styling using st.markdown()
        st.markdown(f"<style>{metric_container_css}</style>", unsafe_allow_html=True)

        # Use the metric_container to display the metric cards
        with metric_container:
            st.write("")
            st.write("")
            st.write("")
            st.write("")
            st.markdown(
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

            st.markdown(
                """
                <div class="metric-container">
                    <div>
                        <p style="font-size: 1.5rem; margin-bottom: 0;"># of Application through Jaccess</p>
                        <p style="font-size: 2.0rem; margin-top: 0;">{}</p>
                    </div>
                </div>
                """.format(Total_Jaccess_Application),
                unsafe_allow_html=True,
            )

            st.markdown(
                """
                <div class="metric-container">
                    <div>
                        <p style="font-size: 1.5rem; margin-bottom: 0;"># of Application through DFD Sales</p>
                        <p style="font-size: 2.0rem; margin-top: 0;">{}</p>
                    </div>
                </div>
                """.format(Total_Manual_Application),
                unsafe_allow_html=True,
            )

        #cutoff_Date = pd.to_datetime(Date_Range[1]).strftime('%Y-%m-%d')
        # Display the data frames in the second column
        col2.subheader("Customer History as of " + cutoff_Date)
        col2.dataframe(Customer_SnapshotDF, use_container_width=True)
        col2.subheader("Customer History by Percentage")
        col2.dataframe(Customer_PercentageDF, use_container_width=True)
        col3.pyplot(fig, use_container_width=True)

except IndexError:
    st.error("Please select both start and end dates")
   
    


