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
import yaml

@st.cache_data(show_spinner="Fetching Data from Database...",ttl=3600, max_entries=500)
def Fetch_Data():

    #Reading secrets from YAML
    with open("secrets.yaml", 'r') as f:
        config = yaml.load(f, Loader=yaml.FullLoader)

    # saving each credential into a variable
    HOST_NAME = config['host']
    DATABASE = config['database']
    PASSWORD = config['credentials']['password']
    USER = config['credentials']['username']
    PORT = config['port']

    # Establish the connection
    try:
        connection = mysql.connector.connect(
            host=HOST_NAME,
            port=PORT,
            user=USER,
            password=PASSWORD,
            database=DATABASE
        )

        # Create a cursor
        cursor = connection.cursor()

        # Execute the first query (Task Summary Query)
        query1 = '''
        SELECT 
            QU_NAME AS 'TASK', 
            stg_name AS 'STAGE', 
            Q_USER AS 'USER', 
            q_timestamp AS 'START_DT', 
            Q_DEL_TIMESTAMP AS 'END_DT', 
            Q_STATUS AS 'STATUS', 
            q_casekey AS 'CASEID', 
            CIF_NAME AS 'CUSTOMER', 
            ORG_NAME AS 'REGION (Originator)', 
            CASE WHEN C_CHANNEL = 'JACCESS' THEN 'JAccess' ELSE 'Manual' END AS C_CHANNEL, 
            GROUP_CONCAT(PROD_NAME || ' RM ' || FORMAT(JF_AMT, 0) ORDER BY jf_id SEPARATOR ', ') AS 'FACILITY REQUEST', 
            GROUP_CONCAT(PROG_NAME ORDER BY jf_id SEPARATOR ', ') AS 'SCHEME', 
            SUM(JF_AMT) AS 'TOTAL FACILITY AMT',
            CASE 
			    WHEN C_EH_FLAG = 'D' THEN 'N'
                WHEN C_EH_FLAG = 'C' THEN 'N'
			    WHEN C_EH_FLAG = '' THEN 'N'
			    ELSE NVL(C_EH_FLAG,'N')
			END as 'EH_FLAG' 
        FROM 
            jqueue 
            JOIN kbqtype k ON q_type = QU_CODE 
            JOIN kbstage k2 ON QU_STAGECAT = stg_stagecat AND QU_TYPE IN ('DATA', 'LIST') 
            JOIN jcif j ON q_casekey = CIF_C_KEY 
            JOIN jrole j2 ON cif_id = jr_cifid AND jr_role = 'MA' 
            JOIN jcase ON c_key = q_casekey 
            LEFT JOIN JORG ON C_ASSIGN_ORG = ORG_CODE 
            LEFT JOIN jfacility ON C_KEY = jf_c_key AND JF_DELETED = 'N' 
            LEFT JOIN kbproduct ON jf_PRODCODE = PROD_CODE 
            LEFT JOIN kbprogram ON JF_SCHEME_CODE = PROG_CODE 
        WHERE 
            C_CREATEDT >= '2022-12-13 00:00:00.000' 
        GROUP BY 
            q_casekey, 
            q_id 
        ORDER BY 
            q_casekey, 
            q_id
        '''

        cursor.execute(query1)

        # Fetch the data from the first query
        data1 = cursor.fetchall()

        # Convert the data to a Pandas DataFrame
        columns1 = [column[0] for column in cursor.description]
        df1 = pd.DataFrame(data1, columns=columns1)   # DF1 for queue table

        # Execute the second query (Amount Applied Query)
        query2 = '''
        SELECT 
            JAI_C_KEY AS 'CASEID', 
            SUM(JAI_AMOUNT) AS 'Amount Applied'
        FROM 
            jaccess_info ji 
        GROUP BY 
            JAI_C_KEY
        ORDER BY 
            JAI_C_KEY
        '''

        cursor.execute(query2)

        # Fetch the data from the second query
        data2 = cursor.fetchall()

        # Convert the data to a Pandas DataFrame
        columns2 = [column[0] for column in cursor.description]
        df2 = pd.DataFrame(data2, columns=columns2)   # DF2 for amount applied lookup table

        # Execute the third query for Customer Information
        query3 = '''
        SELECT 
            CIF_ID,
            CIF_PRIMARYID_SSM as 'SSM_Number',
            CIF_NAME as 'Company_Name',
            CIF_NATIONALITY as 'Nationality',
            CIF_ISETB as 'Customer_History',
            CIF_C_KEY as 'CASEID',
            JR_ROLE
        FROM jcif
        JOIN jrole j  on
        JR_CIFID = CIF_ID
        WHERE JR_ROLE = 'MA'
        '''

        cursor.execute(query3)

        # Fetch the data from the third query
        data3 = cursor.fetchall()

        # Convert the data to a Pandas DataFrame
        columns3 = [column[0] for column in cursor.description]
        df3 = pd.DataFrame(data3, columns=columns3)   # DF3 for customer information/history

         # Execute the fourth query (List of Approved Cases)
        query4 = '''
        select
        C_KEY as CASEID,
        A.ORG_NAME,
        case
            when C_APPROVEBY = 'CICO' then 'CIC ONLINE'
            when C_APPROVEBY = 'CIC' then 'CIC OFFLINE'
            else 'FAST TRACK APPROVAL'
        end as 'APPROVAL TYPE' ,
        C_APPROVEDT as 'APPROVAL DATE',
        C_AMT_APPROVED as 'AMOUNT APPROVED',
        CASE 
            WHEN C_EH_FLAG = 'D' THEN 'N'
            WHEN C_EH_FLAG = 'C' THEN 'N'
            WHEN C_EH_FLAG = '' THEN 'N'
            ELSE NVL(C_EH_FLAG,'N')
        END as 'EH_FLAG'
        from
            jcase j
        left join JORG A on
            J.C_ASSIGN_ORG = A.ORG_CODE
        left join LENS_USERS B on
            J.C_APPROVEBY = B.USERID
        left join JORG C on
            B.ORG = C.ORG_ID
        where
            TRIM(C_AMT_APPROVED) is not null
            and C_CREATEDT >= '2022-12-13 00:00:00.000'
        order by
            C_APPROVEBY ,
            C_KEY;
        '''

        cursor.execute(query4)

        # Fetch the data from the first query
        data4 = cursor.fetchall()

        # Convert the data to a Pandas DataFrame
        columns4 = [column[0] for column in cursor.description]
        df4 = pd.DataFrame(data4, columns=columns4)   # DF4 is for list of approved cases

        # Execute the fifth query for TAT to approved dataframe
        query5 = '''
        select
        C_KEY as 'CASEID',
        C_CREATEDT as 'Created_Date',
        C_APPROVEDT as 'Approved_Date',
        C_CANCELDT as 'Cancelled_Date' ,
        C_CLOSEDT as 'Closed_Date',
        CASE 
            WHEN C_EH_FLAG = 'D' THEN 'N'
            WHEN C_EH_FLAG = 'C' THEN 'N'
            WHEN C_EH_FLAG = '' THEN 'N'
            ELSE NVL(C_EH_FLAG,'N')
        END as 'EH_FLAG'
        from
            jcase
        '''

        cursor.execute(query5)

        # Fetch the data from the second query
        data5 = cursor.fetchall()

        # Convert the data to a Pandas DataFrame
        columns5 = [column[0] for column in cursor.description]
        df5 = pd.DataFrame(data5, columns=columns5)   # DF5 for TAT calculation
        
         # Execute the sixth query (List of Approved Cases for DFD CIC)
        query6 = '''
        select
            C_KEY as CASEID,
            Y.CIF_NAME as 'CUSTOMER NAME',
            A.ORG_NAME as 'ORG - REGION',
            V.ST_NAME as 'STATE',
            Z.NAME as 'CP/SALES AM',
            case
                when C_APPROVEBY = 'CICO' then 'CIC ONLINE'
                when C_APPROVEBY = 'CIC' then 'CIC OFFLINE'
                else 'FAST TRACK APPROVAL'
            end as 'APPROVAL TYPE',
            C_APPROVEDT as 'APPROVAL DATE',
            JF_AMT as 'AMOUNT APPROVED',
            C_APPROVEBY 'APPROVED BY',
            PROG_NAME || ' - ' || PROG_DESC SCHEME,
            C_CREATEDT 'APPLICATION DATE'
        from
            jcase j
        left join jcif Y on
            j.C_KEY = y.CIF_C_KEY
        left join jrole X on
            Y.CIF_ID = X.JR_CIFID
        left join JORG A on
            J.C_ASSIGN_ORG = A.ORG_CODE
        left join LENS_USERS B on
            J.C_APPROVEBY = B.USERID
        left join lens_users Z on
            j.C_ASSIGN_USER = Z.USERID
        left join JORG C on
            B.ORG = C.ORG_ID
        left join jcontact_address W on
            Y.CIF_ID = W.CTA_CIFID
        left join kbstate V on
            W.CTA_STATE = V.ST_CODE
        left join jfacility on
            J.C_KEY = JF_C_KEY
        left join kbprogram on
            JF_SCHEME_CODE = PROG_CODE
        where
            TRIM(C_AMT_APPROVED) is not null
            and X.JR_ROLE = 'MA'
            and W.CTA_TYPE = 'B'
            and JF_DELETED = 'N'
        order by
            C_APPROVEBY ,
            C_KEY;
        '''

        cursor.execute(query6)

        # Fetch the data from the second query
        data6 = cursor.fetchall()

        # Convert the data to a Pandas DataFrame
        columns6 = [column[0] for column in cursor.description]
        df6 = pd.DataFrame(data6, columns=columns6)   # DF2 for amount applied lookup table
        
         # Execute the seventh query for CCRIS data for download section
        query7 = '''
        select
            JF_CCRIS_APPKEY "APP KEY",
            CIF_ENTITY_KEY "ENTITY KEY",
            C_APP_NO "APP NO.",
            CCR_APP_REF "APP REF NO",
            CCRD_NAME "APPLICANT NAME",
            CCRD_BASICGROUP "ENTITY TYPE",
            CCRD_ID1 "CUST ID NO 1",
            CCRD_ID2 "CUST ID NO 2",
            to_char(CCRD_DOB,
            'dd/mm/yyyy') "CUST DOR",
            CCRD_NATIONALITY "CUST NATIONALITY",
            CCR_AMTAPPLIED "AMOUNT APPLIED",
            to_char(CCR_SENT_DATE,
            'dd/mm/yyyy') "Application Date",
            CCRD_REGISTEREDSABAHSARAWAK "REGISTERED SBH SWK",
            CIF_BNM_ASSIGNEDNO "BNM ASSIGNED NO",
            CCR_LOCATION_POSTCODE "APP LOC UTILISATION",
            SUBSTR(CCR_FACILITYTYPE, 0, 5) "FACILITY TYPE",
            SUBSTR(CCR_FINCONCEPT, 0, 2) "FINANCING CONCEPT",
            CCR_BNM_SPEC_FUND "SPEC FUND SCHEME",
            SUBSTR(CCR_PURPOSECD, 0, 4) "PURPOSE LOAN",
            CCR_BNM_PRIORITYSEC "PRIORITY SEC",
            '' as 'APP SYNDICATED',
            CCR_ASSETPUR "VALUE ASSET PURCHASE",
            CCRD_INDUSTRY_SECTOR "INDUS SEC",
            CCRD_CORP_STATUS "CORP STATUS",
            '' as 'SPEC_DFI',
            '' as 'MEM_BANK_RAKYAT',
            CCRD_RESIDENCY_STATUS "RESIDENCY STATUS",
            CCRD_POSTCODE "FLD POSTCODE",
            CCRD_STATE "FLD STATE",
            CCRD_COUNTRY "FLD COUNTRY",
            case
                when CIF_STARTUP = 'Y'
                and CIF_SME = 'Y' then '11'
                when CIF_STARTUP = 'Y'
                and CIF_SME = 'N' then '12'
                when CIF_STARTUP = 'N'
                and CIF_SME = 'Y' then '21'
                when CIF_STARTUP = 'N'
                and CIF_SME = 'N' then '22'
                when CIF_STARTUP = 'NA' then '99'
            end "FLD SME FINANCING CATEGORY",
            '' as 'DEBT SERVICE RATIO',
            CCR_APPSTATUS "APP STATUS",
            CCR_AMTAPPROVED "AMOUNT APPROVED",
            case
                when trim(C_APPROVEDT)<> '0000-00-00 00:00:00.000000' then to_char(C_APPROVEDT,
                'dd/mm/yyyy')
                else null
            end "APPROVED DATE",
            C_REJECT_REASON "REASON REJECTION",
            case
                when ccr_appstatus = 'W' then to_char(CCR_RESPONSE_DATE,
                'dd/mm/yyyy')
                else null
            end "WITHDRAWN DATE",
            case
                when ccr_appstatus = 'R' then to_char(CCR_RESPONSE_DATE,
                'dd/mm/yyyy')
                else null
            end as 'REJECT DATE BY CIC/REG HEAD',
            '' as 'LO ACCEPTED DATE',
            case
                when ccr_appstatus = 'X' then to_char(CCR_RESPONSE_DATE,
                'dd/mm/yyyy')
                else null
            end as "CANCEL DATE",
            case
                when trim(C_MIDF_SALESOWNER) is not null then C_MIDF_SALESOWNER
                else C_MIDF_CPOWNER
            end "APPLICATION CREATED BY",
            CCR_RESPONSE_DATE
        from
            JCCRIS
        join JCIF on
            JCCRIS.CCR_CIFID = JCIF.CIF_ID
        join JCCRIS_DET on
            JCCRIS.CCR_ID = CCRD_CCRID
        join JCASE on
            JCIF.CIF_C_KEY = JCASE.C_KEY
        join JFACILITY on
            JCCRIS.CCR_APPKEY = JFACILITY.JF_ID
        where
            CCRD_ROLE = 'MA'
            and (CCR_TYPE = 'NEWAPP'
                or CCR_TYPE = 'UPDAPP')
            and CCR_STATUS = 'DONE'
        order by
            JF_CCRIS_APPKEY,
            CCR_ID
        '''

        cursor.execute(query7)

        # Fetch the data from the second query
        data7 = cursor.fetchall()

        # Convert the data to a Pandas DataFrame
        columns7 = [column[0] for column in cursor.description]
        df7 = pd.DataFrame(data7, columns=columns7)   # DF5 for TAT calculation

        # Close the cursor and connection
        cursor.close()
        connection.close()

    except mysql.connector.Error as error:
        # Handle any errors that occur during the connection or query execution
        print(f'Error connecting to MySQL: {error}')

    return {
        'df1': df1,
        'df2': df2,
        'df3': df3,
        'df4': df4,
        'df5': df5,
        'df6': df6,
        'df7': df7
        
    }

@st.cache_data(show_spinner="Fetching Downloadable Data from Database...",ttl=3600, max_entries=500)
def Fetch_Download_Data():

    #Reading secrets from YAML
    with open("secrets.yaml", 'r') as f:
        config = yaml.load(f, Loader=yaml.FullLoader)

    # saving each credential into a variable
    HOST_NAME = config['host']
    DATABASE = config['database']
    PASSWORD = config['credentials']['password']
    USER = config['credentials']['username']
    PORT = config['port']

    # Establish the connection
    try:
        connection = mysql.connector.connect(
            host=HOST_NAME,
            port=PORT,
            user=USER,
            password=PASSWORD,
            database=DATABASE
        )

        # Create a cursor
        cursor = connection.cursor()

        # Execute the first query (Cancel/Reject reason)
        query1 = '''
        with Jaccess_Contact as (
        select
                    C_KEY as 'CASEID',
                    CIF_NAME as 'Customer_Name',
                    PROD_NAME  as 'FACILITY_REQUEST',
                    PROG_NAME  as 'SCHEME',
                    JF_AMT as 'FACILITY_AMOUNT',
                    C_CREATEDT as 'Created_Date',
                    case 
                        when C_CANCELDT is null then REJ_TIMESTAMP
                        else C_CANCELDT
                    end as 'Cancellation_Reject_Date',
                    C_STATUS as 'STATUS',
                    case 
                        when k2.KL_DESC is null then k1.KL_DESC
                        else K2.KL_DESC
                    end as 'Cancellation_Reject_reason',
                    trim(SUBSTRING_INDEX(rmk_desc, 'Remarks:',-1)) as 'Cancellation_Remarks',
                    ORG_NAME as 'Region',
                    case 
                        when C_Channel = 'JACCESS' then 'Jaccess'
                        else 'Manual'
                    end as 'Channel',
                    STG_CCC_NAME as 'Contact_Person',
                    STG_CCC_PHONE as 'Contact_Person_HP_Number',
                    STG_CCC_EMAIL as 'Contact_Person_Email',
                    if(C_MIDF_SALESOWNER is null,
                    C_MIDF_CPOWNER,
                    C_MIDF_SALESOWNER) as 'OIC',
                    case
                        when C_EH_FLAG = 'Y' then 'Lane 5'
                        else 'BAU'
                    end as 'EH_FLAG',
                    CIF_ID
                from
                    jcase j
                left join jorg on
                    C_ASSIGN_ORG = ORG_CODE
                left join jcif on
                    C_KEY = CIF_C_KEY
                left join jreject_case jc on
                    C_KEY = REJ_C_KEY
                join jrole on
                    CIF_ID = JR_CIFID
                    and JR_ROLE = 'MA'
                left join jfacility on
                    C_KEY = jf_c_key
                    and JF_DELETED = 'N'
                left join kbproduct on
                    jf_PRODCODE = PROD_CODE
                left join kbprogram on
                    JF_SCHEME_CODE = PROG_CODE
                left join kblookup k1 on
                    C_REJECT_REASON = k1.KL_CODE
                    and k1.kl_key = 'REJECT_REASON'
                left join kblookup k2 on
                    C_CANCELCD = K2.KL_CODE
                    and K2.kl_key = 'CANCEL_CODE'
                left join JREMARK
                on
                    C_KEY = RMK_CASEID
                    and RMK_DECISION = 'CANCEL'
                left join stg_case_creation on
                    STG_JACCESS_CASEID = C_JACCESS_CASEID
                left join stg_cc_contact on
                    STG_ID = STG_CCC_CCID
                where
                    C_CREATEDT > '2022-12-13 00:00:00'
                    and C_STATUS in ('CANCEL', 'REJECT')

                order by
                    C_KEY),
                    
        Manual_Contact as (
        select
            CIF_C_KEY,
            CPSN_CIFID ,
            CPSN_NAME ,
            CPSN_MOBILENO ,
            CPSN_EMAIL,
            row_number() over (partition by CIF_C_KEY
        order by
            CPSN_CIFID desc) as rn
        from
            vw_contact_person vcp
        join jcif on
            CIF_ID = CPSN_CIFID
        where
            CPSN_ACTIVE = 'Y'
        order by
            CIF_C_KEY
        ),

        table_JAI as (
        select
            JAI_C_KEY ,
            sum(JAI_AMOUNT) as 'TOTAL_AMOUNT_APPLIED'
        from
            jaccess_info ji
        group by
            JAI_C_KEY) 

                    
                select
            CASEID,
            Customer_Name,
            Facility_Request,
            Scheme,
            Facility_Amount,
            Total_Amount_Applied,
            Created_Date,
            Cancellation_Reject_Date,
            Status,
            Cancellation_Reject_reason,
            Cancellation_Remarks,
            case
                when Contact_Person is null then CPSN_NAME
                else Contact_Person
            end as 'Contact_Person',
            case
                when Contact_Person_HP_Number is null then CPSN_MOBILENO
                else Contact_Person_HP_Number
            end as 'Contact_Person_HP_No',
            case
                when Contact_Person_Email is null then CPSN_EMAIL
                else Contact_Person_Email
            end as 'Contact_Person_Email',
            Region,
            Channel,
            EH_FLAG,
            OIC
        from
            Jaccess_Contact
        left join Manual_Contact on
            CASEID = CIF_C_KEY
            and rn = 1
        left join table_JAI on
            CASEID = JAI_C_KEY
        where
            Created_Date > '2022-12-13 00:00:00'
        order by CASEID

        '''

        cursor.execute(query1)

        # Fetch the data from the first query
        data1 = cursor.fetchall()

        # Convert the data to a Pandas DataFrame
        columns1 = [column[0] for column in cursor.description]
        df1 = pd.DataFrame(data1, columns=columns1)   # DF1 for queue table 

        # Execute the 2nd query , tasks pending at leads stage
        query2 = '''
        

        with table_A as (
        select
            q_casekey 'CASEID',
            CIF_NAME 'CUSTOMER',
            if(C_MIDF_SALESOWNER is null,
                    C_MIDF_CPOWNER,
                    C_MIDF_SALESOWNER) as 'OIC',
                    ORG_NAME as 'REGION',
                    PROG_NAME as 'SCHEME',
                    Prod_Name as 'PRODUCT',
            JF_AMT as 'FINANCING_AMOUNT',
            Q_USER as 'CURRENT_OFFICER',
            QU_NAME as 'TASK' ,
            stg_name as 'STAGE',
            q_timestamp as 'START_DT',
            Q_STATUS 'STATUS',
            C_Createdt
        from
            jqueue
        join kbqtype k on
            q_type = QU_CODE
        join kbstage k2 on
            QU_STAGECAT = stg_stagecat
            and QU_TYPE in ('DATA', 'LIST')
        join jcif j on
            q_casekey = CIF_C_KEY
        join jrole j2 on
            cif_id = jr_cifid
            and jr_role = 'MA'
        join jcase on
            c_key = q_casekey
        left join JORG on
            C_ASSIGN_ORG = ORG_CODE
        left join jfacility on
            C_KEY = jf_c_key
            and JF_DELETED = 'N'
        left join kbproduct on
            jf_PRODCODE = PROD_CODE
        left join kbprogram on
            JF_SCHEME_CODE = PROG_CODE
        where
            C_CREATEDT >= '2022-12-13 00:00:00.000'
            and q_status = 'actv'
            and stg_name in ('Customer Prospecting')
        order by
            q_casekey,
            q_id),
                    
                table_B as (
        select
            JAI_C_KEY ,
            sum(JAI_AMOUNT) as 'TOTAL_AMOUNT_APPLIED'
        from
            jaccess_info ji
        group by
            JAI_C_KEY) 
                    
                    
        select
            CASEID,
            CUSTOMER,
            OIC,
                REGION,
                    SCHEME,
                    PRODUCT,
            FINANCING_AMOUNT,
            TOTAL_AMOUNT_APPLIED,
            CURRENT_OFFICER,
            TASK,
            STAGE,
            C_Createdt as 'APPLICATION_DATE',
            START_DT as 'TASK_START_DATE'
        from
            table_A
        left join table_B on
            CASEID = JAI_C_KEY
        order by
            CASEID

        '''

        cursor.execute(query2)

        # Fetch the data from the first query
        data2 = cursor.fetchall()

        # Convert the data to a Pandas DataFrame
        columns2 = [column[0] for column in cursor.description]
        df2 = pd.DataFrame(data2, columns=columns2)   # DF2 for tasks pending at leads

        # Execute the 3rd query , tasks pending at prospect
        query3 = '''
        

        with table_A as (
        select
            q_casekey 'CASEID',
            CIF_NAME 'CUSTOMER',
            if(C_MIDF_SALESOWNER is null,
                    C_MIDF_CPOWNER,
                    C_MIDF_SALESOWNER) as 'OIC',
                    ORG_NAME as 'REGION',
                    PROG_NAME as 'SCHEME',
                    Prod_Name as 'PRODUCT',
            JF_AMT as 'FINANCING_AMOUNT',
            Q_USER as 'CURRENT_OFFICER',
            QU_NAME as 'TASK' ,
            stg_name as 'STAGE',
            q_timestamp as 'START_DT',
            Q_STATUS 'STATUS',
            C_Createdt
        from
            jqueue
        join kbqtype k on
            q_type = QU_CODE
        join kbstage k2 on
            QU_STAGECAT = stg_stagecat
            and QU_TYPE in ('DATA', 'LIST')
        join jcif j on
            q_casekey = CIF_C_KEY
        join jrole j2 on
            cif_id = jr_cifid
            and jr_role = 'MA'
        join jcase on
            c_key = q_casekey
        left join JORG on
            C_ASSIGN_ORG = ORG_CODE
        left join jfacility on
            C_KEY = jf_c_key
            and JF_DELETED = 'N'
        left join kbproduct on
            jf_PRODCODE = PROD_CODE
        left join kbprogram on
            JF_SCHEME_CODE = PROG_CODE
        where
            C_CREATEDT >= '2022-12-13 00:00:00.000'
            and q_status = 'actv'
            and stg_name in ('Pre Screening', 'CCRIS DATA ENTRY SUBMISSION', 'Conditional Approved Acceptance')
        order by
            q_casekey,
            q_id),
                    
                table_B as (
        select
            JAI_C_KEY ,
            sum(JAI_AMOUNT) as 'TOTAL_AMOUNT_APPLIED'
        from
            jaccess_info ji
        group by
            JAI_C_KEY) 
                    
                    
        select
            CASEID,
            CUSTOMER,
            OIC,
                REGION,
                    SCHEME,
                    PRODUCT,
            FINANCING_AMOUNT,
            TOTAL_AMOUNT_APPLIED,
            CURRENT_OFFICER,
            TASK,
            STAGE,
            C_Createdt as 'APPLICATION_DATE',
            START_DT as 'TASK_START_DATE'
    
        from
            table_A
        left join table_B on
            CASEID = JAI_C_KEY
        order by
            CASEID

        '''

        cursor.execute(query3)

        # Fetch the data from the first query
        data3 = cursor.fetchall()

        # Convert the data to a Pandas DataFrame
        columns3 = [column[0] for column in cursor.description]
        df3 = pd.DataFrame(data3, columns=columns3)   # DF3 for tasks pending at prospects

        # Execute the 4th query , task pending at mandated
        query4 = '''
        
        with table_A as (
        select
            q_casekey 'CASEID',
            CIF_NAME 'CUSTOMER',
            if(C_MIDF_SALESOWNER is null,
                    C_MIDF_CPOWNER,
                    C_MIDF_SALESOWNER) as 'OIC',
                    ORG_NAME as 'REGION',
                    PROG_NAME as 'SCHEME',
                    Prod_Name as 'PRODUCT',
            JF_AMT as 'FINANCING_AMOUNT',
            Q_USER as 'CURRENT_OFFICER',
            QU_NAME as 'TASK' ,
            stg_name as 'STAGE',
            q_timestamp as 'START_DT',
            Q_STATUS 'STATUS',
            C_Createdt
        from
            jqueue
        join kbqtype k on
            q_type = QU_CODE
        join kbstage k2 on
            QU_STAGECAT = stg_stagecat
            and QU_TYPE in ('DATA', 'LIST')
        join jcif j on
            q_casekey = CIF_C_KEY
        join jrole j2 on
            cif_id = jr_cifid
            and jr_role = 'MA'
        join jcase on
            c_key = q_casekey
        left join JORG on
            C_ASSIGN_ORG = ORG_CODE
        left join jfacility on
            C_KEY = jf_c_key
            and JF_DELETED = 'N'
        left join kbproduct on
            jf_PRODCODE = PROD_CODE
        left join kbprogram on
            JF_SCHEME_CODE = PROG_CODE
        where
            C_CREATEDT >= '2022-12-13 00:00:00.000'
            and q_status = 'actv'
            and stg_name in ('Sales Full Data Entry', 'Group Shariah Checking', 'OCR Verification')
        order by
            q_casekey,
            q_id),
                    
                table_B as (
        select
            JAI_C_KEY ,
            sum(JAI_AMOUNT) as 'TOTAL_AMOUNT_APPLIED'
        from
            jaccess_info ji
        group by
            JAI_C_KEY) 
                    
                    
        select
            CASEID,
            CUSTOMER,
            OIC,
                REGION,
                    SCHEME,
                    PRODUCT,
            FINANCING_AMOUNT,
            TOTAL_AMOUNT_APPLIED,
            CURRENT_OFFICER,
            TASK,
            STAGE,
            C_Createdt as 'APPLICATION_DATE',
            START_DT as 'TASK_START_DATE'
      
        from
            table_A
        left join table_B on
            CASEID = JAI_C_KEY
        order by
            CASEID

        '''

        cursor.execute(query4)

        # Fetch the data from the first query
        data4 = cursor.fetchall()

        # Convert the data to a Pandas DataFrame
        columns4 = [column[0] for column in cursor.description]
        df4 = pd.DataFrame(data4, columns=columns4)   # DF4 for task pending at mandated

         # Execute the 5th query task pending at cp full data entry
        query5 = '''      
        with table_A as (
        select
            q_casekey 'CASEID',
            CIF_NAME 'CUSTOMER',
            if(C_MIDF_SALESOWNER is null,
                    C_MIDF_CPOWNER,
                    C_MIDF_SALESOWNER) as 'OIC',
                    ORG_NAME as 'REGION',
                    PROG_NAME as 'SCHEME',
                    Prod_Name as 'PRODUCT',
            JF_AMT as 'FINANCING_AMOUNT',
            Q_USER as 'CURRENT_OFFICER',
            QU_NAME as 'TASK' ,
            stg_name as 'STAGE',
            q_timestamp as 'START_DT',
            Q_STATUS 'STATUS',
            C_Createdt
        from
            jqueue
        join kbqtype k on
            q_type = QU_CODE
        join kbstage k2 on
            QU_STAGECAT = stg_stagecat
            and QU_TYPE in ('DATA', 'LIST')
        join jcif j on
            q_casekey = CIF_C_KEY
        join jrole j2 on
            cif_id = jr_cifid
            and jr_role = 'MA'
        join jcase on
            c_key = q_casekey
        left join JORG on
            C_ASSIGN_ORG = ORG_CODE
        left join jfacility on
            C_KEY = jf_c_key
            and JF_DELETED = 'N'
        left join kbproduct on
            jf_PRODCODE = PROD_CODE
        left join kbprogram on
            JF_SCHEME_CODE = PROG_CODE
        where
            C_CREATEDT >= '2022-12-13 00:00:00.000'
            and q_status = 'actv'
            and stg_name in ('CP Full Data Entry','Full Data Entry')
        order by
            q_casekey,
            q_id),
                    
                table_B as (
        select
            JAI_C_KEY ,
            sum(JAI_AMOUNT) as 'TOTAL_AMOUNT_APPLIED'
        from
            jaccess_info ji
        group by
            JAI_C_KEY) 
                    
                    
        select
            CASEID,
            CUSTOMER,
            OIC,
                REGION,
                    SCHEME,
                    PRODUCT,
            FINANCING_AMOUNT,
            TOTAL_AMOUNT_APPLIED,
            CURRENT_OFFICER,
            TASK,
            STAGE,
            C_Createdt as 'APPLICATION_DATE',
            START_DT as 'TASK_START_DATE'
        from
            table_A
        left join table_B on
            CASEID = JAI_C_KEY
        order by
            CASEID

        '''

        cursor.execute(query5)

        # Fetch the data from the first query
        data5 = cursor.fetchall()

        # Convert the data to a Pandas DataFrame
        columns5 = [column[0] for column in cursor.description]
        df5 = pd.DataFrame(data5, columns=columns5)   # DF5 for tasks pending at CP

        # Close the cursor and connection
        cursor.close()
        connection.close()

    except mysql.connector.Error as error:
        # Handle any errors that occur during the connection or query execution
        print(f'Error connecting to MySQL: {error}')

    return {
        'df1': df1 ,
        'df2': df2 ,
        'df3': df3 ,
        'df4': df4 ,
        'df5': df5  
    }