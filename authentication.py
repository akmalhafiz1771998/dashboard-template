import streamlit_authenticator as stauth
import yaml 
from yaml.loader import SafeLoader
from pathlib import Path
from streamlit import stop, error, warning
import streamlit as st 

def authenticate_user():
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

    name, authentication_status, username = authenticator.login('Login','main')

    if authentication_status == False:
        st.error("Username/Passwords is incorrect")
        stop()

    if authentication_status == None:
        st.warning("Please enter your username and password")
        stop()

    # Retrieve the user's role from the config file
    role = config['credentials']['usernames'][username]['role']

    st.session_state["role"] = role
    



 