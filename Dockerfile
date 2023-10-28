FROM samdobson/streamlit:0.73.1
WORKDIR /usr/src/app
COPY pages/ ./pages/
COPY Data_Transformation.py ./
COPY Fetch_Data.py ./
COPY MIDF.jpeg ./
COPY MIDF.png ./
COPY MIDF_favicon.png ./
COPY MIDF_sidebar.png ./
COPY Main.py ./
COPY QueueTableDF.csv ./
COPY StageLookup.csv ./
COPY authentication.py ./
COPY config.yaml ./
COPY secrets.yaml ./
COPY .streamlit/ ./.streamlit/
COPY requirements.txt ./
RUN pip install -r requirements.txt
ENV PYTHONUNBUFFERED=1
CMD ["Main.py"]
