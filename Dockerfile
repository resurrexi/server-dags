FROM apache/airflow:2.10.2

# additional packages for airflow
ADD requirements.txt .
RUN pip install apache-airflow==${AIRFLOW_VERSION} -r requirements.txt 