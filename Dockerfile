FROM apache/airflow:2.10.2

USER root
RUN apt-get update
RUN apt-get install -y ffmpeg
USER 50000

# additional packages for airflow
ADD requirements.txt .
RUN pip install apache-airflow==${AIRFLOW_VERSION} -r requirements.txt
