FROM apache/airflow:2.11.1
COPY requirements.txt /
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt
EXPOSE 8050
