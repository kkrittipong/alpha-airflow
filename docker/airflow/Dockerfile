FROM apache/airflow:2.0.0-python3.8
COPY airflow.cfg /usr/local/airflow/airflow.cfg

COPY entrypoint.sh /entrypoint.sh
USER root
RUN ["chmod", "+x", "/entrypoint.sh"]
USER airflow

RUN pip install azure-storage-blob 
ENTRYPOINT ["/entrypoint.sh"]
