import json
import requests 
import os
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.operators.python import task, get_current_context
import io 
from datetime import datetime
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
}
def login_and_get_token():
    SETPORTAL_LOGIN_URL = 'https://api.setportal.set.or.th/download-service/login'
    headers = {'Content-type': 'application/json', 'accept': '*/*',}
    credential = {
        "username": "prem079",
        "password": "@pric0ti0n"
        }
    response = requests.post(SETPORTAL_LOGIN_URL, data = json.dumps(credential), headers=headers)
    tokens = response.json()
    token = tokens['token']
    return token
    

    
@dag(default_args=default_args, schedule_interval='@daily', start_date=days_ago(10))
def psim_etl():
    """
    ### TaskFlow API Tutorial Documentation
    This is a simple ETL data pipeline example which demonstrates the use of
    the TaskFlow API using three simple tasks for Extract, Transform, and Load.
    Documentation that goes along with the Airflow TaskFlow API tutorial is
    located
    [here](https://airflow.apache.org/docs/stable/tutorial_taskflow_api.html)
    """
    
    @task()
    def extract_psims_all():
        """
        #### EXTRACT_PSIMS_ALL
        Download PSIMS data with all group and return 
        """
        context = get_current_context()
        print(f'context = {context}')
        prev_date = datetime.strptime(context['yesterday_ds'], '%Y-%m-%d')

        token = login_and_get_token()
        headers = {
            'accept': '*/*',
            'Authorization': f'Bearer {token}',
        }

        date_string = prev_date.strftime('%d/%m/%Y')
        params = (
            ('date', date_string),
            ('file', 'all'),
            ('group', 'PSIMS'),
        )

        response = requests.get('https://api.setportal.set.or.th/download-service/download', headers=headers, params=params, stream=True)
        if response.status_code == 200:
            filename = response.headers['Content-Disposition'].split('=')[1]
            connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
            blob_service_client = BlobServiceClient.from_connection_string(connect_str)
            container_name = 'set'
            local_file_name = f'psim/{prev_date.strftime("%Y")}/{prev_date.strftime("%m")}/{prev_date.strftime("%d")}/{filename}'

            # Create a blob client using the local file name as the name for the blob
            blob_client = blob_service_client.get_blob_client(container=container_name, blob=local_file_name)

            print("\nUploading to Azure Storage as blob:\n\t" + local_file_name)
            blob_client.upload_blob(io.BytesIO(response.content), overwrite=True)
        elif response.status_code == 422:
            print(f'no data for {prev_date.strftime("%d-%m-%Y")}')
        else:
            raise ValueError(f'Failed to download; response code is{response.status_code}')
        # print(f'execution date ={context['ds']}')
        return token
    
    token = extract_psims_all()
    # order_summary = transform(order_data)
    # load(order_summary["total_order_value"])
psim_etl_dag = psim_etl()