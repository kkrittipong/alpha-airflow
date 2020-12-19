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

def download_set(token, date, file, group, no=None):
    """
    ref by setportal documents
    date: %d/%m/%Y
    file: ex. all
    group: ex PSIMS
    """

    headers = {
        'accept': '*/*',
        'Authorization': f'Bearer {token}',
    }
    if no==None:
        params = (
            ('date', date),
            ('file', file),
            ('group', group),
        )
    else:
        params = (
            ('date', date),
            ('file', file),
            ('group', group),
            ('no', no),
        )
    response = requests.get('https://api.setportal.set.or.th/download-service/download', headers=headers, params=params, stream=True)
    return response



def upload_to_azure(container_name, file_name, content):
    """
    content is response.content from requests
    """
    connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)

    # Create a blob client using the local file name as the name for the blob
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_name)

    print("\nUploading to Azure Storage as blob:\n\t" + file_name)
    blob_client.upload_blob(io.BytesIO(content), overwrite=True)

def extract_psim(token, group, file):
    context = get_current_context()
    prev_date = datetime.strptime(context['yesterday_ds'], '%Y-%m-%d')
    response = download_set(token, prev_date.strftime('%d/%m/%Y'), file=file, group=group)
    
    if response.status_code == 200:
        filename = response.headers['Content-Disposition'].split('=')[1]
        azure_file_name = f'psim/{prev_date.strftime("%Y")}/{prev_date.strftime("%m")}/{prev_date.strftime("%d")}/{group}/{filename}'
        container_name = 'set'
        upload_to_azure(container_name=container_name, file_name=azure_file_name, content=response.content)

    elif response.status_code == 422:
        print(f'no data for {prev_date.strftime("%d-%m-%Y")}')
    else:
        raise ValueError(f'Failed to download; response code is{response.status_code}')

def extract_psim_loopnum(token, group, file):
    context = get_current_context()
    prev_date = datetime.strptime(context['yesterday_ds'], '%Y-%m-%d')
    file_count = 1
    while True:
        response = download_set(token, prev_date.strftime('%d/%m/%Y'), file=file, group=group, no=f'{file_count:02}')
        if response.status_code == 200:
            filename = response.headers['Content-Disposition'].split('=')[1]
            azure_file_name = f'psim/{prev_date.strftime("%Y")}/{prev_date.strftime("%m")}/{prev_date.strftime("%d")}/{group}/{filename}'
            container_name = 'set'
            upload_to_azure(container_name=container_name, file_name=azure_file_name, content=response.content)
            file_count = file_count + 1

        elif response.status_code == 422:
            print(f'no data for {prev_date.strftime("%d-%m-%Y")}')
            break
        else:
            raise ValueError(f'Failed to download; response code is{response.status_code}')
            break



    
@dag(default_args=default_args, schedule_interval='@daily', start_date=days_ago(5))
def set_psim_etl():
    """
    ### TaskFlow API Tutorial Documentation
    This is a simple ETL data pipeline example which demonstrates the use of
    the TaskFlow API using three simple tasks for Extract, Transform, and Load.
    Documentation that goes along with the Airflow TaskFlow API tutorial is
    located
    [here](https://airflow.apache.org/docs/stable/tutorial_taskflow_api.html)
    """
    
    @task()
    def login_setportal():
        token = login_and_get_token()
        return token
        
    @task()
    def extract_psims_all(token):
        """
        #### EXTRACT_PSIMS_ALL
        Download PSIMS data with all group and return 
        """
        extract_psim(token=token, group='PSIMS', file='all')
        return 1

    @task()
    def extract_psims_public(token):
        """
        #### EXTRACT_PSIMS_PUBLIC
        Download PSIMS data with public group and return 
        """
        extract_psim(token=token, group='PSIMS', file='public')
        return 1

    @task()
    def extract_psims_company(token):
        """
        #### EXTRACT_PSIMS_COMPANY
        Download PSIMS data with company group and return 
        """
        extract_psim(token=token, group='PSIMS', file='comnapy')
        return 1

    @task()
    def extract_psims_trading(token):
        """
        #### EXTRACT_PSIMS_TRADING
        Download PSIMS data with trading group and return 
        """
        extract_psim(token=token, group='PSIMS', file='trading')
        return 1

    @task()
    def extract_psims_newsen(token):
        """
        #### EXTRACT_PSIMS_NEWSEN
        Download PSIMS data with newsen group and return 
        """
        extract_psim(token=token, group='PSIMS', file='newseng')
        return 1

    @task()
    def extract_psims_newsth(token):
        """
        #### EXTRACT_PSIMS_NEWSTH
        Download PSIMS data with newsth group and return 
        """
        extract_psim(token=token, group='PSIMS', file='newsthai')
        return 1

    @task()
    def extract_psims_56_1(token):
        """
        #### EXTRACT_PSIMS_56-1
        Download PSIMS data with 56-1 group and return 
        """
        extract_psim_loopnum(token=token, group='PSIMS', file='form56_1data')
        return 1

    @task()
    def extract_psims_annual(token):
        """
        #### EXTRACT_PSIMS_ANNUAL
        Download PSIMS data with annual group and return 
        """
        extract_psim_loopnum(token=token, group='PSIMS', file='annualdata')
        return 1

    @task()
    def transform_financials(status):
        """
        #### TRANSFORM FINANCIALS
        """
        # extract_psim_loopnum(token=token, group='PSIMS', file='annualdata')
        return 1
    
    

    token = login_setportal()
    status_extract_all = extract_psims_all(token)
    extract_psims_public(token)
    extract_psims_company(token)
    extract_psims_trading(token)
    extract_psims_newsen(token)
    extract_psims_newsth(token)
    extract_psims_56_1(token)
    extract_psims_annual(token)

    transform_financials(status_extract_all)
    # order_summary = transform(order_data)
    # load(order_summary["total_order_value"])
psim_etl_dag = set_psim_etl()