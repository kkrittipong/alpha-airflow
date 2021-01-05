import json
import requests 
import os
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.operators.python import task, get_current_context
import io 
from datetime import datetime
from datetime import date
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__
import pandas as pd
import psycopg2


# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'yort',
}

def message_discord(message):
    url = 'https://discord.com/api/webhooks/790652292050452500/o22AuDsdYLkbagb-311saSQ71kaQvdYIsh-Z9nlRyeuCPdv7i8ByHMy1y1rHyiqaXAPb'
    headers = {'Content-type': 'application/json'}
    data = {
        'username': 'john',
        'avatar_url': '',
        'content': message
    }
    r = requests.post(url, data=json.dumps(data), headers=headers)
    return 1


def download_exchanges(token):
    """
    https://eodhistoricaldata.com/api/exchanges-list/?api_token=YOUR_API_TOKEN&fmt=json
    """

    exchanges_df = pd.read_json(f'https://eodhistoricaldata.com/api/exchanges-list/?api_token={token}&fmt=json')
    return exchanges_df

def download_eod_prices(token, market_code):
    """
    https://eodhistoricaldata.com/api/eod-bulk-last-day/{MARKET_CODE}?api_token={YOUR_API_KEY}
    """

    eod_prices_df = pd.read_json(f'https://eodhistoricaldata.com/api/eod-bulk-last-day/{market_code}?api_token={token}')
    return eod_prices_df


def upload_pandas_to_azure(container_name, file_name, df):
    """
    """
    upload_data = df.to_csv(index=False)
    connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)

    # Create a blob client using the local file name as the name for the blob
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_name)

    print("\nUploading to Azure Storage as blob:\n\t" + file_name)
    message_discord("Uploading to Azure Storage as blob:" + file_name)
    blob_client.upload_blob(upload_data, overwrite=True)


@dag(default_args=default_args, schedule_interval='00 23 * * *', start_date=days_ago(3))
def unicorn_etl():
    """
    ### TaskFlow API Tutorial Documentation
    This is a simple ETL data pipeline example which demonstrates the use of
    the TaskFlow API using three simple tasks for Extract, Transform, and Load.
    Documentation that goes along with the Airflow TaskFlow API tutorial is
    located
    [here](https://airflow.apache.org/docs/stable/tutorial_taskflow_api.html)
    """
    TOKEN = '5d66a65679a7c9.784184268264'
    CONN_STR = 'postgres://postgres:M3mF4cvbwG@203.154.234.138:30888/unicorn'
    CONTAINER_NAME = 'unicorn'

    
    @task()
    def load_exchanges():
        exchanges_df = download_exchanges(TOKEN)
        today = date.today()
        azure_file_name = f'eod/{today.strftime("%Y")}/{today.strftime("%m")}/{today.strftime("%d")}/exchanges/data.csv'
        upload_pandas_to_azure(CONTAINER_NAME, azure_file_name ,exchanges_df)
        return azure_file_name
    
    @task()
    def upload_exchanges_to_database(azure_file_name):
        connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
        blob_service_client = BlobServiceClient.from_connection_string(connect_str)
        blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=azure_file_name)
        df = pd.read_csv(io.StringIO(blob_client.download_blob().content_as_text()))
        connection = psycopg2.connect(CONN_STR)
        cursor = connection.cursor()
        print(df)
        print("PostgreSQL server information")
        print(connection.get_dsn_parameters(), "\n")
        # Executing a SQL query
        cursor.execute("SELECT version();")
        # Fetch result
        record = cursor.fetchone()
        print("You are connected to - ", record, "\n")
        return 1
    
    @task()
    def load_eod_prices():
        # for market_code in MARKET_CODES:
        market_code = 'US'
        eod_prices_df = download_eod_prices(TOKEN, market_code)
        today = date.today()
        azure_file_name = f'eod/{today.strftime("%Y")}/{today.strftime("%m")}/{today.strftime("%d")}/prices/{market_code}.csv'
        upload_pandas_to_azure(CONTAINER_NAME, azure_file_name ,eod_prices_df)
        return azure_file_name


    upload_exchanges_to_database(load_exchanges())
    load_eod_prices()

unicorn_etl_dag = unicorn_etl()