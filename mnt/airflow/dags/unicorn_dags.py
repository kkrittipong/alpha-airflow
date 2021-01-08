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
    url = 'https://discord.com/api/webhooks/796951141627068426/D4lhHfKJ1rMQ5YlMQSzSDX8uhrMUoN93RkVcOeM0JhIYx2J4M9FLfvBPbP0kW81yBoh4'
    headers = {'Content-type': 'application/json'}
    data = {
        'username': 'Airflow Hook',
        'avatar_url': '',
        'content': message
    }
    r = requests.post(url, data=json.dumps(data), headers=headers)
    return 1


def download_exchanges(token):
    """
    https://eodhistoricaldata.com/api/exchanges-list/?api_token=YOUR_API_TOKEN&fmt=json
    """
    url = f'https://eodhistoricaldata.com/api/exchanges-list/?api_token={token}&fmt=json'
    print(f'loading {url}')
    exchanges_df = pd.read_json(url)
    return exchanges_df

def download_eod_prices(token, market_code, date):
    """
    https://eodhistoricaldata.com/api/eod-bulk-last-day/{MARKET_CODE}?api_token={YOUR_API_KEY}
    """
    url = f'https://eodhistoricaldata.com/api/eod-bulk-last-day/{market_code}?api_token={token}&date={date}'
    print(f'loading {url}')
    eod_prices_df = pd.read_csv(url)
    return eod_prices_df
    
def download_eod_dividends(token, market_code, date):
    """
    https://eodhistoricaldata.com/api/eod-bulk-last-day/{MARKET_CODE}?api_token={YOUR_API_KEY}&type=dividends
    """
    url = f'https://eodhistoricaldata.com/api/eod-bulk-last-day/{market_code}?api_token={token}&date={date}&type=dividends'
    print(f'loading {url}')
    eod_prices_df = pd.read_csv(url)
    return eod_prices_df

def download_eod_splits(token, market_code, date):
    """
    https://eodhistoricaldata.com/api/eod-bulk-last-day/{MARKET_CODE}?api_token={YOUR_API_KEY}&type=splits
    """
    url = f'https://eodhistoricaldata.com/api/eod-bulk-last-day/{market_code}?api_token={token}&date={date}&type=splits'
    print(f'loading {url}')
    eod_prices_df = pd.read_csv(url)
    return eod_prices_df

def download_entire_prices(token, ticker, matket_code):
    """
    https://eodhistoricaldata.com/api/eod/MCD.US?api_token=OeAFFmMliFG5orCUuwAKQ8l4WWFQ67YX&period=d
    """
    url = f'https://eodhistoricaldata.com/api/eod/{ticker}.{matket_code}?api_token={token}'
    print(f'loading {url}')
    df = pd.read_csv(url)
    return df


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

@dag(default_args=default_args, schedule_interval='20 23 * * *', start_date=days_ago(3))
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
    CONN_STR = 'postgres://postgres:M3mF4cvbwG@192.168.105.32:30888/unicorn'
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
        context = get_current_context()
        current_date = datetime.strptime(context['ds'], '%Y-%m-%d')
        # for market_code in MARKET_CODES:
        market_code = 'US'
        eod_prices_df = download_eod_prices(TOKEN, market_code, context['ds'])
        azure_file_name = f'eod/{current_date.strftime("%Y")}/{current_date.strftime("%m")}/{current_date.strftime("%d")}/prices/{market_code}.csv'
        upload_pandas_to_azure(CONTAINER_NAME, azure_file_name ,eod_prices_df)
        return azure_file_name
    
    @task()
    def load_eod_dividend():
        context = get_current_context()
        current_date = datetime.strptime(context['ds'], '%Y-%m-%d')
        # for market_code in MARKET_CODES:
        market_code = 'US'
        df = download_eod_dividends(TOKEN, market_code, context['ds'])
        azure_file_name = f'eod/{current_date.strftime("%Y")}/{current_date.strftime("%m")}/{current_date.strftime("%d")}/dividends/data.csv'
        upload_pandas_to_azure(CONTAINER_NAME, azure_file_name, df)
        return azure_file_name

    @task()
    def load_eod_splits():
        context = get_current_context()
        current_date = datetime.strptime(context['ds'], '%Y-%m-%d')
        # for market_code in MARKET_CODES:
        market_code = 'US'
        df = download_eod_splits(TOKEN, market_code, context['ds'])
        azure_file_name = f'eod/{current_date.strftime("%Y")}/{current_date.strftime("%m")}/{current_date.strftime("%d")}/splits/data.csv'
        upload_pandas_to_azure(CONTAINER_NAME, azure_file_name, df)
        return azure_file_name
    
    @task()
    def load_entire_prices(ticker, matket_code):
        context = get_current_context()
        current_date = datetime.strptime(context['ds'], '%Y-%m-%d')
        df = download_entire_prices(TOKEN, ticker, matket_code)
        azure_file_name = f'eod/{current_date.strftime("%Y")}/{current_date.strftime("%m")}/{current_date.strftime("%d")}/historical_prices/{ticker}.csv'
        upload_pandas_to_azure(CONTAINER_NAME, azure_file_name, df)
        return azure_file_name
        


    upload_exchanges_to_database(load_exchanges())
    load_eod_prices()
    load_eod_dividend()
    load_eod_splits()
    load_entire_prices(ticker='AAPL', matket_code='US')

unicorn_etl_dag = unicorn_etl()