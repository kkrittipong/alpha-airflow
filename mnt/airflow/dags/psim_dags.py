import json
import requests 
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
}
def login_and_get_token():
    SETPORTAL_LOGIN_URL = 'https://api.setportal.set.or.th/download-service/login'
    credential = {
        "username": "prem079",
        "password": "@pric0ti0n"
        }
    response = requests.post(SETPORTAL_LOGIN_URL, data = credential)
    tokens = response.json()
    token = tokens['token']
    return token
    

    
@dag(default_args=default_args, schedule_interval='@daily', start_date=days_ago(2))
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
        token = login_and_get_token()
        print(f'token is {token}')
        return token
    
    token = extract_psims_all()
    # order_summary = transform(order_data)
    # load(order_summary["total_order_value"])
psim_etl_dag = psim_etl()