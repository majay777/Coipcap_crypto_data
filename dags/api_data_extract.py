import io
import json
import logging
from datetime import timedelta, datetime
from io import BytesIO
from pathlib import Path

import airflow
import boto3
import pandas as pd
import requests
import requests.exceptions as request_exceptions
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from botocore.exceptions import ClientError
from pendulum import duration

pwd = Path('/opt/airflow/data/')
app_path = Path("/opt/airflow/dash_app/app.py")
date = datetime.today().strftime("%Y_%m_%d")
end_epoch_time = (datetime.today() - timedelta(days=1)).timestamp()
start_epoch_time = (datetime.today() - timedelta(days=100)).timestamp()
minio_bucket = "bucket1"
s3_client = boto3.client(
    's3',
    aws_access_key_id=Variable.get("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=Variable.get('AWS_SECRET_ACCESS_KEY'),
    endpoint_url=Variable.get("AWS_S3_ENDPOINT"),
    config=boto3.session.Config(signature_version='v4'),

)

global df


def data_is_available(prefix):
    response = s3_client.list_objects_v2(Bucket=minio_bucket, Prefix=prefix)
    print(prefix)
    # Print the list of file names if 'Contents' in response:
    if 'Contents' in response:
        for obj in response['Contents']:
            print(obj['Key'])
            return True
    else:
        return False


# Function to get data from api and load to minio bucket
def api_data(url, folder, filename):
    dest_file = f'raw/{folder}/{filename}_{date}.json'
    try:
        response = requests.get(url)
        data = response.json()
        fo = io.BytesIO(json.dumps(data).encode('utf-8'))

        try:
            s3_client.upload_fileobj(fo, minio_bucket, dest_file)
        except ClientError as e:
            logging.error(e)

    except request_exceptions.MissingSchema:
        print(f"{url} appears to be invalid url.")
    except request_exceptions.ConnectionError:
        print(f"could not connect to {url}")


@dag(dag_id="coincap_data", schedule='@daily', start_date=airflow.utils.dates.days_ago(1),
     description='Data Extract from Coincap API', tags=['Project'], catchup=False, default_args={
        'owner': 'ajay',
        'retries': 2,
        'retry_delay': duration(seconds=10)}, max_active_runs=1, dagrun_timeout=timedelta(minutes=10))
def coincap_assets():
    @task()
    def data_extract_to_minio():
        base_url = 'https://api.coincap.io/v2'
        folders = ['/assets', '/exchanges', '/markets']
        for i in folders:
            url = base_url + i
            file_name = 'coincap' + i.replace("/", "_")
            folder = i.replace("/", "")
            api_data(url, folder, file_name)

    @task()
    def currency_markets_data():
        currencies = ['bitcoin', 'ethereum', 'ripple', 'bitcoin-cash', 'cardano', 'tether']
        for i in currencies:
            base_url = f'https://api.coincap.io/v2/assets/{i}/markets'
            filename = f"{i}_markets"
            api_data(base_url, i, filename)

    @task()
    def transform_data():
        global df

        response1 = s3_client.list_objects_v2(Bucket=minio_bucket, Prefix='raw')
        for obj in response1.get('Contents', []):
            # print(obj['Key'])
            if str(obj['Key']).endswith(f"{date}.json"):
                file_key = str(obj['Key'])
                response = s3_client.get_object(Bucket=minio_bucket, Key=file_key)
                data = response['Body'].read()
                # Load the JSON data into a Pandas DataFrame
                try:
                    df = pd.read_json(BytesIO(data))
                except ValueError as e:
                    logging.error(e)
                info_df = pd.json_normalize(df['data'])
                result_df = pd.concat([df.drop(columns=['data']), info_df], axis=1)
                if str(result_df.columns).find('updated') != -1:
                    result_df['updated'] = pd.to_datetime(result_df['updated'], unit='ms')
                else:
                    pass

                fo = io.BytesIO(result_df.to_json().encode('utf-8'))
                dest_file = file_key.replace("raw", "clean")
                try:
                    s3_client.upload_fileobj(fo, minio_bucket, dest_file)
                    print(f"File uploaded to {dest_file}")
                except ClientError as e:
                    logging.error(e)
            else:
                print(f"Data files not present for today's date {date} in the minio bucket's raw location.")

    @task()
    def currencies_historic_data():
        currencies = ['bitcoin', 'ethereum', 'ripple', 'bitcoin-cash', 'cardano', 'tether']
        for i in currencies:
            filename = f"{i}_history"
            prefix = f'history/{i}/{filename}/'
            if not data_is_available(prefix):
                raise AirflowSkipException()
            base_url = f'https://api.coincap.io/v2/assets/{i}/history?interval=d1'
            i = 'history/' + i
            api_data(base_url, i, filename)

    dash_app = BashOperator(
        task_id="Run_dash_app",
        bash_command=f"python /opt/airflow/dags/app.py",
        trigger_rule='none_failed'
    )



    data_extract_to_minio() >> currency_markets_data() >> transform_data() >> currencies_historic_data() >> dash_app


coincap_assets()
