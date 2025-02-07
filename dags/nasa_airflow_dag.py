import sys
import os
from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
import pandas as pd
from dotenv import load_dotenv
from airflow.models import Variable


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils.utils import fetch_nasa_data, process_nasa_data, store_in_db, export_to_csv


load_dotenv()
api_key=os.getenv('NASA_API_KEY')

# DAG default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "retries": 1,
}

with DAG(
    dag_id="nasa_neo_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["nasa", "etl", "postgres"],
):

    @task
    def fetch_data():
        """Fetch a list of asteroids from the NASA API."""
        api_key = os.getenv("NASA_API_KEY")
        start_date = os.getenv("START_DATE")
        end_date = os.getenv("END_DATE")
        return fetch_nasa_data(api_key, start_date, end_date)

    @task
    def transform_data(data):
        """Process and clean the fetched NASA data."""
        return process_nasa_data(data)

    @task
    def store_data(df):
        """Store transformed data in PostgreSQL."""
        store_in_db(df)

    @task
    def export_data(df):
        """Export stored data to a CSV file."""
        file_path = Variable.get("EXPORT_FILE_PATH")
        export_to_csv(df, file_path)

    # Task dependencies
    raw_data = fetch_data()
    cleaned_data = transform_data(raw_data)
    store_data(cleaned_data)
    export_data(cleaned_data)
