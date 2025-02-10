from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from datetime import datetime, timedelta
import os
import pandas as pd
from airflow.models import Variable
from utils.students_util import validate_data, cleanup_data, insert_into_db
from airflow.providers.postgres.hooks.postgres import PostgresHook

CSV_FOLDER = Variable.get("CSV_FOLDER")

# Ensure folder exists
if not os.path.exists(CSV_FOLDER):
    raise ValueError(f"CSV folder not found: {CSV_FOLDER}")

# Default DAG arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 2, 8),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define DAG
dag = DAG(
    "student_data_pipeline",
    default_args=default_args,
    description="Processes student data CSV when placed in a local folder",
    schedule_interval=None,  
    catchup=False,
)

# ---------------------- TASK 1: Wait for CSV using FileSensor ----------------------
wait_for_csv_task = FileSensor(
    task_id="wait_for_csv",
    filepath=CSV_FOLDER + "/*.csv",  
    fs_conn_id="fs_default", 
    poke_interval=30, 
    timeout=600,  
    mode="poke",  
    dag=dag,
)

# ---------------------- TASK 2: Process CSV ----------------------
def process_csv(ti):
    """Reads CSV file from CSV_FOLDER and loads it into XCom"""
    files = [f for f in os.listdir(CSV_FOLDER) if f.endswith(".csv")]

    if not files:
        raise ValueError("No CSV files found")

    file_path = os.path.join(CSV_FOLDER, files[0])  
    print(f"Processing file: {file_path}")

    try:
        df = pd.read_csv(file_path)
        csv_dict = df.to_dict(orient="records")  # Convert to a serializable format
        ti.xcom_push(key="raw_data", value=csv_dict)  # Store in XCom
    except Exception as e:
        raise ValueError(f"Error reading CSV: {e}")

process_csv_task = PythonOperator(
    task_id="process_csv",
    python_callable=process_csv,
    dag=dag,
)

# ---------------------- TASK 3: Validate Data ----------------------
def validate_task(ti):
    """Validates student data using predefined rules."""
    

    validation_params = {
        "email_regex": Variable.get("email_regex"),
        "age_min": int(Variable.get("age_min")),
        "age_max": int(Variable.get("age_max")),
        "date_format": Variable.get("date_format"),
    }


    raw_data = ti.xcom_pull(task_ids="process_csv", key="raw_data")
    if not raw_data:
        raise ValueError("No data found for validation")

    df = pd.DataFrame(raw_data)

    
    validated_df = validate_data(df, validation_params) 
    ti.xcom_push(key="validated_data", value=validated_df.to_dict(orient="records"))

validate_op = PythonOperator(
    task_id="validate_task",
    python_callable=validate_task,
    dag=dag,
)


# ---------------------- TASK 4: Cleanup Data ----------------------
def cleanup_task(ti):
    """Cleans CSV data"""
    validated_data = ti.xcom_pull(task_ids="validate_task", key="validated_data")
    if not validated_data:
        raise ValueError("No data found for cleanup")

    df = pd.DataFrame(validated_data)

    cleaned_df = cleanup_data(df)
    ti.xcom_push(key="cleaned_data", value=cleaned_df.to_dict(orient="records"))

cleanup_op = PythonOperator(
    task_id="cleanup_task",
    python_callable=cleanup_task,
    dag=dag,
)

# ---------------------- TASK 5: Insert into Database ----------------------
def insert_db_task(ti):
    """Inserts cleaned data into the database"""
    cleaned_data = ti.xcom_pull(task_ids="cleanup_task", key="cleaned_data")
    if not cleaned_data:
        raise ValueError("No data found for insertion")

    df = pd.DataFrame(cleaned_data)
    insert_into_db(df)

insert_db_op = PythonOperator(
    task_id="insert_db_task",
    python_callable=insert_db_task,
    dag=dag,
)

# ---------------------- DAG Task Dependencies ----------------------
wait_for_csv_task >> process_csv_task >> validate_op >> cleanup_op >> insert_db_op
