# Import libraries

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import tarfile
import os
from datetime import datetime, timedelta

DATA_DIR = os.path.join(os.path.dirname(__file__), "files")

# Define the DAG arguments
default_args = {
    "owner":"admin1",
    "start_date":datetime(2025,10,30),
    "email":["admin1@mail.com"],
    "email_on_failure":True,
    "email_on_retry":False,
    "retries":1,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False
}

# Define the DAG
dag = DAG(
    dag_id="process_web_log",
    default_args=default_args,
    description="Process web server logs: extract, transform and archive daily",
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1
)

# Extract data function
def extract_data():
    input_path = os.path.join(DATA_DIR, "data.txt")
    output_path = os.path.join(DATA_DIR, "extracted_data.txt")
    with open(input_path, "r") as infile, open(output_path, "w") as outfile:
        for line in infile:
            ip_address = line.split()[0]  # toma la primera palabra
            outfile.write(ip_address + "\n")

# Transform data function
def transform_data():
    input_path = os.path.join(DATA_DIR, "extracted_data.txt")
    output_path = os.path.join(DATA_DIR, "transformed_data.txt")
    with open(input_path, "r") as infile, open(output_path, "w") as outfile:
        for line in infile:
            if line.strip() != "198.46.149.143":
                outfile.write(line)

# Load data function
def load_data():
    input_file = os.path.join(DATA_DIR, "transformed_data.txt")
    output_file = os.path.join(DATA_DIR, "weblog.tar")
    with tarfile.open(output_file, "w") as tar:
        tar.add(input_file, arcname="transformed_data.txt")

# Define PythonOperators
extract_data_task = PythonOperator(
    task_id="extract_data",
    python_callable=extract_data,
    dag=dag
)
transform_data_task = PythonOperator(
    task_id="transform_data",
    python_callable=transform_data,
    dag=dag
)
load_data_task = PythonOperator(
    task_id="load_data",
    python_callable=load_data,
    dag=dag
)

# Define task pipeline
extract_data_task >> transform_data_task >> load_data_task