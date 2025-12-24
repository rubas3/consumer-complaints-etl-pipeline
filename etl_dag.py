from airflow.sdk import DAG
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from datetime import datetime, timedelta

from Assignment_week4.etl.extract import extract_data_from_api
from Assignment_week4.etl.load import load_xcom_save_mysql, load_csv_save_google
from Assignment_week4.etl.transform import data_manipulation

with DAG(
    dag_id = 'consumer_financial_etl',
    description="Airflow Orchestration for Consumer Financial Complaints",
    schedule= "0 0 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["kai", "batch9"]
) as dag:
    
    extract_task = PythonOperator(
        task_id = 'extract_data_from_source',
        python_callable = extract_data_from_api,
        do_xcom_push = True,
    )

    dump_mysql_task = PythonOperator(
        task_id = 'dump_data_to_mysql',
        python_callable = load_xcom_save_mysql,
    )

    transform_task = PythonOperator(
        task_id = 'transform_data',
        python_callable = data_manipulation,
    )

    check_file_task = FileSensor(
        task_id = 'check_consumer_financial_csv',
        filepath='/home/rubas/airflow/dags/Assignment_week4/consumer_complaints_transformed.csv',
        poke_interval=10,
        timeout=600,
        mode='poke',       
        fs_conn_id='fs_default2',  
    )

    load_task = PythonOperator(
        task_id = 'dump_googlesheet',
        python_callable = load_csv_save_google
    )

    send_email_task = EmailOperator(
        task_id = 'send_googlesheet_url_via_email',
        to="rubasansari3@gmail.com",          
        subject="Consumer Complaints Data",       
        html_content="""
                <p>Hello,</p>
                <p>This is the Google Spreadsheet of Consumer Complaints Data:</p>
                <a href="https://docs.google.com/spreadsheets/d/1N-yCkKkf94olhShpw2Ug6A7PfcC3r96wdw7XWv_wp0c/edit?usp=sharing">Open Sheet</a>
            """,   
    )

    extract_task >> dump_mysql_task >> transform_task >> check_file_task >> load_task >> send_email_task
