from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

# Assume a function `generate_and_send_report` exists
# from reports import generate_and_send_report 

with DAG(
    dag_id='human_activity_pipeline',
    start_date=datetime(2025, 1, 1),
    schedule_interval='@daily', # Run once a day
    catchup=False
) as dag:

    # This task would run our Spark batch job for daily roll-ups
    generate_daily_summary = SparkSubmitOperator(
        task_id='generate_daily_summary',
        application='/path/to/spark_daily_rollup_job.py',
        conn_id='spark_default', # Airflow connection to Spark
        verbose=False
    )

    # This task generates and emails the report
    # send_report = PythonOperator(
    #     task_id='send_daily_report',
    #     python_callable=generate_and_send_report
    # )

    # generate_daily_summary >> send_report