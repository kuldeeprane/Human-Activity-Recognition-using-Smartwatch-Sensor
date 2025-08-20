from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id='local_kafka_to_delta_pipeline',
    start_date=datetime(2025, 8, 5),  # Set to a date in the past
    schedule_interval='*/5 * * * *',  # Run every 5 minutes
    catchup=False,
    tags=['spark', 'delta', 'kafka'],
) as dag:
    
    submit_spark_job = BashOperator(
        task_id='process_kafka_data_locally',
        # This is the final, successful command for automation
        bash_command="""
        docker exec --user spark har_project-spark-master-1 /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        /opt/spark/work-dir/jobs/process_kafka_to_local.py
        """,
    )
