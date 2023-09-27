from airflow import DAG
from airflow.operators.bash_operator import BashOperator
import datetime as dt
from airflow.utils.dates import days_ago


default_args = {
    'owner': 'gregh',
    'start_date': days_ago(0),
    'email': ['myemail@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': dt.timedelta(minutes=5)
}


dag = DAG(
    dag_id='process_web_log',
    schedule_interval=dt.timedelta(days=1),
    default_args=default_args,
    description='Airflow Web Log Daily Processor'
)


extract_data = BashOperator(
    task_id='extract',
    bash_command='cut -d "-" -f1 /home/project/airflow/dags/capstone/accesslogs.txt > /home/project/airflow/dags/capstone/extracted_data.txt',
    dag=dag
)

transform_data = BashOperator(
    task_id='transform',
    bash_command='sed "/198.46.149.143/d" /home/project/airflow/dags/capstone/extracted_data.txt > /home/project/airflow/dags/capstone/transformed_data.txt',
    dag=dag
)

load_data = BashOperator(
    task_id='load',
    bash_command='tar -cvf /home/project/airflow/dags/capstone/weblog.tar /home/project/airflow/dags/capstone/transformed_data.txt',
    dag=dag
)

extract_data >> transform_data >> load_data