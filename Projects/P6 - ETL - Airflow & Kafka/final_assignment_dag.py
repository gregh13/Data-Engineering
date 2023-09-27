from airflow import DAG
from airflow.operators.bash_operator import BashOperator
import datetime as dt
from airflow.utils.dates import days_ago


default_args = {
    'owner': 'Me',
    'start_date': days_ago(0),
    'email': ['myemail@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5)
}


dag = DAG(
    dag_id='ETL_toll_data',
    schedule_interval=dt.timedelta(days=1),
    default_args=default_args,
    description='Apache Airflow Final Assignment'
)


unzip_data = BashOperator(
    task_id='unzip',
    bash_command='tar -xzf /home/project/airflow/dags/finalassignment/staging/tolldata.tgz',
    dag=dag
)

extract_data_from_csv = BashOperator(
    task_id='csv-extract',
    bash_command='cut -d "," -f1,2,3,4 /home/project/airflow/dags/finalassignment/staging/vehicle-data.csv > csv_data.csv',
    dag=dag
)

extract_data_from_tsv = BashOperator(
    task_id='tsv-extract',
    bash_command='cut -d "  " -f5,6,7 /home/project/airflow/dags/finalassignment/staging/tollplaza-data.tsv > tsv_data.csv',
    dag=dag
)
