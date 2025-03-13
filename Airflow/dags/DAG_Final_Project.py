import datetime as dt
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator


default_args = {
    'owner': 'RMT-004',
    'start_date': dt.datetime(2025, 1, 8),
    'retries': 0,
    'retry_delay': dt.timedelta(minutes=600),
}


with DAG('FinalProject_RMT-004',
         default_args=default_args,
         schedule_interval='1 * * * *',
         catchup=False,
         ) as dag:

    # install_library = BashOperator(task_id='install_library',
    #                            bash_command='python /opt/airflow/dags/extract2.py')
    extract_data = BashOperator(task_id='extract_data',
                               bash_command='sudo -u airflow python /opt/airflow/scripts/extract_fp.py')
    transform_data = BashOperator(task_id='transform_data',
                               bash_command='sudo -u airflow python /opt/airflow/scripts/transform_fp.py')
    load_data = BashOperator(task_id='load_data',
                               bash_command='sudo -u airflow python /opt/airflow/scripts/load_fp.py')
    

extract_data >> transform_data >> load_data
# install_library