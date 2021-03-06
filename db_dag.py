from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from db_conn import db_connection

# instantiating the dag
with DAG(
        'date_time_info',
        default_args={
            'owner': 'danish',
            'start_date': datetime(2022, 3, 18),
            'email': ['danish99011@gmail.com'],
            'email_on_failure': True,
            'email_on_retry': True,
            'retries': 1,
            # 'retries':2,
            # 'retry_delay': timedelta(minutes=5)
        },
        description='A simple dag which inserts current date time info into a postgres database',
        schedule_interval='@daily'
) as dag:
    # dag tasks
    t1 = PythonOperator(task_id='datetime_insert', python_callable=db_connection)

# order of tasks
t1
