from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

from datetime import datetime
from datetime import timedelta

default_args = {
    'owner': 'airflow-simple-ex',
    'depends_on_past': False,
    'start_date': datetime(2020, 1, 25),
    'email_on_failure': False,
    'email_on_retry': False,
    'schedule_interval': '@daily',
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}


def a1_to_b1():
    print('a1_to_b1')


def a2_to_b1():
    print('a2_to_b1')


def a3_to_b1():
    print('a3_to_b1')


dag = DAG(
    dag_id='airflow-simple-ex-dag',
    description='simple example',
    default_args=default_args
)

flow_a1_to_b1 = PythonOperator(
    task_id='a1_to_b1',
    python_callable=a1_to_b1,
    dag=dag
)

flow_a2_to_b1 = PythonOperator(
    task_id='a2_to_b1',
    python_callable=a2_to_b1,
    dag=dag
)

flow_a3_to_b1 = PythonOperator(
    task_id='a3_to_b1',
    python_callable=a3_to_b1,
    dag=dag
)


flow_b1_to_c1 = BashOperator(
    task_id='b1_to_c1',
    python_callable=b1_to_c1,
    bash_command='ls -l'
    dag=dag
)


a1_to_b1 >> b1_to_c1
a2_to_b1 >> b1_to_c1
a3_to_b1 >> b1_to_c1
