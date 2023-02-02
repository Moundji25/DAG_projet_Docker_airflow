from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'example_dag_looping_web_services',
    default_args=default_args,
    schedule_interval=timedelta(hours=1),
)

def IdentifyAuthorLoan(**kwargs):
    # Code to call the first web service
    # ...
    return "IdentifyAuthorLoan called"

def ExctractHashtags(**kwargs):
    # Code to call the second web service
    # ...
    return "ExctractHashtags called"

def FeelingsAnalysis(**kwargs):
    # Code to call the third web service
    # ...
    return "FeelingsAnalysis called"
def IdentifyTopics(**kwargs):
    # alcknjendcqadcak,qdljaebuuifcajnqs
    # ...
    return "IdentifyTopics called"
loop_task = PythonOperator(
    task_id='loop_task',
    python_callable=loop_function,
    op_kwargs={'web_services': ['call_web_service_1', 'call_web_service_2', 'call_web_service_3']},
    dag=dag,
)

def loop_function(**kwargs):
    web_services = kwargs['web_services']
    for service in web_services:
        task = PythonOperator(
            task_id=service,
            python_callable=globals()[service],
            dag=dag,
        )
        task >> loop_task

loop_task
