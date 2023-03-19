from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
import time

def print_hello():
  time.sleep(30)
  print('Hello World!')
  return 'Hello world!'

with DAG('hello_world', description='Simple tutorial DAG', schedule_interval='*/5 * * * *', start_date=datetime(2023, 2, 26), catchup=False):

  hello_operator = PythonOperator(
    task_id='hello_task', 
    python_callable=print_hello, 
    executor_config={ "KubernetesExecutor": 
    { 
    "resources": {
            "requests": {
                "memory": "200Mi",
                "cpu": "0.2"
            },
            "limits": {
                "memory": "200Mi",
                "cpu": "0.2"
            }
        } 
    }
    }
  )

  hello_operator


