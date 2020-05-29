from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import Variable
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta

import logging
import libs.lib as libs

default_args = {
    "owner":"khoipham",
    "start_date":datetime(2020,5,23),
    "retries":1,
    "retry_delay":timedelta(minutes=5),
    "depends_on_past":False
}


libs.create_emr_client()
 
def create_emr():
    """
        This function will create a spark cluster on AWS
    """  
    libs.create_cluster_emr()
    
def submit_spark(**kwargs):
    libs.submit_to_emr(**kwargs)

                                
# Create our DAg
dag = DAG(dag_id = "test_submit_emr",
        default_args = default_args,
        catchup = False,
        schedule_interval = "@daily") 

start_etl = DummyOperator(
    task_id = "start_etl",
    dag = dag
)


create_emr = PythonOperator(
    task_id = "create_emr",
    python_callable = create_emr,
    dag = dag
)

transform_airport = PythonOperator(
    task_id = "transform_airport",
    python_callable = submit_spark,
    params = {"file":"/home/workspace/airflow/dags/transform/airport.py",
              "name":"test"},
    dag = dag,
    provide_context = True
)      
              
   
    
    