from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import Variable
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator


import logging
import libs.lib as libs

from datetime import datetime, timedelta


default_args = {
    "owner":"khoipham",
    "start_date":datetime(2020,5,23),
    "retries":1,
    "retry_delay":timedelta(minutes=5),
    "depends_on_past":False
}

Variable.set("aws_region","us-west-2")
#Variable.set("cluster_emr_id","	j-3R3U2CFOD984W")

libs.create_emr_client()
 
def create_emr():
    """
        This function will create a spark cluster on AWS
    """  
    libs.create_cluster_emr()
    
def submit_spark(**kwargs):
    libs.submit_to_emr(**kwargs)
                 
def terminate_emr():
    libs.terminate_emr()

def modify_ingress_master():
    libs.modify_ingress_emr_master()
    
def clean_ingress_master():
    libs.clean_ingress_emr_master()

# Create DAG
dag = DAG(dag_id = "capstone_dend",
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

terminate_emr = PythonOperator(
    task_id = "terminate_emr",
    python_callable = terminate_emr, 
    dag = dag
)

end_etl = DummyOperator(
    task_id = "end_etl",
    dag = dag
)


modify_ingress_master = PythonOperator(
    task_id = "modify_ingress_master",
    python_callable = modify_ingress_master,
    dag = dag
)

clean_ingress_master = PythonOperator(
    task_id = "clean_ingress_master",
    python_callable = clean_ingress_master,
    dag = dag
)



start_etl >> create_emr >> modify_ingress_master >> transform_airport >> clean_ingress_master >> terminate_emr >>  end_etl