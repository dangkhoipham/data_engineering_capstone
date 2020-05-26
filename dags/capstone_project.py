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

libs.create_client()
 
def create_emr():
    """
        This function will create a spark cluster on AWS
    """  
    libs.create_cluster_emr()
    
def submit_spark(**kwargs):
    logging.info("Submitting task{} to EMR".format(kwargs['name'])
    libs.submit_to_emr(**kwargs)
    
def terminate_emr():
    libs.terminate_emr()


# Create our DAg
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


test = PythonOperator(
    task_id = "test",
    dag = dag,
    python_callable = submit_to_emr,
    params = {"file":"/home/workspace/airflow/dags/",
              "name":"test"}
)


process_data_city = SparkSubmitOperator(
    task_id = "process_data_city",
    dag = dag,
    conn_id = "spark_conn",
    application = "/home/workspace/airflow/transform/airport.py"
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









start_etl >> create_emr >> terminate_emr >>  end_etl