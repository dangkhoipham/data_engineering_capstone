from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import Variable

import logging

from datetime import datetime, timedelta

default_args = {
    "owner":"khoipham",
    "start_date":datetime(2020,5,23),
    "retries":1,
    "retry_delay":timedelta(minutes=5),
    "depends_on_past":False
}

def create_emr():
    """
        This function will create a spark cluster on AWS
    """
    Variable.set("aws_region","us-west-2")
    logging.info("creating an emr")
    aws_hook = AwsHook("aws_credentials")
    credentials = aws_hook.get_credentials()
    logging.info("access key: {} and secret key: {}".format(credentials.access_key, credentials.secret_key))
    pass

def stop_emr():
    """
        This function will stop EMR cluster after finishing the work
    """
    pass


# Create our DAg
with DAG(dag_id = "capstone_dend",
        default_args = default_args,
        catchup = False,
        schedule_interval = "@daily") as dag:

    start_etl = DummyOperator(
        task_id = "start_etl",
    )

    create_emr = PythonOperator(
        task_id = "create_emr",
        python_callable = create_emr
    )

    stop_emr = PythonOperator(
        task_id = "stop_emr",
        python_callable = stop_emr
    )

    end_etl = DummyOperator(
        task_id = "end_etl"
    )









    start_etl >> create_emr >> stop_emr >> end_etl