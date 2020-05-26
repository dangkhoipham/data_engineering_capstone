from airflow.plugins_manager import AirflowPlugin

import operators

class CapstonePlugin(AirflowPlugin):
    name = "capstone_plugin"
    operators = [operators.DataQualityOperator]