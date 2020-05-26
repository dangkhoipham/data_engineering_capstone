import Airflow
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator,self):
    
    @apply_defaults
    def __init__(self,*args, **kwargs):
        super(DataQualityOperator).__init__(*args, **kwargs):
            
        
    def evaluate(self, context):
        