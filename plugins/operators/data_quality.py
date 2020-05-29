import airflow
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    
    @apply_defaults
    def __init__(self,*args, **kwargs):
        super(DataQualityOperator,self).__init__(*args, **kwargs)
            
        
    def evaluate(self, context):
        pass
        