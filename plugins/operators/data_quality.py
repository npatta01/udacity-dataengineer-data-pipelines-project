from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 queries,
                 conditions,
                 database_con_id,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.queries=queries
        self.conditions=conditions
        self.database_con_id = database_con_id

    def execute(self, context):
        self.log.info('DataQualityOperator')
        
        database_hook = PostgresHook(postgres_conn_id=self.database_con_id)
        
        for idx, query in enumerate(self.queries):
            rows = database_hook.get_records(query)
            
            first_row = rows[0]
            actual_row_count = first_row[0]
            expected_row_count = self.conditions[idx]
            if actual_row_count < expected_row_count :
                self.log.info(f"Failed min row count; actual:{actual_row_count}, expected: {expected_row_count}")