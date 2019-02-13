from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 database_con_id,
                 input_sql,
                 target_table,
                 mode,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.database_con_id = database_con_id
        self.input_sql = input_sql
        self.target_table = target_table
        self.mode = mode

    def execute(self, context):
        self.log.info('LoadFactOperator')

        database_hook = PostgresHook(postgres_conn_id=self.database_con_id)

        self.log.info(f"Truncating/inserting into {self.target_table}")
        
        if self.mode=="delete":
            additional_statement = f"""DELETE FROM {self.target_table};"""
        else:
            additional_statement =""
        database_hook.run(f"""
        BEGIN;
        
        {additional_statement}

        INSERT INTO {self.target_table}
        {self.input_sql};
        
        COMMIT;
        """)
