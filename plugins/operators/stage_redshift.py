from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
    """
    
    json_options = """
    
    json 'auto';

    """
    
    csv_options = """
     
    IGNOREHEADER {}
    DELIMITER '{}'
    CSV
    """
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 file_format='csv',
                 delimiter=",",
                 ignore_headers=1,
                 *args, **kwargs):

        
        
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.delimiter = delimiter
        self.ignore_headers = ignore_headers
        self.aws_credentials_id = aws_credentials_id
        self.file_format = file_format


    def execute(self, context):
        self.log.info('StageToRedshiftOperator')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        self.log.info(f'Copying data from {s3_path}')
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,

        )
        
        if self.file_format == 'json':
            formatted_sql = formatted_sql +"\n" +  StageToRedshiftOperator.json_options
        else:
            formatted_sql = formatted_sql +"\n" +  StageToRedshiftOperator.csv_options.format(
                         self.ignore_headers,
                          self.delimiter   
                
            )
            
        redshift.run(formatted_sql)




