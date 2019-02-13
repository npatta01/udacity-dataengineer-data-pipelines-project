from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries



# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2018, 11, 1),
    'end_date': datetime(2018, 11, 30),
    'depends_on_past' : False,
    'retries' : 0 ,
    'email_on_retry': False,
    'retry_delay': timedelta(minutes=5)
   
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@daily',
          catchup = True,
          max_active_runs = 5
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table="public.staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="dend",
    s3_key="log_data/{{ ds }}-events.csv"
)

   
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table="public.staging_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="dend",
    s3_key="song_data/",
    file_format="json",
    
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    input_sql=SqlQueries.artist_table_insert,
    database_con_id="redshift",
    target_table="public.artists",
    mode="delete"

)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    input_sql=SqlQueries.song_table_insert,
    database_con_id="redshift",
    target_table="public.songs",
    mode="delete"

)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    input_sql=SqlQueries.time_table_insert,
    database_con_id="redshift",
    target_table="public.time" ,
    mode="append"
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    input_sql=SqlQueries.user_table_insert,
    database_con_id="redshift",
    target_table="public.users" ,
    mode="append"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    input_sql=SqlQueries.songplay_table_insert,
    database_con_id="redshift",
    target_table="public.songplays" ,
    mode="append"
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    database_con_id="redshift",
    queries=["select count(*) from  public.artists", "select count(*) from public.songs"],
    conditions=[100,100]
)

end_operator = DummyOperator(task_id='End_execution',  dag=dag)



start_operator >> stage_events_to_redshift >> load_songplays_table
start_operator >> stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_user_dimension_table >> run_quality_checks
load_songplays_table >> load_song_dimension_table >> run_quality_checks
load_songplays_table >> load_artist_dimension_table >> run_quality_checks
load_songplays_table >> load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator