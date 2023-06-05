from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': 300,
    'catchup': False,
    'email_on_retry': False
}

dag = DAG('data_pipeline_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@daily'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_staging_events_table = PostgresOperator(
    task_id = "create_staging_events_table",
    dag = dag,
    postgres_conn_id = "redshift_conn",
    sql = SqlQueries.events_staging_table_create
)

create_staging_songs_table = PostgresOperator(
    task_id = "create_staging_songs_table",
    dag = dag,
    postgres_conn_id = "redshift_conn",
    sql = SqlQueries.songs_staging_table_create
)

create_songplay_table = PostgresOperator(
    task_id = "create_songplay_table",
    dag = dag,
    postgres_conn_id = "redshift_conn",
    sql = SqlQueries.songplay_table_create
)

create_song_table = PostgresOperator(
    task_id = "create_song_table",
    dag = dag,
    postgres_conn_id = "redshift_conn",
    sql = SqlQueries.song_table_create
)

create_users_table = PostgresOperator(
    task_id = "create_users_table",
    dag = dag,
    postgres_conn_id = "redshift_conn",
    sql = SqlQueries.users_table_create
)

create_artist_table = PostgresOperator(
    task_id = "create_artist_table",
    dag = dag,
    postgres_conn_id = "redshift_conn",
    sql = SqlQueries.artist_table_create
)

create_time_table = PostgresOperator(
    task_id = "create_time_table",
    dag = dag,
    postgres_conn_id = "redshift_conn",
    sql = SqlQueries.time_table_create
)

schema_create = DummyOperator(
    task_id = 'schema_create',
    dag=dag
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table = "staging_events",
    redshift_conn_id = "redshift_conn",
    aws_credentials_id = "aws_creds",
    s3_bucket = "tungnt",
    s3_key = "log_data",
    json_path="s3://udacity-dend/log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table = "staging_songs",
    redshift_conn_id = "redshift_conn",
    aws_credentials_id = "aws_creds",
    s3_bucket = "tungnt",
    s3_key = "song_data"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table = "songplays",
    select_sql = SqlQueries.songplay_table_insert

)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    table = "users",
    select_sql = SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    table = "songs",
    select_sql = SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    table = "artists",
    select_sql = SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table = "times",
    select_sql = SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
     check_stmts= [
         {
             'sql': 'SELECT COUNT(*) FROM songplays WHERE songid IS NULL',
             'op': 'eq',
             'val': 0            
         }
     ]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> create_staging_events_table
start_operator >> create_staging_songs_table
start_operator >> create_songplay_table
start_operator >> create_song_table
start_operator >> create_artist_table
start_operator >> create_users_table
start_operator >> create_time_table

create_staging_events_table >> schema_create
create_staging_songs_table >> schema_create
create_songplay_table >> schema_create
create_song_table >> schema_create
create_artist_table >> schema_create
create_users_table >> schema_create
create_time_table >> schema_create


