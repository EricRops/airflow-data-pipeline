from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                               LoadDimensionOperator, DataQualityOperator)

from airflow.operators import (PostgresOperator, PythonOperator)
from helpers import SqlQueries

# Unused alternative option: load AWS credentials directly from linux env, and input them as operator parameters
# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'ericr',
    'start_date': datetime(2018, 11, 1),
    'end_date': datetime(2018, 11, 30),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    # PLEASE NOTE: I added an "end_date" and set "catchup" to True to practice backfilling
    'catchup': True,
    'email_on_retry': False,
}

dag = DAG('primary_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@monthly',
          max_active_runs=1
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

# Create tables if they do not already exist using the create_tables.sql file
create_tables_task = PostgresOperator(
  task_id="create_tables",
  dag=dag,
  sql='create_tables.sql',
  postgres_conn_id="redshift"
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    aws_arn_id="aws_arn",
    s3_bucket="udacity-dend",
    s3_key="log_data/2018/{execution_date.month}",
    destination_table="staging_events",
    json_format="s3://udacity-dend/log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    aws_arn_id="aws_arn",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    destination_table="staging_songs",
    json_format="auto"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    destination_table="songplays",
    sql_load_stmt=SqlQueries.songplay_table_insert,
    append_data=True
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    destination_table="users",
    sql_load_stmt=SqlQueries.user_table_insert,
    append_data=False
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    destination_table="songs",
    sql_load_stmt=SqlQueries.song_table_insert,
    append_data=False
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    destination_table="artists",
    sql_load_stmt=SqlQueries.artist_table_insert,
    append_data=False
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    destination_table="time",
    sql_load_stmt=SqlQueries.time_table_insert,
    append_data=False
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    check_statements=[
        # make sure none of the primary keys have nulls
        {'sql': "SELECT COUNT(*) FROM users WHERE userid is null", 'expected_result': 0},
        {'sql': "SELECT COUNT(*) FROM songs WHERE songid is null", 'expected_result': 0},
        {'sql': "SELECT COUNT(*) FROM songplays WHERE playid is null", 'expected_result': 0},
        {'sql': "SELECT COUNT(*) FROM artists WHERE artistid is null", 'expected_result': 0},
        {'sql': "SELECT COUNT(*) FROM time WHERE start_time is null", 'expected_result': 0},
        # Make sure songs table has the same number of rows as S3 song files (14896)
        {'sql': "SELECT COUNT(*) FROM songs", 'expected_result': 14896},
        # Make sure the rest of the tables have at least 1 row of data in them
        {'sql': "select case when (SELECT count(*) FROM users) > 0 then 'true' else 'false' end;", 'expected_result': 'true'},
        {'sql': "select case when (SELECT count(*) FROM artists) > 0 then 'true' else 'false' end;", 'expected_result': 'true'},
        {'sql': "select case when (SELECT count(*) FROM time) > 0 then 'true' else 'false' end;", 'expected_result': 'true'},
        {'sql': "select case when (SELECT count(*) FROM songplays) > 0 then 'true' else 'false' end;", 'expected_result': 'true'}
    ]
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

# Set dependencies
start_operator >> create_tables_task
create_tables_task >> stage_events_to_redshift
create_tables_task >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_artist_dimension_table >> run_quality_checks
load_songplays_table >> load_song_dimension_table >> run_quality_checks
load_songplays_table >> load_time_dimension_table >> run_quality_checks
load_songplays_table >> load_user_dimension_table >> run_quality_checks
run_quality_checks >> end_operator
