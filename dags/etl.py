from datetime import datetime, timedelta
from modulefinder import ReplacePackage
import os
from pickle import APPEND
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
#from airflow.operators import (
#    StageToRedshiftOperator, 
#    LoadFactOperator,
#    LoadDimensionOperator, 
#    DataQualityOperator
#)
from operators.stage_redshift import StageToRedshiftOperator
from operators.load_fact import LoadFactOperator
from operators.load_dimension import LoadDimensionOperator
from operators.data_quality import DataQualityOperator
#from airflow.providers.amazon.aws.transfers import s3_to_redshift
from enum import Enum
from helpers.sql_queries import SqlQueries
from helpers.test import Test

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

class InsertionMethod(Enum):
    APPEND = 'append'
    REPLACE = 'replace'
    UPSERT = 'upsert'

default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 12),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

dag = DAG(
    'sparkify-etl',
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='@hourly',
    tags=['udacity', 'data-pipelines'],
)

start_operator = DummyOperator(
    task_id='Begin_execution', 
    dag=dag
)

# need to make a decision about when to create tables
# there is no explicit mention of when this should occur based on the spec,
# if tasks are supposed to only do a single thing (Single responsibility methods/classes)
# then the tables need to be created either before the dag runs which is not easiliy reusable
# OR a new task needs to be created which creates the tables before staging and analysis tables
# are inserted into

# NOTE: upon reading up on subdags and discovering they've been depricated, 
# I think the staging and load dimension tasks would be good
# candidates for this feature if we chose to use it.
# The rubric does insist on the dag following the data flow provided in the 
# instructions, so i'd prefer not to challenge the rublic/instructions here.

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table='staging_events',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='udacity-dend',
    s3_key='log_data',
    json='s3://udacity-dend/log_json_path.json',
    # truncate_insert = True
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table='staging_songs',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='udacity-dend',
    s3_key='song_data',
    json='auto',
    # truncate_insert=True
)

# built in s3_to_redshift operator would have been nice to use for this if
# spec didn't ask for logging at every stage
# stage_events_to_redshift = s3_to_redshift(
#     task_id = 'stage_from_s3',
#     dag = dag,
#     redshift_conn_id='redshift',
#     table='staging_events',
#     s3_bucket='udacity-dend',
#     s3_key='event_data',
#     method='UPSERT',
#     copy_options=[
#         f"CREDENTIALS 'aws_iam_role={os.environ.get('DATA_PIPELINE_IAM_ROLE')}'", 
#         "JSON 's3://udacity-dend/log_json_path.json'",
#         "REGION 'us-west-2'"
#     ]
# )
# stage_songs_to_redshift = s3_to_redshift(
#     task_id = 'stage_from_s3',
#     dag = dag,
#     redshift_conn_id='redshift',
#     table='staging_songs',
#     s3_bucket='udacity-dend',
#     s3_key='song_data',
#     method='UPSERT',
#     copy_options=[
#         f"IAM_ROLE '{os.environ.get('DATA_PIPELINE_IAM_ROLE')}'",
#         "JSON 'auto'",
#         "REGION 'us-west-2'"]
# )

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='songplays',
    sql_query=SqlQueries.insert_songplays_table,
    truncate_insert = False
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='users',
    sql_query = SqlQueries.insert_users_table,
    truncate_insert = True
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='songs',
    sql_query = SqlQueries.insert_songs_table,
    truncate_insert = True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='artists',
    sql_query = SqlQueries.insert_artists_table,
    truncate_insert = True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='time',
    sql_query = SqlQueries.insert_time_table,
    truncate_insert = True
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='',
    tests=[
        Test(
            SqlQueries.test_artists_count, 
            lambda x : True if x > 0 else False, 
            "Data quality check failed. artists table contained 0 records.",
            "Data quality check passed. artists table contains records."
        ),
        Test(
            SqlQueries.test_artists_nulls, 
            lambda x: False if x > 0 else True,
            "Data quality check failed. Not null columns within the artists table contained nulls.",
            "Data quality check passed. No unexpected nulls in columns of artists table."
        ),
        Test(
            SqlQueries.test_songs_count, 
            lambda x : True if x > 0 else False,
            "Data quality check failed. songs table contained 0 rows.",
            "Data quality check passed. songs table contains records."
        ),
        Test(
            SqlQueries.test_songs_nulls, 
            lambda x: False if x > 0 else True,
            "Data quality check failed. Not null columns within the songs table contained nulls.",
            "Data quality check passed. No unexpected nulls in columns of songs table."
        ),
        Test(
            SqlQueries.test_songplays_count, 
            lambda x : True if x > 0 else False,
            "Data quality check failed. songplays table contained 0 rows.",
            "Data quality check passed. songplays table contains records."
        ),
        Test(
            SqlQueries.test_songplays_nulls, 
            lambda x: False if x > 0 else True,
            "Data quality check failed. Not null columns within the songplays table contained nulls.",
            "Data quality check passed. No unexpected nulls in columns of songplays table."
        ),
        Test(
            SqlQueries.test_time_count, 
            lambda x : True if x > 0 else False,
            "Data quality check failed. time table contained 0 rows.",
            "Data quality check passed. time table contains records."
        ),
        Test(
            SqlQueries.test_time_nulls, 
            lambda x: False if x > 0 else True,
            "Data quality check failed. Not null columns within time table contained nulls.",
            "Data quality check passed. No unexpected nulls in columns of time table."
        ),
        Test(
            SqlQueries.test_users_count, 
            lambda x : True if x > 0 else False,
            "Data quality check failed. users table contained 0 rows.",
            "Data quality check passed. users table contains records."
        ),
        Test(
            SqlQueries.test_users_nulls, 
            lambda x: False if x > 0 else True,
            "Data quality check failed. Not null columns within users table contained nulls.",
            "Data quality check passed. No unexpected nulls in columns of users table."
        )
    ]
)

end_operator = DummyOperator(
    task_id='Stop_execution', 
    dag=dag
)

# Task dependencies defined here
start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator

# If we used task groups, assigning dependencies would be a bit simpler
# again, not trying to stray from the spec
#start_operator >> stage_from_s3
#stage_from_s3 >> load_fact_tables
#load_fact_tables >> load_dim_tables
#load_dim_tables >> run_quality_checks

