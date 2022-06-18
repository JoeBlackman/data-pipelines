from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from plugins.helpers import sql_queries
from plugins.helpers.sql_queries import SqlQueries
from plugins.helpers.test import test

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'Depends_on_past': False,
    'start_date': datetime(2019, 1, 12),
    'Retries': 3,
    'Retry_delay': timedelta(minutes=5),
    'Catchup': False,
    'email_on_retry': False
}

dag = DAG(
    'udac_example_dag',
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval=timedelta(hours=1),
    tags=['udacity', 'data-pipelines'],
)

start_operator = DummyOperator(
    task_id='Begin_execution', 
    dag=dag
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    python_callable = ?
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='',
    tests=[
        test(
            SqlQueries.test_artists_count, 
            lambda x : True if x > 0 else False, 
            "Data quality check failed. artists table contained 0 records.",
            "Data quality check passed. artists table contains records."
        ),
        test(
            SqlQueries.test_artists_nulls, 
            lambda x: False if x > 0 else True,
            "Data quality check failed. Not null columns within the artists table contained nulls.",
            "Data quality check passed. No unexpected nulls in columns of artists table."
        ),
        test(
            SqlQueries.test_songs_count, 
            lambda x : True if x > 0 else False,
            "Data quality check failed. songs table contained 0 rows.",
            "Data quality check passed. songs table contains records."
        ),
        test(
            SqlQueries.test_songs_nulls, 
            lambda x: False if x > 0 else True,
            "Data quality check failed. Not null columns within the songs table contained nulls.",
            "Data quality check passed. No unexpected nulls in columns of songs table."
        ),
        test(
            SqlQueries.test_songplays_count, 
            lambda x : True if x > 0 else False,
            "Data quality check failed. songplays table contained 0 rows.",
            "Data quality check passed. songplays table contains records."
        ),
        test(
            SqlQueries.test_songplays_nulls, 
            lambda x: False if x > 0 else True,
            "Data quality check failed. Not null columns within the songplays table contained nulls.",
            "Data quality check passed. No unexpected nulls in columns of songplays table."
        ),
        test(
            SqlQueries.test_time_count, 
            lambda x : True if x > 0 else False,
            "Data quality check failed. time table contained 0 rows.",
            "Data quality check passed. time table contains records."
        ),
        test(
            SqlQueries.test_time_nulls, 
            lambda x: False if x > 0 else True,
            "Data quality check failed. Not null columns within time table contained nulls.",
            "Data quality check passed. No unexpected nulls in columns of time table."
        ),
        test(
            SqlQueries.test_users_count, 
            lambda x : True if x > 0 else False,
            "Data quality check failed. users table contained 0 rows.",
            "Data quality check passed. users table contains records."
        ),
        test(
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
